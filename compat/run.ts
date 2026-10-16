// Differential compatibility test: runs every scenario with the official JS SDK
// (reference) and with e2b-rs, each through its own recording proxy, then
// compares (1) the results and (2) the requests both SDKs sent.
//
//   cd compat && bun install && bun run.ts [scenario-prefix...] [--jobs N]
//
// Talks to whatever E2B_API_URL / E2B_SANDBOX_URL / E2B_DOMAIN point at
// (E2B Cloud by default, or e2b-minimal). Needs E2B_API_KEY.
import { createHash } from 'node:crypto'
import { mkdirSync, writeFileSync } from 'node:fs'
import { fromBinary, fromJson, toJson, type DescMessage } from '@bufbuild/protobuf'
import { Process } from './gen/process_pb.ts'
import { Filesystem } from './gen/filesystem_pb.ts'

const ROOT = new URL('..', import.meta.url).pathname
const HERE = new URL('.', import.meta.url).pathname
const NODE = process.env.COMPAT_NODE ?? (Bun.which('node', { PATH: '/usr/local/bin:/usr/bin' }) as string)
const domain = process.env.E2B_DOMAIN || 'e2b.app'
const API_UP = (process.env.E2B_API_URL || `https://api.${domain}`).replace(/\/$/, '')
const SBX_UP = (process.env.E2B_SANDBOX_URL || `https://sandbox.${domain}`).replace(/\/$/, '')

if (!process.env.E2B_API_KEY) {
  console.error('E2B_API_KEY is required')
  process.exit(2)
}

// ---------- Connect body decoding ----------

const rpcInputs = new Map<string, DescMessage>()
for (const svc of [Process, Filesystem]) for (const m of svc.methods) rpcInputs.set(`/${svc.typeName}/${m.name}`, m.input)

function decodeRpc(path: string, contentType: string, body: Uint8Array): unknown {
  const schema = rpcInputs.get(path)
  if (!schema) return { undecodable: path }
  let payload = body
  if (contentType.startsWith('application/connect+')) {
    // One enveloped message (flags, u32 length, payload); server streams only.
    const len = new DataView(body.buffer, body.byteOffset).getUint32(1)
    payload = body.subarray(5, 5 + len)
  }
  const msg = contentType.includes('json')
    ? fromJson(schema, JSON.parse(new TextDecoder().decode(payload)))
    : fromBinary(schema, payload)
  return toJson(schema, msg)
}

const sha = (b: Uint8Array) => createHash('sha256').update(b).digest('hex').slice(0, 16)

// ---------- Recording proxy ----------

type Rec = Record<string, unknown>

const KEEP_HEADERS = [
  'authorization', 'e2b-sandbox-port', 'keepalive-ping-interval', 'connect-timeout-ms', 'connect-protocol-version',
]
const PRESENCE_HEADERS = ['x-api-key', 'x-access-token', 'e2b-sandbox-id', 'e2b-traffic-access-token', 'content-encoding']

async function record(kind: 'api' | 'envd', req: Request, body: Uint8Array, status: number): Promise<Rec> {
  const url = new URL(req.url)
  const ct = (req.headers.get('content-type') ?? '').split(';')[0].trim()
  const headers: Rec = {}
  for (const h of KEEP_HEADERS) if (req.headers.has(h)) headers[h] = req.headers.get(h)
  for (const h of PRESENCE_HEADERS) if (req.headers.has(h)) headers[h] = '<set>'
  req.headers.forEach((v, k) => {
    if (k.startsWith('x-metadata-')) headers[k] = v
  })
  const isRpc = rpcInputs.has(url.pathname)
  let decoded: unknown = null
  if (isRpc) {
    decoded = decodeRpc(url.pathname, ct, body)
    headers['content-type'] = ct.startsWith('application/connect+') ? 'connect-stream' : 'connect-unary'
  } else {
    if (ct) headers['content-type'] = ct
    if (ct === 'application/json' && body.length) decoded = JSON.parse(new TextDecoder().decode(body))
    else if (ct === 'multipart/form-data') {
      const form = await new Response(body, { headers: { 'content-type': req.headers.get('content-type')! } }).formData()
      const parts: Rec[] = []
      for (const [field, v] of form.entries()) {
        const f = v as File
        parts.push({ field, filename: f.name, sha: sha(new Uint8Array(await f.arrayBuffer())) })
      }
      decoded = { multipart: parts }
    } else if (body.length) decoded = { bytes: body.length, sha: sha(body) }
  }
  const query: Rec = {}
  url.searchParams.forEach((v, k) => (query[k] = v))
  return normalizeIds({ kind, method: req.method, path: url.pathname, query, headers, body: decoded, status })
}

// Sandbox ids, pids and watcher ids differ between runs.
function normalizeIds(v: any, key = ''): any {
  if (typeof v === 'string') {
    if (['pid', 'sandboxID', 'watcherId'].includes(key)) return `<${key}>`
    return v.replace(/\/sandboxes\/(?!doesnotexist)[a-z0-9]{12,}/g, '/sandboxes/<id>')
  }
  if (typeof v === 'number' && key === 'pid') return '<pid>'
  if (Array.isArray(v)) return v.map((x) => normalizeIds(x))
  if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, normalizeIds(x, k)]))
  return v
}

function startProxy(kind: 'api' | 'envd', upstream: string, log: Rec[]) {
  return Bun.serve({
    port: 0,
    hostname: '127.0.0.1',
    idleTimeout: 255,
    async fetch(req) {
      const url = new URL(req.url)
      const body = new Uint8Array(await req.arrayBuffer())
      const headers = new Headers(req.headers)
      headers.delete('host')
      headers.delete('content-length')
      let resp: Response
      try {
        resp = await fetch(upstream + url.pathname + url.search, {
          method: req.method,
          headers,
          body: body.length ? body : undefined,
          redirect: 'manual',
          decompress: false,
        } as RequestInit)
      } catch (e) {
        resp = new Response(`proxy: ${e}`, { status: 599 })
      }
      log.push(await record(kind, req, body, resp.status))
      return new Response(resp.body, { status: resp.status, headers: resp.headers })
    },
  })
}

// ---------- Running SDKs ----------

type Sdk = 'js' | 'rust'
type Run = { result: unknown; wire: Rec[]; stderr: string; ms: number }

const rustBin = `${ROOT}target/debug/examples/compat`

async function runSdk(sdk: Sdk, scenario: string, arg?: string): Promise<Run> {
  const wire: Rec[] = []
  const api = startProxy('api', API_UP, wire)
  const sbx = startProxy('envd', SBX_UP, wire)
  const env = {
    ...process.env,
    E2B_API_URL: `http://127.0.0.1:${api.port}`,
    E2B_SANDBOX_URL: `http://127.0.0.1:${sbx.port}`,
    // The proxy speaks HTTP/1.1; the JS SDK defaults to HTTP/2.
    E2B_HTTP_VERSION: '1.1',
  }
  const cmd = sdk === 'js' ? [NODE, `${HERE}js/scenarios.mjs`, scenario] : [rustBin, scenario]
  if (arg) cmd.push(arg)
  const t0 = Date.now()
  const p = Bun.spawn(cmd, { env, cwd: HERE, stdout: 'pipe', stderr: 'pipe' })
  const [out, err] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text(), p.exited])
  api.stop(true)
  sbx.stop(true)
  const last = out.trim().split('\n').pop() ?? ''
  let result: unknown
  try {
    result = JSON.parse(last)
  } catch {
    result = { crashed: true, exitCode: p.exitCode, stdout: out.slice(-500) }
  }
  return { result, wire, stderr: err, ms: Date.now() - t0 }
}

// ---------- Comparing ----------

function canon(v: unknown): string {
  return JSON.stringify(v, (_, x) =>
    x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => a.localeCompare(b))) : x
  )
}

function diff(a: any, b: any, path = '$', out: string[] = []): string[] {
  if (canon(a) === canon(b)) return out
  if (a && b && typeof a === 'object' && typeof b === 'object' && Array.isArray(a) === Array.isArray(b)) {
    for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) diff(a[k], b[k], `${path}.${k}`, out)
    return out
  }
  out.push(`${path}: js=${canon(a)} rust=${canon(b)}`)
  return out
}

// Error messages are worded differently per SDK; the error kind is the contract.
function comparable(v: any) {
  if (v && typeof v === 'object' && 'unexpected' in v) return { unexpected: v.unexpected }
  return v
}

// Requests compared as multisets: order between concurrent requests may differ.
function wireDiff(js: Rec[], rust: Rec[]): string[] {
  const count = (l: Rec[]) => l.reduce((m, r) => m.set(canon(r), (m.get(canon(r)) ?? 0) + 1), new Map<string, number>())
  const a = count(js)
  const b = count(rust)
  const out: string[] = []
  for (const [k, n] of a) if ((b.get(k) ?? 0) < n) out.push(`only js:   ${k}`)
  for (const [k, n] of b) if ((a.get(k) ?? 0) < n) out.push(`only rust: ${k}`)
  return out
}

async function pool<T>(items: T[], n: number, f: (t: T) => Promise<void>) {
  const q = [...items]
  await Promise.all(Array.from({ length: Math.min(n, q.length) }, async () => { while (q.length) await f(q.shift()!) }))
}

// ---------- Main ----------

const argv = process.argv.slice(2)
const jobsAt = argv.indexOf('--jobs')
const jobs = jobsAt >= 0 ? Number(argv.splice(jobsAt, 2)[1]) : 4
const filters = argv

const build = Bun.spawnSync(['cargo', 'build', '-q', '--example', 'compat'], { cwd: ROOT, stdout: 'inherit', stderr: 'inherit' })
if (build.exitCode !== 0) process.exit(build.exitCode ?? 1)

const all: string[] = JSON.parse(Bun.spawnSync([NODE, `${HERE}js/scenarios.mjs`, '--list']).stdout.toString())
const selected = all.filter((s) => !s.startsWith('interop.') && (!filters.length || filters.some((f) => s.startsWith(f))))
const doInterop = !filters.length || filters.some((f) => 'interop'.startsWith(f) || f.startsWith('interop'))

console.log(`upstream api=${API_UP} sandbox=${SBX_UP}, ${selected.length} scenarios, ${jobs} at a time\n`)

type Row = { scenario: string; result: string[]; wire: string[]; js: Run; rust: Run }
const rows: Row[] = []

await pool(selected, jobs, async (scenario) => {
  const [js, rust] = await Promise.all([runSdk('js', scenario), runSdk('rust', scenario)])
  const row = { scenario, result: diff(comparable(js.result), comparable(rust.result)), wire: wireDiff(js.wire, rust.wire), js, rust }
  rows.push(row)
  const mark = row.result.length ? 'FAIL' : row.wire.length ? 'WIRE' : ' ok '
  console.log(`[${mark}] ${scenario.padEnd(26)} js ${String(js.ms).padStart(5)}ms  rust ${String(rust.ms).padStart(5)}ms`)
})

if (doInterop) {
  for (const [setup, check] of [['js', 'rust'], ['rust', 'js']] as [Sdk, Sdk][]) {
    const s = await runSdk(setup, 'interop.setup')
    const c = await runSdk(check, 'interop.check', s.result as string)
    const expected = { file: 'shared ✓', sleeper: true, run: '10\n', metadata: { suite: 'e2b-compat' }, killed: true }
    const d = diff(expected, c.result)
    const scenario = `interop.${setup}->${check}`
    rows.push({ scenario, result: d.map((x) => x.replace('js=', 'expected=').replace('rust=', 'got=')), wire: [], js: s, rust: c })
    console.log(`[${d.length ? 'FAIL' : ' ok '}] ${scenario}`)
  }
}

rows.sort((a, b) => a.scenario.localeCompare(b.scenario))
const failed = rows.filter((r) => r.result.length)
const wireOnly = rows.filter((r) => !r.result.length && r.wire.length)

for (const r of [...failed, ...wireOnly]) {
  console.log(`\n── ${r.scenario}`)
  for (const l of r.result) console.log(`  result ${l}`)
  for (const l of r.wire) console.log(`  wire   ${l}`)
}

mkdirSync(`${HERE}out`, { recursive: true })
writeFileSync(`${HERE}out/report.json`, JSON.stringify(rows, null, 2))
console.log(
  `\n${rows.length - failed.length - wireOnly.length} identical, ${wireOnly.length} same result but different requests, ${failed.length} different results` +
    `\nfull report: compat/out/report.json`
)
process.exit(failed.length || wireOnly.length ? 1 : 0)
