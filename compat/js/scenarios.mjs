// Compatibility scenarios for the official JS SDK (the reference).
// Usage: node js/scenarios.mjs <scenario> [arg]
// Prints one JSON value on stdout. Must stay in sync with examples/compat.rs.
import { createHash } from 'node:crypto'
import { Sandbox } from 'e2b'

const META = { suite: 'e2b-compat' }
const create = () => Sandbox.create({ timeoutMs: 120_000, metadata: META })
const sha = (b) => createHash('sha256').update(Buffer.from(b)).digest('hex')
const kind = (e) => e?.name ?? 'Error'
const entry = (e) => ({ name: e.name, type: e.type, path: e.path })
const byPath = (a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0)

async function caught(p) {
  try {
    await p
    return { error: null }
  } catch (e) {
    const r = { error: kind(e) }
    if (e.exitCode !== undefined) Object.assign(r, { exitCode: e.exitCode, stdout: e.stdout, stderr: e.stderr })
    return r
  }
}

async function withSandbox(fn) {
  const sbx = await create()
  try {
    return await fn(sbx)
  } finally {
    await sbx.kill().catch(() => {})
  }
}

const scenarios = {
  'commands.basic': () =>
    withSandbox(async (s) => {
      const r = await s.commands.run('echo out; echo err >&2')
      return { stdout: r.stdout, stderr: r.stderr, exitCode: r.exitCode }
    }),
  'commands.env_cwd': () =>
    withSandbox(async (s) => (await s.commands.run('echo $FOO; pwd', { envs: { FOO: 'bar' }, cwd: '/tmp' })).stdout),
  'commands.users': () =>
    withSandbox(async (s) => [
      (await s.commands.run('whoami; pwd')).stdout,
      (await s.commands.run('whoami; pwd', { user: 'root' })).stdout,
    ]),
  'commands.exit_error': () => withSandbox((s) => caught(s.commands.run('echo o; echo e >&2; exit 3'))),
  'commands.timeout': () => withSandbox((s) => caught(s.commands.run('sleep 10', { timeoutMs: 1000 }))),
  'commands.callbacks': () =>
    withSandbox(async (s) => {
      let out = ''
      let err = ''
      await s.commands.run('for i in 1 2 3; do echo $i; echo e$i >&2; sleep 0.2; done', {
        onStdout: (d) => (out += d),
        onStderr: (d) => (err += d),
      })
      return { out, err }
    }),
  'commands.background': () =>
    withSandbox(async (s) => {
      const h = await s.commands.run('sleep 300', { background: true, timeoutMs: 0 })
      const listed = (await s.commands.list()).some((p) => p.pid === h.pid)
      const killed = await h.kill()
      const waited = await caught(h.wait())
      const again = await s.commands.kill(h.pid)
      return { listed, killed, waited, again }
    }),
  'commands.stdin': () =>
    withSandbox(async (s) => {
      const h = await s.commands.run('cat', { background: true, stdin: true })
      await s.commands.sendStdin(h.pid, 'hello stdin\n')
      await s.commands.closeStdin(h.pid)
      return (await h.wait()).stdout
    }),
  'commands.reconnect': () =>
    withSandbox(async (s) => {
      const h = await s.commands.run('sleep 1; echo done', { background: true })
      await h.disconnect()
      const h2 = await s.commands.connect(h.pid)
      const r = await h2.wait()
      return { stdout: r.stdout, exitCode: r.exitCode }
    }),
  'commands.list_fields': () =>
    withSandbox(async (s) => {
      const h = await s.commands.run('sleep 30', { background: true, envs: { A: '1' }, cwd: '/tmp' })
      const p = (await s.commands.list()).find((p) => p.pid === h.pid)
      await h.kill()
      return { cmd: p.cmd, args: p.args, cwd: p.cwd, envA: p.envs.A }
    }),
  'files.text': () =>
    withSandbox(async (s) => {
      const w = await s.files.write('/tmp/c/a/b.txt', 'hello ✓')
      return { write: entry(w), read: await s.files.read('/tmp/c/a/b.txt') }
    }),
  'files.binary': () =>
    withSandbox(async (s) => {
      const data = new Uint8Array(3 * 1024 * 1024 + 7).map((_, i) => i % 256)
      await s.files.write('/tmp/c/bin', data.buffer)
      const back = await s.files.read('/tmp/c/bin', { format: 'bytes' })
      return { len: back.length, sha: sha(back), same: sha(back) === sha(data) }
    }),
  'files.write_files': () =>
    withSandbox(async (s) => {
      const ws = await s.files.write([
        { path: '/tmp/c/m1', data: '1' },
        { path: '/tmp/c/m2', data: '2' },
      ])
      return { written: ws.map(entry).sort(byPath), m2: await s.files.read('/tmp/c/m2') }
    }),
  'files.stat': () =>
    withSandbox(async (s) => {
      await s.files.write('/tmp/c/f', '12345')
      await s.commands.run('ln -s /tmp/c/f /tmp/c/l')
      const pick = (i) => ({
        name: i.name, type: i.type, path: i.path, size: i.type === 'file' ? i.size : null, mode: i.mode,
        permissions: i.permissions, owner: i.owner, group: i.group, symlinkTarget: i.symlinkTarget ?? null,
        hasModified: i.modifiedTime instanceof Date,
      })
      return [pick(await s.files.getInfo('/tmp/c/f')), pick(await s.files.getInfo('/tmp/c')), pick(await s.files.getInfo('/tmp/c/l'))]
    }),
  'files.list': () =>
    withSandbox(async (s) => {
      await s.files.write('/tmp/c/a/b.txt', 'x')
      await s.files.write('/tmp/c/top', 'x')
      return {
        d1: (await s.files.list('/tmp/c')).map(entry).sort(byPath),
        d2: (await s.files.list('/tmp/c', { depth: 2 })).map(entry).sort(byPath),
      }
    }),
  'files.missing': () =>
    withSandbox(async (s) => ({
      exists: await s.files.exists('/tmp/nope'),
      read: await caught(s.files.read('/tmp/nope')),
      info: await caught(s.files.getInfo('/tmp/nope')),
      list: await caught(s.files.list('/tmp/nope')),
      rename: await caught(s.files.rename('/tmp/nope', '/tmp/nope2')),
    })),
  'files.make_dir': () =>
    withSandbox(async (s) => [await s.files.makeDir('/tmp/c/d/e'), await s.files.makeDir('/tmp/c/d/e'), await s.files.exists('/tmp/c/d/e')]),
  'files.rename_remove': () =>
    withSandbox(async (s) => {
      await s.files.write('/tmp/c/r1', 'x')
      const moved = entry(await s.files.rename('/tmp/c/r1', '/tmp/c/r2'))
      await s.files.remove('/tmp/c/r2')
      return { moved, after: await s.files.exists('/tmp/c/r2') }
    }),
  'files.user': () =>
    withSandbox(async (s) => {
      await s.files.write('/tmp/c/u', 'x')
      await s.files.write('/tmp/c/r', 'x', { user: 'root' })
      return [(await s.files.getInfo('/tmp/c/u')).owner, (await s.files.getInfo('/tmp/c/r')).owner, await s.files.read('/tmp/c/r', { user: 'root' })]
    }),
  'files.metadata': () =>
    withSandbox(async (s) => {
      await s.files.write('/tmp/c/meta', 'm', { metadata: { kind: 'test', owner: 'compat' } })
      return (await s.files.getInfo('/tmp/c/meta')).metadata
    }),
  'files.watch': () =>
    withSandbox(async (s) => {
      await s.files.makeDir('/tmp/w')
      const events = []
      const h = await s.files.watchDir('/tmp/w', (e) => events.push({ name: e.name, type: e.type }))
      await s.files.write('/tmp/w/new.txt', 'x')
      for (let i = 0; i < 50 && !events.length; i++) await new Promise((r) => setTimeout(r, 100))
      await h.stop()
      return events[0] ?? null
    }),
  'pty.basic': () =>
    withSandbox(async (s) => {
      let out = ''
      const t = await s.pty.create({ cols: 80, rows: 24, onData: (d) => (out += new TextDecoder().decode(d)) })
      await s.pty.resize(t.pid, { cols: 120, rows: 40 })
      await s.pty.sendInput(t.pid, new TextEncoder().encode('echo $((6*7)); stty size; exit\n'))
      const r = await t.wait()
      return { exitCode: r.exitCode, has42: out.includes('42'), hasSize: out.includes('40 120') }
    }),
  'sandbox.info': () =>
    withSandbox(async (s) => {
      const i = await s.getInfo()
      return { state: i.state, metadata: i.metadata, idMatches: i.sandboxId === s.sandboxId, running: await s.isRunning() }
    }),
  'sandbox.list': () =>
    withSandbox(async (s) => {
      const p = Sandbox.list({ query: { metadata: { ...META }, state: ['running'] } })
      const all = []
      while (p.hasNext) all.push(...(await p.nextItems()))
      const me = all.find((x) => x.sandboxId === s.sandboxId)
      return { found: !!me, state: me?.state, metadata: me?.metadata }
    }),
  'sandbox.timeout': () =>
    withSandbox(async (s) => {
      await s.setTimeout(90_000)
      const left = ((await s.getInfo()).endAt.getTime() - Date.now()) / 1000
      return left > 75 && left <= 95
    }),
  'sandbox.kill': async () => {
    const s = await create()
    return {
      kill: await s.kill(),
      again: await s.kill(),
      running: await s.isRunning().catch((e) => kind(e)),
      connect: await caught(Sandbox.connect(s.sandboxId)),
      info: await caught(Sandbox.getInfo(s.sandboxId)),
      setTimeout: await caught(Sandbox.setTimeout(s.sandboxId, 10_000)),
      pause: await caught(Sandbox.pause(s.sandboxId)),
      command: await caught(s.commands.run('true')),
    }
  },
  'sandbox.pause_resume': () =>
    withSandbox(async (s) => {
      await s.files.write('/home/user/state.txt', 'kept')
      const h = await s.commands.run('sleep 600', { background: true, timeoutMs: 0 })
      await h.disconnect()
      const paused = await s.pause()
      const state = (await Sandbox.getInfo(s.sandboxId)).state
      const pausedAgain = await Sandbox.pause(s.sandboxId)
      const r = await Sandbox.connect(s.sandboxId, { timeoutMs: 60_000 })
      return {
        paused, state, pausedAgain,
        file: await r.files.read('/home/user/state.txt'),
        process: (await r.commands.list()).some((p) => p.pid === h.pid),
      }
    }),
  'errors.auth': async () => caught(Sandbox.create({ apiKey: 'e2b_0000000000000000000000000000000000000000' })),
  'errors.template': async () => caught(Sandbox.create('no-such-template-e2b-compat')),
  'errors.unknown_id': async () => ({
    info: await caught(Sandbox.getInfo('doesnotexist000')),
    kill: await Sandbox.kill('doesnotexist000'),
    connect: await caught(Sandbox.connect('doesnotexist000')),
  }),
  // Interop: `setup` in one SDK, `check <id>` in the other.
  'interop.setup': async () => {
    const s = await create()
    await s.files.write('/home/user/interop.txt', 'shared ✓')
    await s.commands.run('sleep 600', { background: true, timeoutMs: 0 }).then((h) => h.disconnect())
    return s.sandboxId
  },
  'interop.check': async (id) => {
    const s = await Sandbox.connect(id)
    const r = {
      file: await s.files.read('/home/user/interop.txt'),
      sleeper: (await s.commands.list()).some((p) => p.args.join(' ').includes('sleep 600')),
      run: (await s.commands.run('cat /home/user/interop.txt | wc -c')).stdout,
      metadata: (await s.getInfo()).metadata,
    }
    r.killed = await s.kill()
    return r
  },
}

const [name, arg] = process.argv.slice(2)
if (name === '--list') {
  console.log(JSON.stringify(Object.keys(scenarios)))
} else if (!scenarios[name]) {
  console.error(`unknown scenario ${name}`)
  process.exit(2)
} else {
  try {
    console.log(JSON.stringify(await scenarios[name](arg)))
  } catch (e) {
    // An unexpected error is a result too: the runner compares it.
    console.log(JSON.stringify({ unexpected: kind(e), message: String(e.message ?? e) }))
  }
}
