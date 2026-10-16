# e2b-rs

An async Rust SDK for [E2B](https://e2b.dev) sandboxes. It sends the same
requests as the official JavaScript and Python SDKs (`e2b` 2.56):

- the E2B REST API (`/v2/sandboxes`, …) for the sandbox lifecycle
- envd inside the sandbox: Connect RPC (binary protobuf) for processes and the
  filesystem, and HTTP `/files` for uploads and downloads

Sandboxes created with this crate work with the official SDKs, and the other
way round. It works with E2B Cloud and with self-hosted
[e2b-minimal](https://github.com/traumventures/e2b-minimal).

```rust
use e2b::{CommandOptions, Sandbox, SandboxOptions};

#[tokio::main]
async fn main() -> e2b::Result<()> {
    let sbx = Sandbox::create(SandboxOptions::default()).await?; // template "base"
    let out = sbx.commands.run("echo hello", CommandOptions::default()).await?;
    println!("{}", out.stdout);
    sbx.files.write("/home/user/data.csv", "a,b\n1,2\n").await?;
    sbx.pause().await?;
    let same = Sandbox::connect(sbx.sandbox_id(), None, sbx.connection_config()).await?;
    same.kill().await?;
    Ok(())
}
```

```toml
[dependencies]
e2b-rs = { git = "https://github.com/traumventures/e2b-rs" }   # imported as `e2b`
```

## Configuration

The environment variables are the ones the official SDKs read:

| Variable | Default |
| --- | --- |
| `E2B_API_KEY` | required (sent as `X-API-Key`) |
| `E2B_ACCESS_TOKEN` | optional, sent as `Authorization: Bearer` |
| `E2B_DOMAIN` | `e2b.app` |
| `E2B_API_URL` | `https://api.<domain>` |
| `E2B_SANDBOX_URL` | `https://sandbox.<domain>` for E2B-hosted domains, else `https://49983-<id>.<domain>` |
| `E2B_DEBUG` | `false` (`true`: API on `localhost:3000`, envd on `localhost:49983`) |

To set them in code, build a `ConnectionConfig`
(`ConnectionConfig::from_env().with_api_key(..)`) and pass it with
`SandboxOptions::config`.

For e2b-minimal:

```sh
export E2B_API_URL=http://your-host:3000 E2B_SANDBOX_URL=http://your-host:3000 E2B_API_KEY=any
```

## API

The names are the Python SDK names, in snake_case. A method that the
official SDKs offer as a static method takes an id, with a `_by_id` suffix.

| Official SDK | e2b-rs |
| --- | --- |
| `Sandbox.create(template, opts)` | `Sandbox::create(SandboxOptions)`: template, timeout, metadata, envs, `allow_internet_access`, `network`, `on_timeout` (kill / pause with or without memory), `auto_resume`, `mcp`, volume mounts |
| `Sandbox.connect(id, { timeoutMs })` | `Sandbox::connect(id, timeout, &config)` (resumes a paused sandbox) |
| `Sandbox.list({ query, limit })` | `Sandbox::list(SandboxQuery, limit, &config)`: `SandboxPaginator` with `next_items()` / `collect_all()` |
| `kill`, `pause`, `setTimeout`, `getInfo`, `getMetrics`, `isRunning`, `getHost` | the same, plus `*_by_id` variants; `pause_filesystem_only()` for `pause({ keepMemory: false })` |
| `commands.run(cmd, opts)` | `commands.run(cmd, CommandOptions)`: `envs`, `cwd`, `user`, `timeout`, `stdin`, `on_stdout`/`on_stderr`. A non-zero exit returns `Error::CommandExit` |
| `commands.run(cmd, { background: true })` | `commands.spawn(cmd, opts)`, returns a `CommandHandle` |
| `commands.list`, `kill`, `connect`, `sendStdin`, `closeStdin`, `supportsStdinClose` | the same |
| `CommandHandle.wait`, `kill`, `disconnect`, `sendStdin`, `closeStdin`, `stdout`, `stderr`, `pid` | the same, plus `next_output()` to read the output as a stream and `wait_raw()`, which does not fail on a non-zero exit |
| `files.read`, `write`, `writeFiles`, `list(path, { depth })`, `exists`, `getInfo`, `makeDir`, `rename`, `remove`, `watchDir` | `read` / `read_to_string` / `read_as`, `write` / `write_as` / `write_with` (user and metadata), `write_files`, `list(path, depth)`, `exists`, `get_info`, `make_dir`, `rename`, `remove`, `watch_dir(path, recursive, timeout)` |
| `pty.create`, `sendInput`, `resize`, `kill`, `connect` | the same |
| error classes | `Error::{CommandExit, Timeout, SandboxNotFound, SandboxNotRunning, FileNotFound, NotFound, InvalidArgument, Authentication, RateLimit, NotEnoughSpace, Template, Api, Sandbox, ..}` |

These are not implemented yet: `Template` (the builder), `Volume`, `Secret`,
snapshots and `fork`, `updateNetwork`, signed upload and download URLs, git
helpers (run `git` with `commands.run`) and the MCP gateway.

Timeouts follow the official SDKs: a request times out after 60 s. A command,
PTY or watch stream stops after 60 s unless you set `timeout`.
`Duration::ZERO` means no limit.

## Tests

```sh
cargo test                                  # unit tests, offline
E2B_API_KEY=... cargo test --test live      # creates real sandboxes and kills them afterwards

# against e2b-minimal
E2B_API_URL=http://host:3000 E2B_SANDBOX_URL=http://host:3000 E2B_API_KEY=any cargo test --test live
```

The live suite passes against E2B Cloud and against e2b-minimal (envd 0.9).

The live suite checks the lifecycle (create, info, list, timeout, connect,
kill), commands (env, cwd, user, exit codes, callbacks, timeouts,
background/list/kill, stdin, disconnect/reconnect), files (text, 3 MB binary,
multi-file, stat, list depth, rename, remove, ownership, metadata, watch), PTY
(input, resize, exit), pause/resume with memory, and error mapping.
`examples/quickstart.rs` checks that this crate and the JS SDK can work on the
same sandbox.

## Regenerating the protobuf code

`proto/envd` holds a copy of `spec/envd` from
[e2b-dev/E2B](https://github.com/e2b-dev/E2B). Run this when it changes (needs `protoc`):

```sh
cargo run --manifest-path tools/codegen/Cargo.toml
```

## License

Apache-2.0
