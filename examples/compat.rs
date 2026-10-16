//! Compatibility scenarios for e2b-rs. Mirror of `compat/js/scenarios.mjs`;
//! `compat/run.ts` runs both and compares results and wire traffic.
//!
//! `cargo run --example compat -- <scenario> [arg]` prints one JSON value.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use e2b::*;
use serde_json::{json, Value};

type R = std::result::Result<Value, Error>;

fn cfg() -> ConnectionConfig {
    ConnectionConfig::from_env()
}

async fn create() -> Result<Sandbox> {
    Sandbox::create(
        SandboxOptions::default()
            .timeout(Duration::from_secs(120))
            .metadata("suite", "e2b-compat"),
    )
    .await
}

async fn with_sandbox<F, Fut>(f: F) -> R
where
    F: FnOnce(Sandbox) -> Fut,
    Fut: Future<Output = R>,
{
    let s = create().await?;
    let r = f(s.clone()).await;
    let _ = s.kill().await;
    r
}

/// Like `caught()` in the JS scenarios.
fn caught<T>(r: Result<T>) -> Value {
    match r {
        Ok(_) => json!({ "error": null }),
        Err(Error::CommandExit(c)) => {
            json!({ "error": "CommandExitError", "exitCode": c.exit_code, "stdout": c.stdout, "stderr": c.stderr })
        }
        Err(e) => json!({ "error": e.kind() }),
    }
}

fn write_entry(w: &WriteInfo) -> Value {
    json!({ "name": w.name, "type": w.file_type.map(|t| t.as_str()), "path": w.path })
}

fn entry(e: &EntryInfo) -> Value {
    json!({ "name": e.name, "type": e.file_type.as_str(), "path": e.path })
}

fn sorted(mut v: Vec<Value>) -> Value {
    v.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    Value::Array(v)
}

fn sha(b: &[u8]) -> String {
    // Avoids a sha2 dependency: run sha256sum locally.
    use std::io::Write;
    let mut c = std::process::Command::new("sha256sum")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("sha256sum");
    c.stdin.take().unwrap().write_all(b).unwrap();
    let out = c.wait_with_output().unwrap();
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

fn opts() -> CommandOptions {
    CommandOptions::default()
}

async fn run(name: &str, arg: Option<String>) -> R {
    match name {
        "commands.basic" => {
            with_sandbox(|s| async move {
                let r = s.commands.run("echo out; echo err >&2", opts()).await?;
                Ok(json!({ "stdout": r.stdout, "stderr": r.stderr, "exitCode": r.exit_code }))
            })
            .await
        }
        "commands.env_cwd" => {
            with_sandbox(|s| async move { Ok(json!(s.commands.run("echo $FOO; pwd", opts().env("FOO", "bar").cwd("/tmp")).await?.stdout)) })
                .await
        }
        "commands.users" => {
            with_sandbox(|s| async move {
                Ok(json!([
                    s.commands.run("whoami; pwd", opts()).await?.stdout,
                    s.commands.run("whoami; pwd", opts().user("root")).await?.stdout,
                ]))
            })
            .await
        }
        "commands.exit_error" => with_sandbox(|s| async move { Ok(caught(s.commands.run("echo o; echo e >&2; exit 3", opts()).await)) }).await,
        "commands.timeout" => {
            with_sandbox(|s| async move { Ok(caught(s.commands.run("sleep 10", opts().timeout(Duration::from_secs(1))).await)) }).await
        }
        "commands.callbacks" => {
            with_sandbox(|s| async move {
                let out = Arc::new(Mutex::new(String::new()));
                let err = Arc::new(Mutex::new(String::new()));
                let (o, e) = (out.clone(), err.clone());
                s.commands
                    .run(
                        "for i in 1 2 3; do echo $i; echo e$i >&2; sleep 0.2; done",
                        opts().on_stdout(move |d| o.lock().unwrap().push_str(d)).on_stderr(move |d| e.lock().unwrap().push_str(d)),
                    )
                    .await?;
                let (out, err) = (out.lock().unwrap().clone(), err.lock().unwrap().clone());
                Ok(json!({ "out": out, "err": err }))
            })
            .await
        }
        "commands.background" => {
            with_sandbox(|s| async move {
                let h = s.commands.spawn("sleep 300", opts().timeout(Duration::ZERO)).await?;
                let pid = h.pid();
                let listed = s.commands.list().await?.iter().any(|p| p.pid == pid);
                let killed = h.kill().await?;
                let waited = caught(h.wait().await);
                let again = s.commands.kill(pid).await?;
                Ok(json!({ "listed": listed, "killed": killed, "waited": waited, "again": again }))
            })
            .await
        }
        "commands.stdin" => {
            with_sandbox(|s| async move {
                let h = s.commands.spawn("cat", opts().stdin(true)).await?;
                s.commands.send_stdin(h.pid(), "hello stdin\n").await?;
                s.commands.close_stdin(h.pid()).await?;
                Ok(json!(h.wait().await?.stdout))
            })
            .await
        }
        "commands.reconnect" => {
            with_sandbox(|s| async move {
                let mut h = s.commands.spawn("sleep 1; echo done", opts()).await?;
                h.disconnect();
                let r = s.commands.connect(h.pid(), opts()).await?.wait().await?;
                Ok(json!({ "stdout": r.stdout, "exitCode": r.exit_code }))
            })
            .await
        }
        "commands.list_fields" => {
            with_sandbox(|s| async move {
                let h = s.commands.spawn("sleep 30", opts().env("A", "1").cwd("/tmp")).await?;
                let p = s.commands.list().await?.into_iter().find(|p| p.pid == h.pid()).expect("listed");
                h.kill().await?;
                Ok(json!({ "cmd": p.cmd, "args": p.args, "cwd": p.cwd, "envA": p.envs.get("A") }))
            })
            .await
        }
        "files.text" => {
            with_sandbox(|s| async move {
                let w = s.files.write("/tmp/c/a/b.txt", "hello ✓").await?;
                Ok(json!({ "write": write_entry(&w), "read": s.files.read_to_string("/tmp/c/a/b.txt").await? }))
            })
            .await
        }
        "files.binary" => {
            with_sandbox(|s| async move {
                let data: Vec<u8> = (0..3 * 1024 * 1024 + 7).map(|i| (i % 256) as u8).collect();
                s.files.write("/tmp/c/bin", data.clone()).await?;
                let back = s.files.read("/tmp/c/bin").await?;
                Ok(json!({ "len": back.len(), "sha": sha(&back), "same": back == data }))
            })
            .await
        }
        "files.write_files" => {
            with_sandbox(|s| async move {
                let ws = s.files.write_files(vec![WriteEntry::new("/tmp/c/m1", "1"), WriteEntry::new("/tmp/c/m2", "2")], None).await?;
                Ok(json!({ "written": sorted(ws.iter().map(write_entry).collect()), "m2": s.files.read_to_string("/tmp/c/m2").await? }))
            })
            .await
        }
        "files.stat" => {
            with_sandbox(|s| async move {
                s.files.write("/tmp/c/f", "12345").await?;
                s.commands.run("ln -s /tmp/c/f /tmp/c/l", opts()).await?;
                let pick = |i: EntryInfo| {
                    json!({
                        "name": i.name, "type": i.file_type.as_str(), "path": i.path,
                        "size": if i.file_type == FileType::File { json!(i.size) } else { Value::Null },
                        "mode": i.mode, "permissions": i.permissions, "owner": i.owner, "group": i.group,
                        "symlinkTarget": i.symlink_target, "hasModified": i.modified_time.is_some(),
                    })
                };
                Ok(json!([
                    pick(s.files.get_info("/tmp/c/f").await?),
                    pick(s.files.get_info("/tmp/c").await?),
                    pick(s.files.get_info("/tmp/c/l").await?),
                ]))
            })
            .await
        }
        "files.list" => {
            with_sandbox(|s| async move {
                s.files.write("/tmp/c/a/b.txt", "x").await?;
                s.files.write("/tmp/c/top", "x").await?;
                Ok(json!({
                    "d1": sorted(s.files.list("/tmp/c", 1).await?.iter().map(entry).collect()),
                    "d2": sorted(s.files.list("/tmp/c", 2).await?.iter().map(entry).collect()),
                }))
            })
            .await
        }
        "files.missing" => {
            with_sandbox(|s| async move {
                Ok(json!({
                    "exists": s.files.exists("/tmp/nope").await?,
                    "read": caught(s.files.read("/tmp/nope").await),
                    "info": caught(s.files.get_info("/tmp/nope").await),
                    "list": caught(s.files.list("/tmp/nope", 1).await),
                    "rename": caught(s.files.rename("/tmp/nope", "/tmp/nope2").await),
                }))
            })
            .await
        }
        "files.make_dir" => {
            with_sandbox(|s| async move {
                Ok(json!([s.files.make_dir("/tmp/c/d/e").await?, s.files.make_dir("/tmp/c/d/e").await?, s.files.exists("/tmp/c/d/e").await?]))
            })
            .await
        }
        "files.rename_remove" => {
            with_sandbox(|s| async move {
                s.files.write("/tmp/c/r1", "x").await?;
                let moved = entry(&s.files.rename("/tmp/c/r1", "/tmp/c/r2").await?);
                s.files.remove("/tmp/c/r2").await?;
                Ok(json!({ "moved": moved, "after": s.files.exists("/tmp/c/r2").await? }))
            })
            .await
        }
        "files.user" => {
            with_sandbox(|s| async move {
                s.files.write("/tmp/c/u", "x").await?;
                s.files.write_as("/tmp/c/r", "x", Some("root")).await?;
                let r = String::from_utf8_lossy(&s.files.read_as("/tmp/c/r", Some("root")).await?).into_owned();
                Ok(json!([s.files.get_info("/tmp/c/u").await?.owner, s.files.get_info("/tmp/c/r").await?.owner, r]))
            })
            .await
        }
        "files.metadata" => {
            with_sandbox(|s| async move {
                let metadata = HashMap::from([("kind".to_string(), "test".to_string()), ("owner".to_string(), "compat".to_string())]);
                s.files.write_with("/tmp/c/meta", "m", WriteOptions { metadata, ..Default::default() }).await?;
                Ok(json!(s.files.get_info("/tmp/c/meta").await?.metadata))
            })
            .await
        }
        "files.watch" => {
            with_sandbox(|s| async move {
                s.files.make_dir("/tmp/w").await?;
                let mut w = s.files.watch_dir("/tmp/w", false, None).await?;
                s.files.write("/tmp/w/new.txt", "x").await?;
                let ev = tokio::time::timeout(Duration::from_secs(5), w.next_event()).await.ok().flatten().transpose()?;
                w.stop();
                Ok(ev.map(|e| json!({ "name": e.name, "type": e.event_type.as_str() })).unwrap_or(Value::Null))
            })
            .await
        }
        "pty.basic" => {
            with_sandbox(|s| async move {
                let out = Arc::new(Mutex::new(String::new()));
                let o = out.clone();
                let on_data: OutputCallback = Arc::new(move |d: &str| o.lock().unwrap().push_str(d));
                let mut t = s.pty.create(PtySize { cols: 80, rows: 24 }, PtyOptions { on_data: Some(on_data), ..Default::default() }).await?;
                s.pty.resize(t.pid(), PtySize { cols: 120, rows: 40 }).await?;
                s.pty.send_input(t.pid(), "echo $((6*7)); stty size; exit\n").await?;
                let r = t.wait_raw().await?;
                let out = out.lock().unwrap().clone();
                Ok(json!({ "exitCode": r.exit_code, "has42": out.contains("42"), "hasSize": out.contains("40 120") }))
            })
            .await
        }
        "sandbox.info" => {
            with_sandbox(|s| async move {
                let i = s.get_info().await?;
                Ok(json!({ "state": i.state, "metadata": i.metadata, "idMatches": i.sandbox_id == s.sandbox_id(), "running": s.is_running().await? }))
            })
            .await
        }
        "sandbox.list" => {
            with_sandbox(|s| async move {
                let q = SandboxQuery {
                    metadata: HashMap::from([("suite".into(), "e2b-compat".into())]),
                    state: vec![SandboxState::Running],
                    ..Default::default()
                };
                let all = Sandbox::list(q, None, &cfg()).collect_all().await?;
                let me = all.iter().find(|x| x.sandbox_id == s.sandbox_id());
                Ok(json!({ "found": me.is_some(), "state": me.map(|m| m.state), "metadata": me.map(|m| &m.metadata) }))
            })
            .await
        }
        "sandbox.timeout" => {
            with_sandbox(|s| async move {
                s.set_timeout(Duration::from_secs(90)).await?;
                let left = (s.get_info().await?.end_at - chrono::Utc::now()).num_milliseconds() as f64 / 1000.0;
                Ok(json!(left > 75.0 && left <= 95.0))
            })
            .await
        }
        "sandbox.kill" => {
            let s = create().await?;
            let id = s.sandbox_id().to_string();
            Ok(json!({
                "kill": s.kill().await?,
                "again": s.kill().await?,
                "running": match s.is_running().await { Ok(b) => json!(b), Err(e) => json!(e.kind()) },
                "connect": caught(Sandbox::connect(&id, None, &cfg()).await),
                "info": caught(Sandbox::get_info_by_id(&id, &cfg()).await),
                "setTimeout": caught(Sandbox::set_timeout_by_id(&id, Duration::from_secs(10), &cfg()).await),
                "pause": caught(Sandbox::pause_by_id(&id, true, &cfg()).await),
                "command": caught(s.commands.run("true", opts()).await),
            }))
        }
        "sandbox.pause_resume" => {
            with_sandbox(|s| async move {
                s.files.write("/home/user/state.txt", "kept").await?;
                let h = s.commands.spawn("sleep 600", opts().timeout(Duration::ZERO)).await?;
                let pid = h.pid();
                drop(h);
                let paused = s.pause().await?;
                let state = Sandbox::get_info_by_id(s.sandbox_id(), &cfg()).await?.state;
                let paused_again = Sandbox::pause_by_id(s.sandbox_id(), true, &cfg()).await?;
                let r = Sandbox::connect(s.sandbox_id(), Some(Duration::from_secs(60)), &cfg()).await?;
                Ok(json!({
                    "paused": paused, "state": state, "pausedAgain": paused_again,
                    "file": r.files.read_to_string("/home/user/state.txt").await?,
                    "process": r.commands.list().await?.iter().any(|p| p.pid == pid),
                }))
            })
            .await
        }
        "errors.auth" => {
            let bad = cfg().with_api_key("e2b_0000000000000000000000000000000000000000");
            Ok(caught(Sandbox::create(SandboxOptions::default().config(bad)).await))
        }
        "errors.template" => Ok(caught(Sandbox::create(SandboxOptions::default().template("no-such-template-e2b-compat")).await)),
        "errors.unknown_id" => Ok(json!({
            "info": caught(Sandbox::get_info_by_id("doesnotexist000", &cfg()).await),
            "kill": Sandbox::kill_by_id("doesnotexist000", &cfg()).await?,
            "connect": caught(Sandbox::connect("doesnotexist000", None, &cfg()).await),
        })),
        "interop.setup" => {
            let s = create().await?;
            s.files.write("/home/user/interop.txt", "shared ✓").await?;
            drop(s.commands.spawn("sleep 600", opts().timeout(Duration::ZERO)).await?);
            Ok(json!(s.sandbox_id()))
        }
        "interop.check" => {
            let id = arg.expect("interop.check needs a sandbox id");
            let s = Sandbox::connect(&id, None, &cfg()).await?;
            let mut r = json!({
                "file": s.files.read_to_string("/home/user/interop.txt").await?,
                "sleeper": s.commands.list().await?.iter().any(|p| p.args.join(" ").contains("sleep 600")),
                "run": s.commands.run("cat /home/user/interop.txt | wc -c", opts()).await?.stdout,
                "metadata": s.get_info().await?.metadata,
            });
            r["killed"] = json!(s.kill().await?);
            Ok(r)
        }
        other => {
            eprintln!("unknown scenario {other}");
            std::process::exit(2);
        }
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let name = args.next().unwrap_or_default();
    match run(&name, args.next()).await {
        Ok(v) => println!("{v}"),
        Err(e) => {
            // An unexpected error is a result too: the runner compares it.
            println!("{}", json!({ "unexpected": e.kind(), "message": e.to_string() }));
        }
    }
}
