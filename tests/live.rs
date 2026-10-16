//! Live tests against E2B Cloud or e2b-minimal. Skipped without `E2B_API_KEY`.
//!
//! ```sh
//! E2B_API_KEY=... cargo test --test live -- --test-threads=4
//! # e2b-minimal: also E2B_API_URL=http://host:3000 E2B_SANDBOX_URL=http://host:3000
//! ```

use std::sync::{Arc, Mutex};
use std::time::Duration;

use e2b::*;

fn enabled() -> bool {
    let on = std::env::var("E2B_API_KEY").is_ok_and(|k| !k.is_empty());
    if !on {
        eprintln!("E2B_API_KEY not set, skipping live test");
    }
    on
}

async fn sandbox() -> Sandbox {
    Sandbox::create(
        SandboxOptions::default()
            .timeout(Duration::from_secs(120))
            .metadata("suite", "e2b-rs"),
    )
    .await
    .expect("create sandbox")
}

#[tokio::test]
async fn lifecycle() {
    if !enabled() {
        return;
    }
    let sbx = sandbox().await;
    assert!(sbx.is_running().await.unwrap());
    let info = sbx.get_info().await.unwrap();
    assert_eq!(info.sandbox_id, sbx.sandbox_id());
    assert_eq!(info.state, SandboxState::Running);
    assert_eq!(info.metadata.get("suite").map(String::as_str), Some("e2b-rs"));

    sbx.set_timeout(Duration::from_secs(90)).await.unwrap();

    let mut q = SandboxQuery::default();
    q.metadata.insert("suite".into(), "e2b-rs".into());
    let listed = Sandbox::list(q, None, sbx.connection_config()).collect_all().await.unwrap();
    assert!(listed.iter().any(|s| s.sandbox_id == sbx.sandbox_id()), "sandbox not listed");

    let again = Sandbox::connect(sbx.sandbox_id(), None, sbx.connection_config()).await.unwrap();
    assert_eq!(
        again.commands.run("echo hi", CommandOptions::default()).await.unwrap().stdout,
        "hi\n"
    );

    assert!(sbx.get_host(8080).starts_with(&format!("8080-{}", sbx.sandbox_id())) || sbx.connection_config().debug);

    assert!(sbx.kill().await.unwrap());
    assert!(!sbx.kill().await.unwrap());
    assert!(!sbx.is_running().await.unwrap_or(false));
    match Sandbox::connect(sbx.sandbox_id(), None, sbx.connection_config()).await {
        Err(Error::SandboxNotFound(_)) => {}
        other => panic!("expected SandboxNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn commands() {
    if !enabled() {
        return;
    }
    let sbx = sandbox().await;
    let c = &sbx.commands;

    let r = c
        .run("echo out; echo err >&2; pwd; whoami", CommandOptions::default())
        .await
        .unwrap();
    assert_eq!(r.exit_code, 0);
    assert_eq!(r.stderr, "err\n");
    assert!(r.stdout.starts_with("out\n"), "{r:?}");

    let r = c
        .run("echo $FOO; pwd", CommandOptions::new().env("FOO", "bar").cwd("/tmp"))
        .await
        .unwrap();
    assert_eq!(r.stdout, "bar\n/tmp\n");

    let r = c.run("whoami", CommandOptions::new().user("root")).await.unwrap();
    assert_eq!(r.stdout, "root\n");

    match c.run("echo nope >&2; exit 3", CommandOptions::default()).await {
        Err(Error::CommandExit(res)) => {
            assert_eq!(res.exit_code, 3);
            assert_eq!(res.stderr, "nope\n");
        }
        other => panic!("expected CommandExit, got {other:?}"),
    }

    // Streaming callbacks.
    let seen = Arc::new(Mutex::new(String::new()));
    let s2 = seen.clone();
    c.run(
        "for i in 1 2 3; do echo $i; sleep 0.2; done",
        CommandOptions::new().on_stdout(move |d| s2.lock().unwrap().push_str(d)),
    )
    .await
    .unwrap();
    assert_eq!(*seen.lock().unwrap(), "1\n2\n3\n");

    // Timeout.
    match c.run("sleep 10", CommandOptions::new().timeout(Duration::from_secs(1))).await {
        Err(Error::Timeout(_)) => {}
        other => panic!("expected Timeout, got {other:?}"),
    }

    // Background + list + kill.
    let h = c.spawn("sleep 300", CommandOptions::new().timeout(Duration::ZERO)).await.unwrap();
    let pid = h.pid();
    assert!(c.list().await.unwrap().iter().any(|p| p.pid == pid));
    assert!(h.kill().await.unwrap());
    let res = h.wait().await;
    assert!(matches!(res, Err(Error::CommandExit(_))), "{res:?}");
    assert!(!c.kill(pid).await.unwrap());

    // Stdin.
    let h = c.spawn("cat", CommandOptions::new().stdin(true)).await.unwrap();
    h.send_stdin("hello stdin\n").await.unwrap();
    if c.supports_stdin_close() {
        h.close_stdin().await.unwrap();
        let r = h.wait().await.unwrap();
        assert_eq!(r.stdout, "hello stdin\n");
    } else {
        h.kill().await.unwrap();
    }

    // Disconnect + reconnect.
    let mut h = c.spawn("sleep 1; echo done", CommandOptions::default()).await.unwrap();
    let pid = h.pid();
    h.disconnect();
    let h2 = c.connect(pid, CommandOptions::default()).await.unwrap();
    let r = h2.wait().await.unwrap();
    assert_eq!(r.stdout, "done\n");

    // Streamed output API.
    let mut h = c
        .spawn("printf a; sleep 0.1; printf b >&2", CommandOptions::default())
        .await
        .unwrap();
    let mut outs = vec![];
    while let Some(o) = h.next_output().await {
        outs.push(o);
    }
    assert_eq!(outs, vec![Output::Stdout("a".into()), Output::Stderr("b".into())]);
    h.wait().await.unwrap();

    sbx.kill().await.unwrap();
}

#[tokio::test]
async fn files() {
    if !enabled() {
        return;
    }
    let sbx = sandbox().await;
    let f = &sbx.files;

    let w = f.write("/tmp/e2b-rs/a/b.txt", "hello").await.unwrap();
    assert_eq!(w.path, "/tmp/e2b-rs/a/b.txt");
    assert_eq!(w.name, "b.txt");
    assert_eq!(f.read_to_string("/tmp/e2b-rs/a/b.txt").await.unwrap(), "hello");

    let bin: Vec<u8> = (0..=255u8).cycle().take(3 * 1024 * 1024 + 7).collect();
    f.write("/tmp/e2b-rs/bin", bin.clone()).await.unwrap();
    assert_eq!(f.read("/tmp/e2b-rs/bin").await.unwrap(), bin);

    let ws = f
        .write_files(
            vec![WriteEntry::new("/tmp/e2b-rs/m1", "1"), WriteEntry::new("/tmp/e2b-rs/m2", "2")],
            None,
        )
        .await
        .unwrap();
    assert_eq!(ws.len(), 2);
    assert_eq!(f.read_to_string("/tmp/e2b-rs/m2").await.unwrap(), "2");

    assert!(f.exists("/tmp/e2b-rs/m1").await.unwrap());
    assert!(!f.exists("/tmp/e2b-rs/nope").await.unwrap());
    assert!(matches!(f.read("/tmp/e2b-rs/nope").await, Err(Error::FileNotFound(_))));

    let info = f.get_info("/tmp/e2b-rs/bin").await.unwrap();
    assert_eq!(info.file_type, FileType::File);
    assert_eq!(info.size, bin.len() as i64);
    assert!(info.modified_time.is_some());

    assert!(f.make_dir("/tmp/e2b-rs/dir/x").await.unwrap());
    assert!(!f.make_dir("/tmp/e2b-rs/dir/x").await.unwrap());

    let names: Vec<String> = f.list("/tmp/e2b-rs", 1).await.unwrap().into_iter().map(|e| e.name).collect();
    for n in ["a", "bin", "m1", "m2", "dir"] {
        assert!(names.contains(&n.to_string()), "{n} missing in {names:?}");
    }
    let deep = f.list("/tmp/e2b-rs", 2).await.unwrap();
    assert!(deep.iter().any(|e| e.path == "/tmp/e2b-rs/a/b.txt"));

    let moved = f.rename("/tmp/e2b-rs/m1", "/tmp/e2b-rs/m3").await.unwrap();
    assert_eq!(moved.path, "/tmp/e2b-rs/m3");
    f.remove("/tmp/e2b-rs/m3").await.unwrap();
    assert!(!f.exists("/tmp/e2b-rs/m3").await.unwrap());

    // Ownership: default user vs root.
    f.write_as("/tmp/e2b-rs/root.txt", "r", Some("root")).await.unwrap();
    assert_eq!(f.get_info("/tmp/e2b-rs/root.txt").await.unwrap().owner, "root");

    // Metadata.
    if semver::Version::parse(sbx.envd_version()).unwrap() >= semver::Version::new(0, 6, 2) {
        let mut o = WriteOptions::default();
        o.metadata.insert("kind".into(), "test".into());
        f.write_with("/tmp/e2b-rs/meta", "m", o).await.unwrap();
        assert_eq!(
            f.get_info("/tmp/e2b-rs/meta")
                .await
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("test")
        );
    }

    // Watch.
    f.make_dir("/tmp/e2b-rs/w").await.unwrap();
    let mut w = f.watch_dir("/tmp/e2b-rs/w", false, None).await.unwrap();
    f.write("/tmp/e2b-rs/w/new.txt", "x").await.unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(10), w.next_event())
        .await
        .expect("watch event")
        .unwrap()
        .unwrap();
    assert_eq!(ev.name, "new.txt");
    assert!(matches!(ev.event_type, FilesystemEventType::Create | FilesystemEventType::Write));
    w.stop();

    sbx.kill().await.unwrap();
}

#[tokio::test]
async fn pty() {
    if !enabled() {
        return;
    }
    let sbx = sandbox().await;
    let mut t = sbx.pty.create(PtySize { cols: 80, rows: 24 }, PtyOptions::default()).await.unwrap();
    sbx.pty.resize(t.pid(), PtySize { cols: 120, rows: 40 }).await.unwrap();
    sbx.pty.send_input(t.pid(), "echo $((6*7)); stty size; exit\n").await.unwrap();
    let mut out = Vec::new();
    let collect = async {
        while let Some(o) = t.next_output().await {
            if let Output::Pty(b) = o {
                out.extend_from_slice(&b);
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(20), collect).await.expect("pty ended");
    let s = String::from_utf8_lossy(&out);
    assert!(s.contains("42"), "{s}");
    assert!(s.contains("40 120"), "{s}");
    let r = t.wait_raw().await.unwrap();
    assert_eq!(r.exit_code, 0);
    sbx.kill().await.unwrap();
}

#[tokio::test]
async fn pause_resume() {
    if !enabled() {
        return;
    }
    let sbx = sandbox().await;
    sbx.files.write("/home/user/state.txt", "kept").await.unwrap();
    let bg = sbx
        .commands
        .spawn("sleep 600", CommandOptions::new().timeout(Duration::ZERO))
        .await
        .unwrap();
    let pid = bg.pid();
    drop(bg);
    assert!(sbx.pause().await.unwrap());
    let info = Sandbox::get_info_by_id(sbx.sandbox_id(), sbx.connection_config()).await.unwrap();
    assert_eq!(info.state, SandboxState::Paused);

    let resumed = Sandbox::connect(sbx.sandbox_id(), Some(Duration::from_secs(60)), sbx.connection_config())
        .await
        .unwrap();
    assert_eq!(resumed.files.read_to_string("/home/user/state.txt").await.unwrap(), "kept");
    assert!(
        resumed.commands.list().await.unwrap().iter().any(|p| p.pid == pid),
        "process lost after resume"
    );
    resumed.kill().await.unwrap();
}

#[tokio::test]
async fn errors() {
    if !enabled() {
        return;
    }
    let bad = ConnectionConfig::from_env().with_api_key("e2b_invalid");
    match Sandbox::create(SandboxOptions::default().config(bad)).await {
        Err(Error::Authentication(_)) => {}
        other => panic!("expected Authentication, got {other:?}"),
    }
    match Sandbox::get_info_by_id("doesnotexist000", &ConnectionConfig::from_env()).await {
        Err(Error::SandboxNotFound(_)) => {}
        other => panic!("expected SandboxNotFound, got {other:?}"),
    }
    match Sandbox::create(SandboxOptions::default().template("no-such-template-e2b-rs")).await {
        Err(Error::Api { status, .. }) => assert!(status == 404 || status == 400, "{status}"),
        other => panic!("expected Api error, got {other:?}"),
    }
}
