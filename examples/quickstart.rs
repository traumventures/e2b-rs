//! `E2B_API_KEY=... cargo run --example quickstart [sandbox-id]`
//!
//! Without an argument it creates a sandbox, leaves a file in it and prints the
//! id (the sandbox keeps running for 2 minutes). With an id it connects to an
//! existing sandbox, e.g. one created by the JS or Python SDK, and kills it.

use std::time::Duration;

use e2b::{CommandOptions, ConnectionConfig, Sandbox, SandboxOptions};

#[tokio::main]
async fn main() -> e2b::Result<()> {
    match std::env::args().nth(1) {
        None => {
            let sbx = Sandbox::create(SandboxOptions::default().timeout(Duration::from_secs(120))).await?;
            sbx.files.write("/home/user/from-rust.txt", "written by e2b-rs").await?;
            let r = sbx.commands.run("uname -sr", CommandOptions::default()).await?;
            eprintln!("kernel: {}", r.stdout.trim());
            println!("{}", sbx.sandbox_id());
        }
        Some(id) => {
            let sbx = Sandbox::connect(&id, None, &ConnectionConfig::from_env()).await?;
            let text = sbx.files.read_to_string("/home/user/from-js.txt").await?;
            println!("read: {text}");
            let r = sbx
                .commands
                .run("cat /home/user/from-js.txt | wc -c", CommandOptions::default())
                .await?;
            println!("wc: {}", r.stdout.trim());
            sbx.kill().await?;
        }
    }
    Ok(())
}
