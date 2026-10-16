//! Async Rust SDK for [E2B](https://e2b.dev) sandboxes.
//!
//! Speaks the same wire protocols as the official JavaScript and Python SDKs:
//! the E2B REST API for the sandbox lifecycle and envd's Connect RPC + HTTP API
//! inside the sandbox. Works with E2B Cloud and with self-hosted
//! [e2b-minimal](https://github.com/traumventures/e2b-minimal) (set
//! `E2B_API_URL` and `E2B_SANDBOX_URL`).
//!
//! ```no_run
//! use e2b::{Sandbox, SandboxOptions, CommandOptions};
//!
//! # async fn demo() -> e2b::Result<()> {
//! let sbx = Sandbox::create(SandboxOptions::default()).await?;
//! let out = sbx.commands.run("echo hello", CommandOptions::default()).await?;
//! assert_eq!(out.stdout, "hello\n");
//! sbx.files.write("/tmp/a.txt", "data").await?;
//! sbx.kill().await?;
//! # Ok(()) }
//! ```

mod api;
mod commands;
mod config;
mod connect;
mod envd;
mod error;
mod filesystem;
pub mod proto;
mod sandbox;

pub use api::{SandboxInfo, SandboxLifecycleInfo, SandboxMetrics, SandboxState};
pub use commands::{CommandHandle, CommandOptions, CommandResult, Commands, Output, OutputCallback, ProcessInfo, Pty, PtyOptions, PtySize};
pub use config::{ConnectionConfig, DEFAULT_PROCESS_TIMEOUT, DEFAULT_SANDBOX_TIMEOUT, DEFAULT_USERNAME, ENVD_PORT, REQUEST_TIMEOUT};
pub use error::{Error, Result};
pub use filesystem::{
    EntryInfo, FileType, Filesystem, FilesystemEvent, FilesystemEventType, WatchHandle, WriteEntry, WriteInfo, WriteOptions,
};
pub use sandbox::{NetworkOptions, OnTimeout, Sandbox, SandboxOptions, SandboxPaginator, SandboxQuery, DEFAULT_TEMPLATE};
