//! Running processes in the sandbox (`sandbox.commands`, `sandbox.pty`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::DEFAULT_PROCESS_TIMEOUT;
use crate::connect::RpcStream;
use crate::envd::{versions, EnvdClient};
use crate::error::{Error, Result};
use crate::proto::process as pb;

const SVC: &str = "process.Process";

/// Callback receiving decoded output chunks.
pub type OutputCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Options for [`Commands::run`] and [`Commands::spawn`].
#[derive(Clone, Default)]
pub struct CommandOptions {
    pub envs: HashMap<String, String>,
    pub cwd: Option<String>,
    /// User to run as; default is the template's default user.
    pub user: Option<String>,
    /// How long the command may run. `None` = 60 s, `Some(Duration::ZERO)` = no limit.
    pub timeout: Option<Duration>,
    /// Keep stdin open so [`CommandHandle::send_stdin`] works (envd >= 0.3.0).
    pub stdin: bool,
    pub on_stdout: Option<OutputCallback>,
    pub on_stderr: Option<OutputCallback>,
}

impl std::fmt::Debug for CommandOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandOptions")
            .field("envs", &self.envs)
            .field("cwd", &self.cwd)
            .field("user", &self.user)
            .field("timeout", &self.timeout)
            .field("stdin", &self.stdin)
            .finish_non_exhaustive()
    }
}

impl CommandOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cwd(mut self, cwd: impl Into<String>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }
    pub fn env(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.envs.insert(k.into(), v.into());
        self
    }
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }
    pub fn stdin(mut self, on: bool) -> Self {
        self.stdin = on;
        self
    }
    pub fn on_stdout(mut self, f: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.on_stdout = Some(Arc::new(f));
        self
    }
    pub fn on_stderr(mut self, f: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.on_stderr = Some(Arc::new(f));
        self
    }
}

/// Result of a finished command.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommandResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    /// Error message from envd (e.g. signal), if any.
    pub error: Option<String>,
}

/// A chunk of output from a running process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Stdout(Bytes),
    Stderr(Bytes),
    Pty(Bytes),
}

/// A process running in the sandbox, as reported by [`Commands::list`].
#[derive(Clone, Debug)]
pub struct ProcessInfo {
    pub pid: u32,
    pub tag: Option<String>,
    pub cmd: String,
    pub args: Vec<String>,
    pub envs: HashMap<String, String>,
    pub cwd: Option<String>,
}

/// Handle to a running process (`CommandHandle`).
pub struct CommandHandle {
    pid: u32,
    commands: Commands,
    output_rx: mpsc::UnboundedReceiver<Output>,
    task: Option<JoinHandle<Result<CommandResult>>>,
    collected: Arc<Mutex<(Vec<u8>, Vec<u8>)>>,
}

impl std::fmt::Debug for CommandHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandHandle").field("pid", &self.pid).finish_non_exhaustive()
    }
}

impl CommandHandle {
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Stdout received so far.
    pub fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.collected.lock().unwrap().0).into_owned()
    }

    /// Stderr received so far.
    pub fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.collected.lock().unwrap().1).into_owned()
    }

    /// Next output chunk, `None` once the process ended (or after [`disconnect`](Self::disconnect)).
    pub async fn next_output(&mut self) -> Option<Output> {
        self.output_rx.recv().await
    }

    /// Waits for the process to end. A non-zero exit code is
    /// [`Error::CommandExit`], like `CommandExitError` in the official SDKs.
    pub async fn wait(mut self) -> Result<CommandResult> {
        let res = self.wait_raw().await?;
        if res.exit_code != 0 {
            return Err(Error::CommandExit(Box::new(res)));
        }
        Ok(res)
    }

    /// Waits for the process to end and returns the result whatever the exit code.
    pub async fn wait_raw(&mut self) -> Result<CommandResult> {
        let task = self.task.take().ok_or_else(|| Error::Sandbox {
            code: "disconnected".into(),
            message: "handle was disconnected or already waited on".into(),
        })?;
        match task.await {
            Ok(r) => r,
            Err(e) => Err(Error::Sandbox {
                code: "aborted".into(),
                message: e.to_string(),
            }),
        }
    }

    /// Stops receiving events; the process keeps running and can be reattached
    /// with [`Commands::connect`].
    pub fn disconnect(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }

    /// Kills the process with SIGKILL. `false` when it was not found.
    pub async fn kill(&self) -> Result<bool> {
        self.commands.kill(self.pid).await
    }

    pub async fn send_stdin(&self, data: impl AsRef<[u8]>) -> Result<()> {
        self.commands.send_stdin(self.pid, data).await
    }

    pub async fn close_stdin(&self) -> Result<()> {
        self.commands.close_stdin(self.pid).await
    }
}

impl Drop for CommandHandle {
    fn drop(&mut self) {
        // Dropping the handle detaches; the process keeps running in the sandbox.
        self.disconnect();
    }
}

/// `sandbox.commands`.
#[derive(Clone)]
pub struct Commands {
    pub(crate) envd: Arc<EnvdClient>,
}

fn selector(pid: u32) -> Option<pb::ProcessSelector> {
    Some(pb::ProcessSelector {
        selector: Some(pb::process_selector::Selector::Pid(pid)),
    })
}

impl Commands {
    /// Runs `cmd` with `bash -l -c` and waits for it to finish.
    pub async fn run(&self, cmd: &str, opts: CommandOptions) -> Result<CommandResult> {
        self.spawn(cmd, opts).await?.wait().await
    }

    /// Starts `cmd` in the background (`run(cmd, { background: true })`).
    pub async fn spawn(&self, cmd: &str, opts: CommandOptions) -> Result<CommandHandle> {
        if opts.stdin && !self.envd.at_least(versions::COMMANDS_STDIN) {
            return Err(Error::Template(format!(
                "envd {} always keeps stdin open; rebuild the template to control it",
                self.envd.version_raw
            )));
        }
        let req = pb::StartRequest {
            process: Some(pb::ProcessConfig {
                cmd: "/bin/bash".into(),
                args: vec!["-l".into(), "-c".into(), cmd.into()],
                envs: opts.envs.clone(),
                cwd: opts.cwd.clone(),
            }),
            pty: None,
            tag: None,
            stdin: if self.envd.at_least(versions::COMMANDS_STDIN) {
                Some(opts.stdin)
            } else {
                None
            },
        };
        let headers = self.envd.stream_headers(opts.user.as_deref());
        let stream = self
            .envd
            .rpc
            .server_stream::<_, pb::StartResponse>(
                &format!("{SVC}/Start"),
                &req,
                headers,
                Some(opts.timeout.unwrap_or(DEFAULT_PROCESS_TIMEOUT)),
            )
            .await?;
        self.handle_from_stream(stream, |r| r.event, opts.on_stdout, opts.on_stderr, None)
            .await
    }

    /// Reattaches to a running process.
    pub async fn connect(&self, pid: u32, opts: CommandOptions) -> Result<CommandHandle> {
        let req = pb::ConnectRequest { process: selector(pid) };
        let headers = self.envd.stream_headers(None);
        let stream = self
            .envd
            .rpc
            .server_stream::<_, pb::ConnectResponse>(
                &format!("{SVC}/Connect"),
                &req,
                headers,
                Some(opts.timeout.unwrap_or(DEFAULT_PROCESS_TIMEOUT)),
            )
            .await?;
        self.handle_from_stream(stream, |r| r.event, opts.on_stdout, opts.on_stderr, None)
            .await
    }

    pub(crate) async fn handle_from_stream<R: prost::Message + Default + Send + 'static>(
        &self,
        mut stream: RpcStream<R>,
        event_of: fn(R) -> Option<pb::ProcessEvent>,
        on_stdout: Option<OutputCallback>,
        on_stderr: Option<OutputCallback>,
        on_pty: Option<OutputCallback>,
    ) -> Result<CommandHandle> {
        use pb::process_event::Event;
        // The first non-keepalive event must be `start`.
        let pid = loop {
            let ev = match stream.next().await {
                Some(r) => event_of(r?).and_then(|e| e.event),
                None => return Err(Error::Protocol("process stream ended before start event".into())),
            };
            match ev {
                Some(Event::Start(s)) => break s.pid,
                Some(Event::Keepalive(_)) | None => continue,
                Some(other) => return Err(Error::Protocol(format!("expected start event, got {other:?}"))),
            }
        };

        let (tx, rx) = mpsc::unbounded_channel();
        let collected = Arc::new(Mutex::new((Vec::new(), Vec::new())));
        let col = collected.clone();
        let task = tokio::spawn(async move {
            use pb::process_event::data_event::Output as O;
            while let Some(msg) = stream.next().await {
                let Some(ev) = event_of(msg?).and_then(|e| e.event) else { continue };
                match ev {
                    Event::Data(d) => match d.output {
                        Some(O::Stdout(b)) => {
                            col.lock().unwrap().0.extend_from_slice(&b);
                            if let Some(cb) = &on_stdout {
                                cb(&String::from_utf8_lossy(&b));
                            }
                            let _ = tx.send(Output::Stdout(b));
                        }
                        Some(O::Stderr(b)) => {
                            col.lock().unwrap().1.extend_from_slice(&b);
                            if let Some(cb) = &on_stderr {
                                cb(&String::from_utf8_lossy(&b));
                            }
                            let _ = tx.send(Output::Stderr(b));
                        }
                        Some(O::Pty(b)) => {
                            if let Some(cb) = &on_pty {
                                cb(&String::from_utf8_lossy(&b));
                            }
                            let _ = tx.send(Output::Pty(b));
                        }
                        None => {}
                    },
                    Event::End(end) => {
                        let c = col.lock().unwrap();
                        return Ok(CommandResult {
                            stdout: String::from_utf8_lossy(&c.0).into_owned(),
                            stderr: String::from_utf8_lossy(&c.1).into_owned(),
                            exit_code: end.exit_code,
                            error: end.error.filter(|e| !e.is_empty()),
                        });
                    }
                    Event::Start(_) | Event::Keepalive(_) => {}
                }
            }
            Err(Error::Sandbox {
                code: "unknown".into(),
                message: "process exited without a result".into(),
            })
        });

        Ok(CommandHandle {
            pid,
            commands: self.clone(),
            output_rx: rx,
            task: Some(task),
            collected,
        })
    }

    /// Lists running processes.
    pub async fn list(&self) -> Result<Vec<ProcessInfo>> {
        let resp: pb::ListResponse = self
            .envd
            .rpc
            .unary(&format!("{SVC}/List"), &pb::ListRequest {}, self.envd.auth_headers(None), None)
            .await?;
        Ok(resp
            .processes
            .into_iter()
            .map(|p| {
                let c = p.config.unwrap_or_default();
                ProcessInfo {
                    pid: p.pid,
                    tag: p.tag,
                    cmd: c.cmd,
                    args: c.args,
                    envs: c.envs,
                    cwd: c.cwd,
                }
            })
            .collect())
    }

    /// Kills a process with SIGKILL. `false` when it was not found.
    pub async fn kill(&self, pid: u32) -> Result<bool> {
        let req = pb::SendSignalRequest {
            process: selector(pid),
            signal: pb::Signal::Sigkill as i32,
        };
        match self
            .envd
            .rpc
            .unary::<_, pb::SendSignalResponse>(&format!("{SVC}/SendSignal"), &req, self.envd.auth_headers(None), None)
            .await
        {
            Ok(_) => Ok(true),
            Err(Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Writes to the stdin of a process started with `stdin: true`.
    pub async fn send_stdin(&self, pid: u32, data: impl AsRef<[u8]>) -> Result<()> {
        self.send_input(pid, pb::process_input::Input::Stdin(Bytes::copy_from_slice(data.as_ref())))
            .await
    }

    pub(crate) async fn send_input(&self, pid: u32, input: pb::process_input::Input) -> Result<()> {
        let req = pb::SendInputRequest {
            process: selector(pid),
            input: Some(pb::ProcessInput { input: Some(input) }),
        };
        self.envd
            .rpc
            .unary::<_, pb::SendInputResponse>(&format!("{SVC}/SendInput"), &req, self.envd.auth_headers(None), None)
            .await?;
        Ok(())
    }

    /// Whether this sandbox's envd supports [`close_stdin`](Self::close_stdin).
    pub fn supports_stdin_close(&self) -> bool {
        self.envd.at_least(versions::ENVD_CLOSE)
    }

    /// Closes stdin (EOF) of a process started with `stdin: true`.
    pub async fn close_stdin(&self, pid: u32) -> Result<()> {
        if !self.supports_stdin_close() {
            return Err(Error::Template(format!(
                "envd {} cannot close stdin; rebuild the template",
                self.envd.version_raw
            )));
        }
        let req = pb::CloseStdinRequest { process: selector(pid) };
        self.envd
            .rpc
            .unary::<_, pb::CloseStdinResponse>(&format!("{SVC}/CloseStdin"), &req, self.envd.auth_headers(None), None)
            .await?;
        Ok(())
    }
}

/// Terminal size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PtySize {
    pub cols: u32,
    pub rows: u32,
}

/// Options for [`Pty::create`].
#[derive(Clone, Default)]
pub struct PtyOptions {
    pub envs: HashMap<String, String>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    /// `None` = 60 s, `Some(Duration::ZERO)` = no limit.
    pub timeout: Option<Duration>,
    pub on_data: Option<OutputCallback>,
}

/// `sandbox.pty`: interactive terminals.
#[derive(Clone)]
pub struct Pty {
    pub(crate) commands: Commands,
}

impl Pty {
    /// Starts an interactive login bash in a PTY. Read output with
    /// [`CommandHandle::next_output`] (`Output::Pty`).
    pub async fn create(&self, size: PtySize, opts: PtyOptions) -> Result<CommandHandle> {
        let mut envs = opts.envs.clone();
        envs.entry("TERM".into()).or_insert_with(|| "xterm-256color".into());
        envs.entry("LANG".into()).or_insert_with(|| "C.UTF-8".into());
        envs.entry("LC_ALL".into()).or_insert_with(|| "C.UTF-8".into());
        let req = pb::StartRequest {
            process: Some(pb::ProcessConfig {
                cmd: "/bin/bash".into(),
                args: vec!["-i".into(), "-l".into()],
                envs,
                cwd: opts.cwd.clone(),
            }),
            pty: Some(pb::Pty {
                size: Some(pb::pty::Size {
                    cols: size.cols,
                    rows: size.rows,
                }),
            }),
            tag: None,
            stdin: None,
        };
        let envd = &self.commands.envd;
        let stream = envd
            .rpc
            .server_stream::<_, pb::StartResponse>(
                &format!("{SVC}/Start"),
                &req,
                envd.stream_headers(opts.user.as_deref()),
                Some(opts.timeout.unwrap_or(DEFAULT_PROCESS_TIMEOUT)),
            )
            .await?;
        self.commands
            .handle_from_stream(stream, |r| r.event, None, None, opts.on_data)
            .await
    }

    /// Reattaches to a PTY session.
    pub async fn connect(&self, pid: u32, opts: PtyOptions) -> Result<CommandHandle> {
        let envd = &self.commands.envd;
        let req = pb::ConnectRequest { process: selector(pid) };
        let stream = envd
            .rpc
            .server_stream::<_, pb::ConnectResponse>(
                &format!("{SVC}/Connect"),
                &req,
                envd.stream_headers(None),
                Some(opts.timeout.unwrap_or(DEFAULT_PROCESS_TIMEOUT)),
            )
            .await?;
        self.commands
            .handle_from_stream(stream, |r| r.event, None, None, opts.on_data)
            .await
    }

    /// Sends input (keystrokes) to the PTY.
    pub async fn send_input(&self, pid: u32, data: impl AsRef<[u8]>) -> Result<()> {
        self.commands
            .send_input(pid, pb::process_input::Input::Pty(Bytes::copy_from_slice(data.as_ref())))
            .await
    }

    pub async fn resize(&self, pid: u32, size: PtySize) -> Result<()> {
        let envd = &self.commands.envd;
        let req = pb::UpdateRequest {
            process: selector(pid),
            pty: Some(pb::Pty {
                size: Some(pb::pty::Size {
                    cols: size.cols,
                    rows: size.rows,
                }),
            }),
        };
        envd.rpc
            .unary::<_, pb::UpdateResponse>(&format!("{SVC}/Update"), &req, envd.auth_headers(None), None)
            .await?;
        Ok(())
    }

    pub async fn kill(&self, pid: u32) -> Result<bool> {
        self.commands.kill(pid).await
    }
}
