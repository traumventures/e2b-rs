use crate::commands::CommandResult;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors, mirroring the error classes of the official SDKs.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A command finished with a non-zero exit code (`CommandExitError`).
    #[error("command exited with code {}{}", .0.exit_code, .0.error.as_deref().map(|e| format!(": {e}")).unwrap_or_default())]
    CommandExit(Box<CommandResult>),
    /// Request or operation timed out (`TimeoutError`).
    #[error("timeout: {0}")]
    Timeout(String),
    /// The sandbox was killed or reached its timeout (`SandboxNotRunningError`).
    #[error("sandbox is not running: {0}")]
    SandboxNotRunning(String),
    /// Sandbox does not exist (`SandboxNotFoundError`).
    #[error("sandbox not found: {0}")]
    SandboxNotFound(String),
    /// File or directory not found (`FileNotFoundError`).
    #[error("file not found: {0}")]
    FileNotFound(String),
    /// Other not-found errors (`NotFoundError`).
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("authentication failed: {0}")]
    Authentication(String),
    #[error("rate limited: {0}")]
    RateLimit(String),
    #[error("not enough disk space: {0}")]
    NotEnoughSpace(String),
    /// Template is incompatible with a requested feature (`TemplateError`).
    #[error("template error: {0}")]
    Template(String),
    /// Error response from the E2B API.
    #[error("E2B API error {status}: {message}")]
    Api {
        status: u16,
        code: Option<String>,
        message: String,
    },
    /// Error returned by envd (Connect RPC code or HTTP status).
    #[error("sandbox error ({code}): {message}")]
    Sandbox { code: String, message: String },
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("protocol: {0}")]
    Protocol(String),
}

impl Error {
    /// Maps a Connect error code + message like the official SDK's `handleRpcError`.
    pub(crate) fn from_rpc(code: &str, message: String) -> Self {
        const NOT_FOUND_MSG: &str = "The sandbox was not found";
        match code {
            "invalid_argument" => Error::InvalidArgument(message),
            "unauthenticated" => Error::Authentication(message),
            "not_found" => Error::NotFound(message),
            "resource_exhausted" => Error::RateLimit(message),
            "deadline_exceeded" | "canceled" => Error::Timeout(message),
            "unavailable" if message.contains(NOT_FOUND_MSG) || message.contains("sandbox not found") => Error::SandboxNotRunning(message),
            _ => Error::Sandbox {
                code: code.to_string(),
                message,
            },
        }
    }

    /// Whether this is any of the not-found variants.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound(_) | Error::FileNotFound(_) | Error::SandboxNotFound(_))
    }

    pub(crate) fn from_reqwest(e: reqwest::Error, what: &str) -> Self {
        if e.is_timeout() {
            Error::Timeout(format!("{what}: {e}"))
        } else {
            Error::Http(e)
        }
    }
}
