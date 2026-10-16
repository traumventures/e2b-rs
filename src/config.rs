use std::collections::HashMap;
use std::time::Duration;

/// Port envd listens on inside every sandbox.
pub const ENVD_PORT: u16 = 49983;
/// Default timeout for a single API or envd request.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Default sandbox lifetime when none is given.
pub const DEFAULT_SANDBOX_TIMEOUT: Duration = Duration::from_secs(300);
/// Default limit for a command/stream connection (`Connect-Timeout-Ms`).
pub const DEFAULT_PROCESS_TIMEOUT: Duration = Duration::from_secs(60);
/// Interval at which envd sends keepalive events on streams.
pub const KEEPALIVE_PING_INTERVAL_SECS: u64 = 50;
pub(crate) const KEEPALIVE_PING_HEADER: &str = "Keepalive-Ping-Interval";
/// Username used by envd versions older than 0.4.0 that have no default user.
pub const DEFAULT_USERNAME: &str = "user";

// Hosted domains that serve the shared `sandbox.<domain>` host (routing by header).
const SHARED_SANDBOX_HOST_DOMAINS: &[&str] = &["e2b.app", "e2b.dev", "e2b.pro", "e2b-staging.dev"];

pub(crate) const USER_AGENT: &str = concat!("e2b-rust-sdk/", env!("CARGO_PKG_VERSION"));

/// Connection settings. [`ConnectionConfig::from_env`] (also `Default`) reads the
/// same environment variables as the official SDKs: `E2B_API_KEY`,
/// `E2B_ACCESS_TOKEN`, `E2B_DOMAIN`, `E2B_API_URL`, `E2B_SANDBOX_URL`, `E2B_DEBUG`.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub api_key: Option<String>,
    pub access_token: Option<String>,
    /// Base domain, default `e2b.app`.
    pub domain: String,
    /// Debug mode: API on `http://localhost:3000`, envd on `http://localhost:49983`.
    pub debug: bool,
    /// Explicit API URL; default `https://api.<domain>`.
    pub api_url: Option<String>,
    /// Explicit sandbox URL. All envd traffic goes there, routed with the
    /// `E2b-Sandbox-Id` / `E2b-Sandbox-Port` headers (e2b-minimal uses this).
    pub sandbox_url: Option<String>,
    pub request_timeout: Duration,
    /// Extra headers sent to the E2B API.
    pub headers: HashMap<String, String>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

impl ConnectionConfig {
    pub fn from_env() -> Self {
        Self {
            api_key: env_var("E2B_API_KEY"),
            access_token: env_var("E2B_ACCESS_TOKEN"),
            domain: env_var("E2B_DOMAIN").unwrap_or_else(|| "e2b.app".into()),
            debug: env_var("E2B_DEBUG").is_some_and(|v| v.eq_ignore_ascii_case("true")),
            api_url: env_var("E2B_API_URL"),
            sandbox_url: env_var("E2B_SANDBOX_URL"),
            request_timeout: REQUEST_TIMEOUT,
            headers: HashMap::new(),
        }
    }

    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = domain.into();
        self
    }

    pub fn with_api_url(mut self, url: impl Into<String>) -> Self {
        self.api_url = Some(url.into());
        self
    }

    pub fn with_sandbox_url(mut self, url: impl Into<String>) -> Self {
        self.sandbox_url = Some(url.into());
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_debug(mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }

    /// The resolved E2B API base URL (no trailing slash).
    pub fn api_base(&self) -> String {
        let url = match &self.api_url {
            Some(u) => u.clone(),
            None if self.debug => "http://localhost:3000".into(),
            None => format!("https://api.{}", self.domain),
        };
        url.trim_end_matches('/').to_string()
    }

    /// Host for a port of a sandbox: `<port>-<sandboxId>.<domain>`.
    pub fn host(&self, sandbox_id: &str, port: u16, sandbox_domain: &str) -> String {
        if self.debug {
            return format!("localhost:{port}");
        }
        format!("{port}-{sandbox_id}.{sandbox_domain}")
    }

    /// Base URL for envd of a sandbox, mirroring `getSandboxUrl` of the JS SDK.
    pub fn sandbox_base(&self, sandbox_id: &str, sandbox_domain: &str) -> String {
        if let Some(u) = &self.sandbox_url {
            return u.trim_end_matches('/').to_string();
        }
        if self.debug {
            return format!("http://{}", self.host(sandbox_id, ENVD_PORT, sandbox_domain));
        }
        if SHARED_SANDBOX_HOST_DOMAINS.contains(&sandbox_domain) {
            return format!("https://sandbox.{sandbox_domain}");
        }
        format!("https://{}", self.host(sandbox_id, ENVD_PORT, sandbox_domain))
    }

    /// Direct (per-sandbox host) envd URL, used for signed upload/download URLs.
    pub fn sandbox_direct_base(&self, sandbox_id: &str, sandbox_domain: &str) -> String {
        if let Some(u) = &self.sandbox_url {
            return u.trim_end_matches('/').to_string();
        }
        let scheme = if self.debug { "http" } else { "https" };
        format!("{scheme}://{}", self.host(sandbox_id, ENVD_PORT, sandbox_domain))
    }
}
