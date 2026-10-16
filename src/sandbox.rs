//! The [`Sandbox`] type: lifecycle via the E2B API plus envd modules.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Method;
use serde_json::{json, Value};

use crate::api::{ApiClient, SandboxCreated, SandboxInfo, SandboxMetrics, SandboxState};
use crate::commands::{Commands, Pty};
use crate::config::{ConnectionConfig, DEFAULT_SANDBOX_TIMEOUT};
use crate::envd::{parse_version, EnvdClient};
use crate::error::{Error, Result};
use crate::filesystem::Filesystem;

/// Default template.
pub const DEFAULT_TEMPLATE: &str = "base";

/// What happens when the sandbox times out.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnTimeout {
    #[default]
    Kill,
    /// Pause instead of killing. `keep_memory: false` snapshots the filesystem only.
    Pause { keep_memory: bool },
}

/// Outbound/inbound network rules (`network` option).
#[derive(Clone, Debug, Default)]
pub struct NetworkOptions {
    /// Allowed destinations: CIDRs, IPs or domains (domains need `deny_out: ["0.0.0.0/0"]`).
    pub allow_out: Option<Vec<String>>,
    /// Denied CIDRs or IPs.
    pub deny_out: Option<Vec<String>>,
    /// `false` requires the traffic access token to reach sandbox ports.
    pub allow_public_traffic: Option<bool>,
    pub mask_request_host: Option<String>,
}

/// Options for [`Sandbox::create`] (builder style).
#[derive(Clone, Debug)]
pub struct SandboxOptions {
    pub template: String,
    pub timeout: Duration,
    pub metadata: HashMap<String, String>,
    pub envs: HashMap<String, String>,
    pub allow_internet_access: Option<bool>,
    pub network: Option<NetworkOptions>,
    pub on_timeout: Option<OnTimeout>,
    pub auto_resume: Option<bool>,
    /// Raw MCP config passed through to the API.
    pub mcp: Option<Value>,
    /// mount path -> volume name.
    pub volume_mounts: HashMap<String, String>,
    pub config: ConnectionConfig,
}

impl Default for SandboxOptions {
    fn default() -> Self {
        Self {
            template: DEFAULT_TEMPLATE.into(),
            timeout: DEFAULT_SANDBOX_TIMEOUT,
            metadata: HashMap::new(),
            envs: HashMap::new(),
            allow_internet_access: None,
            network: None,
            on_timeout: None,
            auto_resume: None,
            mcp: None,
            volume_mounts: HashMap::new(),
            config: ConnectionConfig::from_env(),
        }
    }
}

impl SandboxOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn template(mut self, t: impl Into<String>) -> Self {
        self.template = t.into();
        self
    }
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }
    pub fn metadata(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.metadata.insert(k.into(), v.into());
        self
    }
    pub fn env(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.envs.insert(k.into(), v.into());
        self
    }
    pub fn allow_internet_access(mut self, on: bool) -> Self {
        self.allow_internet_access = Some(on);
        self
    }
    pub fn network(mut self, n: NetworkOptions) -> Self {
        self.network = Some(n);
        self
    }
    pub fn on_timeout(mut self, o: OnTimeout) -> Self {
        self.on_timeout = Some(o);
        self
    }
    pub fn auto_resume(mut self, on: bool) -> Self {
        self.auto_resume = Some(on);
        self
    }
    pub fn config(mut self, c: ConnectionConfig) -> Self {
        self.config = c;
        self
    }

    pub(crate) fn body(&self) -> Result<Value> {
        let (pause, memory) = match self.on_timeout {
            Some(OnTimeout::Pause { keep_memory }) => (Some(true), Some(keep_memory)),
            Some(OnTimeout::Kill) => (Some(false), None),
            None => (None, None),
        };
        if self.auto_resume == Some(true) && pause != Some(true) {
            return Err(Error::InvalidArgument(
                "auto_resume can only be true when on_timeout is Pause".into(),
            ));
        }
        if self.auto_resume == Some(true) && memory == Some(false) {
            return Err(Error::InvalidArgument(
                "auto_resume cannot be combined with a filesystem-only pause".into(),
            ));
        }
        let mut b = json!({
            "templateID": self.template,
            "timeout": timeout_secs(self.timeout),
        });
        let o = b.as_object_mut().unwrap();
        if !self.metadata.is_empty() {
            o.insert("metadata".into(), json!(self.metadata));
        }
        if !self.envs.is_empty() {
            o.insert("envVars".into(), json!(self.envs));
        }
        if let Some(a) = self.allow_internet_access {
            o.insert("allow_internet_access".into(), json!(a));
        }
        if let Some(n) = &self.network {
            let mut nb = serde_json::Map::new();
            if let Some(v) = &n.allow_out {
                nb.insert("allowOut".into(), json!(v));
            }
            if let Some(v) = &n.deny_out {
                nb.insert("denyOut".into(), json!(v));
            }
            if let Some(v) = n.allow_public_traffic {
                nb.insert("allowPublicTraffic".into(), json!(v));
            }
            if let Some(v) = &n.mask_request_host {
                nb.insert("maskRequestHost".into(), json!(v));
            }
            o.insert("network".into(), Value::Object(nb));
        }
        if let Some(p) = pause {
            o.insert("autoPause".into(), json!(p));
        }
        if let Some(m) = memory {
            o.insert("autoPauseMemory".into(), json!(m));
        }
        if let Some(r) = self.auto_resume {
            o.insert("autoResume".into(), json!({ "enabled": r }));
        }
        if let Some(m) = &self.mcp {
            o.insert("mcp".into(), m.clone());
        }
        if !self.volume_mounts.is_empty() {
            let v: Vec<Value> = self.volume_mounts.iter().map(|(p, n)| json!({"name": n, "path": p})).collect();
            o.insert("volumeMounts".into(), json!(v));
        }
        Ok(b)
    }
}

fn timeout_secs(t: Duration) -> u64 {
    // The API takes whole seconds; round up like the official SDKs.
    t.as_millis().div_ceil(1000) as u64
}

/// Filter for [`Sandbox::list`].
#[derive(Clone, Debug, Default)]
pub struct SandboxQuery {
    pub metadata: HashMap<String, String>,
    /// Empty means running and paused.
    pub state: Vec<SandboxState>,
    pub template: Option<String>,
    pub started_after: Option<DateTime<Utc>>,
}

/// Pages through sandboxes (`SandboxPaginator`).
pub struct SandboxPaginator {
    api: ApiClient,
    query: SandboxQuery,
    limit: Option<u32>,
    next_token: Option<String>,
    has_next: bool,
}

impl SandboxPaginator {
    pub fn has_next(&self) -> bool {
        self.has_next
    }

    pub fn next_token(&self) -> Option<&str> {
        self.next_token.as_deref()
    }

    /// Fetches the next page.
    pub async fn next_items(&mut self) -> Result<Vec<SandboxInfo>> {
        if !self.has_next {
            return Err(Error::InvalidArgument("no more items to fetch".into()));
        }
        let mut q: Vec<(&str, String)> = Vec::new();
        if !self.query.metadata.is_empty() {
            let enc: String = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(self.query.metadata.iter().map(|(k, v)| (urlencode(k), urlencode(v))))
                .finish();
            q.push(("metadata", enc));
        }
        if !self.query.state.is_empty() {
            q.push(("state", self.query.state.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")));
        }
        if let Some(t) = &self.query.template {
            q.push(("template", t.clone()));
        }
        if let Some(t) = self.query.started_after {
            q.push(("startedAfter", t.to_rfc3339()));
        }
        if let Some(l) = self.limit {
            q.push(("limit", l.to_string()));
        }
        if let Some(t) = &self.next_token {
            q.push(("nextToken", t.clone()));
        }
        let resp = self.api.send(Method::GET, "/v2/sandboxes", &q, None).await?;
        self.next_token = resp
            .headers()
            .get("x-next-token")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        self.has_next = self.next_token.is_some();
        resp.json::<Vec<SandboxInfo>>()
            .await
            .map_err(|e| Error::from_reqwest(e, "/v2/sandboxes"))
    }

    /// Fetches all remaining pages.
    pub async fn collect_all(mut self) -> Result<Vec<SandboxInfo>> {
        let mut all = Vec::new();
        while self.has_next {
            all.extend(self.next_items().await?);
        }
        Ok(all)
    }
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// A running E2B sandbox. Cloning is cheap and shares the connection.
#[derive(Clone)]
pub struct Sandbox {
    sandbox_id: String,
    sandbox_domain: String,
    envd_version: String,
    envd_access_token: Option<String>,
    traffic_access_token: Option<String>,
    api: ApiClient,
    envd: Arc<EnvdClient>,
    /// Run commands (`sandbox.commands`).
    pub commands: Commands,
    /// Read and write files (`sandbox.files`).
    pub files: Filesystem,
    /// Interactive terminals (`sandbox.pty`).
    pub pty: Pty,
}

impl std::fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sandbox")
            .field("sandbox_id", &self.sandbox_id)
            .field("sandbox_domain", &self.sandbox_domain)
            .field("envd_version", &self.envd_version)
            .finish_non_exhaustive()
    }
}

impl Sandbox {
    fn from_created(api: ApiClient, c: SandboxCreated) -> Self {
        let domain = c.domain.filter(|d| !d.is_empty()).unwrap_or_else(|| api.config.domain.clone());
        let envd = Arc::new(EnvdClient::new(
            api.http.clone(),
            api.config.clone(),
            &c.sandbox_id,
            &domain,
            &c.envd_version,
            c.envd_access_token.clone(),
        ));
        let commands = Commands { envd: envd.clone() };
        Sandbox {
            files: Filesystem { envd: envd.clone() },
            pty: Pty {
                commands: commands.clone(),
            },
            commands,
            envd,
            sandbox_id: c.sandbox_id,
            sandbox_domain: domain,
            envd_version: c.envd_version,
            envd_access_token: c.envd_access_token,
            traffic_access_token: c.traffic_access_token,
            api,
        }
    }

    /// Creates a sandbox (`Sandbox.create`).
    pub async fn create(opts: SandboxOptions) -> Result<Self> {
        let body = opts.body()?;
        let api = ApiClient::new(opts.config.clone());
        let (_, created): (_, SandboxCreated) = api.json(Method::POST, "/v2/sandboxes", &[], Some(&body)).await?;
        if parse_version(&created.envd_version) < semver::Version::new(0, 1, 0) {
            let _ = Self::kill_by_id(&created.sandbox_id, &opts.config).await;
            return Err(Error::Template(
                "the template's envd is too old for this SDK; rebuild the template".into(),
            ));
        }
        Ok(Self::from_created(api, created))
    }

    /// Creates a sandbox from `template` with default options.
    pub async fn create_from(template: &str) -> Result<Self> {
        Self::create(SandboxOptions::default().template(template)).await
    }

    /// Connects to a running or paused sandbox, resuming it if paused
    /// (`Sandbox.connect`). `timeout` extends the lifetime (only ever extended).
    pub async fn connect(sandbox_id: &str, timeout: Option<Duration>, config: &ConnectionConfig) -> Result<Self> {
        let api = ApiClient::new(config.clone());
        let mut body = serde_json::Map::new();
        if let Some(t) = timeout {
            body.insert("timeout".into(), json!(timeout_secs(t)));
        }
        let body = Value::Object(body);
        let res = api
            .json::<SandboxCreated>(Method::POST, &format!("/v2/sandboxes/{sandbox_id}/connect"), &[], Some(&body))
            .await;
        match res {
            Ok((_, c)) => Ok(Self::from_created(api, c)),
            Err(Error::Api { status: 404, message, .. }) => Err(Error::SandboxNotFound(format!("{sandbox_id}: {message}"))),
            Err(e) => Err(e),
        }
    }

    /// Lists sandboxes (running and paused unless filtered).
    pub fn list(query: SandboxQuery, limit: Option<u32>, config: &ConnectionConfig) -> SandboxPaginator {
        SandboxPaginator {
            api: ApiClient::new(config.clone()),
            query,
            limit,
            next_token: None,
            has_next: true,
        }
    }

    /// Kills a sandbox by id. `false` when it did not exist.
    pub async fn kill_by_id(sandbox_id: &str, config: &ConnectionConfig) -> Result<bool> {
        let api = ApiClient::new(config.clone());
        match api.send(Method::DELETE, &format!("/sandboxes/{sandbox_id}"), &[], None).await {
            Ok(_) => Ok(true),
            Err(Error::Api { status: 404, .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Pauses a sandbox by id. `keep_memory: false` keeps only the filesystem.
    /// Returns `false` when it was already paused.
    pub async fn pause_by_id(sandbox_id: &str, keep_memory: bool, config: &ConnectionConfig) -> Result<bool> {
        let api = ApiClient::new(config.clone());
        let body = if keep_memory { json!({}) } else { json!({ "memory": false }) };
        match api
            .send(Method::POST, &format!("/sandboxes/{sandbox_id}/pause"), &[], Some(&body))
            .await
        {
            Ok(_) => Ok(true),
            Err(Error::Api { status: 409, .. }) => Ok(false),
            Err(Error::Api { status: 404, message, .. }) => Err(Error::SandboxNotFound(format!("{sandbox_id}: {message}"))),
            Err(e) => Err(e),
        }
    }

    /// Sets the remaining lifetime by id.
    pub async fn set_timeout_by_id(sandbox_id: &str, timeout: Duration, config: &ConnectionConfig) -> Result<()> {
        let api = ApiClient::new(config.clone());
        let body = json!({ "timeout": timeout_secs(timeout) });
        match api
            .send(Method::POST, &format!("/sandboxes/{sandbox_id}/timeout"), &[], Some(&body))
            .await
        {
            Ok(_) => Ok(()),
            Err(Error::Api { status: 404, message, .. }) => Err(Error::SandboxNotFound(format!("{sandbox_id}: {message}"))),
            Err(e) => Err(e),
        }
    }

    /// Info about a sandbox by id.
    pub async fn get_info_by_id(sandbox_id: &str, config: &ConnectionConfig) -> Result<SandboxInfo> {
        let api = ApiClient::new(config.clone());
        match api
            .json::<SandboxInfo>(Method::GET, &format!("/sandboxes/{sandbox_id}"), &[], None)
            .await
        {
            Ok((_, i)) => Ok(i),
            Err(Error::Api { status: 404, message, .. }) => Err(Error::SandboxNotFound(format!("{sandbox_id}: {message}"))),
            Err(e) => Err(e),
        }
    }

    /// Resource metrics by id.
    pub async fn get_metrics_by_id(sandbox_id: &str, config: &ConnectionConfig) -> Result<Vec<SandboxMetrics>> {
        let api = ApiClient::new(config.clone());
        Ok(api
            .json(Method::GET, &format!("/sandboxes/{sandbox_id}/metrics"), &[], None)
            .await?
            .1)
    }

    pub fn sandbox_id(&self) -> &str {
        &self.sandbox_id
    }

    pub fn sandbox_domain(&self) -> &str {
        &self.sandbox_domain
    }

    pub fn envd_version(&self) -> &str {
        &self.envd_version
    }

    /// Token envd requires (`X-Access-Token`); `None` for insecure sandboxes.
    pub fn envd_access_token(&self) -> Option<&str> {
        self.envd_access_token.as_deref()
    }

    /// Token for `e2b-traffic-access-token` when public traffic is disabled.
    pub fn traffic_access_token(&self) -> Option<&str> {
        self.traffic_access_token.as_deref()
    }

    pub fn connection_config(&self) -> &ConnectionConfig {
        &self.api.config
    }

    /// Host for a port in the sandbox: `<port>-<id>.<domain>`.
    pub fn get_host(&self, port: u16) -> String {
        self.api.config.host(&self.sandbox_id, port, &self.sandbox_domain)
    }

    /// Whether envd answers (`isRunning`).
    pub async fn is_running(&self) -> Result<bool> {
        self.envd.health().await
    }

    pub async fn kill(&self) -> Result<bool> {
        Self::kill_by_id(&self.sandbox_id, &self.api.config).await
    }

    /// Pauses the sandbox with memory. Resume with [`Sandbox::connect`].
    pub async fn pause(&self) -> Result<bool> {
        Self::pause_by_id(&self.sandbox_id, true, &self.api.config).await
    }

    /// Pauses the sandbox, keeping only the filesystem.
    pub async fn pause_filesystem_only(&self) -> Result<bool> {
        Self::pause_by_id(&self.sandbox_id, false, &self.api.config).await
    }

    pub async fn set_timeout(&self, timeout: Duration) -> Result<()> {
        Self::set_timeout_by_id(&self.sandbox_id, timeout, &self.api.config).await
    }

    pub async fn get_info(&self) -> Result<SandboxInfo> {
        Self::get_info_by_id(&self.sandbox_id, &self.api.config).await
    }

    pub async fn get_metrics(&self) -> Result<Vec<SandboxMetrics>> {
        Self::get_metrics_by_id(&self.sandbox_id, &self.api.config).await
    }

    /// Reconnects this handle (resumes a paused sandbox) and returns a fresh one.
    pub async fn reconnect(&self, timeout: Option<Duration>) -> Result<Self> {
        Self::connect(&self.sandbox_id, timeout, &self.api.config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_body_matches_official_sdk() {
        let o = SandboxOptions::default()
            .template("base")
            .timeout(Duration::from_millis(1500))
            .metadata("a", "b")
            .env("X", "1")
            .allow_internet_access(false)
            .on_timeout(OnTimeout::Pause { keep_memory: false });
        let b = o.body().unwrap();
        assert_eq!(
            b,
            json!({
                "templateID": "base", "timeout": 2, "metadata": {"a": "b"}, "envVars": {"X": "1"},
                "allow_internet_access": false, "autoPause": true, "autoPauseMemory": false
            })
        );
        assert!(SandboxOptions::default().auto_resume(true).body().is_err());
    }

    #[test]
    fn sandbox_urls() {
        let c = ConnectionConfig {
            sandbox_url: None,
            api_url: None,
            debug: false,
            ..ConnectionConfig::from_env()
        }
        .with_domain("e2b.app");
        assert_eq!(c.sandbox_base("abc", "e2b.app"), "https://sandbox.e2b.app");
        assert_eq!(c.sandbox_base("abc", "example.com"), "https://49983-abc.example.com");
        assert_eq!(c.api_base(), "https://api.e2b.app");
        let m = c.clone().with_sandbox_url("http://h:3000/");
        assert_eq!(m.sandbox_base("abc", "e2b.app"), "http://h:3000");
    }
}
