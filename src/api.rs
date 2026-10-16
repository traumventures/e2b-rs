//! REST client for the E2B control-plane API (`api.e2b.app`).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use reqwest::{Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::config::{ConnectionConfig, USER_AGENT};
use crate::error::{Error, Result};

#[derive(Clone)]
pub(crate) struct ApiClient {
    pub(crate) http: reqwest::Client,
    pub(crate) config: ConnectionConfig,
}

pub(crate) fn build_http() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("reqwest client")
}

#[derive(Deserialize)]
struct ApiErrorBody {
    #[serde(default)]
    error_code: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

impl ApiClient {
    pub(crate) fn new(config: ConnectionConfig) -> Self {
        Self {
            http: build_http(),
            config,
        }
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let mut rb = self
            .http
            .request(method, format!("{}{}", self.config.api_base(), path))
            .timeout(self.config.request_timeout);
        if let Some(k) = &self.config.api_key {
            rb = rb.header("X-API-Key", k);
        }
        if let Some(t) = &self.config.access_token {
            rb = rb.bearer_auth(t);
        }
        for (k, v) in &self.config.headers {
            rb = rb.header(k, v);
        }
        rb
    }

    pub(crate) async fn send(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&serde_json::Value>,
    ) -> Result<reqwest::Response> {
        if self.config.api_key.is_none() && self.config.access_token.is_none() {
            return Err(Error::Authentication(
                "API key is required: set E2B_API_KEY or pass it in ConnectionConfig".into(),
            ));
        }
        let mut rb = self.request(method, path).query(query);
        if let Some(b) = body {
            rb = rb.json(b);
        }
        let resp = rb.send().await.map_err(|e| Error::from_reqwest(e, path))?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        Err(api_error(resp).await)
    }

    pub(crate) async fn json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&serde_json::Value>,
    ) -> Result<(StatusCode, T)> {
        let resp = self.send(method, path, query, body).await?;
        let status = resp.status();
        let v = resp.json::<T>().await.map_err(|e| Error::from_reqwest(e, path))?;
        Ok((status, v))
    }
}

pub(crate) async fn api_error(resp: reqwest::Response) -> Error {
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    let (code, message) = match serde_json::from_str::<ApiErrorBody>(&text) {
        Ok(b) => (b.error_code, b.message.unwrap_or_else(|| text.clone())),
        Err(_) => (None, text.clone()),
    };
    let message = if message.is_empty() { format!("HTTP {status}") } else { message };
    match status {
        401 | 403 => Error::Authentication(message),
        429 => Error::RateLimit(message),
        _ => Error::Api { status, code, message },
    }
}

/// Sandbox state as reported by the API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxState {
    Running,
    Paused,
}

impl SandboxState {
    pub fn as_str(&self) -> &'static str {
        match self {
            SandboxState::Running => "running",
            SandboxState::Paused => "paused",
        }
    }
}

/// Response of create / connect.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SandboxCreated {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(rename = "templateID")]
    #[allow(dead_code)]
    pub template_id: String,
    pub envd_version: String,
    #[serde(default)]
    pub envd_access_token: Option<String>,
    #[serde(default)]
    pub traffic_access_token: Option<String>,
    #[serde(default)]
    pub domain: Option<String>,
}

/// Information about a sandbox (`getInfo` / `list`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxInfo {
    #[serde(rename = "sandboxID")]
    pub sandbox_id: String,
    #[serde(rename = "templateID")]
    pub template_id: String,
    /// Template alias.
    #[serde(default, rename = "alias")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub metadata: HashMap<String, String>,
    pub started_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub state: SandboxState,
    pub cpu_count: u32,
    #[serde(rename = "memoryMB")]
    pub memory_mb: u32,
    #[serde(default, rename = "diskSizeMB")]
    pub disk_size_mb: Option<u32>,
    pub envd_version: String,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub allow_internet_access: Option<bool>,
    #[serde(default)]
    pub lifecycle: Option<SandboxLifecycleInfo>,
    #[serde(default)]
    pub network: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxLifecycleInfo {
    pub auto_resume: bool,
    pub on_timeout: String,
}

/// One sample of sandbox resource usage.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxMetrics {
    pub timestamp: DateTime<Utc>,
    pub cpu_count: u32,
    pub cpu_used_pct: f64,
    pub mem_used: u64,
    pub mem_total: u64,
    #[serde(default)]
    pub disk_used: u64,
    #[serde(default)]
    pub disk_total: u64,
}

fn null_as_default<'de, D, T>(d: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}
