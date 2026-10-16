//! Shared plumbing for talking to envd inside a sandbox.

use base64::Engine;
use reqwest::header::HeaderMap;
use semver::Version;

use crate::config::{ConnectionConfig, DEFAULT_USERNAME, ENVD_PORT, KEEPALIVE_PING_HEADER, KEEPALIVE_PING_INTERVAL_SECS, USER_AGENT};
use crate::connect::{header_value, RpcClient};
use crate::error::{Error, Result};

pub(crate) mod versions {
    pub const RECURSIVE_WATCH: &str = "0.1.4";
    pub const COMMANDS_STDIN: &str = "0.3.0";
    pub const DEFAULT_USER: &str = "0.4.0";
    pub const ENVD_CLOSE: &str = "0.5.2";
    pub const OCTET_STREAM_UPLOAD: &str = "0.5.7";
    pub const FILE_METADATA: &str = "0.6.2";
    pub const FS_EVENT_ENTRY_INFO: &str = "0.6.3";
}

pub(crate) struct EnvdClient {
    pub(crate) rpc: RpcClient,
    pub(crate) http: reqwest::Client,
    pub(crate) version: Version,
    pub(crate) version_raw: String,
    pub(crate) config: ConnectionConfig,
}

pub(crate) fn parse_version(v: &str) -> Version {
    let v = v.trim().trim_start_matches('v');
    Version::parse(v).unwrap_or_else(|_| {
        // Accept "0.2" style versions.
        let mut parts = v.split('.').map(|p| p.parse::<u64>().unwrap_or(0));
        Version::new(parts.next().unwrap_or(0), parts.next().unwrap_or(0), parts.next().unwrap_or(0))
    })
}

impl EnvdClient {
    pub(crate) fn new(
        http: reqwest::Client,
        config: ConnectionConfig,
        sandbox_id: &str,
        sandbox_domain: &str,
        envd_version: &str,
        access_token: Option<String>,
    ) -> Self {
        let base = config.sandbox_base(sandbox_id, sandbox_domain);
        let mut headers = HeaderMap::new();
        headers.insert("User-Agent", header_value(USER_AGENT));
        headers.insert("E2b-Sandbox-Id", header_value(sandbox_id));
        headers.insert("E2b-Sandbox-Port", header_value(&ENVD_PORT.to_string()));
        if let Some(t) = &access_token {
            headers.insert("X-Access-Token", header_value(t));
        }
        let rpc = RpcClient {
            http: http.clone(),
            base,
            headers,
            request_timeout: config.request_timeout,
        };
        Self {
            rpc,
            http,
            version: parse_version(envd_version),
            version_raw: envd_version.to_string(),
            config,
        }
    }

    pub(crate) fn at_least(&self, v: &str) -> bool {
        self.version >= parse_version(v)
    }

    /// Resolves the effective user: old envd versions need an explicit one.
    pub(crate) fn user<'a>(&self, user: Option<&'a str>) -> Option<&'a str> {
        match user {
            None if !self.at_least(versions::DEFAULT_USER) => Some(DEFAULT_USERNAME),
            u => u,
        }
    }

    /// `Authorization: Basic base64(user:)` like the official SDKs.
    pub(crate) fn auth_headers(&self, user: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(u) = self.user(user).filter(|u| !u.is_empty()) {
            let enc = base64::engine::general_purpose::STANDARD.encode(format!("{u}:"));
            h.insert("Authorization", header_value(&format!("Basic {enc}")));
        }
        h
    }

    pub(crate) fn stream_headers(&self, user: Option<&str>) -> HeaderMap {
        let mut h = self.auth_headers(user);
        h.insert(KEEPALIVE_PING_HEADER, header_value(&KEEPALIVE_PING_INTERVAL_SECS.to_string()));
        h
    }

    /// Plain HTTP request to envd (e.g. `/files`, `/health`).
    pub(crate) fn http_request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{}", self.rpc.base, path))
            .headers(self.rpc.headers.clone())
    }

    /// `GET /health`: `Ok(true)` when envd answers, `Ok(false)` when the proxy says
    /// the sandbox is gone.
    pub(crate) async fn health(&self) -> Result<bool> {
        let resp = self
            .http_request(reqwest::Method::GET, "/health")
            .timeout(self.config.request_timeout)
            .send()
            .await
            .map_err(|e| Error::from_reqwest(e, "/health"))?;
        match resp.status().as_u16() {
            200..=299 => Ok(true),
            502 => Ok(false),
            s => Err(Error::Sandbox {
                code: s.to_string(),
                message: resp.text().await.unwrap_or_default(),
            }),
        }
    }
}

/// Maps an envd HTTP (non-RPC) error response.
pub(crate) async fn http_error(resp: reqwest::Response, path_ctx: &str) -> Error {
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    let message = serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|v| v.get("message").and_then(|m| m.as_str()).map(str::to_string))
        .unwrap_or_else(|| if text.is_empty() { format!("HTTP {status}") } else { text.clone() });
    match status {
        400 => Error::InvalidArgument(message),
        401 => Error::Authentication(message),
        404 => Error::FileNotFound(format!("{path_ctx}: {message}")),
        429 => Error::RateLimit(message),
        502 => Error::SandboxNotRunning(message),
        507 => Error::NotEnoughSpace(message),
        _ => Error::Sandbox {
            code: status.to_string(),
            message,
        },
    }
}
