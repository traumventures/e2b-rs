//! Files in the sandbox (`sandbox.files`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Method;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::envd::{http_error, versions, EnvdClient};
use crate::error::{Error, Result};
use crate::proto::filesystem as pb;

const SVC: &str = "filesystem.Filesystem";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileType {
    File,
    Dir,
    Symlink,
    #[serde(other)]
    Unknown,
}

impl FileType {
    /// Same strings as the official SDKs: `file`, `dir`, `symlink`.
    pub fn as_str(&self) -> &'static str {
        match self {
            FileType::File => "file",
            FileType::Dir => "dir",
            FileType::Symlink => "symlink",
            FileType::Unknown => "unknown",
        }
    }
}

/// Result of a write (`WriteInfo`).
#[derive(Clone, Debug, Deserialize)]
pub struct WriteInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub file_type: Option<FileType>,
    pub path: String,
}

/// Information about a file or directory (`EntryInfo`).
#[derive(Clone, Debug)]
pub struct EntryInfo {
    pub name: String,
    pub file_type: FileType,
    pub path: String,
    pub size: i64,
    pub mode: u32,
    pub permissions: String,
    pub owner: String,
    pub group: String,
    pub modified_time: Option<DateTime<Utc>>,
    pub symlink_target: Option<String>,
    pub is_symlink: bool,
    pub metadata: HashMap<String, String>,
}

impl From<pb::EntryInfo> for EntryInfo {
    fn from(e: pb::EntryInfo) -> Self {
        // New envd marks symlinks with `is_symlink` (type is the target's), like the JS SDK.
        let file_type = match pb::FileType::try_from(e.r#type) {
            _ if e.is_symlink => FileType::Symlink,
            Ok(pb::FileType::File) => FileType::File,
            Ok(pb::FileType::Directory) => FileType::Dir,
            Ok(pb::FileType::Symlink) => FileType::Symlink,
            _ => FileType::Unknown,
        };
        EntryInfo {
            name: e.name,
            file_type,
            path: e.path,
            size: e.size,
            mode: e.mode,
            permissions: e.permissions,
            owner: e.owner,
            group: e.group,
            modified_time: e
                .modified_time
                .and_then(|t| DateTime::from_timestamp(t.seconds, t.nanos.max(0) as u32)),
            symlink_target: e.symlink_target,
            is_symlink: e.is_symlink,
            metadata: e.metadata,
        }
    }
}

/// Filesystem event type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilesystemEventType {
    Create,
    Write,
    Remove,
    Rename,
    Chmod,
    Unknown,
}

impl FilesystemEventType {
    /// Same strings as the official SDKs: `create`, `write`, `remove`, `rename`, `chmod`.
    pub fn as_str(&self) -> &'static str {
        match self {
            FilesystemEventType::Create => "create",
            FilesystemEventType::Write => "write",
            FilesystemEventType::Remove => "remove",
            FilesystemEventType::Rename => "rename",
            FilesystemEventType::Chmod => "chmod",
            FilesystemEventType::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug)]
pub struct FilesystemEvent {
    /// Path relative to the watched directory.
    pub name: String,
    pub event_type: FilesystemEventType,
    pub entry: Option<EntryInfo>,
}

/// One file for [`Filesystem::write_files`].
#[derive(Clone, Debug)]
pub struct WriteEntry {
    pub path: String,
    pub data: Vec<u8>,
}

/// Options for [`Filesystem::write_with`].
#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub user: Option<String>,
    /// User-defined metadata stored as `user.e2b.*` xattrs (envd >= 0.6.2).
    pub metadata: HashMap<String, String>,
    /// Upload as `application/octet-stream` (envd >= 0.5.7) instead of
    /// `multipart/form-data`. Off by default, like the official SDKs for
    /// in-memory data.
    pub use_octet_stream: bool,
}

/// Options for [`Filesystem::watch_dir_with`].
#[derive(Clone, Debug, Default)]
pub struct WatchOptions {
    pub recursive: bool,
    /// `None` = 60 s, `Some(Duration::ZERO)` = no limit.
    pub timeout: Option<Duration>,
    /// Ask envd to attach the entry info to each event (envd >= 0.6.3).
    pub include_entry: bool,
}

impl WriteEntry {
    pub fn new(path: impl Into<String>, data: impl Into<Vec<u8>>) -> Self {
        Self {
            path: path.into(),
            data: data.into(),
        }
    }
}

/// Handle for [`Filesystem::watch_dir`]. Events arrive via [`next_event`](Self::next_event);
/// dropping the handle or calling [`stop`](Self::stop) ends the watch.
pub struct WatchHandle {
    rx: mpsc::UnboundedReceiver<Result<FilesystemEvent>>,
    task: JoinHandle<()>,
}

impl WatchHandle {
    pub async fn next_event(&mut self) -> Option<Result<FilesystemEvent>> {
        self.rx.recv().await
    }

    pub fn stop(&mut self) {
        self.task.abort();
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `sandbox.files`.
#[derive(Clone)]
pub struct Filesystem {
    pub(crate) envd: Arc<EnvdClient>,
}

fn fs_err(e: Error) -> Error {
    match e {
        Error::NotFound(m) => Error::FileNotFound(m),
        Error::Sandbox { code, message } if code == "resource_exhausted" => Error::NotEnoughSpace(message),
        e => e,
    }
}

impl Filesystem {
    async fn rpc<Req: prost::Message, Resp: prost::Message + Default>(&self, method: &str, req: &Req, user: Option<&str>) -> Result<Resp> {
        self.envd
            .rpc
            .unary(&format!("{SVC}/{method}"), req, self.envd.auth_headers(user), None)
            .await
            .map_err(fs_err)
    }

    fn files_query(&self, path: &str, user: Option<&str>) -> Vec<(&'static str, String)> {
        let mut q = vec![("path", path.to_string())];
        if let Some(u) = self.envd.user(user) {
            q.push(("username", u.to_string()));
        }
        q
    }

    /// Reads a file as bytes.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.read_as(path, None).await
    }

    /// Reads a file as UTF-8 text (lossy).
    pub async fn read_to_string(&self, path: &str) -> Result<String> {
        Ok(String::from_utf8_lossy(&self.read(path).await?).into_owned())
    }

    /// Reads a file as a given user.
    pub async fn read_as(&self, path: &str, user: Option<&str>) -> Result<Vec<u8>> {
        let resp = self
            .envd
            .http_request(Method::GET, "/files")
            .query(&self.files_query(path, user))
            .timeout(self.envd.config.request_timeout)
            .send()
            .await
            .map_err(|e| Error::from_reqwest(e, path))?;
        if !resp.status().is_success() {
            return Err(http_error(resp, path).await);
        }
        Ok(resp.bytes().await.map_err(|e| Error::from_reqwest(e, path))?.to_vec())
    }

    /// Writes a file, creating parent directories; overwrites existing files.
    pub async fn write(&self, path: &str, data: impl Into<Vec<u8>>) -> Result<WriteInfo> {
        self.write_as(path, data, None).await
    }

    /// Writes a file as a given user.
    pub async fn write_as(&self, path: &str, data: impl Into<Vec<u8>>, user: Option<&str>) -> Result<WriteInfo> {
        self.write_with(
            path,
            data,
            WriteOptions {
                user: user.map(str::to_string),
                ..Default::default()
            },
        )
        .await
    }

    /// Writes a file with options (user, metadata).
    pub async fn write_with(&self, path: &str, data: impl Into<Vec<u8>>, opts: WriteOptions) -> Result<WriteInfo> {
        let user = opts.user.as_deref();
        let data = data.into();
        if !opts.metadata.is_empty() && !self.envd.at_least(versions::FILE_METADATA) {
            return Err(Error::Template("file metadata requires envd 0.6.2 or later".into()));
        }
        for (k, v) in &opts.metadata {
            let ok = |s: &str| !s.is_empty() && s.bytes().all(|b| (0x21..0x7f).contains(&b));
            if !ok(k) || v.bytes().any(|b| !(0x20..0x7f).contains(&b)) {
                return Err(Error::InvalidArgument(format!("invalid metadata entry {k:?}")));
            }
        }
        let mut infos = if opts.use_octet_stream && self.envd.at_least(versions::OCTET_STREAM_UPLOAD) {
            let mut rb = self
                .envd
                .http_request(Method::POST, "/files")
                .query(&self.files_query(path, user))
                .header("Content-Type", "application/octet-stream");
            for (k, v) in &opts.metadata {
                rb = rb.header(format!("X-Metadata-{k}"), v);
            }
            let resp = rb
                .timeout(self.envd.config.request_timeout)
                .body(data)
                .send()
                .await
                .map_err(|e| Error::from_reqwest(e, path))?;
            if !resp.status().is_success() {
                return Err(http_error(resp, path).await);
            }
            resp.json::<Vec<WriteInfo>>().await.map_err(|e| Error::from_reqwest(e, path))?
        } else {
            self.write_multipart(&[WriteEntry::new(path, data)], user, Some(path), &opts.metadata)
                .await?
        };
        if infos.is_empty() {
            return Err(Error::Protocol("expected information about the written file".into()));
        }
        Ok(infos.remove(0))
    }

    /// Writes several files in one request.
    pub async fn write_files(&self, files: Vec<WriteEntry>, user: Option<&str>) -> Result<Vec<WriteInfo>> {
        if files.is_empty() {
            return Ok(vec![]);
        }
        self.write_multipart(&files, user, None, &HashMap::new()).await
    }

    async fn write_multipart(
        &self,
        files: &[WriteEntry],
        user: Option<&str>,
        path: Option<&str>,
        metadata: &HashMap<String, String>,
    ) -> Result<Vec<WriteInfo>> {
        let mut form = reqwest::multipart::Form::new();
        for f in files {
            form = form.part("file", reqwest::multipart::Part::bytes(f.data.clone()).file_name(f.path.clone()));
        }
        let mut q = Vec::new();
        if let Some(p) = path {
            q.push(("path", p.to_string()));
        }
        if let Some(u) = self.envd.user(user) {
            q.push(("username", u.to_string()));
        }
        let mut rb = self.envd.http_request(Method::POST, "/files").query(&q);
        for (k, v) in metadata {
            rb = rb.header(format!("X-Metadata-{k}"), v);
        }
        let resp = rb
            .timeout(self.envd.config.request_timeout)
            .multipart(form)
            .send()
            .await
            .map_err(|e| Error::from_reqwest(e, "/files"))?;
        if !resp.status().is_success() {
            return Err(http_error(resp, path.unwrap_or("/files")).await);
        }
        resp.json::<Vec<WriteInfo>>().await.map_err(|e| Error::from_reqwest(e, "/files"))
    }

    /// Lists a directory. `depth` 1 lists direct children.
    pub async fn list(&self, path: &str, depth: u32) -> Result<Vec<EntryInfo>> {
        if depth < 1 {
            return Err(Error::InvalidArgument("depth should be at least one".into()));
        }
        let r: pb::ListDirResponse = self.rpc("ListDir", &pb::ListDirRequest { path: path.into(), depth }, None).await?;
        Ok(r.entries.into_iter().map(Into::into).collect())
    }

    pub async fn exists(&self, path: &str) -> Result<bool> {
        match self.get_info(path).await {
            Ok(_) => Ok(true),
            Err(Error::FileNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub async fn get_info(&self, path: &str) -> Result<EntryInfo> {
        let r: pb::StatResponse = self.rpc("Stat", &pb::StatRequest { path: path.into() }, None).await?;
        r.entry
            .map(Into::into)
            .ok_or_else(|| Error::Protocol("stat returned no entry".into()))
    }

    /// Creates a directory and its parents. `false` when it already existed.
    pub async fn make_dir(&self, path: &str) -> Result<bool> {
        match self
            .rpc::<_, pb::MakeDirResponse>("MakeDir", &pb::MakeDirRequest { path: path.into() }, None)
            .await
        {
            Ok(_) => Ok(true),
            Err(Error::Sandbox { code, .. }) if code == "already_exists" => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub async fn rename(&self, old_path: &str, new_path: &str) -> Result<EntryInfo> {
        let r: pb::MoveResponse = self
            .rpc(
                "Move",
                &pb::MoveRequest {
                    source: old_path.into(),
                    destination: new_path.into(),
                },
                None,
            )
            .await?;
        r.entry
            .map(Into::into)
            .ok_or_else(|| Error::Protocol("move returned no entry".into()))
    }

    pub async fn remove(&self, path: &str) -> Result<()> {
        self.rpc::<_, pb::RemoveResponse>("Remove", &pb::RemoveRequest { path: path.into() }, None)
            .await?;
        Ok(())
    }

    /// Watches a directory for changes. `timeout` limits the watch (`None` = 60 s,
    /// zero = unlimited), like `timeoutMs` in the official SDKs.
    pub async fn watch_dir(&self, path: &str, recursive: bool, timeout: Option<Duration>) -> Result<WatchHandle> {
        self.watch_dir_with(
            path,
            WatchOptions {
                recursive,
                timeout,
                include_entry: false,
            },
        )
        .await
    }

    /// Watches a directory with options.
    pub async fn watch_dir_with(&self, path: &str, opts: WatchOptions) -> Result<WatchHandle> {
        let WatchOptions {
            recursive,
            timeout,
            include_entry,
        } = opts;
        if include_entry && !self.envd.at_least(versions::FS_EVENT_ENTRY_INFO) {
            return Err(Error::Template(format!(
                "envd {} cannot include entry info; rebuild the template",
                self.envd.version_raw
            )));
        }
        if recursive && !self.envd.at_least(versions::RECURSIVE_WATCH) {
            return Err(Error::Template(format!(
                "envd {} cannot watch recursively; rebuild the template",
                self.envd.version_raw
            )));
        }
        let req = pb::WatchDirRequest {
            path: path.into(),
            recursive,
            include_entry,
            allow_network_mounts: false,
        };
        let mut stream = self
            .envd
            .rpc
            .server_stream::<_, pb::WatchDirResponse>(
                &format!("{SVC}/WatchDir"),
                &req,
                self.envd.stream_headers(None),
                Some(timeout.unwrap_or(crate::config::DEFAULT_PROCESS_TIMEOUT)),
            )
            .await
            .map_err(fs_err)?;
        use pb::watch_dir_response::Event;
        loop {
            match stream.next().await {
                Some(Ok(m)) => match m.event {
                    Some(Event::Start(_)) => break,
                    Some(Event::Keepalive(_)) | None => continue,
                    Some(Event::Filesystem(_)) => return Err(Error::Protocol("expected start event".into())),
                },
                Some(Err(e)) => return Err(fs_err(e)),
                None => return Err(Error::Protocol("watch stream ended before start".into())),
            }
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            while let Some(m) = stream.next().await {
                let ev = match m {
                    Ok(pb::WatchDirResponse {
                        event: Some(Event::Filesystem(f)),
                    }) => {
                        let event_type = match pb::EventType::try_from(f.r#type) {
                            Ok(pb::EventType::Create) => FilesystemEventType::Create,
                            Ok(pb::EventType::Write) => FilesystemEventType::Write,
                            Ok(pb::EventType::Remove) => FilesystemEventType::Remove,
                            Ok(pb::EventType::Rename) => FilesystemEventType::Rename,
                            Ok(pb::EventType::Chmod) => FilesystemEventType::Chmod,
                            _ => FilesystemEventType::Unknown,
                        };
                        Ok(FilesystemEvent {
                            name: f.name,
                            event_type,
                            entry: f.entry.map(Into::into),
                        })
                    }
                    Ok(_) => continue,
                    Err(e) => Err(fs_err(e)),
                };
                if tx.send(ev).is_err() {
                    break;
                }
            }
        });
        Ok(WatchHandle { rx, task })
    }
}
