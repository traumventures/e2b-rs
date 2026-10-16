//! Minimal Connect protocol client (binary protobuf) for envd.
//!
//! Unary calls: `POST /<service>/<method>` with `application/proto`.
//! Server streams: `application/connect+proto` with 5-byte enveloped frames
//! (flags, big-endian length); a frame with flag `0x02` ends the stream and
//! carries a JSON trailer that may contain an error.

use std::pin::Pin;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use prost::Message;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;

use crate::error::{Error, Result};

const FLAG_COMPRESSED: u8 = 0x01;
const FLAG_END_STREAM: u8 = 0x02;

#[derive(Clone)]
pub(crate) struct RpcClient {
    pub(crate) http: reqwest::Client,
    pub(crate) base: String,
    /// Headers sent with every call (routing, access token, user agent).
    pub(crate) headers: HeaderMap,
    pub(crate) request_timeout: Duration,
}

#[derive(Deserialize, Default)]
struct ConnectError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize, Default)]
struct EndStream {
    error: Option<ConnectError>,
}

fn code_from_http(status: u16) -> &'static str {
    match status {
        400 => "invalid_argument",
        401 => "unauthenticated",
        403 => "permission_denied",
        404 => "not_found",
        408 => "deadline_exceeded",
        429 | 502 | 503 | 504 => "unavailable",
        _ => "unknown",
    }
}

/// Turns a non-200 Connect response into an [`Error`].
async fn error_from_response(resp: reqwest::Response) -> Error {
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    match serde_json::from_str::<ConnectError>(&body) {
        Ok(e) if !e.code.is_empty() => Error::from_rpc(&e.code, e.message),
        _ => {
            let message = if body.trim().is_empty() {
                format!("HTTP {status}")
            } else {
                body.trim().to_string()
            };
            Error::from_rpc(code_from_http(status), message)
        }
    }
}

impl RpcClient {
    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base, path.trim_start_matches('/'))
    }

    /// Unary call.
    pub(crate) async fn unary<Req: Message, Resp: Message + Default>(
        &self,
        path: &str,
        req: &Req,
        extra: HeaderMap,
        timeout: Option<Duration>,
    ) -> Result<Resp> {
        let timeout = timeout.unwrap_or(self.request_timeout);
        let mut rb = self
            .http
            .post(self.url(path))
            .headers(self.headers.clone())
            .headers(extra)
            .header(CONTENT_TYPE, "application/proto")
            .header("Connect-Protocol-Version", "1")
            .body(req.encode_to_vec());
        if !timeout.is_zero() {
            rb = rb.timeout(timeout);
        }
        let resp = rb.send().await.map_err(|e| Error::from_reqwest(e, path))?;
        if !resp.status().is_success() {
            return Err(error_from_response(resp).await);
        }
        let bytes = resp.bytes().await.map_err(|e| Error::from_reqwest(e, path))?;
        Resp::decode(bytes).map_err(|e| Error::Protocol(format!("decode {path}: {e}")))
    }

    /// Server-streaming call. `stream_timeout` becomes `Connect-Timeout-Ms`
    /// (zero means no limit); establishing the stream is bounded by the request timeout.
    pub(crate) async fn server_stream<Req: Message, Resp: Message + Default>(
        &self,
        path: &str,
        req: &Req,
        extra: HeaderMap,
        stream_timeout: Option<Duration>,
    ) -> Result<RpcStream<Resp>> {
        let msg = req.encode_to_vec();
        let mut body = BytesMut::with_capacity(msg.len() + 5);
        body.put_u8(0);
        body.put_u32(msg.len() as u32);
        body.put_slice(&msg);

        let mut rb = self
            .http
            .post(self.url(path))
            .headers(self.headers.clone())
            .headers(extra)
            .header(CONTENT_TYPE, "application/connect+proto")
            .header("Connect-Protocol-Version", "1")
            .body(body.freeze());
        if let Some(t) = stream_timeout.filter(|t| !t.is_zero()) {
            rb = rb.header("Connect-Timeout-Ms", t.as_millis().to_string());
        }
        let send = rb.send();
        let resp = if self.request_timeout.is_zero() {
            send.await
        } else {
            tokio::time::timeout(self.request_timeout, send)
                .await
                .map_err(|_| Error::Timeout(format!("{path}: no response within {:?}", self.request_timeout)))?
        }
        .map_err(|e| Error::from_reqwest(e, path))?;

        let ct_ok = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("application/connect+proto"));
        if !resp.status().is_success() || !ct_ok {
            return Err(error_from_response(resp).await);
        }
        Ok(RpcStream {
            body: Box::pin(resp.bytes_stream()),
            buf: BytesMut::new(),
            done: false,
            path: path.to_string(),
            _p: std::marker::PhantomData,
        })
    }
}

pub(crate) fn header_value(v: &str) -> HeaderValue {
    HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static(""))
}

type ByteStream = Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>;

/// A stream of decoded messages from a server-streaming call.
pub(crate) struct RpcStream<T> {
    body: ByteStream,
    buf: BytesMut,
    done: bool,
    path: String,
    _p: std::marker::PhantomData<fn() -> T>,
}

impl<T: Message + Default> RpcStream<T> {
    /// Next message; `None` once the server ended the stream cleanly.
    pub(crate) async fn next(&mut self) -> Option<Result<T>> {
        if self.done {
            return None;
        }
        loop {
            if self.buf.len() >= 5 {
                let flags = self.buf[0];
                let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
                if self.buf.len() >= 5 + len {
                    self.buf.advance(5);
                    let frame = self.buf.split_to(len).freeze();
                    if flags & FLAG_COMPRESSED != 0 {
                        self.done = true;
                        return Some(Err(Error::Protocol("compressed frame not supported".into())));
                    }
                    if flags & FLAG_END_STREAM != 0 {
                        self.done = true;
                        let end: EndStream = serde_json::from_slice(&frame).unwrap_or_default();
                        return end.error.map(|e| Err(Error::from_rpc(&e.code, e.message)));
                    }
                    return Some(T::decode(frame).map_err(|e| Error::Protocol(format!("decode {}: {e}", self.path))));
                }
            }
            match self.body.next().await {
                Some(Ok(chunk)) => self.buf.extend_from_slice(&chunk),
                Some(Err(e)) => {
                    self.done = true;
                    let msg = format!("{}: stream interrupted: {e}", self.path);
                    return Some(Err(if e.is_timeout() {
                        Error::Timeout(msg)
                    } else {
                        Error::Sandbox {
                            code: "unknown".into(),
                            message: msg,
                        }
                    }));
                }
                None => {
                    self.done = true;
                    return Some(Err(Error::Protocol(format!(
                        "{}: stream ended without end-of-stream frame",
                        self.path
                    ))));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::process as pb;

    fn frame(flags: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![flags];
        v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        v.extend_from_slice(payload);
        v
    }

    fn stream_of(chunks: Vec<Vec<u8>>) -> RpcStream<pb::StartResponse> {
        let s = futures::stream::iter(chunks.into_iter().map(|c| Ok(Bytes::from(c))));
        RpcStream {
            body: Box::pin(s),
            buf: BytesMut::new(),
            done: false,
            path: "t".into(),
            _p: std::marker::PhantomData,
        }
    }

    fn start(pid: u32) -> Vec<u8> {
        pb::StartResponse {
            event: Some(pb::ProcessEvent {
                event: Some(pb::process_event::Event::Start(pb::process_event::StartEvent { pid })),
            }),
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn frames_split_across_chunks() {
        let mut all = frame(0, &start(7));
        all.extend(frame(0, &start(8)));
        all.extend(frame(FLAG_END_STREAM, b"{}"));
        // Deliver one byte at a time.
        let mut s = stream_of(all.into_iter().map(|b| vec![b]).collect());
        let pid = |m: pb::StartResponse| match m.event.unwrap().event.unwrap() {
            pb::process_event::Event::Start(s) => s.pid,
            _ => panic!(),
        };
        assert_eq!(pid(s.next().await.unwrap().unwrap()), 7);
        assert_eq!(pid(s.next().await.unwrap().unwrap()), 8);
        assert!(s.next().await.is_none());
        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn end_stream_error() {
        let mut s = stream_of(vec![frame(
            FLAG_END_STREAM,
            br#"{"error":{"code":"deadline_exceeded","message":"too slow"}}"#,
        )]);
        assert!(matches!(s.next().await, Some(Err(Error::Timeout(m))) if m == "too slow"));
        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn truncated_stream_is_error() {
        let mut s = stream_of(vec![frame(0, &start(1))[..3].to_vec()]);
        assert!(matches!(s.next().await, Some(Err(Error::Protocol(_)))));
    }
}
