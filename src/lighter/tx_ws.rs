//! Persistent tx WebSocket — port of `TxWebSocket`. Sends `jsonapi/sendtxbatch` frames
//! (excluded from the 200 msg/min limit). Preserves the critical outcome semantics:
//!   * NotSent  — no frame written; REST fallback is SAFE.
//!   * Unknown  — a frame may have reached Lighter; caller must NOT retry (pause+reconcile).
//!   * Ok/Rejected — definite outcome from the response code (200 normalized to 0).
//!
//! Sends are serialized upstream (the sign+send Mutex), so we read the response inline
//! rather than running a separate recv loop.

use crate::types::{TxSendResult, TxSendStatus};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct TxWebSocket {
    url: String,
    conn: Mutex<Option<Ws>>,
}

impl TxWebSocket {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            conn: Mutex::new(None),
        }
    }

    /// Pre-connect (best effort). Safe to call at startup.
    pub async fn connect(&self) -> Result<()> {
        let ws = self.open().await?;
        *self.conn.lock().await = Some(ws);
        Ok(())
    }

    async fn open(&self) -> Result<Ws> {
        let (mut ws, _) = connect_async(&self.url).await.context("tx ws connect")?;
        // Consume an optional init/connected message (5s).
        let _ = timeout(Duration::from_secs(5), ws.next()).await;
        tracing::info!("TxWebSocket connected to {}", self.url);
        Ok(ws)
    }

    /// Extract a message field as its RAW string content. For a JSON string this returns the
    /// inner text (so an empty message `""` becomes the empty Rust string, NOT `"\"\""`) — the
    /// reject classifier and the empty-message code-fallback depend on this (codex).
    fn extract_message(v: Option<&Value>) -> String {
        match v {
            Some(Value::String(s)) => s.clone(),
            Some(other) if !other.is_null() => other.to_string(),
            _ => String::new(),
        }
    }

    fn code_message(resp: &Value) -> (i64, String) {
        if let Some(err) = resp.get("error") {
            if let Some(obj) = err.as_object() {
                let code = obj.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
                let msg = Self::extract_message(obj.get("message"));
                return (if code == 200 { 0 } else { code }, msg);
            } else if !err.is_null() {
                return (-1, err.to_string());
            }
        }
        let code = resp
            .get("code")
            .or_else(|| resp.get("status_code"))
            .and_then(|c| c.as_i64())
            .unwrap_or(0);
        let msg = Self::extract_message(resp.get("message"));
        (if code == 200 { 0 } else { code }, msg)
    }

    /// Send a batch. `tx_types`/`tx_infos` are JSON-encoded into strings inside `data`.
    pub async fn send_batch(&self, tx_types: &[u8], tx_infos: &[String]) -> TxSendResult {
        let mut guard = self.conn.lock().await;
        if guard.is_none() {
            match self.open().await {
                Ok(ws) => *guard = Some(ws),
                Err(_) => return TxSendResult::not_sent("connect_failed"),
            }
        }
        let ws = guard.as_mut().unwrap();

        let frame = serde_json::json!({
            "type": "jsonapi/sendtxbatch",
            "data": {
                "tx_types": serde_json::to_string(tx_types).unwrap_or_default(),
                "tx_infos": serde_json::to_string(tx_infos).unwrap_or_default(),
            }
        })
        .to_string();

        if let Err(e) = ws.send(Message::Text(frame)).await {
            *guard = None;
            return TxSendResult::unknown(format!("send_failed:{e}"));
        }

        // Read the response (10s budget), handling ping/info frames.
        let deadline = Duration::from_secs(10);
        let result = timeout(deadline, async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(t))) => {
                        let v: Value = match serde_json::from_str(&t) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        match v.get("type").and_then(|x| x.as_str()) {
                            Some("ping") => {
                                let _ = ws.send(Message::Text(r#"{"type":"pong"}"#.into())).await;
                                continue;
                            }
                            Some("connected") | Some("subscribed") => continue,
                            _ => return Some(v),
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = ws.send(Message::Pong(p)).await;
                        continue;
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(_)) | None => return None,
                }
            }
        })
        .await;

        match result {
            Ok(Some(resp)) => {
                let (code, message) = Self::code_message(&resp);
                let quota = resp.get("volume_quota_remaining").and_then(|q| q.as_i64());
                let status = if code == 0 { TxSendStatus::Ok } else { TxSendStatus::Rejected };
                TxSendResult { status, code, message, quota_remaining: quota }
            }
            Ok(None) => {
                *guard = None;
                TxSendResult::unknown("disconnected_after_send")
            }
            Err(_) => {
                *guard = None;
                TxSendResult::unknown("response_timeout")
            }
        }
    }
}
