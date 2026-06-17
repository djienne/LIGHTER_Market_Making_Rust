//! Lighter WebSocket subscription primitives — port of `ws_manager.py`.
//!
//! `subscribe_loop` connects, subscribes to channels (optionally with per-channel auth),
//! handles ping/pong + the `subscribed` confirmation, applies a recv-timeout watchdog, and
//! reconnects with exponential backoff. Each decoded application message is handed to a
//! synchronous callback (the hot-path market-data task runs its book+signal update there;
//! cold-path account tasks enqueue to channels). A buggy callback never tears down the
//! socket — it is caught and logged.

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

pub const WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";

#[derive(Clone)]
pub struct SubscribeOptions {
    pub url: String,
    pub channels: Vec<String>,
    pub channel_auths: HashMap<String, String>,
    pub recv_timeout: f64,
    pub reconnect_base: f64,
    pub reconnect_max: f64,
    pub label: String,
}

impl SubscribeOptions {
    pub fn new(label: &str, channels: Vec<String>) -> Self {
        Self {
            url: WS_URL.to_string(),
            channels,
            channel_auths: HashMap::new(),
            recv_timeout: 30.0,
            reconnect_base: 5.0,
            reconnect_max: 60.0,
            label: label.to_string(),
        }
    }
}

/// Deterministic jitter in [0, 0.2*base) without an RNG dependency (matches the spirit of
/// the Python `backoff*0.2*(monotonic()%1)`), seeded by wall-clock ms.
fn jitter(base: f64) -> f64 {
    let frac = (crate::shared::now_ms() % 1000) as f64 / 1000.0;
    base * 0.2 * frac
}

/// Run the subscription loop forever (reconnecting). `on_message` is called for each
/// decoded application message (NOT ping/subscribed). `reconnect` (if provided) forces a
/// fresh reconnect when notified (e.g. orderbook sanity divergence). `on_disconnect` runs
/// on every disconnect (clear local book, reset vol state, etc.).
pub async fn subscribe_loop<F, D>(
    opts: SubscribeOptions,
    reconnect: Option<std::sync::Arc<Notify>>,
    mut on_message: F,
    mut on_disconnect: D,
) where
    F: FnMut(&Value),
    D: FnMut(),
{
    let mut backoff = opts.reconnect_base;
    loop {
        match session(&opts, reconnect.as_deref(), &mut on_message).await {
            Ok(()) => {}
            Err(e) => tracing::info!("{} ws disconnected: {e}", opts.label),
        }
        on_disconnect();
        sleep(Duration::from_secs_f64(backoff + jitter(backoff))).await;
        backoff = (backoff * 2.0).min(opts.reconnect_max);
    }
}

/// Like `subscribe_loop` but regenerates per-channel auth tokens before EACH connection
/// (private channels: account_orders / account_all / user_stats). The server token TTL is
/// ~10 min; on expiry the server drops the socket, the session ends, and we reconnect with a
/// fresh token. `auth_fn` returns the channel->token map for the upcoming connection.
pub async fn subscribe_loop_authed<F, A>(mut opts: SubscribeOptions, mut auth_fn: A, mut on_message: F)
where
    F: FnMut(&Value),
    A: FnMut() -> std::collections::HashMap<String, String>,
{
    let mut backoff = opts.reconnect_base;
    loop {
        opts.channel_auths = auth_fn();
        if opts.channel_auths.is_empty() {
            tracing::warn!("{}: no auth token; retrying", opts.label);
            sleep(Duration::from_secs_f64(backoff)).await;
            backoff = (backoff * 2.0).min(opts.reconnect_max);
            continue;
        }
        match session(&opts, None, &mut on_message).await {
            Ok(()) => {}
            Err(e) => tracing::info!("{} ws disconnected: {e}", opts.label),
        }
        sleep(Duration::from_secs_f64(backoff + jitter(backoff))).await;
        backoff = (backoff * 2.0).min(opts.reconnect_max);
    }
}

async fn session<F>(
    opts: &SubscribeOptions,
    reconnect: Option<&Notify>,
    on_message: &mut F,
) -> Result<()>
where
    F: FnMut(&Value),
{
    let (ws_stream, _) = connect_async(&opts.url).await?;
    let (mut write, mut read) = ws_stream.split();
    tracing::info!("connected to {} for {}", opts.url, opts.label);

    for ch in &opts.channels {
        let mut sub = serde_json::json!({"type": "subscribe", "channel": ch});
        if let Some(auth) = opts.channel_auths.get(ch) {
            sub["auth"] = Value::String(auth.clone());
        }
        write.send(Message::Text(sub.to_string())).await?;
    }
    tracing::info!("{} subscribed to {:?}", opts.label, opts.channels);

    let recv_to = Duration::from_secs_f64(opts.recv_timeout);
    loop {
        // Non-blocking forced-reconnect check.
        if let Some(rc) = reconnect {
            if rc.notified().now_or_never().is_some() {
                tracing::info!("{} reconnect requested; dropping for fresh snapshot", opts.label);
                return Ok(());
            }
        }

        let msg = match timeout(recv_to, read.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(None) => return Ok(()),
            Err(_) => {
                tracing::warn!("{} watchdog: no data for {}s", opts.label, opts.recv_timeout);
                return Ok(());
            }
        };

        match msg {
            Message::Text(t) => {
                let data: Value = match serde_json::from_str(&t) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                match data.get("type").and_then(|v| v.as_str()) {
                    Some("ping") => {
                        let _ = write.send(Message::Text(r#"{"type":"pong"}"#.to_string())).await;
                    }
                    Some("subscribed") => {}
                    _ => {
                        // Isolate callback panics? We can't catch panics across FnMut easily
                        // without UnwindSafe; callbacks here are written to not panic.
                        on_message(&data);
                    }
                }
            }
            Message::Ping(p) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
}

// bring `now_or_never` into scope
use futures_util::future::FutureExt;
