//! Lighter WebSocket subscription primitives — port of `ws_manager.py`.
//!
//! `subscribe_loop` connects, subscribes to channels (optionally with per-channel auth),
//! handles ping/pong + the `subscribed` confirmation, applies a recv-timeout watchdog, and
//! reconnects with exponential backoff. Each application message is handed to a synchronous
//! callback as `(msg_type, raw_json_text)` — the ws layer only probes the `type` field (for
//! ping/subscribed routing), so the callback performs the ONE full deserialize per message
//! (the previous Value-parse + deep-clone + from_value did three passes on the hot path).

use crate::util::{next_reconnect_backoff, reconnect_delay_after_session};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// Minimal probe: extracts only `type` (borrowed where possible); serde skips all other
/// fields without materializing them.
#[derive(serde::Deserialize)]
struct TypeProbe<'a> {
    #[serde(rename = "type", borrow, default)]
    mtype: Option<Cow<'a, str>>,
}

pub const WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";

#[derive(Clone)]
pub struct SubscribeOptions {
    pub url: String,
    pub channels: Vec<String>,
    pub channel_auths: HashMap<String, String>,
    pub recv_timeout: f64,
    pub reconnect_base: f64,
    pub reconnect_max: f64,
    /// Proactive client-ping interval (config `websocket.ping_interval`). Lighter closes any
    /// connection that sends NO frame for 2 minutes, so this is clamped well under that.
    pub ping_interval: f64,
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
            ping_interval: 20.0,
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

/// Run the subscription loop forever (reconnecting). `on_message(msg_type, raw_text)` is
/// called for each application message (NOT ping/subscribed). `reconnect` (if provided)
/// forces a fresh reconnect when notified (e.g. orderbook nonce gap / sanity divergence).
/// `on_disconnect` runs on every disconnect (clear local book, reset vol state, etc.).
pub async fn subscribe_loop<F, D>(
    opts: SubscribeOptions,
    reconnect: Option<std::sync::Arc<Notify>>,
    mut on_message: F,
    mut on_disconnect: D,
) where
    F: FnMut(&str, &str),
    D: FnMut(),
{
    let mut backoff = opts.reconnect_base;
    loop {
        let started = Instant::now();
        match session(&opts, reconnect.as_deref(), &mut on_message).await {
            Ok(()) => {}
            Err(e) => tracing::info!("{} ws disconnected: {e}", opts.label),
        }
        on_disconnect();
        let elapsed = started.elapsed();
        let delay = reconnect_delay_after_session(backoff, opts.reconnect_base, elapsed);
        let sleep_for = delay + jitter(delay);
        tracing::info!(
            "{} reconnecting in {:.3}s after session {:.3}s (next_backoff_base={:.3}s)",
            opts.label,
            sleep_for,
            elapsed.as_secs_f64(),
            next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed),
        );
        sleep(Duration::from_secs_f64(sleep_for)).await;
        backoff = next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed);
    }
}

/// Like `subscribe_loop` but regenerates per-channel auth tokens before EACH connection
/// (private channels: account_orders / account_all / user_stats). The server token TTL is
/// ~10 min; on expiry the server drops the socket, the session ends, and we reconnect with a
/// fresh token. `auth_fn` returns the channel->token map for the upcoming connection.
pub async fn subscribe_loop_authed<F, A>(
    mut opts: SubscribeOptions,
    mut auth_fn: A,
    mut on_message: F,
) where
    F: FnMut(&str, &str),
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
        let started = Instant::now();
        match session(&opts, None, &mut on_message).await {
            Ok(()) => {}
            Err(e) => tracing::info!("{} ws disconnected: {e}", opts.label),
        }
        let elapsed = started.elapsed();
        let delay = reconnect_delay_after_session(backoff, opts.reconnect_base, elapsed);
        let sleep_for = delay + jitter(delay);
        tracing::info!(
            "{} reconnecting in {:.3}s after session {:.3}s (next_backoff_base={:.3}s)",
            opts.label,
            sleep_for,
            elapsed.as_secs_f64(),
            next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed),
        );
        sleep(Duration::from_secs_f64(sleep_for)).await;
        backoff = next_reconnect_backoff(backoff, opts.reconnect_base, opts.reconnect_max, elapsed);
    }
}

async fn session<F>(
    opts: &SubscribeOptions,
    reconnect: Option<&Notify>,
    on_message: &mut F,
) -> Result<()>
where
    F: FnMut(&str, &str),
{
    let (ws_stream, _) = connect_async(&opts.url).await?;
    let (mut write, mut read) = ws_stream.split();
    tracing::info!("connected to {} for {}", opts.url, opts.label);

    for ch in &opts.channels {
        let mut sub = serde_json::json!({"type": "subscribe", "channel": ch});
        if let Some(auth) = opts.channel_auths.get(ch) {
            sub["auth"] = serde_json::Value::String(auth.clone());
        }
        write.send(Message::Text(sub.to_string())).await?;
    }
    tracing::info!("{} subscribed to {:?}", opts.label, opts.channels);

    let recv_to = Duration::from_secs_f64(opts.recv_timeout);
    // Clamp: never slower than Lighter's 2-min idle close, never a busy-loop.
    let mut ping_tick = tokio::time::interval(Duration::from_secs_f64(
        opts.ping_interval.clamp(5.0, 110.0),
    ));
    ping_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_tick.tick().await; // consume the immediate first tick (just connected)
    let mut last_data = Instant::now();
    loop {
        // Non-blocking forced-reconnect check.
        if let Some(rc) = reconnect {
            if rc.notified().now_or_never().is_some() {
                tracing::info!(
                    "{} reconnect requested; dropping for fresh snapshot",
                    opts.label
                );
                return Ok(());
            }
        }

        // Race the read against the keepalive tick. On the tick we send a proactive client Ping
        // (so a quiet stream still satisfies Lighter's 2-min "send a frame" rule) and enforce the
        // dead-feed watchdog via `last_data` (the read itself has no timeout, so the ping cannot
        // mask a stalled feed).
        let msg = tokio::select! {
            _ = ping_tick.tick() => {
                // Dead-feed watchdog: trip on stale APPLICATION data (last_data is refreshed ONLY
                // by real messages below — NOT by pings/pongs/subscribed — so the keepalive pings
                // and their pong replies cannot mask a stalled feed). Check before pinging so we
                // reconnect promptly rather than pinging a dead stream.
                if last_data.elapsed() > recv_to {
                    tracing::warn!("{} watchdog: no data for {}s", opts.label, opts.recv_timeout);
                    return Ok(());
                }
                if write.send(Message::Ping(Vec::new())).await.is_err() {
                    return Ok(()); // socket dead -> reconnect
                }
                continue;
            }
            res = read.next() => match res {
                Some(Ok(m)) => m,
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            },
        };

        match msg {
            Message::Text(t) => {
                // Probe ONLY the type field; the callback does the single full deserialize.
                let probe: TypeProbe = match serde_json::from_str(&t) {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                match probe.mtype.as_deref() {
                    Some("ping") => {
                        // Server app-level keepalive — reply, but do NOT count as feed data.
                        let _ = write
                            .send(Message::Text(r#"{"type":"pong"}"#.to_string()))
                            .await;
                    }
                    Some("subscribed") => {}
                    mtype => {
                        // Real application message — this is the only thing that refreshes the
                        // dead-feed watchdog. Callbacks here are written to not panic.
                        last_data = Instant::now();
                        on_message(mtype.unwrap_or(""), &t);
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
