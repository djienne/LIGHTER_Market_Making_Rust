//! Binance USDⓈ-M futures diff-depth client (@depth@100ms) with REST snapshot sync.
//! Maintains a local book and publishes OBI alpha to SharedAlpha. Cold-path async task
//! with reconnect/backoff.
//!
//! Sync follows the OFFICIAL futures algorithm ("How to manage a local order book
//! correctly", USDⓈ-M docs) — note these are the FUTURES rules, which differ from spot:
//!   * While unsynced, drop events with `u < lastUpdateId` (spot drops `u <= lastUpdateId`).
//!   * The FIRST applied event must bracket the snapshot: `U <= lastUpdateId AND
//!     u >= lastUpdateId`. An event entirely past the snapshot (`U > lastUpdateId`) means
//!     the bracket was missed -> re-fetch the snapshot (no reconnect required).
//!   * Once synced, every event must satisfy `pu == previous u`; on mismatch re-fetch the
//!     snapshot in-session.
//!   * An empty (or all-stale) buffer is NOT an error — proceed to the live stream unsynced
//!     and validate the first live event. (The old code errored here, which flapped the
//!     connection forever on quiet symbols and permanently starved the OBI alpha.)

use crate::binance::obi::BinanceObi;
use crate::shared::SharedAlpha;
use crate::util::{next_reconnect_backoff, reconnect_delay_after_session};
use anyhow::{anyhow, Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const WS_BASE: &str = "wss://fstream.binance.com/ws";
const REST_BASE: &str = "https://fapi.binance.com";

/// Consecutive in-session re-snapshot attempts before giving up and reconnecting the socket.
const MAX_RESYNC_ATTEMPTS: u32 = 5;
/// Small pause between in-session re-snapshots so a persistent desync cannot hammer REST.
const RESYNC_PAUSE: Duration = Duration::from_millis(250);

type WsWrite = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsRead = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// What to do with one diff event, per the official USDⓈ-M futures sync rules.
/// Pure so the whole sync state machine is unit-testable without a socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventAction {
    /// Pre-snapshot event while unsynced — discard.
    Drop,
    /// First event bracketing the snapshot (`U <= lastUpdateId <= u`) — apply, now synced.
    ApplyFirst,
    /// Synced and continuous (`pu == prev_u`) — apply.
    Apply,
    /// Unsynced event past the snapshot, or synced continuity break — re-fetch snapshot.
    Resync,
}

fn classify_event(
    synced: bool,
    last_update_id: i64,
    prev_u: i64,
    big_u: i64,
    u: i64,
    pu: i64,
) -> EventAction {
    if !synced {
        if u < last_update_id {
            return EventAction::Drop;
        }
        if big_u <= last_update_id {
            return EventAction::ApplyFirst;
        }
        // u >= lastUpdateId but U > lastUpdateId: the bracketing event was missed.
        EventAction::Resync
    } else if pu == prev_u {
        EventAction::Apply
    } else {
        EventAction::Resync
    }
}

pub struct BinanceDepthClient {
    symbol: String, // lowercase, e.g. "btcusdt"
    snapshot_limit: usize,
    obi: BinanceObi,
    reconnect_base: f64,
    reconnect_max: f64,
}

fn parse_levels(v: Option<&Value>) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    if let Some(Value::Array(arr)) = v {
        out.reserve(arr.len());
        for lvl in arr {
            if let Some(pair) = lvl.as_array() {
                if pair.len() >= 2 {
                    let p = pair[0]
                        .as_str()
                        .and_then(|s| fast_float::parse::<f64, _>(s).ok());
                    let q = pair[1]
                        .as_str()
                        .and_then(|s| fast_float::parse::<f64, _>(s).ok());
                    if let (Some(p), Some(q)) = (p, q) {
                        out.push((p, q));
                    }
                }
            }
        }
    }
    out
}

impl BinanceDepthClient {
    pub fn new(
        symbol_usdt: &str,
        snapshot_limit: usize,
        window: usize,
        looking_depth: f64,
        shared: Arc<SharedAlpha>,
    ) -> Self {
        Self {
            symbol: symbol_usdt.to_lowercase(),
            snapshot_limit,
            obi: BinanceObi::new(window, looking_depth, shared),
            reconnect_base: 5.0,
            reconnect_max: 60.0,
        }
    }

    fn ws_url(&self) -> String {
        format!("{}/{}@depth@100ms", WS_BASE, self.symbol)
    }

    async fn fetch_snapshot(&self, http: &reqwest::Client) -> Result<Value> {
        let url = format!("{}/fapi/v1/depth", REST_BASE);
        let v: Value = http
            .get(url)
            .query(&[
                ("symbol", self.symbol.to_uppercase()),
                ("limit", self.snapshot_limit.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse binance depth snapshot")?;
        Ok(v)
    }

    /// Run forever, reconnecting on error. Spawn via tokio::spawn.
    pub async fn run(mut self) {
        let url = self.ws_url();
        let http = reqwest::Client::new();
        let mut backoff = self.reconnect_base;
        loop {
            self.obi.reset();
            let started = Instant::now();
            match self.session(&url, &http).await {
                Ok(()) => {}
                Err(e) => tracing::warn!("binance depth session ended: {e}"),
            }
            let elapsed = started.elapsed();
            let delay = reconnect_delay_after_session(backoff, self.reconnect_base, elapsed);
            tracing::info!(
                "binance depth reconnecting in {:.3}s after session {:.3}s (next_backoff_base={:.3}s)",
                delay,
                elapsed.as_secs_f64(),
                next_reconnect_backoff(backoff, self.reconnect_base, self.reconnect_max, elapsed),
            );
            sleep(Duration::from_secs_f64(delay)).await;
            backoff =
                next_reconnect_backoff(backoff, self.reconnect_base, self.reconnect_max, elapsed);
        }
    }

    /// Fetch a REST snapshot while continuing to read (and buffer) diff events from the open
    /// socket, so we never fall behind mid-resync. Replies to pings while buffering.
    async fn fetch_snapshot_buffering(
        &self,
        http: &reqwest::Client,
        read: &mut WsRead,
        write: &mut WsWrite,
    ) -> Result<(Value, Vec<Value>)> {
        let mut buffer: Vec<Value> = Vec::new();
        let snap_fut = self.fetch_snapshot(http);
        tokio::pin!(snap_fut);
        let snapshot = loop {
            tokio::select! {
                s = &mut snap_fut => break s.context("snapshot fetch")?,
                msg = read.next() => match msg {
                    Some(Ok(Message::Text(t))) => {
                        if let Ok(v) = serde_json::from_str::<Value>(&t) {
                            if v.get("U").is_some() && v.get("u").is_some() { buffer.push(v); }
                        }
                    }
                    Some(Ok(Message::Ping(p))) => { let _ = write.send(Message::Pong(p)).await; }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(anyhow!("ws closed during snapshot")),
                }
            }
        };
        Ok((snapshot, buffer))
    }

    /// Apply the snapshot, then run the buffered events through the sync state machine.
    /// Returns whether the book is synced (an all-stale/empty buffer is fine — the live
    /// loop validates the first live event) or `None` to request a re-snapshot.
    fn apply_snapshot_and_drain(&mut self, snapshot: &Value, buffer: &[Value]) -> Option<bool> {
        let last_update_id = snapshot
            .get("lastUpdateId")
            .and_then(|x| x.as_i64())
            .unwrap_or(0);
        let bids = parse_levels(snapshot.get("bids"));
        let asks = parse_levels(snapshot.get("asks"));
        self.obi
            .apply_snapshot(bids, asks, last_update_id);
        tracing::info!("binance depth snapshot applied: lastUpdateId={last_update_id}");

        let mut synced = false;
        for event in buffer {
            match self.step(synced, event) {
                EventAction::Drop => {}
                EventAction::ApplyFirst => {
                    self.apply_event(event);
                    synced = true;
                }
                EventAction::Apply => self.apply_event(event),
                EventAction::Resync => return None,
            }
        }
        Some(synced)
    }

    fn step(&self, synced: bool, event: &Value) -> EventAction {
        let u = event.get("u").and_then(|x| x.as_i64()).unwrap_or(0);
        let big_u = event.get("U").and_then(|x| x.as_i64()).unwrap_or(0);
        let pu = event.get("pu").and_then(|x| x.as_i64()).unwrap_or(0);
        classify_event(
            synced,
            self.obi.last_update_id(),
            self.obi.prev_u(),
            big_u,
            u,
            pu,
        )
    }

    async fn session(&mut self, url: &str, http: &reqwest::Client) -> Result<()> {
        let (ws_stream, _) = connect_async(url).await.context("binance ws connect")?;
        let (mut write, mut read) = ws_stream.split();
        tracing::info!("binance depth connected: {url}");

        let mut resync_attempts: u32 = 0;
        'resync: loop {
            // (Re)initialize: clear book + published alpha, snapshot while buffering the stream.
            self.obi.reset();
            let (snapshot, buffer) = self
                .fetch_snapshot_buffering(http, &mut read, &mut write)
                .await?;
            let mut synced = match self.apply_snapshot_and_drain(&snapshot, &buffer) {
                Some(s) => s,
                None => {
                    resync_attempts += 1;
                    if resync_attempts >= MAX_RESYNC_ATTEMPTS {
                        return Err(anyhow!("depth resync failed {resync_attempts}x; reconnecting"));
                    }
                    tracing::warn!("binance depth buffer past snapshot; re-snapshotting");
                    sleep(RESYNC_PAUSE).await;
                    continue 'resync;
                }
            };
            if synced {
                resync_attempts = 0;
                tracing::info!("binance depth synced from buffer, entering live stream");
            } else {
                tracing::info!("binance depth awaiting first bracketing live event");
            }

            // Live loop.
            loop {
                let raw = match timeout(Duration::from_secs(30), read.next()).await {
                    Ok(Some(Ok(Message::Text(t)))) => t,
                    Ok(Some(Ok(Message::Ping(p)))) => {
                        let _ = write.send(Message::Pong(p)).await;
                        continue;
                    }
                    Ok(Some(Ok(_))) => continue,
                    Ok(Some(Err(e))) => return Err(e.into()),
                    Ok(None) => return Err(anyhow!("ws closed")),
                    Err(_) => return Err(anyhow!("no data for 30s")),
                };
                let event: Value = match serde_json::from_str(&raw) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if event.get("U").is_none() || event.get("u").is_none() {
                    continue;
                }
                match self.step(synced, &event) {
                    EventAction::Drop => continue,
                    EventAction::ApplyFirst => {
                        self.apply_event(&event);
                        synced = true;
                        resync_attempts = 0;
                        tracing::info!("binance depth synced on live event");
                        self.obi.update_alpha();
                    }
                    EventAction::Apply => {
                        self.apply_event(&event);
                        self.obi.update_alpha();
                    }
                    EventAction::Resync => {
                        resync_attempts += 1;
                        if resync_attempts >= MAX_RESYNC_ATTEMPTS {
                            return Err(anyhow!(
                                "depth resync failed {resync_attempts}x; reconnecting"
                            ));
                        }
                        tracing::warn!(
                            "binance depth sequence break (synced={synced}); re-snapshotting in-session"
                        );
                        sleep(RESYNC_PAUSE).await;
                        continue 'resync;
                    }
                }
            }
        }
    }

    fn apply_event(&mut self, event: &Value) {
        let bids = parse_levels(event.get("b"));
        let asks = parse_levels(event.get("a"));
        let u = event.get("u").and_then(|x| x.as_i64()).unwrap_or(0);
        self.obi.apply_diff(&bids, &asks, u);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // classify_event: the pure futures-rules state machine.

    #[test]
    fn unsynced_pre_snapshot_events_drop() {
        // u < lastUpdateId -> Drop (futures rule: strict <, u == lastUpdateId is kept).
        assert_eq!(classify_event(false, 100, 0, 90, 99, 0), EventAction::Drop);
        assert_ne!(classify_event(false, 100, 0, 95, 100, 0), EventAction::Drop);
    }

    #[test]
    fn unsynced_bracketing_event_applies_first() {
        // U <= lastUpdateId <= u -> ApplyFirst.
        assert_eq!(
            classify_event(false, 100, 0, 95, 105, 0),
            EventAction::ApplyFirst
        );
        assert_eq!(
            classify_event(false, 100, 0, 100, 100, 0),
            EventAction::ApplyFirst
        );
    }

    #[test]
    fn unsynced_event_past_snapshot_resyncs() {
        // U > lastUpdateId: missed the bracket -> Resync (never silently applied).
        assert_eq!(
            classify_event(false, 100, 0, 101, 110, 0),
            EventAction::Resync
        );
    }

    #[test]
    fn synced_continuity() {
        // pu == prev_u -> Apply; mismatch -> Resync.
        assert_eq!(classify_event(true, 100, 105, 106, 110, 105), EventAction::Apply);
        assert_eq!(
            classify_event(true, 100, 105, 108, 112, 107),
            EventAction::Resync
        );
    }

    fn ev(big_u: i64, u: i64, pu: i64) -> Value {
        serde_json::json!({"U": big_u, "u": u, "pu": pu, "b": [], "a": []})
    }

    fn snap(last_update_id: i64) -> Value {
        serde_json::json!({
            "lastUpdateId": last_update_id,
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "1.0"]],
        })
    }

    fn client() -> BinanceDepthClient {
        BinanceDepthClient::new("btcusdt", 1000, 100, 0.025, Arc::new(SharedAlpha::new(3)))
    }

    #[test]
    fn drain_empty_buffer_is_not_an_error() {
        // The old code returned Err("no valid event in buffer") whenever a non-empty buffer
        // contained only stale events — this flapped forever on quiet symbols. Now: unsynced.
        let mut c = client();
        assert_eq!(c.apply_snapshot_and_drain(&snap(100), &[]), Some(false));
        // All-stale buffer: also just "not synced yet".
        let stale = vec![ev(80, 90, 79), ev(90, 99, 90)];
        assert_eq!(c.apply_snapshot_and_drain(&snap(100), &stale), Some(false));
    }

    #[test]
    fn drain_bracketing_buffer_syncs_and_tracks_prev_u() {
        let mut c = client();
        let buf = vec![ev(80, 95, 70), ev(95, 105, 94), ev(106, 110, 105)];
        assert_eq!(c.apply_snapshot_and_drain(&snap(100), &buf), Some(true));
        assert_eq!(c.obi.prev_u(), 110);
        // Next live event must be continuous.
        assert_eq!(c.step(true, &ev(111, 115, 110)), EventAction::Apply);
        assert_eq!(c.step(true, &ev(120, 125, 119)), EventAction::Resync);
    }

    #[test]
    fn drain_gapped_buffer_requests_resync() {
        let mut c = client();
        // First kept event starts past the snapshot -> bracket missed -> None (re-snapshot).
        let buf = vec![ev(101, 110, 100)];
        assert_eq!(c.apply_snapshot_and_drain(&snap(100), &buf), None);
    }

    #[test]
    fn live_first_event_validated_when_buffer_was_empty() {
        let mut c = client();
        assert_eq!(c.apply_snapshot_and_drain(&snap(100), &[]), Some(false));
        // Stale live event: dropped, still unsynced.
        assert_eq!(c.step(false, &ev(90, 99, 89)), EventAction::Drop);
        // Bracketing live event: syncs.
        assert_eq!(c.step(false, &ev(98, 104, 97)), EventAction::ApplyFirst);
        // Past-snapshot live event without bracket: resync.
        assert_eq!(c.step(false, &ev(101, 108, 100)), EventAction::Resync);
    }
}
