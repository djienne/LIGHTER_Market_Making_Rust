//! Binance USDⓈ-M futures diff-depth client (@depth@100ms) with REST snapshot sync.
//! Port of `binance_obi.py::BinanceDiffDepthClient.run`. Maintains a local book and
//! publishes OBI alpha to SharedAlpha. Cold-path async task with reconnect/backoff.

use crate::binance::obi::BinanceObi;
use crate::shared::SharedAlpha;
use crate::util::{next_reconnect_backoff, reconnect_delay_after_session};
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const WS_BASE: &str = "wss://fstream.binance.com/ws";
const REST_BASE: &str = "https://fapi.binance.com";

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

    async fn session(&mut self, url: &str, http: &reqwest::Client) -> Result<()> {
        let (ws_stream, _) = connect_async(url).await.context("binance ws connect")?;
        let (mut write, mut read) = ws_stream.split();
        tracing::info!("binance depth connected: {url}");

        // Phase 1: buffer diff events while the REST snapshot is in flight.
        let mut buffer: Vec<Value> = Vec::new();
        let snapshot = {
            let snap_fut = self.fetch_snapshot(http);
            tokio::pin!(snap_fut);
            loop {
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
            }
        };

        // Brief grace to catch a few more events that arrived just after snapshot.
        let _ = timeout(Duration::from_millis(100), async {
            while let Some(Ok(Message::Text(t))) = read.next().await {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    if v.get("U").is_some() && v.get("u").is_some() {
                        buffer.push(v);
                    }
                }
            }
        })
        .await;

        // Phase 2: apply snapshot.
        let last_update_id = snapshot
            .get("lastUpdateId")
            .and_then(|x| x.as_i64())
            .unwrap_or(0);
        let bids = parse_levels(snapshot.get("bids"));
        let asks = parse_levels(snapshot.get("asks"));
        self.obi.apply_snapshot(bids, asks, last_update_id);
        tracing::info!("binance depth snapshot applied: lastUpdateId={last_update_id}");

        // Phase 3: drain buffer with sequence alignment.
        let mut first_valid = false;
        let had_buffer = !buffer.is_empty();
        for event in &buffer {
            let u = event.get("u").and_then(|x| x.as_i64()).unwrap_or(0);
            let big_u = event.get("U").and_then(|x| x.as_i64()).unwrap_or(0);
            if u <= self.obi.last_update_id() {
                continue;
            }
            if !first_valid {
                if big_u <= self.obi.last_update_id() + 1 && u >= self.obi.last_update_id() + 1 {
                    first_valid = true;
                } else {
                    continue;
                }
            }
            self.apply_event(event);
        }
        if !first_valid && had_buffer {
            return Err(anyhow!("no valid event in buffer; re-snapshotting"));
        }
        tracing::info!("binance depth synced, entering live stream");

        // Phase 4: live loop.
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
            let pu = event.get("pu").and_then(|x| x.as_i64()).unwrap_or(0);
            if self.obi.prev_u() != 0 && pu != self.obi.prev_u() {
                return Err(anyhow!(
                    "sequence gap: pu={pu} expected={}",
                    self.obi.prev_u()
                ));
            }
            self.apply_event(&event);
            self.obi.update_alpha();
        }
    }

    fn apply_event(&mut self, event: &Value) {
        let bids = parse_levels(event.get("b"));
        let asks = parse_levels(event.get("a"));
        let u = event.get("u").and_then(|x| x.as_i64()).unwrap_or(0);
        self.obi.apply_diff(&bids, &asks, u);
    }
}
