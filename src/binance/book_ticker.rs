//! Binance @bookTicker client → SharedBbo. Port of `binance_obi.py::BinanceBookTickerClient`.
//! Lightweight best-bid/ask feed (reference / sanity). Cold-path async task.

use crate::shared::SharedBbo;
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

pub struct BinanceBookTickerClient {
    symbol: String,
    shared: Arc<SharedBbo>,
    reconnect_base: f64,
    reconnect_max: f64,
}

#[inline]
fn f(v: &Value, k: &str) -> Option<f64> {
    v.get(k)
        .and_then(|x| x.as_str())
        .and_then(|s| fast_float::parse::<f64, _>(s).ok())
}

impl BinanceBookTickerClient {
    pub fn new(symbol_usdt: &str, shared: Arc<SharedBbo>) -> Self {
        Self {
            symbol: symbol_usdt.to_lowercase(),
            shared,
            reconnect_base: 5.0,
            reconnect_max: 60.0,
        }
    }

    pub async fn run(self) {
        let url = format!("{}/{}@bookTicker", WS_BASE, self.symbol);
        let mut backoff = self.reconnect_base;
        loop {
            let started = Instant::now();
            match self.session(&url).await {
                Ok(()) => {}
                Err(e) => tracing::warn!("binance bookTicker session ended: {e}"),
            }
            // Drop the stale BBO on disconnect so consumers see it as cold/stale until re-warm.
            self.shared.reset();
            let elapsed = started.elapsed();
            let delay = reconnect_delay_after_session(backoff, self.reconnect_base, elapsed);
            tracing::info!(
                "binance bookTicker reconnecting in {:.3}s after session {:.3}s (next_backoff_base={:.3}s)",
                delay,
                elapsed.as_secs_f64(),
                next_reconnect_backoff(backoff, self.reconnect_base, self.reconnect_max, elapsed),
            );
            sleep(Duration::from_secs_f64(delay)).await;
            backoff =
                next_reconnect_backoff(backoff, self.reconnect_base, self.reconnect_max, elapsed);
        }
    }

    async fn session(&self, url: &str) -> Result<()> {
        let (ws_stream, _) = connect_async(url)
            .await
            .context("binance bookTicker connect")?;
        let (mut write, mut read) = ws_stream.split();
        tracing::info!("binance bookTicker connected: {url}");
        loop {
            // @bookTicker only emits on BBO CHANGE: a quiet symbol can be silent well past
            // 30s (the old timeout flapped the connection and reset SharedBbo). Binance
            // server pings arrive every few minutes and also count as liveness; downstream
            // consumers gate on age_ms anyway. 300s only catches genuinely dead sockets.
            match timeout(Duration::from_secs(300), read.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&t) {
                        // {b,B,a,A,u,E,T}
                        if let (Some(bid), Some(ask)) = (f(&v, "b"), f(&v, "a")) {
                            let bid_qty = f(&v, "B").unwrap_or(0.0);
                            let ask_qty = f(&v, "A").unwrap_or(0.0);
                            if bid.is_finite() && ask.is_finite() && ask > bid {
                                self.shared.update(bid, ask, bid_qty, ask_qty);
                            }
                        }
                    }
                }
                Ok(Some(Ok(Message::Ping(p)))) => {
                    let _ = write.send(Message::Pong(p)).await;
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) => return Err(anyhow!("ws closed")),
                Err(_) => return Err(anyhow!("no data for 30s")),
            }
        }
    }
}
