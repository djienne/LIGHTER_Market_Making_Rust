//! Binance @bookTicker client → SharedBbo. Port of `binance_obi.py::BinanceBookTickerClient`.
//! Lightweight best-bid/ask feed (reference / sanity). Cold-path async task.

use crate::shared::SharedBbo;
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
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
    v.get(k).and_then(|x| x.as_str()).and_then(|s| fast_float::parse::<f64, _>(s).ok())
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
            match self.session(&url).await {
                Ok(()) => {}
                Err(e) => tracing::warn!("binance bookTicker session ended: {e}"),
            }
            sleep(Duration::from_secs_f64(backoff)).await;
            backoff = (backoff * 2.0).min(self.reconnect_max);
        }
    }

    async fn session(&self, url: &str) -> Result<()> {
        let (ws_stream, _) = connect_async(url).await.context("binance bookTicker connect")?;
        let (mut write, mut read) = ws_stream.split();
        tracing::info!("binance bookTicker connected: {url}");
        loop {
            match timeout(Duration::from_secs(30), read.next()).await {
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
