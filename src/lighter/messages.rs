//! Serde models for Lighter REST responses and WebSocket payloads.
//!
//! Field names verified against the live API (`/api/v1/orderBooks`) and live WS captures.
//! Prices/sizes arrive as STRINGS — parse with `fast-float`. Account-channel models are
//! modeled from the Python handlers and refined against live (authenticated) captures.

use serde::Deserialize;
use std::collections::HashMap;

#[inline]
pub fn parse_f64(s: &str) -> f64 {
    fast_float::parse(s).unwrap_or(0.0)
}

// ----------------------------- REST -----------------------------

#[derive(Debug, Deserialize)]
pub struct OrderBooksResponse {
    #[serde(default)]
    pub order_books: Vec<OrderBookDetail>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderBookDetail {
    pub symbol: String,
    pub market_id: u32,
    #[serde(default)]
    pub min_base_amount: String,
    #[serde(default)]
    pub min_quote_amount: String,
    #[serde(default)]
    pub supported_size_decimals: u32,
    #[serde(default)]
    pub supported_price_decimals: u32,
    #[serde(default)]
    pub maker_fee: String,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct NextNonceResponse {
    pub nonce: i64,
}

/// Response shape shared by REST sendTx[Batch] and the tx WebSocket.
#[derive(Debug, Deserialize, Default)]
pub struct TxResponse {
    #[serde(default)]
    pub code: i64,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub volume_quota_remaining: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AccountActiveOrdersResponse {
    #[serde(default)]
    pub orders: Vec<RemoteOrder>,
}

/// A live order as reported by the exchange (REST or account_orders WS).
#[derive(Debug, Deserialize, Clone, Default)]
pub struct RemoteOrder {
    #[serde(default)]
    pub client_order_index: Option<i64>,
    #[serde(default)]
    pub order_index: Option<i64>,
    #[serde(default)]
    pub is_ask: Option<bool>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub remaining_base_amount: Option<String>,
    #[serde(default)]
    pub filled_base_amount: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

impl RemoteOrder {
    /// Live = order still resting. `accountActiveOrders` rows may omit `status` entirely
    /// (every row there is by definition active) — treat a missing status as LIVE so the
    /// reconcile poller never mass-clears tracked slots (codex review). Only an explicit
    /// terminal status (filled/cancelled/expired) marks an order dead.
    pub fn is_live(&self) -> bool {
        match self.status.as_deref() {
            None => true,
            Some(s) => matches!(s, "open" | "partial_filled" | "pending" | "in-progress"),
        }
    }
}

// ----------------------------- WebSocket -----------------------------

/// A single order book level (strings on the wire).
#[derive(Debug, Deserialize, Clone)]
pub struct PriceLevel {
    pub price: String,
    pub size: String,
}

impl PriceLevel {
    #[inline]
    pub fn parsed(&self) -> (f64, f64) {
        (parse_f64(&self.price), parse_f64(&self.size))
    }
}

#[derive(Debug, Deserialize)]
pub struct OrderBookPayload {
    #[serde(default)]
    pub bids: Vec<PriceLevel>,
    #[serde(default)]
    pub asks: Vec<PriceLevel>,
    #[serde(default)]
    pub offset: Option<u64>,
}

/// `order_book/{m}` envelope. `type` is `subscribed/order_book` (snapshot) or
/// `update/order_book` (delta).
#[derive(Debug, Deserialize)]
pub struct OrderBookMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub offset: Option<u64>,
    pub order_book: OrderBookPayload,
}

impl OrderBookMsg {
    pub fn is_snapshot(&self) -> bool {
        self.msg_type.contains("subscribed")
    }
    /// Prefer envelope offset, fall back to payload offset.
    pub fn effective_offset(&self) -> Option<u64> {
        self.offset.or(self.order_book.offset)
    }
}

/// `ticker/{m}` — best bid/ask nested under `ticker`.
#[derive(Debug, Deserialize)]
pub struct TickerMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub ticker: HashMap<String, serde_json::Value>,
}

impl TickerMsg {
    fn field(&self, k: &str) -> Option<f64> {
        self.ticker.get(k).and_then(|v| match v {
            serde_json::Value::String(s) => fast_float::parse(s).ok(),
            serde_json::Value::Number(n) => n.as_f64(),
            _ => None,
        })
    }
    pub fn best_bid(&self) -> Option<f64> {
        self.field("best_bid").or_else(|| self.field("bid"))
    }
    pub fn best_ask(&self) -> Option<f64> {
        self.field("best_ask").or_else(|| self.field("ask"))
    }
}

/// `account_orders/{m}/{a}` — `orders` keyed by market id.
#[derive(Debug, Deserialize)]
pub struct AccountOrdersMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub orders: HashMap<String, Vec<RemoteOrder>>,
}

/// `account_all/{a}` — positions + trades keyed by market id.
#[derive(Debug, Deserialize)]
pub struct AccountAllMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub positions: HashMap<String, PositionPayload>,
    #[serde(default)]
    pub trades: HashMap<String, Vec<TradePayload>>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct PositionPayload {
    #[serde(default)]
    pub position: Option<String>,
    #[serde(default)]
    pub sign: Option<i32>,
    #[serde(default)]
    pub avg_entry_price: Option<String>,
}

impl PositionPayload {
    /// Signed position size in base units.
    pub fn signed(&self) -> f64 {
        let mag = self.position.as_deref().map(parse_f64).unwrap_or(0.0);
        match self.sign {
            Some(s) if s < 0 => -mag.abs(),
            _ => mag.abs(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct TradePayload {
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub is_maker_ask: Option<bool>,
    #[serde(default)]
    pub ask_account_id: Option<i64>,
    #[serde(default)]
    pub bid_account_id: Option<i64>,
    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub trade_id: Option<i64>,
}

/// `user_stats/{a}` — capital/portfolio.
#[derive(Debug, Deserialize)]
pub struct UserStatsMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub stats: StatsPayload,
}

#[derive(Debug, Deserialize, Default)]
pub struct StatsPayload {
    #[serde(default)]
    pub available_balance: Option<serde_json::Value>,
    #[serde(default)]
    pub portfolio_value: Option<serde_json::Value>,
}

fn val_f64(v: &Option<serde_json::Value>) -> Option<f64> {
    match v {
        Some(serde_json::Value::String(s)) => fast_float::parse(s).ok(),
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

impl StatsPayload {
    pub fn available_capital(&self) -> Option<f64> {
        val_f64(&self.available_balance)
    }
    pub fn portfolio_value(&self) -> Option<f64> {
        val_f64(&self.portfolio_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_orderbook_snapshot() {
        let raw = r#"{"type":"subscribed/order_book","offset":405053,
            "order_book":{"bids":[{"price":"64820.2","size":"0.00051"}],
            "asks":[{"price":"64820.3","size":"0.19283"}],"offset":405053}}"#;
        let m: OrderBookMsg = serde_json::from_str(raw).unwrap();
        assert!(m.is_snapshot());
        assert_eq!(m.effective_offset(), Some(405053));
        assert_eq!(m.order_book.bids[0].parsed(), (64820.2, 0.00051));
    }

    #[test]
    fn parse_orderbooks_rest() {
        let raw = r#"{"code":200,"order_books":[{"symbol":"BTC","market_id":1,
            "min_base_amount":"0.00020","min_quote_amount":"10.000000",
            "supported_size_decimals":5,"supported_price_decimals":1,"maker_fee":"0.0000","status":"active"}]}"#;
        let r: OrderBooksResponse = serde_json::from_str(raw).unwrap();
        let btc = &r.order_books[0];
        assert_eq!(btc.market_id, 1);
        assert_eq!(btc.supported_price_decimals, 1);
        assert_eq!(parse_f64(&btc.min_base_amount), 0.0002);
    }

    #[test]
    fn position_sign() {
        let p = PositionPayload {
            position: Some("0.0050".into()),
            sign: Some(-1),
            avg_entry_price: None,
        };
        assert!((p.signed() + 0.005).abs() < 1e-12);
    }
}
