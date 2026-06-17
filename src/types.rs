//! Core shared types used across the hot and cold paths.

use std::fmt;

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[inline]
    pub fn is_ask(self) -> bool {
        matches!(self, Side::Sell)
    }
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What to do with an order slot this cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderAction {
    Create,
    Modify,
    Cancel,
}

/// Per-(side, level) order operation produced by `collect_order_operations`.
#[derive(Debug, Clone)]
pub struct BatchOp {
    pub side: Side,
    pub level: usize,
    pub action: OrderAction,
    pub price: f64,
    pub size: f64,
    /// Client order index (bot-assigned). For modify/cancel this is the existing one.
    pub client_order_id: i64,
    /// Exchange order index (for modify/cancel).
    pub exchange_id: Option<i64>,
    pub reduce_only: bool,
}

/// Transport outcome classification (mirrors Python `TxSendStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxSendStatus {
    /// code == 0 (or HTTP 200 normalized): accepted.
    Ok,
    /// code != 0: a definite rejection.
    Rejected,
    /// No frame was written (safe to retry via another transport).
    NotSent,
    /// A frame may have been written but the response was lost. DO NOT retry —
    /// pause + hard_refresh_nonce + reconcile (codex review).
    Unknown,
}

#[derive(Debug, Clone)]
pub struct TxSendResult {
    pub status: TxSendStatus,
    pub code: i64,
    pub message: String,
    pub quota_remaining: Option<i64>,
}

impl TxSendResult {
    pub fn not_sent(reason: impl Into<String>) -> Self {
        Self {
            status: TxSendStatus::NotSent,
            code: -1,
            message: reason.into(),
            quota_remaining: None,
        }
    }
    pub fn unknown(reason: impl Into<String>) -> Self {
        Self {
            status: TxSendStatus::Unknown,
            code: -1,
            message: reason.into(),
            quota_remaining: None,
        }
    }
}

/// Lifecycle of one (side, level) order slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideStatus {
    Idle,
    Placing,
    Live,
    Modifying,
    Canceling,
}

/// Hot-path order events (LOSSLESS queue — must never be dropped silently).
#[derive(Debug, Clone)]
pub enum OrderEvent {
    /// Order confirmed live on exchange: bind client->exchange id, mark Live.
    BindLive {
        side: Side,
        level: usize,
        client_order_id: i64,
        exchange_id: Option<i64>,
        price: f64,
        size: f64,
    },
    /// Order left the book (filled/cancelled/expired): clear the slot.
    ClearLive {
        side: Side,
        level: usize,
        client_order_id: i64,
    },
    /// Clear all slots (e.g. cancel-all).
    ClearAll,
}

/// Static per-market config resolved from REST at startup.
#[derive(Debug, Clone)]
pub struct MarketConfig {
    pub market_id: u32,
    pub symbol: String,
    pub price_tick: f64,
    pub amount_tick: f64,
    pub min_base_amount: f64,
    pub min_quote_amount: f64,
    /// price/amount decimal places (for raw int sizing if needed).
    pub price_decimals: u32,
    pub size_decimals: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_helpers() {
        assert!(Side::Sell.is_ask());
        assert!(!Side::Buy.is_ask());
        assert_eq!(Side::Buy.as_str(), "buy");
    }
}
