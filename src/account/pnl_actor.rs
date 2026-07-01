//! Cold-path session PnL tracking for live trading.
//!
//! The actor consumes accepted bot client ids from the paced sender plus account_all fills
//! from the private websocket. It dedupes exchange trades, attributes fills to this process,
//! appends audit rows to CSV, and periodically writes a compact session summary.

use crate::account::fill_accounting::FillAccounting;
use crate::account::persistence::{LiveState, LiveStateStore};
use crate::config::PnlCfg;
use crate::lighter::messages::TradePayload;
use crate::metrics::trade_log::{TradeLogger, TradeRow};
use crate::shared::{Derived, SharedBbo};
use crate::types::Side;
use chrono::{SecondsFormat, TimeZone, Utc};
use serde::Serialize;
use std::collections::{HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const EPSILON: f64 = 1e-9;
const PENDING_LIMIT: usize = 2_000;
const MID_FRESH_MS: u64 = 10_000;
/// Cap on the seen-trade / strategy-client id sets (FIFO eviction) — a continuously
/// re-quoting maker previously grew both without bound.
const ID_SET_CAP: usize = 10_000;

#[derive(Debug, Clone)]
pub enum PnlEvent {
    RegisterClientIds(Vec<i64>),
    AccountAll {
        position: Option<PositionSnapshot>,
        trades: Vec<TradePayload>,
    },
    Capital {
        available_capital: Option<f64>,
        portfolio_value: Option<f64>,
    },
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
pub struct PositionSnapshot {
    pub signed_position: f64,
    pub entry_vwap: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PnlSummary {
    pub session_id: String,
    pub symbol: String,
    pub market_id: u32,
    pub account_index: i64,
    pub started_at: String,
    pub updated_at: String,
    pub strategy_realized_pnl_usdc: f64,
    pub strategy_unrealized_pnl_usdc: f64,
    pub strategy_mtm_pnl_usdc: f64,
    /// Realized PnL from account fills that could not be attributed to this strategy
    /// (no client id: liquidations, manual orders). Tracked separately so it is neither
    /// silently dropped nor mixed into strategy PnL.
    pub unattributed_realized_pnl_usdc: f64,
    pub open_position_base: f64,
    pub entry_vwap: f64,
    pub last_mid: Option<f64>,
    /// True when both the Lighter and Binance mid feeds are stale — unrealized/MTM carry
    /// the last computed value instead of silently marking against an old price.
    pub mid_stale: bool,
    pub fill_count: u64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub notional_usdc: f64,
    pub duplicate_fill_count: u64,
    pub unattributed_fill_count: u64,
    pub pending_unattributed_fill_count: usize,
    pub available_capital: Option<f64>,
    pub portfolio_value: Option<f64>,
}

pub struct PnlActor {
    cfg: PnlCfg,
    symbol: String,
    market_id: u32,
    account_index: i64,
    maker_fee_rate: f64,
    session_id: String,
    started_at: String,
    shared_bbo: Arc<SharedBbo>,
    /// Lighter-side mid + freshness (published by the hot task) — preferred MTM source.
    derived: Arc<Derived>,
    trade_logger: TradeLogger,
    state_store: LiveStateStore,
    summary_path: PathBuf,
    snapshots_path: PathBuf,
    accounting: FillAccounting,
    strategy_client_ids: HashSet<i64>,
    strategy_client_order: VecDeque<i64>,
    seen_trade_ids: HashSet<i64>,
    seen_trade_order: VecDeque<i64>,
    pending_trade_ids: HashSet<i64>,
    /// Pending trades carry the accounting sync-generation at queue time: a trade queued
    /// BEFORE a position-snapshot re-seed is already included in that snapshot, so its
    /// replay must attribute PnL only and never re-apply position/fees (double-count bug).
    pending: VecDeque<(TradePayload, u64)>,
    sync_generation: u64,
    strategy_realized_pnl_usdc: f64,
    unattributed_realized_pnl_usdc: f64,
    fill_count: u64,
    buy_count: u64,
    sell_count: u64,
    notional_usdc: f64,
    duplicate_fill_count: u64,
    unattributed_fill_count: u64,
    exchange_position_seen: bool,
    exchange_position: f64,
    exchange_entry_vwap: Option<f64>,
    available_capital: Option<f64>,
    portfolio_value: Option<f64>,
    last_mid: Option<f64>,
    mid_stale: bool,
    last_unrealized: f64,
}

impl PnlActor {
    pub fn new(
        cfg: PnlCfg,
        symbol: String,
        market_id: u32,
        account_index: i64,
        maker_fee_rate: f64,
        shared_bbo: Arc<SharedBbo>,
        derived: Arc<Derived>,
    ) -> std::io::Result<Self> {
        let dir = PathBuf::from(&cfg.persist_dir);
        fs::create_dir_all(&dir)?;
        let trade_logger = TradeLogger::new(&dir, &symbol)?;
        let state_store = LiveStateStore::new(&dir, &symbol);
        let session_stamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let session_id = format!("{symbol}-{market_id}-{session_stamp}");
        let summary_path = dir.join(format!("pnl_session_{symbol}.json"));
        let snapshots_path = dir.join(format!("pnl_snapshots_{symbol}.csv"));
        ensure_snapshot_header(&snapshots_path)?;
        let started_at = utc_now();

        // Restart durability: restore cumulative realized/fills/notional + the local
        // accounting estimate from the persisted live state (only when it belongs to this
        // account+market). The exchange snapshot remains authoritative at runtime.
        let restored = state_store.load();
        let restore = restored.account_index == account_index
            && restored.market_id == market_id
            && !restored.updated_at.is_empty();
        let (accounting, realized, fills, notional) = if restore {
            tracing::info!(
                "PNL_RESTORE symbol={symbol} realized={:.6} fills={} notional={:.6} pos_est={:.8} (saved {})",
                restored.realized_pnl_cumulative,
                restored.fill_count,
                restored.volume_usd,
                restored.position_size_est,
                restored.updated_at
            );
            (
                FillAccounting::from_snapshot(
                    maker_fee_rate,
                    restored.position_size_est,
                    restored.entry_vwap,
                    restored.realized_pnl_cumulative,
                ),
                restored.realized_pnl_cumulative,
                restored.fill_count,
                restored.volume_usd,
            )
        } else {
            (FillAccounting::new(maker_fee_rate), 0.0, 0, 0.0)
        };

        let mut actor = Self {
            cfg,
            symbol,
            market_id,
            account_index,
            maker_fee_rate,
            session_id,
            started_at,
            shared_bbo,
            derived,
            trade_logger,
            state_store,
            summary_path,
            snapshots_path,
            accounting,
            strategy_client_ids: HashSet::new(),
            strategy_client_order: VecDeque::new(),
            seen_trade_ids: HashSet::new(),
            seen_trade_order: VecDeque::new(),
            pending_trade_ids: HashSet::new(),
            pending: VecDeque::new(),
            sync_generation: 0,
            strategy_realized_pnl_usdc: realized,
            unattributed_realized_pnl_usdc: 0.0,
            fill_count: fills,
            buy_count: 0,
            sell_count: 0,
            notional_usdc: notional,
            duplicate_fill_count: 0,
            unattributed_fill_count: 0,
            exchange_position_seen: false,
            exchange_position: 0.0,
            exchange_entry_vwap: None,
            available_capital: None,
            portfolio_value: None,
            last_mid: None,
            mid_stale: false,
            last_unrealized: 0.0,
        };
        actor.persist_summary();
        Ok(actor)
    }

    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<PnlEvent>) {
        tracing::info!(
            "PNL_START session_id={} symbol={} market_id={} persist_dir={}",
            self.session_id,
            self.symbol,
            self.market_id,
            self.cfg.persist_dir
        );
        let interval = Duration::from_secs_f64(self.cfg.snapshot_interval_seconds.max(1.0));
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tokio::select! {
                evt = rx.recv() => {
                    // Match on the Option so a closed channel EXITS the loop — the old
                    // `Some(evt) = rx.recv()` pattern merely disabled this arm on close
                    // while the tick arm kept the actor alive as a zombie forever.
                    let Some(evt) = evt else {
                        self.flush_all();
                        break;
                    };
                    if matches!(evt, PnlEvent::Shutdown) {
                        self.flush_all();
                        let s = self.summary();
                        tracing::info!(
                            "PNL_SUMMARY session_id={} realized={:.6} mtm={:.6} unrealized={:.6} unattributed={:.6} fills={} notional={:.6} open_pos={:.8} entry_vwap={:.4} mid={:?} mid_stale={}",
                            s.session_id,
                            s.strategy_realized_pnl_usdc,
                            s.strategy_mtm_pnl_usdc,
                            s.strategy_unrealized_pnl_usdc,
                            s.unattributed_realized_pnl_usdc,
                            s.fill_count,
                            s.notional_usdc,
                            s.open_position_base,
                            s.entry_vwap,
                            s.last_mid,
                            s.mid_stale
                        );
                        break;
                    }
                    self.handle_event(evt);
                }
                _ = tick.tick() => {
                    self.flush_all();
                    let s = self.summary();
                    tracing::info!(
                        "PNL_HEALTH session_id={} realized={:.6} mtm={:.6} unrealized={:.6} unattributed={:.6} fills={} notional={:.6} open_pos={:.8} entry_vwap={:.4} mid={:?} mid_stale={} pending_unattributed={}",
                        s.session_id,
                        s.strategy_realized_pnl_usdc,
                        s.strategy_mtm_pnl_usdc,
                        s.strategy_unrealized_pnl_usdc,
                        s.unattributed_realized_pnl_usdc,
                        s.fill_count,
                        s.notional_usdc,
                        s.open_position_base,
                        s.entry_vwap,
                        s.last_mid,
                        s.mid_stale,
                        s.pending_unattributed_fill_count
                    );
                }
            }
        }
    }

    fn handle_event(&mut self, evt: PnlEvent) {
        match evt {
            PnlEvent::RegisterClientIds(ids) => {
                for id in ids {
                    if id > 0 && self.strategy_client_ids.insert(id) {
                        self.strategy_client_order.push_back(id);
                        while self.strategy_client_order.len() > ID_SET_CAP {
                            if let Some(old) = self.strategy_client_order.pop_front() {
                                self.strategy_client_ids.remove(&old);
                            }
                        }
                    }
                }
                self.retry_pending();
            }
            PnlEvent::AccountAll { position, trades } => {
                for trade in trades {
                    self.process_or_queue(trade);
                }
                self.retry_pending();
                if let Some(p) = position {
                    self.update_position(p);
                }
            }
            PnlEvent::Capital {
                available_capital,
                portfolio_value,
            } => {
                if available_capital.is_some() {
                    self.available_capital = available_capital;
                }
                if portfolio_value.is_some() {
                    self.portfolio_value = portfolio_value;
                }
            }
            PnlEvent::Shutdown => {}
        }
    }

    fn update_position(&mut self, p: PositionSnapshot) {
        self.exchange_position_seen = true;
        self.exchange_position = p.signed_position;
        self.exchange_entry_vwap = p.entry_vwap;
        if p.signed_position.abs() < EPSILON || p.entry_vwap.is_some() {
            self.accounting = FillAccounting::from_snapshot(
                self.maker_fee_rate,
                p.signed_position,
                p.entry_vwap.unwrap_or(0.0),
                self.strategy_realized_pnl_usdc,
            );
            // Bump the sync generation: trades queued BEFORE this snapshot are already
            // included in it — their eventual replay must be PnL-attribution-only and
            // never re-apply position/fees (the double-count bug).
            self.sync_generation += 1;
        }
    }

    /// Mark a trade id as processed, with FIFO-bounded memory.
    fn mark_seen(&mut self, trade_id: i64) {
        if self.seen_trade_ids.insert(trade_id) {
            self.seen_trade_order.push_back(trade_id);
            while self.seen_trade_order.len() > ID_SET_CAP {
                if let Some(old) = self.seen_trade_order.pop_front() {
                    self.seen_trade_ids.remove(&old);
                }
            }
        }
    }

    fn process_or_queue(&mut self, trade: TradePayload) {
        let trade_id = match trade.trade_id {
            Some(id) => id,
            None => {
                self.unattributed_fill_count += 1;
                tracing::warn!("PNL_SKIP reason=missing_trade_id");
                return;
            }
        };
        if self.seen_trade_ids.contains(&trade_id) {
            self.duplicate_fill_count += 1;
            return;
        }
        if self.pending_trade_ids.contains(&trade_id) {
            return;
        }
        let generation = self.sync_generation;
        match self.try_accept_trade(&trade, generation) {
            TradeDecision::Accepted => {}
            TradeDecision::PendingClientId(client_id) => {
                self.pending_trade_ids.insert(trade_id);
                self.pending.push_back((trade, generation));
                while self.pending.len() > PENDING_LIMIT {
                    if let Some((old, _)) = self.pending.pop_front() {
                        if let Some(id) = old.trade_id {
                            self.pending_trade_ids.remove(&id);
                        }
                        self.unattributed_fill_count += 1;
                        tracing::warn!(
                            "PNL_SKIP reason=pending_limit client_id={:?} trade_id={:?}",
                            client_id,
                            old.trade_id
                        );
                    }
                }
            }
            TradeDecision::Skipped(reason) => {
                self.unattributed_fill_count += 1;
                tracing::warn!("PNL_SKIP reason={} trade_id={}", reason, trade_id);
            }
        }
    }

    fn retry_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let mut still_pending = VecDeque::new();
        while let Some((trade, generation)) = self.pending.pop_front() {
            if let Some(id) = trade.trade_id {
                self.pending_trade_ids.remove(&id);
            }
            match self.try_accept_trade(&trade, generation) {
                TradeDecision::Accepted => {}
                TradeDecision::PendingClientId(_) => {
                    if let Some(id) = trade.trade_id {
                        self.pending_trade_ids.insert(id);
                    }
                    still_pending.push_back((trade, generation));
                }
                TradeDecision::Skipped(reason) => {
                    self.unattributed_fill_count += 1;
                    tracing::warn!("PNL_SKIP reason={} trade_id={:?}", reason, trade.trade_id);
                }
            }
        }
        self.pending = still_pending;
    }

    fn try_accept_trade(&mut self, trade: &TradePayload, queued_generation: u64) -> TradeDecision {
        let trade_id = match trade.trade_id {
            Some(id) => id,
            None => return TradeDecision::Skipped("missing_trade_id"),
        };
        if self.seen_trade_ids.contains(&trade_id) {
            self.duplicate_fill_count += 1;
            return TradeDecision::Accepted;
        }
        let fill = match FillView::from_trade(trade, self.account_index) {
            Some(f) => f,
            None => return TradeDecision::Skipped("account_not_in_trade"),
        };
        if fill.price <= 0.0 || fill.size <= 0.0 {
            return TradeDecision::Skipped("invalid_price_or_size");
        }
        let is_known_strategy = fill
            .client_order_id
            .is_some_and(|c| self.strategy_client_ids.contains(&c));
        let attribute_to_strategy = is_known_strategy || self.cfg.include_unattributed_account_fills;
        if !attribute_to_strategy {
            if let Some(cid) = fill.client_order_id {
                // Our order, id not yet registered by the sender: queue for replay.
                return TradeDecision::PendingClientId(cid);
            }
            // No client id at all (liquidation / manual order / venue omission): it can never
            // be attributed later, but the account UNQUESTIONABLY traded — fall through and
            // book it now (position accounting + the unattributed PnL bucket) instead of the
            // old behavior of silently dropping it as "account_not_in_trade".
        }

        // A trade queued before the last position-snapshot re-seed is ALREADY reflected in
        // the snapshot: attribute its PnL but never re-apply position/fees (double-count bug).
        let superseded = queued_generation < self.sync_generation;
        let (local_delta, fee_usd) = if superseded {
            (fill.exchange_pnl.unwrap_or(0.0), 0.0)
        } else {
            let local = self.accounting.apply(fill.side, fill.price, fill.size);
            (local.realized_delta, local.fee_usd)
        };
        let realized_delta = fill.exchange_pnl.unwrap_or(local_delta);
        if let (Some(exchange_delta), false) = (fill.exchange_pnl, superseded) {
            if (exchange_delta - local_delta).abs() > 0.01 {
                tracing::warn!(
                    "PNL_LOCAL_MISMATCH trade_id={} exchange_delta={:.6} local_delta={:.6}",
                    trade_id,
                    exchange_delta,
                    local_delta
                );
            }
        }

        self.mark_seen(trade_id);
        if attribute_to_strategy {
            self.strategy_realized_pnl_usdc += realized_delta;
        } else {
            self.unattributed_realized_pnl_usdc += realized_delta;
            self.unattributed_fill_count += 1;
        }
        self.fill_count += 1;
        match fill.side {
            Side::Buy => self.buy_count += 1,
            Side::Sell => self.sell_count += 1,
        }
        self.notional_usdc += fill.notional_usd;
        self.refresh_mid();
        let mid_at_fill = self.last_mid;
        let spread_capture_bps =
            mid_at_fill.and_then(|mid| spread_capture_bps(fill.side, fill.price, mid));
        let position_after = self.accounting.position_size();
        let inventory_after_usd = mid_at_fill.map(|mid| position_after * mid);

        self.trade_logger.log_fill(TradeRow {
            timestamp: trade.event_time_ms().and_then(timestamp_from_ms),
            side: fill.side.as_str().to_string(),
            price: fill.price,
            size: fill.size,
            level: -1,
            position_after,
            realized_pnl: realized_delta,
            available_capital: self.available_capital.unwrap_or(0.0),
            portfolio_value: self.portfolio_value.unwrap_or(0.0),
            simulated: false,
            notional_usd: Some(fill.notional_usd),
            fee_usd: Some(fee_usd),
            entry_vwap_after: Some(self.accounting.entry_vwap()),
            realized_pnl_cumulative: Some(self.strategy_realized_pnl_usdc),
            mid_at_fill,
            spread_capture_bps,
            inventory_after_usd,
            client_order_index: fill.client_order_id.map(|c| c.to_string()),
            exchange_order_index: fill.exchange_order_id.map(|id| id.to_string()),
            fill_source: match (is_known_strategy, superseded) {
                (true, false) => "account_all".to_string(),
                (true, true) => "account_all_late_attributed".to_string(),
                (false, _) => "account_all_unattributed".to_string(),
            },
        });
        tracing::info!(
            "PNL_FILL session_id={} trade_id={} side={} price={:.4} size={:.8} notional={:.6} realized_delta={:.6} realized_cum={:.6} client_id={:?} exchange_id={:?} superseded={}",
            self.session_id,
            trade_id,
            fill.side,
            fill.price,
            fill.size,
            fill.notional_usd,
            realized_delta,
            self.strategy_realized_pnl_usdc,
            fill.client_order_id,
            fill.exchange_order_id,
            superseded
        );
        TradeDecision::Accepted
    }

    /// Refresh the MTM mid: prefer the Lighter mid (same venue, hot-task-published), fall
    /// back to the Binance BBO; if BOTH are stale, keep the last value but flag it stale so
    /// unrealized/MTM figures are never silently marked against an old price.
    fn refresh_mid(&mut self) {
        let lighter_mid = self.derived.mid();
        if lighter_mid > 0.0 && self.derived.md_age_ms() <= MID_FRESH_MS {
            self.last_mid = Some(lighter_mid);
            self.mid_stale = false;
            return;
        }
        let mid = self.shared_bbo.mid();
        if mid > 0.0 && self.shared_bbo.age_ms() <= MID_FRESH_MS {
            self.last_mid = Some(mid);
            self.mid_stale = false;
        } else if self.last_mid.is_some() {
            self.mid_stale = true;
        }
    }

    fn open_position(&self) -> f64 {
        if self.exchange_position_seen {
            self.exchange_position
        } else {
            self.accounting.position_size()
        }
    }

    fn entry_vwap(&self) -> f64 {
        self.exchange_entry_vwap
            .unwrap_or_else(|| self.accounting.entry_vwap())
    }

    fn unrealized_pnl(&self) -> f64 {
        let pos = self.open_position();
        let entry = self.entry_vwap();
        let mid = match self.last_mid {
            Some(m) if m > 0.0 && entry > 0.0 => m,
            _ => return 0.0,
        };
        if pos > EPSILON {
            (mid - entry) * pos
        } else if pos < -EPSILON {
            (entry - mid) * pos.abs()
        } else {
            0.0
        }
    }

    fn summary(&mut self) -> PnlSummary {
        self.refresh_mid();
        // With a stale mid, carry the last computed unrealized value (flagged) instead of
        // recomputing against an arbitrarily old price.
        let unrealized = if self.mid_stale {
            self.last_unrealized
        } else {
            let u = self.unrealized_pnl();
            self.last_unrealized = u;
            u
        };
        PnlSummary {
            session_id: self.session_id.clone(),
            symbol: self.symbol.clone(),
            market_id: self.market_id,
            account_index: self.account_index,
            started_at: self.started_at.clone(),
            updated_at: utc_now(),
            strategy_realized_pnl_usdc: self.strategy_realized_pnl_usdc,
            strategy_unrealized_pnl_usdc: unrealized,
            strategy_mtm_pnl_usdc: self.strategy_realized_pnl_usdc + unrealized,
            unattributed_realized_pnl_usdc: self.unattributed_realized_pnl_usdc,
            open_position_base: self.open_position(),
            entry_vwap: self.entry_vwap(),
            last_mid: self.last_mid,
            mid_stale: self.mid_stale,
            fill_count: self.fill_count,
            buy_count: self.buy_count,
            sell_count: self.sell_count,
            notional_usdc: self.notional_usdc,
            duplicate_fill_count: self.duplicate_fill_count,
            unattributed_fill_count: self.unattributed_fill_count,
            pending_unattributed_fill_count: self.pending.len(),
            available_capital: self.available_capital,
            portfolio_value: self.portfolio_value,
        }
    }

    fn persist_summary(&mut self) {
        let summary = self.summary();
        if let Err(e) = atomic_json_write(&self.summary_path, &summary) {
            tracing::warn!("PNL summary write failed: {e}");
        }
    }

    /// Durable restart state for [`LiveStateStore`].
    fn live_state(&self) -> LiveState {
        LiveState {
            account_index: self.account_index,
            market_id: self.market_id,
            position_size_est: self.accounting.position_size(),
            entry_vwap: self.accounting.entry_vwap(),
            realized_pnl_cumulative: self.strategy_realized_pnl_usdc,
            fill_count: self.fill_count,
            volume_usd: self.notional_usdc,
            exchange_position_size: self.exchange_position,
            exchange_entry_vwap: self.exchange_entry_vwap,
            portfolio_value: self.portfolio_value.unwrap_or(0.0),
            available_capital: self.available_capital.unwrap_or(0.0),
            symbol: String::new(),     // stamped by the store
            updated_at: String::new(), // stamped by the store
        }
    }

    fn flush_all(&mut self) {
        if let Err(e) = self.trade_logger.flush() {
            tracing::warn!("PNL trade log flush failed: {e}");
        }
        let summary = self.summary();
        if let Err(e) = append_snapshot(&self.snapshots_path, &summary) {
            tracing::warn!("PNL snapshot append failed: {e}");
        }
        if let Err(e) = atomic_json_write(&self.summary_path, &summary) {
            tracing::warn!("PNL summary write failed: {e}");
        }
        if let Err(e) = self.state_store.save(&self.live_state()) {
            tracing::warn!("PNL live-state save failed: {e}");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeDecision {
    Accepted,
    PendingClientId(i64),
    Skipped(&'static str),
}

#[derive(Debug, Clone, Copy)]
struct FillView {
    side: Side,
    price: f64,
    size: f64,
    notional_usd: f64,
    /// May be absent (liquidations, manual orders, venue payload omission) — membership is
    /// decided by ACCOUNT id, so such fills are still ours and must still be booked.
    client_order_id: Option<i64>,
    exchange_order_id: Option<i64>,
    exchange_pnl: Option<f64>,
}

impl FillView {
    fn from_trade(trade: &TradePayload, account_index: i64) -> Option<Self> {
        let price = trade.price_f64()?;
        let size = trade.size_f64()?;
        let notional_usd = trade.usd_amount_f64().unwrap_or(price * size);
        if trade.ask_account_id == Some(account_index) {
            Some(Self {
                side: Side::Sell,
                price,
                size,
                notional_usd,
                client_order_id: trade.ask_client_id,
                exchange_order_id: trade.ask_id,
                exchange_pnl: trade.ask_account_pnl_f64(),
            })
        } else if trade.bid_account_id == Some(account_index) {
            Some(Self {
                side: Side::Buy,
                price,
                size,
                notional_usd,
                client_order_id: trade.bid_client_id,
                exchange_order_id: trade.bid_id,
                exchange_pnl: trade.bid_account_pnl_f64(),
            })
        } else {
            None
        }
    }
}

fn spread_capture_bps(side: Side, price: f64, mid: f64) -> Option<f64> {
    if mid <= 0.0 {
        return None;
    }
    Some(match side {
        Side::Buy => (mid - price) / mid * 10_000.0,
        Side::Sell => (price - mid) / mid * 10_000.0,
    })
}

fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn timestamp_from_ms(ms: i64) -> Option<String> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

const SNAPSHOT_HEADER: [&str; 18] = [
    "timestamp",
    "session_id",
    "symbol",
    "market_id",
    "account_index",
    "strategy_realized_pnl_usdc",
    "strategy_unrealized_pnl_usdc",
    "strategy_mtm_pnl_usdc",
    "open_position_base",
    "entry_vwap",
    "last_mid",
    "fill_count",
    "buy_count",
    "sell_count",
    "notional_usdc",
    "duplicate_fill_count",
    "unattributed_fill_count",
    "pending_unattributed_fill_count",
];

fn ensure_snapshot_header(path: &Path) -> std::io::Result<()> {
    if path.exists() && fs::metadata(path)?.len() > 0 {
        return Ok(());
    }
    let mut wtr = csv::WriterBuilder::new().from_path(path)?;
    wtr.write_record(SNAPSHOT_HEADER)?;
    wtr.flush()?;
    Ok(())
}

fn append_snapshot(path: &Path, s: &PnlSummary) -> std::io::Result<()> {
    ensure_snapshot_header(path)?;
    let mut wtr = csv::WriterBuilder::new().from_writer(Vec::new());
    wtr.write_record([
        s.updated_at.clone(),
        s.session_id.clone(),
        s.symbol.clone(),
        s.market_id.to_string(),
        s.account_index.to_string(),
        format!("{:.6}", s.strategy_realized_pnl_usdc),
        format!("{:.6}", s.strategy_unrealized_pnl_usdc),
        format!("{:.6}", s.strategy_mtm_pnl_usdc),
        format!("{:.8}", s.open_position_base),
        format!("{:.4}", s.entry_vwap),
        s.last_mid.map(|m| format!("{m:.4}")).unwrap_or_default(),
        s.fill_count.to_string(),
        s.buy_count.to_string(),
        s.sell_count.to_string(),
        format!("{:.6}", s.notional_usdc),
        s.duplicate_fill_count.to_string(),
        s.unattributed_fill_count.to_string(),
        s.pending_unattributed_fill_count.to_string(),
    ])?;
    wtr.flush()?;
    let bytes = wtr
        .into_inner()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&bytes)?;
    Ok(())
}

fn atomic_json_write<T: Serialize>(path: &Path, payload: &T) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(dir)?;
    let mut json = serde_json::to_vec_pretty(payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    json.push(b'\n');
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp = dir.join(format!(".tmp-pnl-{}-{}.json", std::process::id(), nanos));
    let result = (|| -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(&json)?;
        f.flush()?;
        let _ = f.sync_all();
        drop(f);
        fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "pnl_actor_test_{}_{}_{}",
            tag,
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cfg(dir: &Path) -> PnlCfg {
        PnlCfg {
            enabled: true,
            snapshot_interval_seconds: 60.0,
            persist_dir: dir.display().to_string(),
            include_unattributed_account_fills: false,
        }
    }

    fn actor(dir: &Path) -> PnlActor {
        PnlActor::new(
            cfg(dir),
            "BTC".to_string(),
            1,
            7,
            0.0,
            Arc::new(SharedBbo::new(1)),
            Arc::new(Derived::new()),
        )
        .unwrap()
    }

    fn trade(id: i64, client_id: i64, side: Side, pnl: Option<&str>) -> TradePayload {
        match side {
            Side::Buy => TradePayload {
                trade_id: Some(id),
                price: Some("100.0".to_string()),
                size: Some("0.5".to_string()),
                usd_amount: Some("50.0".to_string()),
                bid_account_id: Some(7),
                bid_client_id: Some(client_id),
                bid_id: Some(99),
                bid_account_pnl: pnl.map(str::to_string),
                ..Default::default()
            },
            Side::Sell => TradePayload {
                trade_id: Some(id),
                price: Some("101.0".to_string()),
                size: Some("0.5".to_string()),
                usd_amount: Some("50.5".to_string()),
                ask_account_id: Some(7),
                ask_client_id: Some(client_id),
                ask_id: Some(100),
                ask_account_pnl: pnl.map(str::to_string),
                ..Default::default()
            },
        }
    }

    #[test]
    fn infers_side_and_uses_exchange_pnl_for_known_client() {
        let dir = temp_dir("known");
        let mut a = actor(&dir);
        a.handle_event(PnlEvent::RegisterClientIds(vec![42]));
        a.handle_event(PnlEvent::AccountAll {
            position: Some(PositionSnapshot {
                signed_position: 0.0,
                entry_vwap: None,
            }),
            trades: vec![trade(1, 42, Side::Sell, Some("1.25"))],
        });
        assert_eq!(a.fill_count, 1);
        assert_eq!(a.sell_count, 1);
        assert!((a.strategy_realized_pnl_usdc - 1.25).abs() < 1e-12);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn queues_then_accepts_trade_when_client_id_is_registered() {
        let dir = temp_dir("pending");
        let mut a = actor(&dir);
        a.handle_event(PnlEvent::AccountAll {
            position: None,
            trades: vec![trade(1, 42, Side::Buy, None)],
        });
        assert_eq!(a.fill_count, 0);
        assert_eq!(a.pending.len(), 1);
        a.handle_event(PnlEvent::RegisterClientIds(vec![42]));
        assert_eq!(a.fill_count, 1);
        assert_eq!(a.pending.len(), 0);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn duplicate_trade_is_ignored() {
        let dir = temp_dir("dup");
        let mut a = actor(&dir);
        a.handle_event(PnlEvent::RegisterClientIds(vec![42]));
        let t = trade(1, 42, Side::Buy, Some("0.5"));
        a.handle_event(PnlEvent::AccountAll {
            position: None,
            trades: vec![t.clone(), t],
        });
        assert_eq!(a.fill_count, 1);
        assert_eq!(a.duplicate_fill_count, 1);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn books_account_fill_without_client_id_as_unattributed() {
        let dir = temp_dir("no_cid");
        let mut a = actor(&dir);
        // Liquidation/manual-order shape: our account id, no client id. The old code dropped
        // this as "account_not_in_trade"; it must be booked (position + unattributed bucket).
        let mut t = trade(1, 42, Side::Buy, Some("0.75"));
        t.bid_client_id = None;
        a.handle_event(PnlEvent::AccountAll {
            position: None,
            trades: vec![t],
        });
        assert_eq!(a.fill_count, 1);
        assert_eq!(a.unattributed_fill_count, 1);
        assert!((a.unattributed_realized_pnl_usdc - 0.75).abs() < 1e-12);
        assert!(a.strategy_realized_pnl_usdc.abs() < 1e-12);
        assert!((a.accounting.position_size() - 0.5).abs() < 1e-12);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn pending_fill_superseded_by_snapshot_does_not_double_apply() {
        let dir = temp_dir("superseded");
        let mut a = actor(&dir);
        // Fill races RegisterClientIds -> queued pending; the SAME message's position
        // snapshot already includes it (position 0.5) and re-seeds accounting.
        a.handle_event(PnlEvent::AccountAll {
            position: Some(PositionSnapshot {
                signed_position: 0.5,
                entry_vwap: Some(100.0),
            }),
            trades: vec![trade(1, 42, Side::Buy, None)],
        });
        assert_eq!(a.pending.len(), 1);
        assert!((a.accounting.position_size() - 0.5).abs() < 1e-12);
        // Late registration: PnL-attribution only — position must NOT double to 1.0
        // (the old replay re-applied the fill on top of the synced snapshot).
        a.handle_event(PnlEvent::RegisterClientIds(vec![42]));
        assert_eq!(a.fill_count, 1);
        assert_eq!(a.pending.len(), 0);
        assert!((a.accounting.position_size() - 0.5).abs() < 1e-12);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn restores_live_state_across_restarts() {
        let dir = temp_dir("restore");
        {
            let mut a = actor(&dir);
            a.handle_event(PnlEvent::RegisterClientIds(vec![42]));
            a.handle_event(PnlEvent::AccountAll {
                position: None,
                trades: vec![trade(1, 42, Side::Sell, Some("1.25"))],
            });
            a.flush_all();
        }
        // "Restart": a fresh actor over the same persist dir restores cumulative state.
        let b = actor(&dir);
        assert_eq!(b.fill_count, 1);
        assert!((b.strategy_realized_pnl_usdc - 1.25).abs() < 1e-12);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn applies_trade_before_syncing_same_message_position_snapshot() {
        let dir = temp_dir("position_sync");
        let mut a = actor(&dir);
        a.handle_event(PnlEvent::RegisterClientIds(vec![42]));
        a.handle_event(PnlEvent::AccountAll {
            position: Some(PositionSnapshot {
                signed_position: 0.5,
                entry_vwap: Some(90.0),
            }),
            trades: vec![],
        });
        a.handle_event(PnlEvent::AccountAll {
            position: Some(PositionSnapshot {
                signed_position: 1.0,
                entry_vwap: Some(95.0),
            }),
            trades: vec![trade(1, 42, Side::Buy, None)],
        });
        assert_eq!(a.fill_count, 1);
        assert!((a.accounting.position_size() - 1.0).abs() < 1e-12);
        assert!((a.accounting.entry_vwap() - 95.0).abs() < 1e-12);
        assert!((a.open_position() - 1.0).abs() < 1e-12);
        fs::remove_dir_all(&dir).ok();
    }
}
