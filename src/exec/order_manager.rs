//! Order lifecycle state machine + op collection — port of `OrderManager`,
//! `collect_order_operations`, the client->exchange id map, and reconcile processing.
//!
//! OWNED by the synchronous decision (market-data) task — single owner, no locks. Cross-task
//! inputs (account_orders WS confirmations, paced-send results) arrive as LOSSLESS
//! `OrderEvent`s drained each cycle; reconcile snapshots arrive via a last-writer-wins slot.
//! On send success the sender enqueues an optimistic `BindLive` so the slot is occupied
//! immediately (prevents duplicate creates); the exchange id is resolved later from WS.

use crate::lighter::messages::{parse_f64, RemoteOrder};
use crate::types::{BatchOp, OrderAction, OrderEvent, Side, SideStatus};
use crate::util::price_change_bps;
use std::collections::HashMap;
use std::time::Instant;

const MAX_CLIENT_ORDER_INDEX: i64 = 281_474_976_710_655; // 2^48 - 1
const ID_MAP_CAP: usize = 200;
pub const EPSILON: f64 = 1e-9;

#[derive(Debug, Clone)]
pub struct OrderSlot {
    pub status: SideStatus,
    pub order_id: Option<i64>,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub updated_at: Instant,
    /// Consecutive reconcile snapshots in which this slot's order was MISSING. A slot is only
    /// cleared after the order is absent for `MISSING_RECONCILE_DEBOUNCE` polls (plus the grace
    /// window) — so a single stale/partial REST snapshot cannot false-clear a still-live order and
    /// trigger a duplicate create (which would breach the hard ≤max_live cap). Reset to 0 whenever
    /// the order is seen live / (re)bound.
    miss: u32,
}

impl OrderSlot {
    fn idle() -> Self {
        Self {
            status: SideStatus::Idle,
            order_id: None,
            price: None,
            size: None,
            updated_at: Instant::now(),
            miss: 0,
        }
    }
}

/// Consecutive missing-from-snapshot polls before a tracked slot is cleared (codex: debounce a
/// stale REST snapshot so it cannot false-clear a live order → recreate → >max_live).
const MISSING_RECONCILE_DEBOUNCE: u32 = 2;

pub struct OrderManager {
    num_levels: usize,
    amount_tick: f64,
    bids: Vec<OrderSlot>,
    asks: Vec<OrderSlot>,
    client_to_exchange: HashMap<i64, i64>,
    last_client_order_index: i64,
}

impl OrderManager {
    pub fn new(num_levels: usize, amount_tick: f64) -> Self {
        Self {
            num_levels,
            amount_tick,
            bids: vec![OrderSlot::idle(); num_levels],
            asks: vec![OrderSlot::idle(); num_levels],
            client_to_exchange: HashMap::new(),
            last_client_order_index: 0,
        }
    }

    #[inline]
    fn slot(&self, side: Side, level: usize) -> &OrderSlot {
        match side {
            Side::Buy => &self.bids[level],
            Side::Sell => &self.asks[level],
        }
    }
    #[inline]
    fn slot_mut(&mut self, side: Side, level: usize) -> &mut OrderSlot {
        match side {
            Side::Buy => &mut self.bids[level],
            Side::Sell => &mut self.asks[level],
        }
    }

    /// `next_client_order_index`: monotonic-ish id from time_ns, never reusing the last.
    pub fn next_client_order_index(&mut self, now_ns: i64) -> i64 {
        let mut new_id = now_ns.rem_euclid(MAX_CLIENT_ORDER_INDEX);
        if new_id <= self.last_client_order_index {
            new_id = (self.last_client_order_index + 1).rem_euclid(MAX_CLIENT_ORDER_INDEX + 1);
        }
        if new_id == 0 {
            // 0 is the ClearLive wildcard sentinel and is excluded from PnL registration —
            // never hand it out (reachable once per ~2^48ns wrap).
            new_id = 1;
        }
        self.last_client_order_index = new_id;
        new_id
    }

    pub fn resolve_exchange_id(&self, client_id: i64) -> Option<i64> {
        self.client_to_exchange.get(&client_id).copied()
    }

    fn size_change_requires_update(&self, existing: Option<f64>, new: f64) -> bool {
        match existing {
            None => true,
            Some(e) => {
                let tol = if self.amount_tick > 0.0 { self.amount_tick } else { EPSILON };
                (e - new).abs() >= tol.max(EPSILON)
            }
        }
    }

    fn is_reducing_side(side: Side, position: f64) -> bool {
        (position > EPSILON && side == Side::Sell) || (position < -EPSILON && side == Side::Buy)
    }

    /// Port of `collect_order_operations`. `level_prices` is `[(Option<bid>, Option<ask>)]`.
    ///
    /// `hold_mutations`: post-fill settle window — suppress Modify/Cancel ops (their target
    /// exchange ids are unverified until fills/reconcile settle; modifying a just-filled id
    /// triggers a maker-only batch reject) while still emitting Creates on idle slots (fresh
    /// ids cannot collide). Replaces the old full-ladder freeze.
    pub fn collect_order_operations(
        &mut self,
        level_prices: &[(Option<f64>, Option<f64>)],
        base_amount: f64,
        position: f64,
        effective_threshold_bps: f64,
        now_ns: i64,
        hold_mutations: bool,
    ) -> Vec<BatchOp> {
        let mut ops = Vec::new();
        for (level, &(buy_price, sell_price)) in level_prices.iter().enumerate() {
            for (side, new_price) in [(Side::Buy, buy_price), (Side::Sell, sell_price)] {
                let new_size = base_amount;
                let reduce_only = Self::is_reducing_side(side, position);

                let slot = self.slot(side, level);
                let existing_id = slot.order_id;
                let existing_price = slot.price;
                let existing_size = slot.size;

                let new_price = match new_price {
                    None => {
                        // suppressed -> cancel any live order
                        if hold_mutations {
                            continue;
                        }
                        if let Some(cid) = existing_id {
                            if let Some(eid) = self.resolve_exchange_id(cid) {
                                ops.push(BatchOp {
                                    side,
                                    level,
                                    action: OrderAction::Cancel,
                                    price: 0.0,
                                    size: 0.0,
                                    client_order_id: cid,
                                    exchange_id: Some(eid),
                                    reduce_only: false,
                                });
                            }
                        }
                        continue;
                    }
                    Some(p) => p,
                };
                if new_size <= 0.0 || new_price <= 0.0 {
                    continue;
                }

                if let Some(cid) = existing_id {
                    if hold_mutations {
                        continue;
                    }
                    let exchange_id = match self.resolve_exchange_id(cid) {
                        Some(e) => e,
                        None => continue, // awaiting exchange order_index
                    };
                    let change_bps = price_change_bps(existing_price.unwrap_or(0.0), new_price);
                    let size_changed = self.size_change_requires_update(existing_size, new_size);
                    let needs_modify = existing_price.is_none() || change_bps > effective_threshold_bps || size_changed;
                    if !needs_modify {
                        continue;
                    }
                    ops.push(BatchOp {
                        side,
                        level,
                        action: OrderAction::Modify,
                        price: new_price,
                        size: new_size,
                        client_order_id: cid,
                        exchange_id: Some(exchange_id),
                        reduce_only,
                    });
                } else {
                    let new_order_id = self.next_client_order_index(now_ns);
                    ops.push(BatchOp {
                        side,
                        level,
                        action: OrderAction::Create,
                        price: new_price,
                        size: new_size,
                        client_order_id: new_order_id,
                        exchange_id: None,
                        reduce_only,
                    });
                }
            }
        }
        ops
    }

    // ---- slot mutation (only via events / reconcile) ----

    pub fn mark_status(&mut self, side: Side, level: usize, status: SideStatus) {
        let s = self.slot_mut(side, level);
        s.status = status;
        s.updated_at = Instant::now();
    }

    /// Occupy a slot the MOMENT an op is emitted (before it is signed/sent), so the next
    /// quote cycle's `collect_order_operations` never duplicates an in-flight order
    /// (Python marks PLACING pre-send). For Create this binds the client id locally with
    /// exchange_id still unknown (collect will skip modify/cancel until reconcile resolves it).
    pub fn mark_pending(&mut self, op: &BatchOp) {
        let s = self.slot_mut(op.side, op.level);
        match op.action {
            OrderAction::Create => {
                s.order_id = Some(op.client_order_id);
                s.price = Some(op.price);
                s.size = Some(op.size);
                s.status = SideStatus::Placing;
            }
            OrderAction::Modify => {
                s.price = Some(op.price);
                s.size = Some(op.size);
                s.status = SideStatus::Modifying;
            }
            OrderAction::Cancel => {
                s.status = SideStatus::Canceling;
            }
        }
        s.updated_at = Instant::now();
        s.miss = 0;
    }

    fn bind_live(&mut self, side: Side, level: usize, order_id: i64, price: f64, size: f64) {
        let s = self.slot_mut(side, level);
        s.order_id = Some(order_id);
        s.price = Some(price);
        s.size = Some(size);
        s.status = SideStatus::Live;
        s.updated_at = Instant::now();
        s.miss = 0;
    }

    fn clear_live(&mut self, side: Side, level: usize) {
        *self.slot_mut(side, level) = OrderSlot::idle();
    }

    fn clear_all(&mut self) {
        for lvl in 0..self.num_levels {
            self.clear_live(Side::Buy, lvl);
            self.clear_live(Side::Sell, lvl);
        }
    }

    /// Drain one hot order event (lossless). Call until the queue is empty.
    pub fn apply_event(&mut self, evt: OrderEvent) {
        match evt {
            OrderEvent::BindLive { side, level, client_order_id, exchange_id, price, size } => {
                if let Some(eid) = exchange_id {
                    self.record_id_mapping(client_order_id, eid);
                }
                self.bind_live(side, level, client_order_id, price, size);
            }
            OrderEvent::ClearLive { side, level, client_order_id } => {
                // Only clear if the slot still holds THIS order (a late clear for an old
                // order must not wipe a freshly-placed one in the same slot). id 0 = wildcard.
                let cur = self.slot(side, level).order_id;
                if client_order_id == 0 || cur == Some(client_order_id) {
                    self.clear_live(side, level);
                }
            }
            OrderEvent::ClearAll => self.clear_all(),
            // Mapping ONLY — never mutates slot state (codex #1: incremental WS deltas
            // clearing slots caused duplicate creates).
            OrderEvent::IdResolved { client_order_id, exchange_id } => {
                self.record_id_mapping(client_order_id, exchange_id);
            }
            OrderEvent::Fill { client_order_id, filled_size } => {
                self.apply_fill(client_order_id, filled_size);
            }
        }
    }

    /// Own fill observed on the account stream: full fill clears the slot immediately (the
    /// level re-quotes on the next tick instead of waiting ~2 reconcile polls); partial fill
    /// shrinks the tracked size (the next cycle tops the order back up via Modify, matching
    /// what the reconcile refresh would do).
    fn apply_fill(&mut self, client_order_id: i64, filled_size: f64) {
        for lvl in 0..self.num_levels {
            for side in [Side::Buy, Side::Sell] {
                if self.slot(side, lvl).order_id != Some(client_order_id) {
                    continue;
                }
                let tol = self.amount_tick.max(EPSILON);
                let remaining = self.slot(side, lvl).size.map(|sz| sz - filled_size);
                match remaining {
                    Some(r) if r > tol => {
                        let s = self.slot_mut(side, lvl);
                        s.size = Some(r);
                        s.updated_at = Instant::now();
                        s.miss = 0;
                    }
                    // Full fill (or unknown tracked size): the order is gone.
                    _ => self.clear_live(side, lvl),
                }
                return;
            }
        }
    }

    fn record_id_mapping(&mut self, client_id: i64, exchange_id: i64) {
        self.client_to_exchange.insert(client_id, exchange_id);
        if self.client_to_exchange.len() > ID_MAP_CAP {
            // Keep currently-live ids + drop arbitrary extras (bounded growth).
            let live: std::collections::HashSet<i64> = self
                .bids
                .iter()
                .chain(self.asks.iter())
                .filter_map(|s| s.order_id)
                .collect();
            let mut kept: HashMap<i64, i64> = HashMap::new();
            for (&k, &v) in self.client_to_exchange.iter() {
                if live.contains(&k) {
                    kept.insert(k, v);
                }
            }
            // top up to ~100 recent (by id order) without exceeding cap
            let mut others: Vec<(i64, i64)> = self
                .client_to_exchange
                .iter()
                .filter(|(k, _)| !live.contains(k))
                .map(|(&k, &v)| (k, v))
                .collect();
            others.sort_by_key(|(k, _)| *k);
            for (k, v) in others.into_iter().rev().take(100) {
                kept.insert(k, v);
            }
            self.client_to_exchange = kept;
        }
    }

    /// Update the client->exchange id map from an exchange order snapshot.
    pub fn update_id_mapping_from_orders(&mut self, remote: &[RemoteOrder]) {
        for o in remote {
            if let (Some(c), Some(e)) = (o.client_order_index, o.order_index) {
                self.record_id_mapping(c, e);
            }
        }
    }

    /// Apply a full reconcile snapshot: clear slots whose id is no longer live, refresh
    /// price/size for tracked orders. Returns true if any slot changed (for logging).
    pub fn process_reconcile(&mut self, remote: &[RemoteOrder]) {
        self.update_id_mapping_from_orders(remote);
        let live_ids: std::collections::HashSet<i64> =
            remote.iter().filter(|o| o.is_live()).filter_map(|o| o.client_order_index).collect();

        // Grace: a freshly-placed/bound order may not appear in a (possibly slightly stale)
        // snapshot yet. Don't clear ANY slot updated within the grace window (regardless of
        // Placing/Live), or a stale snapshot could reopen it and allow a duplicate create.
        // A genuinely dead slot just lingers at most `grace` before the next snapshot clears it.
        let grace = std::time::Duration::from_secs(5);
        for lvl in 0..self.num_levels {
            for side in [Side::Buy, Side::Sell] {
                let (id, aged) = {
                    let s = self.slot(side, lvl);
                    (s.order_id, s.updated_at.elapsed() >= grace)
                };
                let id = match id {
                    Some(id) => id,
                    None => continue,
                };
                if live_ids.contains(&id) {
                    // Seen live -> not missing; reset the debounce counter.
                    self.slot_mut(side, lvl).miss = 0;
                } else {
                    // Missing from THIS snapshot. Only clear after it has been missing for
                    // MISSING_RECONCILE_DEBOUNCE consecutive snapshots AND the grace window — a
                    // single stale/partial REST snapshot must not false-clear a live order (which
                    // would let the hot task recreate it -> two live orders -> breach ≤max_live).
                    let m = self.slot(side, lvl).miss + 1;
                    self.slot_mut(side, lvl).miss = m;
                    if m >= MISSING_RECONCILE_DEBOUNCE && aged {
                        self.clear_live(side, lvl);
                    }
                }
            }
        }

        let by_client: HashMap<i64, &RemoteOrder> =
            remote.iter().filter_map(|o| o.client_order_index.map(|c| (c, o))).collect();
        for lvl in 0..self.num_levels {
            for side in [Side::Buy, Side::Sell] {
                if let Some(cid) = self.slot(side, lvl).order_id {
                    if let Some(o) = by_client.get(&cid) {
                        // skip if side mismatch
                        match (side, o.is_ask) {
                            (Side::Buy, Some(true)) | (Side::Sell, Some(false)) => continue,
                            _ => {}
                        }
                        let price = o.price.as_deref().map(parse_f64).or(self.slot(side, lvl).price);
                        let size = o
                            .remaining_base_amount
                            .as_deref()
                            .map(parse_f64)
                            .or(self.slot(side, lvl).size);
                        match (price, size) {
                            (Some(p), Some(s)) => self.bind_live(side, lvl, cid, p, s),
                            _ => self.mark_status(side, lvl, SideStatus::Live),
                        }
                    }
                }
            }
        }
    }

    /// Cancel ops for every tracked slot with a resolved exchange id — used when the live
    /// gate trips (capital gone / position feed lost) so a stale ladder never keeps resting.
    /// Slots already Canceling are skipped (no duplicate cancels on repeated gated ticks);
    /// a lost cancel self-heals when reconcile re-binds the slot Live.
    pub fn collect_cancel_ops(&self) -> Vec<BatchOp> {
        let mut ops = Vec::new();
        for lvl in 0..self.num_levels {
            for side in [Side::Buy, Side::Sell] {
                let s = self.slot(side, lvl);
                if s.status == SideStatus::Canceling {
                    continue;
                }
                if let Some(cid) = s.order_id {
                    if let Some(eid) = self.resolve_exchange_id(cid) {
                        ops.push(BatchOp {
                            side,
                            level: lvl,
                            action: OrderAction::Cancel,
                            price: 0.0,
                            size: 0.0,
                            client_order_id: cid,
                            exchange_id: Some(eid),
                            reduce_only: false,
                        });
                    }
                }
            }
        }
        ops
    }

    /// Set of currently-tracked client ids (for reconcile diffing).
    pub fn tracked_client_ids(&self) -> Vec<i64> {
        self.bids.iter().chain(self.asks.iter()).filter_map(|s| s.order_id).collect()
    }

    /// Allocation-free equality check against a previously published tracked-id vec (the hot
    /// path republishes only on change).
    pub fn tracked_ids_equal(&self, prev: &[i64]) -> bool {
        let mut i = 0;
        for s in self.bids.iter().chain(self.asks.iter()) {
            if let Some(id) = s.order_id {
                if i >= prev.len() || prev[i] != id {
                    return false;
                }
                i += 1;
            }
        }
        i == prev.len()
    }

    pub fn slot_status(&self, side: Side, level: usize) -> SideStatus {
        self.slot(side, level).status
    }
    pub fn slot_age(&self, side: Side, level: usize) -> std::time::Duration {
        self.slot(side, level).updated_at.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvls(n: usize, b: Option<f64>, a: Option<f64>) -> Vec<(Option<f64>, Option<f64>)> {
        let mut v = vec![(None, None); n];
        v[0] = (b, a);
        v
    }

    #[test]
    fn creates_then_skips_after_bind() {
        let mut om = OrderManager::new(1, 0.00001);
        let ops = om.collect_order_operations(&lvls(1, Some(99.0), Some(101.0)), 0.001, 0.0, 8.0, 1_000, false);
        assert_eq!(ops.len(), 2); // create bid + ask
        assert!(ops.iter().all(|o| o.action == OrderAction::Create));
        // optimistic bind on send success
        for o in &ops {
            om.apply_event(OrderEvent::BindLive {
                side: o.side, level: o.level, client_order_id: o.client_order_id,
                exchange_id: Some(o.client_order_id + 1_000_000), price: o.price, size: o.size,
            });
        }
        // same prices -> no ops (within threshold, size unchanged)
        let ops2 = om.collect_order_operations(&lvls(1, Some(99.0), Some(101.0)), 0.001, 0.0, 8.0, 2_000, false);
        assert_eq!(ops2.len(), 0);
    }

    #[test]
    fn reprices_when_moved_beyond_threshold() {
        let mut om = OrderManager::new(1, 0.00001);
        let ops = om.collect_order_operations(&lvls(1, Some(100.0), Some(101.0)), 0.001, 0.0, 8.0, 1, false);
        for o in &ops {
            om.apply_event(OrderEvent::BindLive {
                side: o.side, level: o.level, client_order_id: o.client_order_id,
                exchange_id: Some(o.client_order_id + 1), price: o.price, size: o.size,
            });
        }
        // move bid by ~100 bps (100 -> 99) => modify
        let ops2 = om.collect_order_operations(&lvls(1, Some(99.0), Some(101.0)), 0.001, 0.0, 8.0, 2, false);
        assert!(ops2.iter().any(|o| o.action == OrderAction::Modify && o.side == Side::Buy));
    }

    #[test]
    fn hold_blocks_modify_cancel_but_allows_creates() {
        let mut om = OrderManager::new(2, 0.00001);
        // Bind level 0 live on both sides; level 1 stays idle.
        let l0 = vec![(Some(100.0), Some(101.0)), (None, None)];
        let ops = om.collect_order_operations(&l0, 0.001, 0.0, 8.0, 1, false);
        for o in &ops {
            om.apply_event(OrderEvent::BindLive {
                side: o.side, level: o.level, client_order_id: o.client_order_id,
                exchange_id: Some(o.client_order_id + 1), price: o.price, size: o.size,
            });
        }
        // During hold: L0 bid moved >threshold (would Modify), L0 ask suppressed (would
        // Cancel) — both blocked; only the idle L1 Creates flow.
        let levels = vec![(Some(99.0), None), (Some(98.0), Some(102.0))];
        let held = om.collect_order_operations(&levels, 0.001, 0.0, 8.0, 2, true);
        assert!(!held.is_empty());
        assert!(held.iter().all(|o| o.action == OrderAction::Create && o.level == 1));
        // Same input without hold: the Modify and Cancel appear.
        let free = om.collect_order_operations(&levels, 0.001, 0.0, 8.0, 3, false);
        assert!(free.iter().any(|o| o.action == OrderAction::Modify && o.side == Side::Buy));
        assert!(free.iter().any(|o| o.action == OrderAction::Cancel && o.side == Side::Sell));
    }

    #[test]
    fn fill_event_clears_full_and_shrinks_partial() {
        let mut om = OrderManager::new(1, 0.00001);
        let ops = om.collect_order_operations(&lvls(1, Some(99.0), Some(101.0)), 0.001, 0.0, 8.0, 1, false);
        let bid = ops.iter().find(|o| o.side == Side::Buy).unwrap().clone();
        let ask = ops.iter().find(|o| o.side == Side::Sell).unwrap().clone();
        for o in [&bid, &ask] {
            om.apply_event(OrderEvent::BindLive {
                side: o.side, level: o.level, client_order_id: o.client_order_id,
                exchange_id: Some(o.client_order_id + 1), price: o.price, size: o.size,
            });
        }
        // Partial fill on the bid: slot shrinks but stays tracked -> next cycle tops it up.
        om.apply_event(OrderEvent::Fill { client_order_id: bid.client_order_id, filled_size: 0.0004 });
        assert_eq!(om.slot_status(Side::Buy, 0), SideStatus::Live);
        let ops2 = om.collect_order_operations(&lvls(1, Some(99.0), Some(101.0)), 0.001, 0.0, 8.0, 2, false);
        assert!(ops2.iter().any(|o| o.action == OrderAction::Modify && o.side == Side::Buy));
        // Full fill on the ask: slot clears -> Create re-emitted immediately (no reconcile wait).
        om.apply_event(OrderEvent::Fill { client_order_id: ask.client_order_id, filled_size: 0.001 });
        assert_eq!(om.slot_status(Side::Sell, 0), SideStatus::Idle);
        let ops3 = om.collect_order_operations(&lvls(1, Some(99.0), Some(101.0)), 0.001, 0.0, 8.0, 3, false);
        assert!(ops3.iter().any(|o| o.action == OrderAction::Create && o.side == Side::Sell));
    }

    #[test]
    fn id_resolved_maps_without_touching_slots() {
        let mut om = OrderManager::new(1, 0.00001);
        let ops = om.collect_order_operations(&lvls(1, Some(99.0), None), 0.001, 0.0, 8.0, 1, false);
        let bid = ops[0].clone();
        om.mark_pending(&bid); // Placing, exchange id unknown
        // Price moved beyond threshold but no mapping yet -> no Modify possible.
        let ops2 = om.collect_order_operations(&lvls(1, Some(97.0), None), 0.001, 0.0, 8.0, 2, false);
        assert!(ops2.is_empty());
        // account_orders WS resolves the id: mapping only, slot state untouched.
        om.apply_event(OrderEvent::IdResolved { client_order_id: bid.client_order_id, exchange_id: 777 });
        assert_eq!(om.slot_status(Side::Buy, 0), SideStatus::Placing);
        let ops3 = om.collect_order_operations(&lvls(1, Some(97.0), None), 0.001, 0.0, 8.0, 3, false);
        assert!(ops3.iter().any(|o| o.action == OrderAction::Modify && o.exchange_id == Some(777)));
    }

    #[test]
    fn suppressed_side_cancels() {
        let mut om = OrderManager::new(1, 0.00001);
        let ops = om.collect_order_operations(&lvls(1, Some(99.0), Some(101.0)), 0.001, 0.0, 8.0, 1, false);
        for o in &ops {
            om.apply_event(OrderEvent::BindLive {
                side: o.side, level: o.level, client_order_id: o.client_order_id,
                exchange_id: Some(o.client_order_id + 1), price: o.price, size: o.size,
            });
        }
        // suppress bid (None) -> cancel op for the live bid
        let ops2 = om.collect_order_operations(&lvls(1, None, Some(101.0)), 0.001, 0.0, 8.0, 2, false);
        assert!(ops2.iter().any(|o| o.action == OrderAction::Cancel && o.side == Side::Buy));
    }
}
