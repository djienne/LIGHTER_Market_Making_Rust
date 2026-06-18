//! Top-level orchestration: bootstrap, the synchronous market-data HOT task (book ->
//! vol/OBI signal -> quote ladder -> order ops), Binance alpha feeds, and (live) the cold
//! send/account/reconcile tasks. Mirrors `main()` + `market_making_loop`.
//!
//! HOT PATH (single-writer, lock-free): the market-data task owns `LocalBook`,
//! `VolObiCalculator`, and `OrderManager`. It reads cross-task signals (Binance alpha,
//! position, capital-derived params) via lock-free atomics, and emits order ops to the
//! freshest-wins `watch` mailbox. COLD PATH: everything async/IO behind that boundary.

use crate::book::local_book::LocalBook;
use crate::config::{Config, Credentials};
use crate::exec::instance_lock::InstanceLock;
use crate::exec::order_manager::OrderManager;
use crate::exec::paced_send::{self, SenderCtx};
use crate::exec::rate_limit::RateLimiter;
use crate::lighter::auth::generate_ws_auth_token;
use crate::lighter::messages::{AccountAllMsg, OrderBookMsg, RemoteOrder, TickerMsg, UserStatsMsg};
use crate::lighter::nonce::NonceManager;
use crate::lighter::rest::{RestClient, BASE_URL};
use crate::lighter::signer::Signer;
use crate::lighter::tx_ws::TxWebSocket;
use crate::lighter::ws::{subscribe_loop, subscribe_loop_authed, SubscribeOptions, WS_URL};
use crate::risk::RiskController;
use crate::shared::{Derived, SharedAlpha, SharedBbo, SharedPosition};
use crate::strategy::quotes::{
    apply_inventory_exit_bias, apply_quality_spread_multiplier, build_quote_levels,
    fallback_reduce_only, normalize_live_order_size, spread_factors,
};
use crate::strategy::vol_obi::{VolObiCalculator, VolObiConfig};
use crate::types::{BatchOp, MarketConfig, OrderEvent};
use crate::util::dynamic_max_position;
use anyhow::{Context, Result};
use arc_swap::{ArcSwap, ArcSwapOption};
use parking_lot::Mutex as PMutex;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Notify};

type ReconcileSwap = Arc<ArcSwapOption<Vec<RemoteOrder>>>;
/// Client order-ids the hot task currently tracks, published for the reconcile poller's
/// orphan detection.
type TrackedIds = Arc<ArcSwap<Vec<i64>>>;
/// Set by shutdown to stop the sender placing new orders before cancel-all.
type Halt = Arc<AtomicBool>;
/// After a fill the account websocket can report the new position before active-order REST has
/// stopped returning the filled order. Hold longer than one reconcile interval so the next quote
/// cycle does not modify a just-filled exchange order id and trigger a maker-only batch reject.
const POSITION_RECONCILE_SETTLE_BUFFER_SEC: f64 = 7.0;
const POSITION_RECONCILE_MIN_HOLD_SEC: f64 = 10.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Full hot path against live data, but NO orders sent (safe verification).
    Shadow,
    /// Live trading (sends real orders). Gated behind --live + order.enabled.
    Live,
}

pub struct App {
    pub config: Config,
    pub creds: Credentials,
    pub market: MarketConfig,
    pub rest: Arc<RestClient>,
}

/// Live cold-path handles returned to `run()` so it can (a) hold the single-instance lock for
/// the process lifetime, (b) serialize the shutdown cancel-all on the nonce, and (c) watch the
/// critical tasks so an unexpected death triggers a clean cancel-all instead of leaving orders.
struct LiveDeps {
    signer: Arc<Signer>,
    nonce: Arc<NonceManager>,
    sdk_lock: Arc<tokio::sync::Mutex<()>>,
    reconcile_notify: Arc<Notify>,
    sender: tokio::task::JoinHandle<()>,
    /// Held (not used) for the process lifetime; releasing it frees the per-account flock.
    _instance_lock: InstanceLock,
}

impl App {
    /// Resolve market details (ticks, mins) from REST and build the app context.
    pub async fn bootstrap(config: Config, creds: Credentials) -> Result<Self> {
        let rest = Arc::new(RestClient::new(BASE_URL)?);
        let detail = rest
            .market_detail(&creds.market_symbol)
            .await
            .with_context(|| format!("resolving market {}", creds.market_symbol))?;
        let market = MarketConfig {
            market_id: detail.market_id,
            symbol: detail.symbol.clone(),
            price_tick: 10f64.powi(-(detail.supported_price_decimals as i32)),
            amount_tick: 10f64.powi(-(detail.supported_size_decimals as i32)),
            min_base_amount: crate::lighter::messages::parse_f64(&detail.min_base_amount),
            min_quote_amount: crate::lighter::messages::parse_f64(&detail.min_quote_amount),
            price_decimals: detail.supported_price_decimals,
            size_decimals: detail.supported_size_decimals,
        };
        tracing::info!(
            "market {} id={} price_tick={} amount_tick={} min_base={} min_quote={}",
            market.symbol,
            market.market_id,
            market.price_tick,
            market.amount_tick,
            market.min_base_amount,
            market.min_quote_amount
        );
        Ok(Self {
            config,
            creds,
            market,
            rest,
        })
    }

    fn binance_symbol(&self) -> String {
        format!("{}usdt", self.creds.market_symbol.to_lowercase())
    }

    pub async fn run(self, mode: Mode) -> Result<()> {
        let shared_alpha = Arc::new(SharedAlpha::new(self.config.trading.alpha.min_samples));
        let shared_bbo = Arc::new(SharedBbo::new(self.config.trading.alpha.bbo_min_samples));
        let shared_pos = Arc::new(SharedPosition::new());
        let derived = Arc::new(Derived::new());

        // Seed derived params (refined by user_stats in live mode).
        let base_amount = if self.config.trading.base_amount > 0.0 {
            self.config.trading.base_amount
        } else {
            self.market.min_base_amount.max(0.00001)
        };
        tracing::info!(
            "runtime config: mode={:?} symbol={} market_id={} levels_per_side={} base_amount_seed={:.8} capital_usage_pct={:.4} leverage={} min_order_value_usd={:.2} warmup_seconds={:.1} quote_threshold_bps={:.2} send_interval_sec={:.3} max_live_orders={} stale_poller_sec={:.1} ws_ping_interval_sec={:.1} ws_recv_timeout_sec={:.1} ws_account_timeout_sec={:.1} reconnect_base_sec={:.1} reconnect_max_sec={:.1}",
            mode,
            self.market.symbol,
            self.market.market_id,
            self.config.trading.levels_per_side,
            base_amount,
            self.config.trading.capital_usage_percent,
            self.config.trading.leverage,
            self.config.trading.min_order_value_usd,
            self.config.trading.vol_obi.warmup_seconds,
            self.config.trading.default_quote_update_threshold_bps,
            self.config.performance.rate_limit_send_interval,
            self.config.safety.max_live_orders_per_market,
            self.config.safety.stale_order_poller_interval_sec,
            self.config.websocket.ping_interval,
            self.config.websocket.recv_timeout,
            self.config.websocket.account_recv_timeout,
            self.config.websocket.reconnect_base_delay,
            self.config.websocket.reconnect_max_delay,
        );
        derived.set_base_amount(base_amount);
        // Live: seed 0 so we NEVER quote before capital+position feeds arrive (codex #7).
        // Shadow: effectively unlimited so the pipeline is exercised without account feeds.
        derived.set_max_pos_usd(if mode == Mode::Live { 0.0 } else { 1.0e12 });

        // --- Binance alpha feeds (cold) ---
        if self
            .config
            .trading
            .alpha
            .source
            .eq_ignore_ascii_case("binance")
        {
            let depth = crate::binance::depth_client::BinanceDepthClient::new(
                &self.binance_symbol(),
                self.config.trading.alpha.depth_snapshot_limit,
                self.config.trading.alpha.window_size,
                self.config.trading.alpha.looking_depth,
                shared_alpha.clone(),
            );
            tokio::spawn(depth.run());
            let bt = crate::binance::book_ticker::BinanceBookTickerClient::new(
                &self.binance_symbol(),
                shared_bbo.clone(),
            );
            tokio::spawn(bt.run());
        }

        // --- Channels: hot->cold ops mailbox; cold->hot lossless events; reconcile snapshot ---
        let (ops_tx, ops_rx) = watch::channel::<Vec<BatchOp>>(Vec::new());
        let (evt_tx, evt_rx) = mpsc::unbounded_channel::<OrderEvent>();
        let reconcile_swap: ReconcileSwap = Arc::new(ArcSwapOption::empty());
        let tracked_ids: TrackedIds = Arc::new(ArcSwap::from_pointee(Vec::new()));
        let halt: Halt = Arc::new(AtomicBool::new(false));

        let mut live_deps = if mode == Mode::Live {
            Some(
                self.spawn_live_cold_tasks(
                    shared_pos.clone(),
                    derived.clone(),
                    ops_rx,
                    evt_tx,
                    reconcile_swap.clone(),
                    tracked_ids.clone(),
                    halt.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        if mode == Mode::Live {
            spawn_live_health_logger(
                self.market.symbol.clone(),
                shared_alpha.clone(),
                shared_bbo.clone(),
                shared_pos.clone(),
                derived.clone(),
            );
        }

        // --- HOT market-data task (owns book + signal + order state) ---
        let hot = HotTask::new(
            self.market.clone(),
            &self.config,
            base_amount,
            shared_alpha.clone(),
            shared_pos.clone(),
            derived.clone(),
            ops_tx,
            evt_rx,
            reconcile_swap,
            tracked_ids,
            live_deps.as_ref().map(|d| d.reconcile_notify.clone()),
            mode,
        );
        let mut md_handle = tokio::spawn(hot.run(self.config.clone(), self.market.clone()));

        // Wait for a shutdown signal OR for a critical task to die unexpectedly. With
        // `panic = "unwind"` (release profile) a panic in the hot task or sender unwinds that
        // task and resolves its JoinHandle (Err on panic) instead of aborting the process — so
        // we always reach the cancel-all path below and never leave orders resting on a crash.
        if let Some(deps) = live_deps.as_mut() {
            tokio::select! {
                _ = wait_for_shutdown() => tracing::info!("shutdown signal received"),
                r = &mut md_handle => tracing::error!("HOT (market-data) task exited unexpectedly ({r:?}); shutting down"),
                r = &mut deps.sender => tracing::error!("paced sender task exited unexpectedly ({r:?}); shutting down"),
            }
        } else {
            tokio::select! {
                _ = wait_for_shutdown() => tracing::info!("shutdown signal received"),
                r = &mut md_handle => tracing::error!("HOT (market-data) task exited unexpectedly ({r:?}); shutting down"),
            }
        }

        // SAFETY shutdown: stop placing NEW orders, drain any in-flight send, then verify flat.
        halt.store(true, Ordering::SeqCst);
        md_handle.abort();
        if let Some(deps) = live_deps {
            // Acquire sdk_lock BEFORE aborting the sender: this WAITS for any in-flight send_once
            // to finish its outcome handling and RELEASE the lock. Aborting it mid-send could let
            // a tx land on the exchange AFTER our cancel-all verified zero — a resting orphan
            // post-exit (codex). With `halt` set, once the sender releases the lock it will not
            // start another send, so it is safe to stop it now and then verify a flat book.
            let _g = deps.sdk_lock.lock().await;
            deps.sender.abort();
            cancel_all_and_verify(
                &deps.signer,
                &deps.nonce,
                &self.rest,
                self.creds.api_key_index,
                self.creds.account_index,
                self.market.market_id,
            )
            .await;
        }
        Ok(())
    }

    /// Build and spawn the LIVE cold-path tasks: paced sender, account WS streams
    /// (orders/all/user_stats with 9-min auth-token refresh), and the reconcile poller.
    async fn spawn_live_cold_tasks(
        &self,
        shared_pos: Arc<SharedPosition>,
        derived: Arc<Derived>,
        ops_rx: watch::Receiver<Vec<BatchOp>>,
        evt_tx: mpsc::UnboundedSender<OrderEvent>,
        reconcile_swap: ReconcileSwap,
        tracked_ids: TrackedIds,
        halt: Halt,
    ) -> Result<LiveDeps> {
        let aki = self.creds.api_key_index;
        let acct = self.creds.account_index;
        let mkt_id = self.market.market_id;
        if self.creds.api_key_private_key.is_empty() {
            anyhow::bail!("LIVE mode requires API_KEY_PRIVATE_KEY in .env");
        }

        // SAFETY (single-instance): acquire the per-(account, api-key) lock BEFORE touching the
        // nonce or placing any order. Two bots on the same pair share one exchange nonce sequence
        // and would corrupt each other ('invalid nonce' cascade) + double-place orders — the
        // dominant failure of the first smoke test (two Rust instances ran at once).
        let instance_lock = InstanceLock::acquire(acct, aki)?;
        // The flock only guards same-host processes; a containerized bot with the same creds is
        // invisible to it. Make the operator rule explicit and loud.
        tracing::warn!(
            "LIVE single-instance lock held for account_index={acct} api_key_index={aki}. \
             Do NOT run any other bot (e.g. the production `lighter-mm` docker container or the \
             Python market_maker_v2) on these SAME credentials concurrently — they share one \
             nonce sequence and will collide."
        );
        let signers_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("signers");
        let signer = Arc::new(Signer::load(
            &signers_dir,
            BASE_URL,
            &self.creds.api_key_private_key,
            aki,
            acct,
        )?);
        let nonce = Arc::new(NonceManager::init(&self.rest, acct, aki).await?);
        let tx_ws = Arc::new(TxWebSocket::new(WS_URL));
        let _ = tx_ws.connect().await;

        // SAFETY: clear any pre-existing orders AND verify a flat book before enabling sending.
        if !cancel_all_and_verify(&signer, &nonce, &self.rest, aki, acct, mkt_id).await {
            anyhow::bail!("startup: could not verify a flat order book; aborting LIVE start");
        }
        let rate = RateLimiter::new(
            self.config.trading.default_quote_update_threshold_bps,
            self.config.performance.rate_limit_send_interval,
        );
        let risk = Arc::new(PMutex::new(RiskController::new(
            self.config.safety.max_consecutive_order_rejections,
            self.config.safety.circuit_breaker_cooldown_sec,
        )));
        let reconcile_notify = Arc::new(Notify::new());
        let sdk_lock = Arc::new(tokio::sync::Mutex::new(()));
        // Nonce-trust flag: set false if a mandatory hard_refresh exhausts retries; the sender then
        // refuses new orders and the reconcile poller re-syncs the nonce + clears it (codex).
        let nonce_ok = Arc::new(AtomicBool::new(true));
        let recv_to = self.config.websocket.account_recv_timeout;

        // Paced sender (mailbox -> rate gate -> sign -> send). Handle is watched by `run()` so a
        // sender death triggers a clean cancel-all shutdown.
        let sender = tokio::spawn(paced_send::run(
            SenderCtx {
                signer: signer.clone(),
                nonce: nonce.clone(),
                rest: self.rest.clone(),
                tx_ws: tx_ws.clone(),
                market: self.market.clone(),
                account_index: acct,
                derived: derived.clone(),
                risk: risk.clone(),
                reconcile: reconcile_notify.clone(),
                sdk_lock: sdk_lock.clone(),
                events: evt_tx.clone(),
                halt: halt.clone(),
                nonce_ok: nonce_ok.clone(),
            },
            rate,
            ops_rx,
        ));

        // NOTE: order-state reconciliation is driven ONLY by the REST stale-poller below,
        // which returns FULL active-order snapshots (correct for process_reconcile). The
        // account_orders WS sends INCREMENTAL deltas that must NOT go through full reconcile
        // (codex #1: would clear real resting orders and cause duplicates), so it is not wired.

        // account_all -> position (atomic, lock-free read in hot path).
        {
            let sp = shared_pos.clone();
            let sgn = signer.clone();
            let ch = format!("account_all/{}", acct);
            let ch_auth = ch.clone();
            let mkt_str = mkt_id.to_string();
            let mut opts = SubscribeOptions::new("account_all", vec![ch]);
            opts.recv_timeout = recv_to;
            tokio::spawn(async move {
                subscribe_loop_authed(
                    opts,
                    move || auth_map(&sgn, aki, &ch_auth),
                    move |data| {
                        if let Ok(msg) = serde_json::from_value::<AccountAllMsg>(data.clone()) {
                            if let Some(p) = msg.positions.get(&mkt_str) {
                                sp.set(p.signed());
                            }
                        }
                    },
                )
                .await;
            });
        }

        // user_stats -> available capital (hot task recomputes base/max-pos from it + mid).
        {
            let der = derived.clone();
            let sgn = signer.clone();
            let ch = format!("user_stats/{}", acct);
            let ch_auth = ch.clone();
            let mut opts = SubscribeOptions::new("user_stats", vec![ch]);
            opts.recv_timeout = recv_to;
            tokio::spawn(async move {
                subscribe_loop_authed(
                    opts,
                    move || auth_map(&sgn, aki, &ch_auth),
                    move |data| {
                        if let Ok(msg) = serde_json::from_value::<UserStatsMsg>(data.clone()) {
                            if let Some(c) = msg.stats.available_capital() {
                                der.set_capital(c);
                            }
                        }
                    },
                )
                .await;
            });
        }

        // REST reconcile stale-poller: full active-order snapshots drive process_reconcile, and
        // it enforces safety — cancels orphan exchange orders (codex #2) and caps live count.
        {
            let rsw = reconcile_swap.clone();
            let sgn = signer.clone();
            let nm = nonce.clone();
            let rest = self.rest.clone();
            let notify = reconcile_notify.clone();
            let tids = tracked_ids.clone();
            let sdk = sdk_lock.clone();
            let sp = shared_pos.clone();
            let rsk = risk.clone();
            let nok = nonce_ok.clone();
            let interval = self.config.safety.stale_order_poller_interval_sec.max(1.0);
            let max_live = self.config.safety.max_live_orders_per_market.max(1);
            // Consecutive reconcile-mismatch polls before the circuit breaker arms a cooldown pause
            // (Python `STALE_ORDER_DEBOUNCE_COUNT`, floored at 1).
            let debounce_min = self.config.safety.stale_order_debounce_count.max(1) as u32;
            tokio::spawn(async move {
                // Arm a cooldown pause once reconcile mismatches persist past the debounce
                // (Python L2496-2499). Called under one lock right after a not-ok mark_reconcile.
                let pause_if_streak = |reason: &str| {
                    let mut r = rsk.lock();
                    let streak = r.mismatch_streak();
                    if streak >= debounce_min {
                        r.trigger_pause(&format!("reconcile mismatch ({reason}) streak={streak}"));
                    }
                };
                let mut prev_orphans: HashSet<i64> = HashSet::new();
                loop {
                    tokio::select! {
                        _ = notify.notified() => {}
                        _ = tokio::time::sleep(std::time::Duration::from_secs_f64(interval)) => {}
                    }
                    let tok = match generate_ws_auth_token(&sgn, aki) {
                        Ok(t) => t,
                        // A failed reconcile must BLOCK pause-recovery (Python mark_reconcile(ok=False))
                        // and, once it persists past the debounce, arm the cooldown pause.
                        Err(_) => {
                            rsk.lock().mark_reconcile(false, "auth_token");
                            pause_if_streak("auth_token");
                            continue;
                        }
                    };
                    let orders = match rest.account_active_orders(acct, mkt_id, &tok).await {
                        Ok(o) => o,
                        Err(_) => {
                            rsk.lock().mark_reconcile(false, "rest_active_orders");
                            pause_if_streak("rest_active_orders");
                            continue;
                        }
                    };

                    // Record reconcile HEALTH synchronously, the instant the snapshot is in hand —
                    // BEFORE rsw.store and BEFORE any `.await` (codex TOCTOU): otherwise a paused
                    // sender could recover on the previous `last_reconcile_ok=true` during the
                    // position-fetch await while an orphan/over-cap snapshot is already known, then
                    // place a full tracked set on top → exceeding the hard ≤max_live cap.
                    // A snapshot is CLEAN only if it is in-bounds AND has NO untracked (orphan) orders.
                    let tracked: HashSet<i64> = tids.load().iter().copied().collect();
                    let now_orphans: HashSet<i64> = orders
                        .iter()
                        .filter_map(|o| o.client_order_index)
                        .filter(|cid| !tracked.contains(cid))
                        .collect();
                    let over_cap = orders.len() > max_live;
                    if over_cap {
                        // Hard desync — arm the cooldown pause directly (Python L2390-2396) so
                        // trading does not resume the instant the cancel-all below clears the book.
                        let mut r = rsk.lock();
                        r.mark_reconcile(false, "too_many_orders");
                        r.trigger_pause(&format!(
                            "exchange has {} live orders (> {})",
                            orders.len(),
                            max_live
                        ));
                    } else if now_orphans.is_empty() {
                        rsk.lock().mark_reconcile(true, "poll_ok");
                    } else {
                        rsk.lock().mark_reconcile(false, "orphans_present");
                        pause_if_streak("orphans_present");
                    }

                    // Full snapshot -> hot task refreshes/clears tracked slots.
                    rsw.store(Some(Arc::new(orders.clone())));
                    // Authoritative position via REST (never stale even if account WS dies).
                    if let Ok(pos) = rest.account_position(acct, mkt_id).await {
                        sp.set(pos);
                    }

                    // SAFETY: too many live orders => something desynced; cancel-all + keep pause parked.
                    if over_cap {
                        tracing::error!(
                            "SAFETY: {} active orders > max {} -> cancel-all",
                            orders.len(),
                            max_live
                        );
                        let _g = sdk.lock().await; // serialize nonce use with the sender
                        let _ = cancel_all_and_verify(&sgn, &nm, &rest, aki, acct, mkt_id).await;
                        prev_orphans.clear();
                        continue;
                    }

                    // If the nonce is untrusted (a prior mandatory refresh failed), REST is clearly
                    // reachable now (we just fetched orders), so re-sync it under sdk_lock and clear
                    // the flag — resume must never fire a batch with a known-bad nonce (codex).
                    if !nok.load(Ordering::SeqCst) {
                        let _g = sdk.lock().await;
                        if nm.hard_refresh(&rest).await.is_ok() {
                            nok.store(true, Ordering::SeqCst);
                            tracing::info!("nonce re-synced by reconcile poller; resume unblocked");
                        }
                    }

                    // Cancel orphans, but only after TWO consecutive polls (debounce vs the
                    // create->appear race). Runs even while paused (mirrors Python; an orphan is an
                    // extra resting order). sdk.lock() serializes nonce use with the sender.
                    for o in &orders {
                        if let Some(cid) = o.client_order_index {
                            if !tracked.contains(&cid) && prev_orphans.contains(&cid) {
                                if let Some(eid) = o.order_index {
                                    let _g = sdk.lock().await;
                                    let n = nm.next();
                                    match sgn.sign_cancel_order(mkt_id as i32, eid, n, aki) {
                                        Ok(tx) => {
                                            match rest.send_tx(tx.tx_type, &tx.tx_info).await {
                                                Ok(r) if r.code == 0 || r.code == 200 => {
                                                    tracing::warn!("cancelled orphan client_id={cid} exch={eid}");
                                                }
                                                Ok(r) => {
                                                    tracing::warn!(
                                                        "orphan cancel rejected code={} msg={}",
                                                        r.code,
                                                        r.message
                                                    );
                                                    if nm.hard_refresh(&rest).await.is_err() {
                                                        nok.store(false, Ordering::SeqCst);
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::warn!("orphan cancel send err: {e}");
                                                    if nm.hard_refresh(&rest).await.is_err() {
                                                        nok.store(false, Ordering::SeqCst);
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            nm.acknowledge_failure();
                                            tracing::warn!("orphan cancel sign failed: {e}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    prev_orphans = now_orphans;
                }
            });
        }

        tracing::warn!(
            "LIVE mode: order sending ENABLED for {} (market_id={})",
            self.market.symbol,
            mkt_id
        );
        Ok(LiveDeps {
            signer,
            nonce,
            sdk_lock,
            reconcile_notify,
            sender,
            _instance_lock: instance_lock,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_market() -> MarketConfig {
        MarketConfig {
            market_id: 1,
            symbol: "BTC".to_string(),
            price_tick: 0.1,
            amount_tick: 0.00001,
            min_base_amount: 0.0002,
            min_quote_amount: 10.0,
            price_decimals: 1,
            size_decimals: 5,
        }
    }

    fn test_config() -> Config {
        let mut cfg = Config {
            trading: crate::config::Trading::default(),
            performance: crate::config::Performance::default(),
            websocket: crate::config::WebsocketCfg::default(),
            safety: crate::config::Safety::default(),
        };
        cfg.trading.leverage = 2;
        cfg.trading.levels_per_side = 2;
        cfg.trading.base_amount = 0.0002;
        cfg.trading.capital_usage_percent = 0.15;
        cfg.trading.default_quote_update_threshold_bps = 8.0;
        cfg.trading.spread_factor_level1 = 1.0;
        cfg.trading.min_order_value_usd = 14.5;
        cfg.trading.position_value_threshold_usd = 1.0;
        cfg.performance.min_loop_interval = 0.0;
        cfg.safety.stale_order_poller_interval_sec = 3.0;
        cfg
    }

    fn test_hot_task(mode: Mode, reconcile_notify: Option<Arc<Notify>>) -> HotTask {
        let cfg = test_config();
        let (ops_tx, _ops_rx) = watch::channel(Vec::<BatchOp>::new());
        let (_evt_tx, evt_rx) = mpsc::unbounded_channel();
        HotTask::new(
            test_market(),
            &cfg,
            cfg.trading.base_amount,
            Arc::new(SharedAlpha::new(1)),
            Arc::new(SharedPosition::new()),
            Arc::new(Derived::new()),
            ops_tx,
            evt_rx,
            Arc::new(ArcSwapOption::empty()),
            Arc::new(ArcSwap::from_pointee(Vec::new())),
            reconcile_notify,
            mode,
        )
    }

    #[tokio::test]
    async fn live_position_change_holds_quotes_and_notifies_reconcile() {
        let notify = Arc::new(Notify::new());
        let mut hot = test_hot_task(Mode::Live, Some(notify.clone()));
        let now = Instant::now();

        assert!((hot.position_hold.as_secs_f64() - 10.0).abs() < f64::EPSILON);
        assert!(!hot.hold_quotes_for_position_reconcile(0.0, now));

        let changed_at = now + Duration::from_millis(1);
        assert!(hot.hold_quotes_for_position_reconcile(0.00046, changed_at));
        tokio::time::timeout(Duration::from_millis(10), notify.notified())
            .await
            .expect("position change notifies reconcile poller");

        assert!(hot.hold_quotes_for_position_reconcile(
            0.00046,
            changed_at + Duration::from_secs(1)
        ));
        assert!(!hot.hold_quotes_for_position_reconcile(
            0.00046,
            changed_at + hot.position_hold + Duration::from_millis(1)
        ));
        assert!(hot.position_hold_until.is_none());
    }

    #[test]
    fn shadow_position_change_does_not_hold_quotes() {
        let mut hot = test_hot_task(Mode::Shadow, None);
        let now = Instant::now();

        assert!(!hot.hold_quotes_for_position_reconcile(0.0, now));
        assert!(!hot.hold_quotes_for_position_reconcile(
            0.00046,
            now + Duration::from_millis(1)
        ));
        assert!(hot.position_hold_until.is_none());
    }
}

/// Sign + send an immediate cancel-all (used at startup and on shutdown for a clean book).
async fn cancel_all_orders(
    signer: &Signer,
    nonce: &NonceManager,
    rest: &RestClient,
    aki: i32,
) -> Result<()> {
    // IMMEDIATE cancel-all requires a nil time (the signer rejects a real timestamp with
    // "CancelAllTime should be nil"); Python passes timestamp_ms=0.
    let ts = 0i64;
    let n = nonce.next();
    let tx = match signer.sign_cancel_all_orders(
        crate::lighter::signer::CANCEL_ALL_TIF_IMMEDIATE,
        ts,
        n,
        aki,
    ) {
        Ok(t) => t,
        Err(e) => {
            nonce.acknowledge_failure();
            return Err(e);
        }
    };
    let resp = rest.send_tx(tx.tx_type, &tx.tx_info).await?;
    if resp.code != 0 && resp.code != 200 {
        anyhow::bail!(
            "cancel-all rejected: code={} msg={}",
            resp.code,
            resp.message
        );
    }
    tracing::info!("cancel-all OK (code={})", resp.code);
    Ok(())
}

/// Wait for SIGINT or SIGTERM (so service stop also triggers the clean shutdown path).
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match (
            signal(SignalKind::interrupt()),
            signal(SignalKind::terminate()),
        ) {
            (Ok(mut sigint), Ok(mut sigterm)) => {
                tokio::select! { _ = sigint.recv() => {}, _ = sigterm.recv() => {} }
            }
            _ => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Cancel-all on shutdown, retrying with nonce refresh, then REST-verify zero active orders
/// before returning (codex #5). Logs loudly if it cannot confirm a flat book.
/// Cancel-all, retrying with nonce refresh, then REST-verify zero active orders. Returns true
/// only if a flat book was confirmed. Used both at startup (abort live if false) and shutdown.
pub(crate) async fn cancel_all_and_verify(
    signer: &Signer,
    nonce: &NonceManager,
    rest: &RestClient,
    aki: i32,
    acct: i64,
    market_id: u32,
) -> bool {
    tracing::info!("cancelling all orders and verifying flat...");
    for attempt in 1..=5 {
        if let Err(e) = cancel_all_orders(signer, nonce, rest, aki).await {
            tracing::error!("cancel-all attempt {attempt}: {e}");
            let _ = nonce.hard_refresh(rest).await;
        }
        if let Ok(tok) = generate_ws_auth_token(signer, aki) {
            if let Ok(orders) = rest.account_active_orders(acct, market_id, &tok).await {
                if orders.is_empty() {
                    tracing::info!("verified 0 active orders");
                    return true;
                }
                tracing::warn!(
                    "{} orders still active after attempt {attempt}",
                    orders.len()
                );
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    }
    tracing::error!("WARNING: could NOT verify zero active orders — CHECK MANUALLY");
    false
}

/// One-shot channel->token map for an authed subscription.
fn auth_map(signer: &Signer, aki: i32, channel: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Ok(tok) = generate_ws_auth_token(signer, aki) {
        m.insert(channel.to_string(), tok);
    }
    m
}

fn age_for_log(age_ms: u64) -> String {
    if age_ms == u64::MAX {
        "none".to_string()
    } else {
        age_ms.to_string()
    }
}

fn spawn_live_health_logger(
    symbol: String,
    shared_alpha: Arc<SharedAlpha>,
    shared_bbo: Arc<SharedBbo>,
    shared_pos: Arc<SharedPosition>,
    derived: Arc<Derived>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await; // skip immediate tick; startup logs already describe initial state.
        loop {
            tick.tick().await;
            tracing::info!(
                target: "lighter_mm::health",
                "HEALTH symbol={} position={:.8} position_age_ms={} capital_usd={:.4} max_pos_usd={:.2} quota_remaining={:?} md_age_ms={} alpha={:.6} alpha_age_ms={} alpha_samples={} alpha_warmed={} bbo_mid={:.2} bbo_age_ms={} bbo_samples={} bbo_warmed={}",
                symbol,
                shared_pos.get(),
                age_for_log(shared_pos.age_ms()),
                derived.capital(),
                derived.max_pos_usd(),
                derived.quota(),
                age_for_log(derived.md_age_ms()),
                shared_alpha.alpha(),
                age_for_log(shared_alpha.age_ms()),
                shared_alpha.sample_count(),
                shared_alpha.warmed_up(),
                shared_bbo.mid(),
                age_for_log(shared_bbo.age_ms()),
                shared_bbo.sample_count(),
                shared_bbo.warmed_up(),
            );
        }
    });
}

/// Owned hot-path state + the synchronous decision logic.
struct HotTask {
    market: MarketConfig,
    book: LocalBook,
    calc: VolObiCalculator,
    om: OrderManager,
    base_amount: f64,
    num_levels: usize,
    factors: Vec<f64>,
    alpha_stale_ms: u64,
    fallback_bps: f64,
    min_loop_interval: f64,
    adverse_threshold_bps: f64,
    /// Base requote threshold (bps) from config; widened under quota pressure each cycle
    /// (Python `_adaptive_threshold_bps`). A modify is suppressed unless the price moves more.
    quote_threshold_bps: f64,
    inv_bias: crate::config::InventoryExitBias,
    /// Per-order size as a fraction of capital (Python `CAPITAL_USAGE_PERCENT`); drives the
    /// capital-derived dynamic base size in live mode.
    capital_usage_percent: f64,
    /// Minimum order value (USD) for quota generation; folds into the size min-quote floor.
    min_order_value_usd: f64,
    /// Wall-clock warmup: suppress ALL quoting for this many seconds after start (Python
    /// `WARMUP_SECONDS`), except a reduce-only exit if already holding inventory.
    warmup_seconds: f64,
    /// Set on the first book tick; the warmup window is measured from here.
    loop_start: Option<Instant>,
    /// Set true by the market-data WS `on_disconnect`; the next tick discards the stale book +
    /// vol/OBI state (Python resets the calc on DISCONNECT, not on in-connection snapshots).
    reset_flag: Arc<AtomicBool>,
    shared_alpha: Arc<SharedAlpha>,
    shared_pos: Arc<SharedPosition>,
    derived: Arc<Derived>,
    ops_tx: watch::Sender<Vec<BatchOp>>,
    evt_rx: mpsc::UnboundedReceiver<OrderEvent>,
    reconcile_swap: ReconcileSwap,
    tracked_ids: TrackedIds,
    reconcile_notify: Option<Arc<Notify>>,
    leverage: i32,
    mode: Mode,
    last_quote: Option<Instant>,
    last_seen_position: Option<f64>,
    position_hold_until: Option<Instant>,
    position_hold: Duration,
    mid: f64,
}

impl HotTask {
    #[allow(clippy::too_many_arguments)]
    fn new(
        market: MarketConfig,
        config: &Config,
        base_amount: f64,
        shared_alpha: Arc<SharedAlpha>,
        shared_pos: Arc<SharedPosition>,
        derived: Arc<Derived>,
        ops_tx: watch::Sender<Vec<BatchOp>>,
        evt_rx: mpsc::UnboundedReceiver<OrderEvent>,
        reconcile_swap: ReconcileSwap,
        tracked_ids: TrackedIds,
        reconcile_notify: Option<Arc<Notify>>,
        mode: Mode,
    ) -> Self {
        let t = &config.trading;
        let vo = &t.vol_obi;
        let cfg = VolObiConfig {
            window_steps: vo.window_steps,
            step_ns: vo.step_ns,
            vol_to_half_spread: vo.vol_to_half_spread,
            min_half_spread_bps: vo.min_half_spread_bps,
            c1_ticks: vo.c1_ticks,
            c1: 0.0,
            skew: vo.skew,
            looking_depth: vo.looking_depth,
            min_warmup_samples: vo.min_warmup_samples,
        };
        let num_levels = t.levels_per_side.max(1);
        let calc = VolObiCalculator::new(&cfg, market.price_tick, derived.max_pos_usd());
        Self {
            om: OrderManager::new(num_levels, market.amount_tick),
            factors: spread_factors(t.spread_factor_level1, num_levels),
            fallback_bps: vo.min_half_spread_bps.max(1.0),
            min_loop_interval: config.performance.min_loop_interval,
            adverse_threshold_bps: t.live_quality.adverse_threshold_bps,
            quote_threshold_bps: t.default_quote_update_threshold_bps,
            inv_bias: t.inventory_exit_bias.clone(),
            capital_usage_percent: t.capital_usage_percent,
            min_order_value_usd: t.min_order_value_usd,
            warmup_seconds: vo.warmup_seconds,
            loop_start: None,
            reset_flag: Arc::new(AtomicBool::new(false)),
            alpha_stale_ms: (t.alpha.stale_seconds * 1000.0) as u64,
            market,
            book: LocalBook::new(),
            calc,
            base_amount,
            num_levels,
            shared_alpha,
            shared_pos,
            derived,
            ops_tx,
            evt_rx,
            reconcile_swap,
            tracked_ids,
            reconcile_notify,
            leverage: t.leverage,
            mode,
            last_quote: None,
            last_seen_position: None,
            position_hold_until: None,
            position_hold: Duration::from_secs_f64(
                (config.safety.stale_order_poller_interval_sec.max(1.0)
                    + POSITION_RECONCILE_SETTLE_BUFFER_SEC)
                    .max(POSITION_RECONCILE_MIN_HOLD_SEC),
            ),
            mid: 0.0,
        }
    }

    fn hold_quotes_for_position_reconcile(&mut self, position: f64, now: Instant) -> bool {
        if self.mode != Mode::Live {
            return false;
        }
        if let Some(prev) = self.last_seen_position {
            if (position - prev).abs() >= crate::strategy::quotes::EPSILON {
                let until = now + self.position_hold;
                self.position_hold_until = Some(until);
                if let Some(notify) = &self.reconcile_notify {
                    notify.notify_one();
                }
                tracing::warn!(
                    "position changed {:.8} -> {:.8}; holding quote updates for {:.2}s pending reconcile",
                    prev,
                    position,
                    self.position_hold.as_secs_f64()
                );
            }
        }
        self.last_seen_position = Some(position);
        if let Some(until) = self.position_hold_until {
            if now < until {
                return true;
            }
            self.position_hold_until = None;
        }
        false
    }

    async fn run(mut self, config: Config, market: MarketConfig) {
        let channels = vec![
            format!("order_book/{}", market.market_id),
            format!("ticker/{}", market.market_id),
        ];
        let mut opts = SubscribeOptions::new("market-data", channels);
        opts.recv_timeout = config.websocket.recv_timeout;
        opts.reconnect_base = config.websocket.reconnect_base_delay;
        opts.reconnect_max = config.websocket.reconnect_max_delay;

        // Start the wall-clock warmup window now (Python `_loop_start_time` at loop start).
        self.loop_start = Some(Instant::now());
        // on_disconnect flag (set from a separate closure that cannot borrow `self`): the next
        // tick discards stale book + vol/OBI state. Mirrors Python resetting the calc on DISCONNECT.
        let reset_flag = self.reset_flag.clone();

        // The subscribe callback IS the synchronous hot path.
        subscribe_loop(
            opts,
            None,
            |data| self.on_message(data),
            move || {
                reset_flag.store(true, Ordering::SeqCst);
            },
        )
        .await;
    }

    fn on_message(&mut self, data: &serde_json::Value) {
        let mtype = data.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if mtype.contains("order_book") {
            if let Ok(msg) = serde_json::from_value::<OrderBookMsg>(data.clone()) {
                self.on_orderbook(msg);
            }
        } else if mtype.contains("ticker") {
            let _ = serde_json::from_value::<TickerMsg>(data.clone());
            // ticker used for sanity only; ignored in shadow.
        }
    }

    fn on_orderbook(&mut self, msg: OrderBookMsg) {
        // WS reconnected since the last tick — discard stale book + vol/OBI state so the upcoming
        // snapshot is treated as a fresh (re)initialization (Python resets the calc on DISCONNECT).
        if self.reset_flag.swap(false, Ordering::SeqCst) {
            self.book = LocalBook::new();
            self.calc.reset();
        }

        // Offset stale-guard for deltas.
        let offset = msg.effective_offset();
        let is_snapshot = msg.is_snapshot();
        if !is_snapshot {
            if let (Some(off), Some(last)) = (offset, self.book.last_offset) {
                if self.book.initialized && off <= last {
                    return; // stale/out-of-order delta
                }
            }
        }
        let bids: Vec<(f64, f64)> = msg.order_book.bids.iter().map(|l| l.parsed()).collect();
        let asks: Vec<(f64, f64)> = msg.order_book.asks.iter().map(|l| l.parsed()).collect();
        // Reset the vol/OBI calc ONLY on the FIRST snapshot of a connection (book not yet
        // initialized) — NOT on in-connection server snapshot refreshes, which would wipe the
        // accumulated volatility/OBI windows and re-trigger warmup (codex/audit: Python preserves
        // calc state across in-connection snapshots and resets only on disconnect, handled above).
        if is_snapshot || !self.book.initialized {
            let was_initialized = self.book.initialized;
            self.book.apply_snapshot(bids, asks);
            if !was_initialized {
                self.calc.reset();
            }
        } else {
            self.book.apply_delta(&bids, &asks);
        }
        if let Some(off) = offset {
            self.book.last_offset = Some(off);
        }

        // Stamp market-data freshness (read by the sender as a WS-health proxy for pause recovery).
        self.derived.set_md_now();

        // Hot signal update.
        if let Some(mid) = self.book.mid() {
            self.mid = mid;
            self.calc
                .on_book_update(mid, &self.book.bids, &self.book.asks);
            self.maybe_quote();
        }
    }

    fn maybe_quote(&mut self) {
        // Drain inbound order events (lossless) + the latest reconcile snapshot EVERY tick,
        // before the quote throttle, so order state never lags by min_loop_interval.
        while let Ok(evt) = self.evt_rx.try_recv() {
            self.om.apply_event(evt);
        }
        if let Some(orders) = self.reconcile_swap.swap(None) {
            self.om.process_reconcile(&orders);
        }

        // Throttle the (heavier) quote+collect step to min_loop_interval.
        let now = Instant::now();
        if let Some(t) = self.last_quote {
            if now.duration_since(t).as_secs_f64() < self.min_loop_interval {
                return;
            }
        }
        self.last_quote = Some(now);

        // Inject Binance alpha override (lock-free read).
        let ov = self.shared_alpha.usable_alpha(self.alpha_stale_ms);
        self.calc.set_alpha_override(ov);

        // Live feed-readiness gate (codex #7): never quote before capital + position snapshots
        // have arrived, so we never quote (or decide reduce-only) on a stale/zero position. This
        // runs BEFORE the warmup/warmed checks so the reduce-only-during-warmup bypass below has a
        // trustworthy position.
        if self.mode == Mode::Live
            && (self.derived.capital() <= 0.0 || self.shared_pos.age_ms() == u64::MAX)
        {
            let _ = self.ops_tx.send(Vec::new());
            return;
        }
        let mid = self.mid;
        let position = self.shared_pos.get();
        let capital = self.derived.capital();
        let tick = self.market.price_tick;
        if self.hold_quotes_for_position_reconcile(position, now) {
            let _ = self.ops_tx.send(Vec::new());
            return;
        }

        // Capital-derived dynamic order size (Python `calculate_dynamic_base_amount`): a fixed
        // fraction of capital * leverage / mid, normalized to exchange minimums. Falls back to the
        // static seed when capital is unknown (shadow). `capital_usage_percent` was dead config.
        let order_size = if capital > 0.0 {
            let raw = capital * self.capital_usage_percent * (self.leverage as f64) / mid;
            let min_quote = self.market.min_quote_amount.max(self.min_order_value_usd);
            normalize_live_order_size(
                raw,
                mid,
                self.market.amount_tick,
                self.market.min_base_amount,
                min_quote,
            )
        } else {
            self.base_amount
        };

        // Position limit from live capital + the ACTUAL (dynamic) order size (margin reserved for
        // the resting ladder scales with the real clip size).
        let max_pos_usd = if capital > 0.0 {
            let mp = dynamic_max_position(
                mid,
                capital,
                self.leverage,
                order_size,
                self.num_levels as i32,
            );
            self.derived.set_max_pos_usd(mp);
            mp
        } else {
            self.derived.max_pos_usd()
        };
        self.calc.set_max_position_dollar(max_pos_usd);

        let now_ns = crate::shared::now_ms() as i64 * 1_000_000;
        // Requote threshold: config base, widened under quota pressure (Python _adaptive_threshold_bps).
        // A resting order is only modified when the price moves more than this — sub-threshold ticks
        // are skipped (matches the Python; conserves quota, which is why it scales with quota).
        let threshold_bps = crate::exec::rate_limit::quota_adaptive_threshold_bps(
            self.quote_threshold_bps,
            self.derived.quota(),
        );

        // NOT READY for normal quoting if EITHER the count-based vol/OBI warmup is incomplete
        // (`!calc.warmed_up()`) OR we are inside the MANDATORY wall-clock warmup window (Python
        // `WARMUP_SECONDS`, live only). In both cases we suppress normal quotes but STILL emit a
        // passive reduce-only exit if we hold inventory (Python: reduce-only bypass works even
        // before the calc is warmed, since the exit is a fixed-bps fallback that needs no vol/OBI).
        // Shadow has no wall-clock warmup so it stays a fast verification tool.
        let in_wallclock_warmup = self.mode == Mode::Live
            && self
                .loop_start
                .map(|s| s.elapsed().as_secs_f64() < self.warmup_seconds)
                .unwrap_or(true);
        let not_ready = !self.calc.warmed_up() || in_wallclock_warmup;

        let levels = if not_ready {
            if position.abs() >= crate::strategy::quotes::EPSILON {
                // Holding inventory while not ready -> quote only a passive reduce-only exit.
                fallback_reduce_only(mid, position, tick, self.fallback_bps, self.num_levels)
            } else {
                // Flat and not ready -> no quotes at all.
                let _ = self.ops_tx.send(Vec::new());
                return;
            }
        } else {
            let l0 = self.calc.quote(mid, position);
            let mut lv = build_quote_levels(
                l0,
                mid,
                position,
                max_pos_usd,
                tick,
                self.num_levels,
                &self.factors,
                self.fallback_bps,
            );
            // Quality multiplier + inventory-exit bias. The live-metrics adverse-selection quality
            // loop is NOT used in the Python production path, so neutral 1.0 / 0.0 is correct parity.
            lv = apply_quality_spread_multiplier(&lv, mid, 1.0, tick);
            lv = apply_inventory_exit_bias(
                &lv,
                mid,
                position,
                max_pos_usd,
                0.0,
                self.adverse_threshold_bps,
                &self.inv_bias,
                tick,
            );
            lv
        };

        let ops =
            self.om
                .collect_order_operations(&levels, order_size, position, threshold_bps, now_ns);

        // KEYSTONE: occupy slots the instant ops are emitted (before signing/sending) so the
        // next quote cycle's collect can never duplicate an in-flight order. On send failure
        // the sender enqueues ClearLive to reset the slot; reconcile clears any that never go live.
        for op in &ops {
            self.om.mark_pending(op);
        }
        // Publish tracked client-ids for the reconcile poller's orphan check.
        self.tracked_ids
            .store(Arc::new(self.om.tracked_client_ids()));

        match self.mode {
            Mode::Shadow => {
                if let Some((b, a)) = levels.first().copied() {
                    tracing::info!(
                        "SHADOW vol={:.6} alpha={:.4} mid={:.4} L0_bid={:?} L0_ask={:?} ops={}",
                        self.calc.volatility(),
                        self.calc.alpha(),
                        mid,
                        b,
                        a,
                        ops.len()
                    );
                }
            }
            Mode::Live => {
                if !ops.is_empty() {
                    let _ = self.ops_tx.send(ops);
                }
            }
        }
    }
}
