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
use crate::exec::order_manager::OrderManager;
use crate::exec::paced_send::{self, SenderCtx};
use crate::exec::rate_limit::RateLimiter;
use crate::lighter::auth::generate_ws_auth_token;
use crate::lighter::messages::{
    AccountAllMsg, AccountOrdersMsg, OrderBookMsg, RemoteOrder, TickerMsg, UserStatsMsg,
};
use crate::lighter::nonce::NonceManager;
use crate::lighter::rest::{RestClient, BASE_URL};
use crate::lighter::signer::Signer;
use crate::lighter::tx_ws::TxWebSocket;
use crate::lighter::ws::{subscribe_loop, subscribe_loop_authed, SubscribeOptions, WS_URL};
use crate::risk::RiskController;
use crate::shared::{Derived, SharedAlpha, SharedBbo, SharedPosition};
use crate::strategy::quotes::{
    apply_inventory_exit_bias, apply_quality_spread_multiplier, build_quote_levels, spread_factors,
};
use crate::strategy::vol_obi::{VolObiCalculator, VolObiConfig};
use crate::types::{BatchOp, MarketConfig, OrderEvent};
use crate::util::dynamic_max_position;
use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use parking_lot::Mutex as PMutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, watch, Notify};

type ReconcileSwap = Arc<ArcSwapOption<Vec<RemoteOrder>>>;

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
            market.symbol, market.market_id, market.price_tick, market.amount_tick,
            market.min_base_amount, market.min_quote_amount
        );
        Ok(Self { config, creds, market, rest })
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
        derived.set_base_amount(base_amount);
        // Shadow: no capital feed -> effectively unlimited so quoting is exercised.
        derived.set_max_pos_usd(1.0e12);

        // --- Binance alpha feeds (cold) ---
        if self.config.trading.alpha.source.eq_ignore_ascii_case("binance") {
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

        if mode == Mode::Live {
            self.spawn_live_cold_tasks(shared_pos.clone(), derived.clone(), ops_rx, evt_tx, reconcile_swap.clone())
                .await?;
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
            mode,
        );
        let md_handle = tokio::spawn(hot.run(self.config.clone(), self.market.clone()));

        tokio::signal::ctrl_c().await.ok();
        tracing::info!("shutdown signal received");
        md_handle.abort();
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
    ) -> Result<()> {
        let aki = self.creds.api_key_index;
        let acct = self.creds.account_index;
        let mkt_id = self.market.market_id;
        if self.creds.api_key_private_key.is_empty() {
            anyhow::bail!("LIVE mode requires API_KEY_PRIVATE_KEY in .env");
        }
        let signers_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("signers");
        let signer = Arc::new(Signer::load(
            &signers_dir, BASE_URL, &self.creds.api_key_private_key, aki, acct,
        )?);
        let nonce = Arc::new(NonceManager::init(&self.rest, acct, aki).await?);
        let tx_ws = Arc::new(TxWebSocket::new(WS_URL));
        let _ = tx_ws.connect().await;
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
        let recv_to = self.config.websocket.account_recv_timeout;

        // Paced sender (mailbox -> rate gate -> sign -> send).
        tokio::spawn(paced_send::run(
            SenderCtx {
                signer: signer.clone(),
                nonce: nonce.clone(),
                rest: self.rest.clone(),
                tx_ws: tx_ws.clone(),
                market: self.market.clone(),
                derived: derived.clone(),
                risk: risk.clone(),
                reconcile: reconcile_notify.clone(),
                sdk_lock: sdk_lock.clone(),
                events: evt_tx.clone(),
            },
            rate,
            ops_rx,
        ));

        // account_orders -> reconcile snapshot (drives bind/clear in the hot task).
        {
            let rsw = reconcile_swap.clone();
            let sgn = signer.clone();
            let ch = format!("account_orders/{}/{}", mkt_id, acct);
            let ch_auth = ch.clone();
            let mut opts = SubscribeOptions::new("account_orders", vec![ch]);
            opts.recv_timeout = recv_to;
            tokio::spawn(async move {
                subscribe_loop_authed(
                    opts,
                    move || auth_map(&sgn, aki, &ch_auth),
                    move |data| {
                        if let Ok(msg) = serde_json::from_value::<AccountOrdersMsg>(data.clone()) {
                            let mut all = Vec::new();
                            for (_k, v) in msg.orders {
                                all.extend(v);
                            }
                            rsw.store(Some(Arc::new(all)));
                        }
                    },
                )
                .await;
            });
        }

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

        // REST reconcile stale-poller (correctness backstop; also fires on unknown-outcome).
        {
            let rsw = reconcile_swap.clone();
            let sgn = signer.clone();
            let rest = self.rest.clone();
            let notify = reconcile_notify.clone();
            let interval = self.config.safety.stale_order_poller_interval_sec.max(1.0);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = notify.notified() => {}
                        _ = tokio::time::sleep(std::time::Duration::from_secs_f64(interval)) => {}
                    }
                    if let Ok(tok) = generate_ws_auth_token(&sgn, aki) {
                        if let Ok(orders) = rest.account_active_orders(acct, mkt_id, &tok).await {
                            rsw.store(Some(Arc::new(orders)));
                        }
                    }
                }
            });
        }

        tracing::warn!("LIVE mode: order sending ENABLED for {} (market_id={})", self.market.symbol, mkt_id);
        Ok(())
    }
}

/// One-shot channel->token map for an authed subscription.
fn auth_map(signer: &Signer, aki: i32, channel: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Ok(tok) = generate_ws_auth_token(signer, aki) {
        m.insert(channel.to_string(), tok);
    }
    m
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
    inv_bias: crate::config::InventoryExitBias,
    shared_alpha: Arc<SharedAlpha>,
    shared_pos: Arc<SharedPosition>,
    derived: Arc<Derived>,
    ops_tx: watch::Sender<Vec<BatchOp>>,
    evt_rx: mpsc::UnboundedReceiver<OrderEvent>,
    reconcile_swap: ReconcileSwap,
    leverage: i32,
    mode: Mode,
    last_quote: Option<Instant>,
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
            inv_bias: t.inventory_exit_bias.clone(),
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
            leverage: t.leverage,
            mode,
            last_quote: None,
            mid: 0.0,
        }
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

        // The subscribe callback IS the synchronous hot path.
        subscribe_loop(
            opts,
            None,
            |data| self.on_message(data),
            || {
                // on_disconnect: reset book + vol state (matches Python).
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
        if is_snapshot || !self.book.initialized {
            self.book.apply_snapshot(bids, asks);
            self.calc.reset();
        } else {
            self.book.apply_delta(&bids, &asks);
        }
        if let Some(off) = offset {
            self.book.last_offset = Some(off);
        }

        // Hot signal update.
        if let Some(mid) = self.book.mid() {
            self.mid = mid;
            self.calc.on_book_update(mid, &self.book.bids, &self.book.asks);
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

        if !self.calc.warmed_up() {
            return;
        }
        let mid = self.mid;
        let position = self.shared_pos.get();
        // Recompute the position limit from live capital (user_stats) + current mid.
        let capital = self.derived.capital();
        let max_pos_usd = if capital > 0.0 {
            let mp = dynamic_max_position(mid, capital, self.leverage, self.base_amount, self.num_levels as i32);
            self.derived.set_max_pos_usd(mp);
            mp
        } else {
            self.derived.max_pos_usd()
        };
        self.calc.set_max_position_dollar(max_pos_usd);

        let l0 = self.calc.quote(mid, position);
        let mut levels = build_quote_levels(
            l0, mid, position, max_pos_usd, self.market.price_tick, self.num_levels, &self.factors,
            self.fallback_bps,
        );
        // Quality multiplier + inventory exit bias (adverse_bps=0 without live metrics in shadow).
        levels = apply_quality_spread_multiplier(&levels, mid, 1.0, self.market.price_tick);
        levels = apply_inventory_exit_bias(
            &levels, mid, position, max_pos_usd, 0.0, self.adverse_threshold_bps, &self.inv_bias,
            self.market.price_tick,
        );

        let now_ns = crate::shared::now_ms() as i64 * 1_000_000;
        let ops = self.om.collect_order_operations(&levels, self.base_amount, position, 8.0, now_ns);

        match self.mode {
            Mode::Shadow => {
                if let (Some((b, _)), Some((_, a))) = (levels.first().copied(), levels.first().copied()) {
                    tracing::info!(
                        "SHADOW vol={:.6} alpha={:.4} mid={:.4} L0_bid={:?} L0_ask={:?} ops={}",
                        self.calc.volatility(), self.calc.alpha(), mid, b, a, ops.len()
                    );
                }
                // Shadow never sends; but exercise optimistic binding so collect dedupes.
                for op in &ops {
                    if matches!(op.action, crate::types::OrderAction::Create | crate::types::OrderAction::Modify) {
                        self.om.apply_event(OrderEvent::BindLive {
                            side: op.side, level: op.level, client_order_id: op.client_order_id,
                            exchange_id: Some(op.client_order_id), price: op.price, size: op.size,
                        });
                    }
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
