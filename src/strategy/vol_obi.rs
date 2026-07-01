//! Vol + OBI quote engine — port of `_vol_obi_fast.pyx::VolObiCalculator`.
//!
//! Hot path: `on_book_update(now_ns, mid, bids, asks)` per orderbook tick updates rolling vol
//! and OBI z-score (alpha). Trading loop: `quote(mid, position)` returns bid/ask in dollars.
//! Binance external alpha is injected via `set_alpha_override`.
//!
//! CADENCE (deliberate divergence from the raw pyx port): samples are pushed into the rolling
//! windows on the `step_ns` clock, NOT on every raw book message. `vol_scale =
//! sqrt(1e9/step_ns)` annualizes per-sample mid changes assuming step_ns spacing; Lighter
//! publishes ~2x faster than the assumed 10/s, which silently underscaled volatility by
//! ~sqrt(2) (spreads ~30% too tight) when every message was pushed. The z-score of the
//! CURRENT imbalance is still refreshed every tick for alpha responsiveness — only the
//! window contents are cadence-sampled.

use crate::book::local_book::BookSide;
use crate::strategy::rolling::RollingStats;
use crate::util::clamp;

/// Static config (from config.json `trading.vol_obi`).
#[derive(Debug, Clone)]
pub struct VolObiConfig {
    pub window_steps: usize,
    pub step_ns: i64,
    pub vol_to_half_spread: f64,
    pub min_half_spread_bps: f64,
    pub c1_ticks: f64,
    /// Optional absolute c1 override (dollars); if > 0 used instead of c1_ticks*tick.
    pub c1: f64,
    pub skew: f64,
    pub looking_depth: f64,
    pub min_warmup_samples: i64,
}

impl Default for VolObiConfig {
    fn default() -> Self {
        // Matches pyx __init__ defaults.
        Self {
            window_steps: 6000,
            step_ns: 100_000_000,
            vol_to_half_spread: 0.8,
            min_half_spread_bps: 2.0,
            c1_ticks: 160.0,
            c1: 0.0,
            skew: 1.0,
            looking_depth: 0.025,
            min_warmup_samples: 100,
        }
    }
}

#[derive(Debug)]
pub struct VolObiCalculator {
    mid_stats: RollingStats,
    imb_stats: RollingStats,
    prev_mid: f64,
    has_prev_mid: bool,
    volatility: f64,
    alpha: f64,
    local_alpha: f64,
    alpha_override: f64,
    has_alpha_override: bool,
    warmed_up: bool,
    total_samples: i64,

    // config / derived
    tick_size: f64,
    step_ns: i64,
    /// Timestamp (ns) of the last accepted sample; 0 = none yet.
    last_sample_ns: i64,
    vol_scale: f64,
    vol_to_half_spread: f64,
    min_half_spread_bps: f64,
    c1: f64,
    skew: f64,
    looking_depth: f64,
    min_warmup_samples: i64,
    max_position_dollar: f64,
}

impl VolObiCalculator {
    pub fn new(cfg: &VolObiConfig, tick_size: f64, max_position_dollar: f64) -> Self {
        assert!(tick_size > 0.0, "tick_size must be positive");
        let c1 = if cfg.c1 > 0.0 {
            cfg.c1
        } else {
            cfg.c1_ticks * tick_size
        };
        Self {
            mid_stats: RollingStats::new(cfg.window_steps),
            imb_stats: RollingStats::new(cfg.window_steps),
            prev_mid: 0.0,
            has_prev_mid: false,
            volatility: 0.0,
            alpha: 0.0,
            local_alpha: 0.0,
            alpha_override: 0.0,
            has_alpha_override: false,
            warmed_up: false,
            total_samples: 0,
            tick_size,
            step_ns: cfg.step_ns.max(1),
            last_sample_ns: 0,
            vol_scale: (1_000_000_000.0 / cfg.step_ns as f64).sqrt(),
            vol_to_half_spread: cfg.vol_to_half_spread,
            min_half_spread_bps: cfg.min_half_spread_bps,
            c1,
            skew: cfg.skew,
            looking_depth: cfg.looking_depth,
            min_warmup_samples: cfg.min_warmup_samples,
            max_position_dollar,
        }
    }

    /// Hot path. Feed a new mid + book sides with a monotonic-ish ns timestamp.
    /// Window samples are accepted only every `step_ns` (see module docs); the alpha
    /// z-score of the current imbalance refreshes every call.
    #[inline]
    pub fn on_book_update(&mut self, now_ns: i64, mid_price: f64, bids: &BookSide, asks: &BookSide) {
        let sample_due = self.last_sample_ns == 0 || now_ns - self.last_sample_ns >= self.step_ns;

        // 1. mid-price change over one step -> volatility input (dollars). `prev_mid` is
        // the mid at the PREVIOUS SAMPLE, so each pushed change spans ~step_ns regardless
        // of the raw message rate.
        if sample_due {
            if self.has_prev_mid {
                let change = mid_price - self.prev_mid;
                self.mid_stats.push(change);
                self.total_samples += 1;
            }
            self.prev_mid = mid_price;
            self.has_prev_mid = true;
            // Schedule the next sample step_ns from NOW (not last+step): after a quiet gap
            // this avoids a burst of sub-step-spaced samples "catching up".
            self.last_sample_ns = now_ns;
        }

        // 2. OBI imbalance -> alpha input (quantity units). Computed every tick (the
        // z-score below keeps alpha responsive); pushed into the window on the step clock.
        let lower = mid_price * (1.0 - self.looking_depth);
        let upper = mid_price * (1.0 + self.looking_depth);
        let imbalance = bids.sum_sizes_from(lower) - asks.sum_sizes_to(upper);
        if sample_due {
            self.imb_stats.push(imbalance);
        }

        // 3. update cached vol & alpha once warmed up
        if self.total_samples >= self.min_warmup_samples {
            self.warmed_up = true;
            if sample_due {
                self.volatility = self.mid_stats.std() * self.vol_scale;
            }
            self.local_alpha = self.imb_stats.zscore(imbalance);
            self.alpha = if self.has_alpha_override {
                self.alpha_override
            } else {
                self.local_alpha
            };
        }
    }

    /// Trading loop. Returns (bid, ask) in dollars, or None if not warmed / crossed.
    /// Mirrors `quote`.
    pub fn quote(&self, mid_price: f64, position_size: f64) -> Option<(f64, f64)> {
        if !self.warmed_up {
            return None;
        }
        let tick = self.tick_size;

        let half_spread_price = self.volatility * self.vol_to_half_spread;
        let half_spread_tick = half_spread_price / tick;

        let fair_price = mid_price + self.c1 * self.alpha;

        let norm_pos = if self.max_position_dollar > 0.0 {
            clamp((position_size * mid_price) / self.max_position_dollar, -1.0, 1.0)
        } else {
            0.0
        };

        let mut bid_depth_tick = half_spread_tick * (1.0 + self.skew * norm_pos);
        let mut ask_depth_tick = half_spread_tick * (1.0 - self.skew * norm_pos);
        if bid_depth_tick < 0.0 {
            bid_depth_tick = 0.0;
        }
        if ask_depth_tick < 0.0 {
            ask_depth_tick = 0.0;
        }

        let mut raw_bid = fair_price - bid_depth_tick * tick;
        let mut raw_ask = fair_price + ask_depth_tick * tick;

        // min spread floor in bps
        if self.min_half_spread_bps > 0.0 {
            let min_bid = mid_price * (1.0 - self.min_half_spread_bps / 10_000.0);
            if raw_bid > min_bid {
                raw_bid = min_bid;
            }
            let min_ask = mid_price * (1.0 + self.min_half_spread_bps / 10_000.0);
            if raw_ask < min_ask {
                raw_ask = min_ask;
            }
        }

        // snap to tick grid
        let bid_price = (raw_bid / tick).floor() * tick;
        let ask_price = (raw_ask / tick).ceil() * tick;

        if bid_price >= ask_price {
            return None;
        }
        Some((bid_price, ask_price))
    }

    /// Inject external alpha (e.g. Binance OBI). `None` reverts to local alpha.
    pub fn set_alpha_override(&mut self, alpha: Option<f64>) {
        match alpha {
            None => {
                self.has_alpha_override = false;
                if self.warmed_up {
                    self.alpha = self.local_alpha;
                }
            }
            Some(a) => {
                self.has_alpha_override = true;
                self.alpha_override = a;
                if self.warmed_up {
                    self.alpha = a;
                }
            }
        }
    }

    pub fn set_max_position_dollar(&mut self, value: f64) {
        self.max_position_dollar = if value > 0.0 { value } else { 0.0 };
    }

    pub fn reset(&mut self) {
        self.mid_stats.clear();
        self.imb_stats.clear();
        self.prev_mid = 0.0;
        self.has_prev_mid = false;
        self.volatility = 0.0;
        self.alpha = 0.0;
        self.local_alpha = 0.0;
        self.has_alpha_override = false;
        self.warmed_up = false;
        self.total_samples = 0;
        self.last_sample_ns = 0;
    }

    #[inline]
    pub fn warmed_up(&self) -> bool {
        self.warmed_up
    }
    #[inline]
    pub fn volatility(&self) -> f64 {
        self.volatility
    }
    #[inline]
    pub fn alpha(&self) -> f64 {
        self.alpha
    }
    #[inline]
    pub fn vol_scale(&self) -> f64 {
        self.vol_scale
    }
    #[inline]
    pub fn total_samples(&self) -> i64 {
        self.total_samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::local_book::LocalBook;

    #[test]
    fn not_warmed_returns_none() {
        let c = VolObiCalculator::new(&VolObiConfig::default(), 0.1, 1000.0);
        assert_eq!(c.quote(100.0, 0.0), None);
    }

    #[test]
    fn warms_up_and_quotes() {
        let cfg = VolObiConfig {
            min_warmup_samples: 5,
            window_steps: 100,
            min_half_spread_bps: 0.0,
            vol_to_half_spread: 1.0,
            c1_ticks: 0.0,
            skew: 0.0,
            ..Default::default()
        };
        let mut c = VolObiCalculator::new(&cfg, 0.1, 1_000_000.0);
        let mut book = LocalBook::new();
        book.apply_snapshot(
            vec![(99.0, 1.0), (100.0, 1.0)],
            vec![(101.0, 1.0), (102.0, 1.0)],
        );
        // feed > warmup samples with varying mids to create nonzero vol; timestamps advance
        // one full step per update so every update is sampled.
        for i in 0..10 {
            let mid = 100.5 + (i as f64) * 0.1;
            c.on_book_update((i as i64 + 1) * 100_000_000, mid, &book.bids, &book.asks);
        }
        assert!(c.warmed_up());
        assert!(c.volatility() > 0.0);
        let q = c.quote(101.0, 0.0);
        assert!(q.is_some());
        let (bid, ask) = q.unwrap();
        assert!(bid < ask);
    }

    #[test]
    fn samples_on_step_clock_not_per_message() {
        // Messages arriving at 2x the step rate (50ms vs step_ns=100ms) must be sampled
        // every OTHER message — pushing every message underscaled volatility by ~sqrt(2).
        let cfg = VolObiConfig {
            min_warmup_samples: 1,
            window_steps: 100,
            ..Default::default()
        };
        let mut c = VolObiCalculator::new(&cfg, 0.1, 1000.0);
        let mut book = LocalBook::new();
        book.apply_snapshot(vec![(99.0, 1.0)], vec![(101.0, 1.0)]);
        for i in 0..20i64 {
            c.on_book_update(
                (i + 1) * 50_000_000,
                100.0 + i as f64 * 0.1,
                &book.bids,
                &book.asks,
            );
        }
        // Accepted samples at i=0,2,4,...,18 (10 total); the first only seeds prev_mid.
        assert_eq!(c.total_samples(), 9);
    }

    #[test]
    fn alpha_override_roundtrip() {
        let cfg = VolObiConfig {
            min_warmup_samples: 2,
            window_steps: 50,
            ..Default::default()
        };
        let mut c = VolObiCalculator::new(&cfg, 0.1, 1000.0);
        let mut book = LocalBook::new();
        book.apply_snapshot(vec![(99.0, 1.0)], vec![(101.0, 1.0)]);
        for i in 0..5 {
            c.on_book_update((i as i64 + 1) * 100_000_000, 100.0, &book.bids, &book.asks);
        }
        c.set_alpha_override(Some(2.5));
        assert!((c.alpha() - 2.5).abs() < 1e-12);
        c.set_alpha_override(None);
        // reverts to local alpha (0 here since imbalance constant)
        assert!((c.alpha() - c.alpha()).abs() < 1e-12);
    }
}
