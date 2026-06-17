//! Live quality metrics tracker — markouts and defensive quote adjustments.
//!
//! Port of the live-quoting–relevant parts of `lighter_MM/live_metrics.py`
//! (`LiveMetricsTracker`). This is the hot-path consumer side that feeds back into
//! quoting: it tracks per-horizon markouts of recent fills and derives a
//! [`QualityAdjustment`] (spread widen / size reduce) when realized adverse
//! selection exceeds a threshold.
//!
//! What is ported (vs. the Python module):
//! - `record_fill` -> enqueues a [`PendingFill`] into a bounded `_pending` deque.
//! - `update` / `_settle_markouts` -> ages out pending fills past each horizon and
//!   appends `(ts_ms, markout_bps, adverse_bps)` into bounded per-horizon windows.
//! - `_compute_adjustment` -> the spread/size multiplier logic, including the
//!   long-horizon adverse boost (`max(adverse, long_adverse * 0.75)`).
//! - `_prune` -> window-second eviction of stale markout samples.
//!
//! What is intentionally NOT ported (it does not feed live quoting): the durable
//! JSON state store, the `snapshot`/`flush_metrics` live-metrics JSON, the
//! inventory / spread-capture / fill-event bookkeeping, and the live "score".
//! The markout CSV writer is left as an optional stub ([`LiveMetricsTracker::set_markout_csv`]).
//!
//! Timing parity note: the Python module uses `time.monotonic()` (seconds). Here
//! [`LiveMetricsTracker::update`] takes `now_ms: u64` (epoch milliseconds, matching
//! [`crate::shared::now_ms`]), and horizons (seconds) are converted to milliseconds
//! for age comparisons. The markout/adverse formulas are unchanged.

use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;

use crate::config::LiveQuality;
use crate::types::Side;
use crate::util::clamp;

/// Maximum number of un-settled fills retained (mirrors Python `deque(maxlen=2_000)`).
const PENDING_CAP: usize = 2_000;
/// Per-horizon rolling window capacity (mirrors Python `deque(maxlen=10_000)`).
const MARKOUT_CAP: usize = 10_000;
/// Minimum settled markout samples at the adaptive horizon before we adjust quotes.
/// Mirrors the Python guard `if len(samples) < 4`.
const MIN_SAMPLE_COUNT: usize = 4;
/// Weight applied to the long-horizon adverse mean (Python `long_adverse * 0.75`).
const LONG_HORIZON_WEIGHT: f64 = 0.75;

/// A recorded fill awaiting markout settlement at one or more horizons.
#[derive(Debug, Clone)]
struct PendingFill {
    /// Epoch milliseconds at which the fill was recorded.
    recorded_at_ms: u64,
    side: Side,
    price: f64,
    size: f64,
    mid_at_fill: Option<f64>,
    /// Horizons (seconds) already settled for this fill.
    settled_horizons: Vec<f64>,
}

/// Defensive quote adjustment derived from recent adverse selection.
///
/// Mirrors Python `QualityAdjustment`. Neutral default is `1.0 / 1.0`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QualityAdjustment {
    /// Multiplier applied to the half-spread (>= 1.0 widens).
    pub spread_multiplier: f64,
    /// Multiplier applied to order size (<= 1.0 shrinks).
    pub size_multiplier: f64,
    /// Mean adverse bps used for the decision (at the adaptive horizon, boosted).
    pub adverse_bps: f64,
    /// Number of markout samples backing the decision.
    pub sample_count: usize,
    /// Human-readable reason tag (parity with Python `reason`).
    pub reason: &'static str,
}

impl Default for QualityAdjustment {
    #[inline]
    fn default() -> Self {
        Self {
            spread_multiplier: 1.0,
            size_multiplier: 1.0,
            adverse_bps: 0.0,
            sample_count: 0,
            reason: "neutral",
        }
    }
}

/// One settled markout sample: `(ts_ms, markout_bps, adverse_bps)`.
type MarkoutSample = (u64, f64, f64);

/// Tracks markouts and derives defensive quote adjustments.
///
/// Construct with [`LiveMetricsTracker::new`] from a [`LiveQuality`] config slice.
#[derive(Debug)]
pub struct LiveMetricsTracker {
    // --- config (validated, mirrors Python `__init__` clamping) ---
    /// Positive, ascending, de-duplicated horizons in seconds.
    horizons: Vec<f64>,
    window_ms: u64,
    adaptive_enabled: bool,
    adaptive_horizon: f64,
    adverse_threshold_bps: f64,
    spread_widen_per_bps: f64,
    max_spread_multiplier: f64,
    size_reduce_per_bps: f64,
    min_size_multiplier: f64,

    // --- state ---
    pending: VecDeque<PendingFill>,
    /// Per-horizon rolling windows, index-aligned with `horizons`.
    markouts: Vec<VecDeque<MarkoutSample>>,
    last_adjustment: QualityAdjustment,

    // --- optional CSV sink (stub) ---
    markout_csv: Option<PathBuf>,
}

impl LiveMetricsTracker {
    /// Build a tracker from the `[trading.live_quality]` config.
    ///
    /// Mirrors the validation in the Python `__init__`:
    /// - horizons: keep `> 0`, sorted ascending, de-duplicated;
    /// - `window_seconds = max(window_seconds, max_horizon)` (default 1.0 if empty);
    /// - `spread_widen_per_bps >= 0`, `max_spread_multiplier >= 1.0`,
    ///   `size_reduce_per_bps >= 0`, `min_size_multiplier` clamped to `[0.05, 1.0]`.
    pub fn new(cfg: LiveQuality) -> Self {
        // Positive, sorted, de-duplicated horizons.
        let mut horizons: Vec<f64> = cfg
            .markout_horizons_sec
            .iter()
            .copied()
            .filter(|h| *h > 0.0)
            .collect();
        horizons.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        horizons.dedup_by(|a, b| (*a - *b).abs() < f64::EPSILON);

        let max_horizon = horizons.iter().copied().fold(1.0_f64, f64::max);
        let window_seconds = cfg.window_seconds.max(max_horizon);

        let n = horizons.len();
        Self {
            horizons,
            window_ms: (window_seconds * 1000.0) as u64,
            adaptive_enabled: cfg.adaptive_enabled,
            adaptive_horizon: cfg.adaptive_horizon_sec,
            adverse_threshold_bps: cfg.adverse_threshold_bps,
            spread_widen_per_bps: cfg.spread_widen_per_adverse_bps.max(0.0),
            max_spread_multiplier: cfg.max_spread_multiplier.max(1.0),
            size_reduce_per_bps: cfg.size_reduce_per_adverse_bps.max(0.0),
            min_size_multiplier: clamp(cfg.min_size_multiplier, 0.05, 1.0),
            pending: VecDeque::with_capacity(PENDING_CAP.min(256)),
            markouts: (0..n).map(|_| VecDeque::new()).collect(),
            last_adjustment: QualityAdjustment::default(),
            markout_csv: None,
        }
    }

    /// Optionally enable markout CSV output. The header is written lazily on the
    /// first settled row (or here if the file is absent/empty). Errors are
    /// swallowed — observability must never break quoting.
    pub fn set_markout_csv(&mut self, path: PathBuf) {
        if let Some(dir) = path.parent() {
            let _ = fs::create_dir_all(dir);
        }
        let needs_header = match fs::metadata(&path) {
            Ok(m) => m.len() == 0,
            Err(_) => true,
        };
        if needs_header {
            if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
                let _ = writeln!(
                    f,
                    "timestamp_ms,side,fill_price,size,mid_at_fill,mid_at_markout,horizon_sec,markout_bps,adverse_bps"
                );
            }
        }
        self.markout_csv = Some(path);
    }

    /// Record a fill for later markout settlement.
    ///
    /// Mirrors Python `record_fill`: rejects `price <= 0` or `size <= 0`, then
    /// enqueues a pending observation (bounded; the oldest is dropped at capacity).
    ///
    /// `now_ms` is epoch milliseconds (use [`crate::shared::now_ms`]).
    pub fn record_fill(
        &mut self,
        side: Side,
        price: f64,
        size: f64,
        mid_at_fill: Option<f64>,
        now_ms: u64,
    ) {
        if !(price > 0.0) || !(size > 0.0) {
            return;
        }
        if self.pending.len() >= PENDING_CAP {
            self.pending.pop_front();
        }
        self.pending.push_back(PendingFill {
            recorded_at_ms: now_ms,
            side,
            price,
            size,
            mid_at_fill: finite(mid_at_fill),
            settled_horizons: Vec::new(),
        });
    }

    /// Advance time: settle any pending fills that have aged past a horizon, prune
    /// stale samples, and recompute the cached adjustment.
    ///
    /// Mirrors the live-quoting–relevant parts of Python `update`. `mid <= 0` (or
    /// non-finite) skips settlement (no valid mark) but still recomputes the
    /// adjustment from existing samples.
    pub fn update(&mut self, now_ms: u64, mid: f64) {
        if mid.is_finite() && mid > 0.0 {
            self.settle_markouts(now_ms, mid);
        }
        self.prune(now_ms);
        self.last_adjustment = self.compute_adjustment();
    }

    /// The most recently computed quality adjustment (cached by [`Self::update`]).
    #[inline]
    pub fn current_adjustment(&self) -> QualityAdjustment {
        self.last_adjustment
    }

    /// For each pending fill and each horizon not yet settled, once the fill has
    /// aged past the horizon compute the markout and append to the per-horizon
    /// window. Fully-settled fills are dropped. Mirrors Python `_settle_markouts`.
    ///
    /// `markout_bps`:
    /// - buy:  `(mid - price) / price * 1e4`
    /// - sell: `(price - mid) / price * 1e4`
    ///
    /// `adverse_bps = max(0, -markout_bps)`.
    fn settle_markouts(&mut self, now_ms: u64, mid: f64) {
        // Collect rows for optional CSV after the borrow on `self.pending` ends.
        let mut csv_rows: Vec<String> = Vec::new();

        // Iterate the pending deque in place; rebuild the survivors.
        let mut keep: VecDeque<PendingFill> = VecDeque::with_capacity(self.pending.len());
        let pending = std::mem::take(&mut self.pending);
        for mut obs in pending {
            for (hi, &horizon) in self.horizons.iter().enumerate() {
                let already = obs
                    .settled_horizons
                    .iter()
                    .any(|s| (*s - horizon).abs() < f64::EPSILON);
                let age_ms = now_ms.saturating_sub(obs.recorded_at_ms);
                let horizon_ms = (horizon * 1000.0) as u64;
                if already || age_ms < horizon_ms {
                    continue;
                }
                let markout_bps = match obs.side {
                    Side::Buy => (mid - obs.price) / obs.price * 10_000.0,
                    Side::Sell => (obs.price - mid) / obs.price * 10_000.0,
                };
                let adverse_bps = (-markout_bps).max(0.0);
                obs.settled_horizons.push(horizon);

                let win = &mut self.markouts[hi];
                if win.len() >= MARKOUT_CAP {
                    win.pop_front();
                }
                win.push_back((now_ms, markout_bps, adverse_bps));

                if self.markout_csv.is_some() {
                    let mid_at_fill = obs
                        .mid_at_fill
                        .map(|m| format!("{m:.10}"))
                        .unwrap_or_default();
                    csv_rows.push(format!(
                        "{},{},{:.10},{:.8},{},{:.10},{:.3},{:.6},{:.6}",
                        now_ms,
                        obs.side.as_str(),
                        obs.price,
                        obs.size,
                        mid_at_fill,
                        mid,
                        horizon,
                        markout_bps,
                        adverse_bps,
                    ));
                }
            }
            if obs.settled_horizons.len() < self.horizons.len() {
                keep.push_back(obs);
            }
        }
        self.pending = keep;

        if let Some(path) = &self.markout_csv {
            if !csv_rows.is_empty() {
                if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) {
                    for row in csv_rows {
                        let _ = writeln!(f, "{row}");
                    }
                }
            }
        }
    }

    /// Evict markout samples older than `window_ms`. Mirrors Python `_prune`
    /// (restricted to the per-horizon markout windows, the only state we keep).
    fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(self.window_ms);
        for win in &mut self.markouts {
            while let Some(&(ts, _, _)) = win.front() {
                if ts < cutoff {
                    win.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    /// Index of the horizon nearest `target` (Python `min(..., key=|h-target|)`).
    /// Returns `None` when there are no horizons / no samples at all.
    fn nearest_horizon_idx(&self, target: f64) -> Option<usize> {
        if self.markouts.iter().all(|w| w.is_empty()) {
            return None;
        }
        self.horizons
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (*a - target)
                    .abs()
                    .partial_cmp(&(*b - target).abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(i, _)| i)
    }

    /// Mean adverse bps over a horizon's window.
    fn mean_adverse(&self, idx: usize) -> f64 {
        let win = &self.markouts[idx];
        if win.is_empty() {
            return 0.0;
        }
        let sum: f64 = win.iter().map(|s| s.2).sum();
        sum / win.len() as f64
    }

    /// Derive the quality adjustment from recent adverse selection.
    ///
    /// Mirrors Python `_compute_adjustment`:
    /// 1. disabled -> neutral with reason "disabled".
    /// 2. fewer than [`MIN_SAMPLE_COUNT`] samples at the adaptive horizon -> neutral
    ///    "insufficient_markouts".
    /// 3. `adverse = mean(adverse @ adaptive_horizon)`, then if a strictly longer
    ///    horizon exists with enough samples, `adverse = max(adverse, long * 0.75)`.
    /// 4. `excess = max(0, adverse - threshold)`; `excess <= 0` -> neutral "healthy".
    /// 5. otherwise widen spread / reduce size, each clamped.
    fn compute_adjustment(&self) -> QualityAdjustment {
        if !self.adaptive_enabled {
            return QualityAdjustment {
                reason: "disabled",
                ..Default::default()
            };
        }
        let Some(idx) = self.nearest_horizon_idx(self.adaptive_horizon) else {
            return QualityAdjustment {
                reason: "insufficient_markouts",
                ..Default::default()
            };
        };
        let count = self.markouts[idx].len();
        if count < MIN_SAMPLE_COUNT {
            return QualityAdjustment {
                sample_count: count,
                reason: "insufficient_markouts",
                ..Default::default()
            };
        }

        let mut adverse = self.mean_adverse(idx);

        // Long-horizon boost: max(adverse, long_adverse * 0.75).
        let long_horizon = self.horizons.iter().copied().fold(self.adaptive_horizon, f64::max);
        if (long_horizon - self.adaptive_horizon).abs() > f64::EPSILON {
            if let Some(long_idx) = self.nearest_horizon_idx(long_horizon) {
                if self.markouts[long_idx].len() >= MIN_SAMPLE_COUNT {
                    let long_adverse = self.mean_adverse(long_idx);
                    adverse = adverse.max(long_adverse * LONG_HORIZON_WEIGHT);
                }
            }
        }

        let excess = (adverse - self.adverse_threshold_bps).max(0.0);
        if excess <= 0.0 {
            return QualityAdjustment {
                spread_multiplier: 1.0,
                size_multiplier: 1.0,
                adverse_bps: adverse,
                sample_count: count,
                reason: "healthy",
            };
        }

        // spread_multiplier = clamp(1 + excess*widen, 1.0, max_spread_multiplier)
        let spread_multiplier =
            (1.0 + excess * self.spread_widen_per_bps).min(self.max_spread_multiplier);
        // size_multiplier = clamp(1 - excess*reduce, min_size_multiplier, 1.0)
        let size_multiplier =
            (1.0 - excess * self.size_reduce_per_bps).max(self.min_size_multiplier);

        QualityAdjustment {
            spread_multiplier,
            size_multiplier,
            adverse_bps: adverse,
            sample_count: count,
            reason: "adverse_markout",
        }
    }
}

/// `Some(x)` iff `x` is finite. Mirrors Python `_finite`.
#[inline]
fn finite(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v.is_finite() => Some(v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LiveQuality;

    fn cfg() -> LiveQuality {
        // Matches the Python defaults / config defaults.
        LiveQuality::default()
    }

    /// Buy fills followed by a worse (lower) mid produce adverse selection: the
    /// adverse mean clears the threshold, so spread widens (>1) and size shrinks
    /// (<1, clamped at the floor for a large enough move).
    #[test]
    fn buy_fills_adverse_widen_and_shrink() {
        let mut t = LiveMetricsTracker::new(cfg());
        // Record several buy fills at price 100.
        let t0 = 1_000_000u64;
        for i in 0..6 {
            t.record_fill(Side::Buy, 100.0, 1.0, Some(100.0), t0 + i);
        }
        // 30s adaptive horizon. Advance 31s with a mid 1% below fill: markout_bps
        // for a buy = (99 - 100)/100 * 1e4 = -100 bps, adverse = 100 bps.
        t.update(t0 + 31_000, 99.0);
        let adj = t.current_adjustment();

        assert_eq!(adj.reason, "adverse_markout");
        assert!(adj.sample_count >= MIN_SAMPLE_COUNT);
        assert!(
            adj.adverse_bps > 2.0,
            "adverse {} should exceed threshold",
            adj.adverse_bps
        );
        assert!(
            adj.spread_multiplier > 1.0,
            "spread_multiplier {} should widen",
            adj.spread_multiplier
        );
        assert!(
            adj.size_multiplier < 1.0,
            "size_multiplier {} should shrink",
            adj.size_multiplier
        );
        // Large adverse (100 bps) saturates both clamps.
        assert!((adj.spread_multiplier - 1.5).abs() < 1e-9); // max_spread_multiplier
        assert!((adj.size_multiplier - 0.55).abs() < 1e-9); // min_size_multiplier
    }

    /// Healthy fills (mid moves in our favor) -> neutral 1.0 / 1.0.
    #[test]
    fn healthy_fills_neutral() {
        let mut t = LiveMetricsTracker::new(cfg());
        let t0 = 2_000_000u64;
        for i in 0..6 {
            t.record_fill(Side::Buy, 100.0, 1.0, Some(100.0), t0 + i);
        }
        // mid moves UP for a buy => favorable, adverse = 0.
        t.update(t0 + 31_000, 101.0);
        let adj = t.current_adjustment();

        assert_eq!(adj.reason, "healthy");
        assert!((adj.spread_multiplier - 1.0).abs() < 1e-12);
        assert!((adj.size_multiplier - 1.0).abs() < 1e-12);
        assert!(adj.adverse_bps.abs() < 1e-12);
    }

    /// Below the min-sample threshold -> neutral, reason "insufficient_markouts".
    #[test]
    fn insufficient_samples_neutral() {
        let mut t = LiveMetricsTracker::new(cfg());
        let t0 = 3_000_000u64;
        // Only 2 fills => 2 settled samples < MIN_SAMPLE_COUNT.
        t.record_fill(Side::Buy, 100.0, 1.0, Some(100.0), t0);
        t.record_fill(Side::Buy, 100.0, 1.0, Some(100.0), t0 + 1);
        t.update(t0 + 31_000, 99.0);
        let adj = t.current_adjustment();

        assert_eq!(adj.reason, "insufficient_markouts");
        assert_eq!(adj.sample_count, 2);
        assert!((adj.spread_multiplier - 1.0).abs() < 1e-12);
        assert!((adj.size_multiplier - 1.0).abs() < 1e-12);
    }

    /// No samples at all (no fills aged out) -> neutral, reason "insufficient_markouts".
    #[test]
    fn no_samples_neutral() {
        let mut t = LiveMetricsTracker::new(cfg());
        t.update(5_000, 100.0);
        let adj = t.current_adjustment();
        assert_eq!(adj.reason, "insufficient_markouts");
        assert_eq!(adj.sample_count, 0);
    }

    /// Exact markout formula parity for sell fills.
    #[test]
    fn sell_markout_formula() {
        let mut t = LiveMetricsTracker::new(cfg());
        let t0 = 4_000_000u64;
        for i in 0..5 {
            // Sell at 200; mid later drops to 199 => favorable for a sell.
            // markout_bps = (200 - 199)/200 * 1e4 = +50 bps, adverse = 0.
            t.record_fill(Side::Sell, 200.0, 1.0, Some(200.0), t0 + i);
        }
        t.update(t0 + 31_000, 199.0);
        let adj = t.current_adjustment();
        assert_eq!(adj.reason, "healthy");
        assert!(adj.adverse_bps.abs() < 1e-12);

        // Now the opposite: a fresh tracker, sells then mid RISES (adverse).
        let mut t2 = LiveMetricsTracker::new(cfg());
        for i in 0..5 {
            t2.record_fill(Side::Sell, 200.0, 1.0, Some(200.0), t0 + i);
        }
        // markout_bps = (200 - 201)/200 * 1e4 = -50 bps, adverse = 50 bps.
        t2.update(t0 + 31_000, 201.0);
        let adj2 = t2.current_adjustment();
        assert_eq!(adj2.reason, "adverse_markout");
        assert!((adj2.adverse_bps - 50.0).abs() < 1e-9);
    }

    /// A modest excess yields un-clamped, intermediate multipliers.
    #[test]
    fn modest_excess_intermediate_multipliers() {
        let mut t = LiveMetricsTracker::new(cfg());
        let t0 = 6_000_000u64;
        // Choose a mid so adverse ~ 5 bps: buy at 10000, mid 9995 => (9995-10000)/10000*1e4 = -5 bps.
        for i in 0..6 {
            t.record_fill(Side::Buy, 10_000.0, 1.0, Some(10_000.0), t0 + i);
        }
        t.update(t0 + 31_000, 9_995.0);
        let adj = t.current_adjustment();
        assert_eq!(adj.reason, "adverse_markout");
        assert!((adj.adverse_bps - 5.0).abs() < 1e-9);
        // excess = 5 - 2 = 3. spread = 1 + 3*0.05 = 1.15; size = 1 - 3*0.06 = 0.82.
        assert!((adj.spread_multiplier - 1.15).abs() < 1e-9);
        assert!((adj.size_multiplier - 0.82).abs() < 1e-9);
    }

    /// Disabled config -> neutral with reason "disabled" regardless of fills.
    #[test]
    fn disabled_is_neutral() {
        let mut c = cfg();
        c.adaptive_enabled = false;
        let mut t = LiveMetricsTracker::new(c);
        let t0 = 7_000_000u64;
        for i in 0..6 {
            t.record_fill(Side::Buy, 100.0, 1.0, Some(100.0), t0 + i);
        }
        t.update(t0 + 31_000, 99.0);
        let adj = t.current_adjustment();
        assert_eq!(adj.reason, "disabled");
        assert!((adj.spread_multiplier - 1.0).abs() < 1e-12);
        assert!((adj.size_multiplier - 1.0).abs() < 1e-12);
    }

    /// Invalid fills (non-positive price/size) are rejected by `record_fill`.
    #[test]
    fn invalid_fills_rejected() {
        let mut t = LiveMetricsTracker::new(cfg());
        let t0 = 8_000_000u64;
        t.record_fill(Side::Buy, 0.0, 1.0, Some(100.0), t0);
        t.record_fill(Side::Buy, 100.0, 0.0, Some(100.0), t0);
        t.record_fill(Side::Buy, -5.0, 1.0, Some(100.0), t0);
        t.update(t0 + 31_000, 99.0);
        let adj = t.current_adjustment();
        // Nothing settled.
        assert_eq!(adj.sample_count, 0);
        assert_eq!(adj.reason, "insufficient_markouts");
    }

    /// Fills younger than the horizon do not settle yet.
    #[test]
    fn unsettled_before_horizon() {
        let mut t = LiveMetricsTracker::new(cfg());
        let t0 = 9_000_000u64;
        for i in 0..6 {
            t.record_fill(Side::Buy, 100.0, 1.0, Some(100.0), t0 + i);
        }
        // Only 4 seconds elapsed: under the smallest horizon (5s) -> nothing settles.
        t.update(t0 + 4_000, 99.0);
        let adj = t.current_adjustment();
        assert_eq!(adj.sample_count, 0);
    }
}
