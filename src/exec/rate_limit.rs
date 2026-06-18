//! Adaptive rate limiter — sliding-window token bucket + volume-quota pacing
//! + global 429 backoff.
//!
//! Direct port of the rate-limiter helpers in `market_maker_v2.py`
//! (`_prune_op_window`, `_ops_available`, `_time_until_ops_free`,
//! `_quota_pace_multiplier`, `_adaptive_threshold_bps`, `_record_ops_sent`,
//! `_update_volume_quota`, `_wait_for_write_slot`, `_trigger_global_backoff`,
//! `_reset_global_backoff`, `_is_quota_error`, `_is_transient_error`).
//!
//! Parity notes (read against the Python source, NOT the spec prose):
//! * The binding limit is the documented "default" per-tx-type limit of
//!   40 ops / rolling 60 s window.
//! * `quota_pace_multiplier`: `>=500 (or unknown) -> 1.0`, `>=50 -> 1.5`,
//!   `>=10 -> 3.0`, `<10 -> +inf` (free-slot-only pacing).
//! * `adaptive_threshold_bps`: `>=500 (or unknown) -> base`, `>=50 -> 2.0x`,
//!   `>=10 -> 3.5x`, `<10 -> 5.0x`. (The Python uses **2.0x** at the medium
//!   band, not 1.5x — the source is authoritative.)
//! * The phase-3 min send-interval floor uses `rate_limit_send_interval`
//!   (default 0.1 s) for normal batches and 0.5 s for cancel-only batches.
//!   (The Python docstring says "1.5s normal" but the code uses the configured
//!   floor — we follow the code.)
//! * 429 backoff: `min(15 * 2^(level-1), 120)` seconds; the escalation counter
//!   resets after 2 consecutive write successes, or auto-decays after 5 minutes
//!   without a fresh 429.
//!
//! `std::time::Instant` is the Rust analogue of Python's `time.monotonic()`.
//! The pure helpers never sleep; only [`RateLimiter::write_slot`] awaits, using
//! `tokio::time::sleep`.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

// --- Constants (mirror the module-level `_RL_*` globals) -------------------

/// Ops allowed per rolling window (`_RL_OPS_PER_WINDOW`).
pub const RL_OPS_PER_WINDOW: usize = 40;
/// Rolling-window length in seconds (`_RL_WINDOW_SECONDS`).
pub const RL_WINDOW_SECONDS: f64 = 60.0;
/// Shorter floor for cancel-only batches (`_RL_CANCEL_MIN_INTERVAL`).
pub const RL_CANCEL_MIN_INTERVAL: f64 = 0.5;

/// Volume-quota band thresholds (`_RL_QUOTA_HIGH/MEDIUM/LOW`).
pub const RL_QUOTA_HIGH: i64 = 500;
pub const RL_QUOTA_MEDIUM: i64 = 50;
pub const RL_QUOTA_LOW: i64 = 10;
/// One free tx every 15 s when quota is critical (`_RL_FREE_SLOT_INTERVAL`).
pub const RL_FREE_SLOT_INTERVAL: f64 = 15.0;

/// 429 backoff base / cap / reset-after (`_RL_BACKOFF_BASE/MAX/RESET_AFTER`).
pub const RL_BACKOFF_BASE: f64 = 15.0;
pub const RL_BACKOFF_MAX: f64 = 120.0;
pub const RL_BACKOFF_RESET_AFTER: u32 = 2;

/// Auto-decay the escalation counter after this many seconds without a 429.
const BACKOFF_DECAY_SECS: f64 = 300.0;
/// If a global backoff is within this many seconds of expiry, sleep it out
/// instead of skipping the cycle.
const BACKOFF_SLEEP_THRESHOLD: f64 = 2.0;
/// If the sliding window needs more than this many seconds to free up, skip the
/// cycle instead of waiting.
const WINDOW_SKIP_THRESHOLD: f64 = 30.0;

// --- Free functions: error classification ----------------------------------

/// True if the error is specifically a volume-quota exhaustion
/// (`_is_quota_error`). Case-insensitive.
pub fn is_quota_error(msg: &str) -> bool {
    let m = msg.to_lowercase();
    (m.contains("not enough") && m.contains("quota")) || (m.contains("quota") && m.contains("exhausted"))
}

/// Quote-update threshold (bps) scaled by quota pressure — the standalone form of
/// `_adaptive_threshold_bps`, callable from the hot path (which reads the live quota from the
/// shared `Derived` atomic). Bands: `≥500 (or unknown) → base`, `≥50 → 2.0×`, `≥10 → 3.5×`,
/// `<10 → 5.0×`. Used by both the hot task (requote decision) and [`RateLimiter`].
pub fn quota_adaptive_threshold_bps(base: f64, quota: Option<i64>) -> f64 {
    match quota {
        None => base,
        Some(q) if q >= RL_QUOTA_HIGH => base,
        Some(q) if q >= RL_QUOTA_MEDIUM => base * 2.0,
        Some(q) if q >= RL_QUOTA_LOW => base * 3.5,
        Some(_) => base * 5.0,
    }
}

/// Classification of a rejected-batch error message, mirroring the EXACT substring
/// tests and ordering of the Python batch-reject handler (`sign_and_send_batch`,
/// `market_maker_v2.py` ~L4340-4366). Each variant drives a distinct nonce-correction
/// policy in the sender (see `paced_send::send_once`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectKind {
    /// Volume-quota exhausted. Python: force quota=0, roll back reserved nonces, hard-refresh.
    Quota,
    /// 429 / too-many-requests. Python: global backoff, roll back reserved nonces (NO refresh —
    /// don't hit REST while rate-limited; a 429 never consumed the nonce).
    RateLimit,
    /// Nonce desync. Python: authoritative hard-refresh (do NOT also roll back).
    Nonce,
    /// Maker-only key restriction. Treat as a business rejection, but label it distinctly so
    /// post-mortems can separate exchange permission issues from margin/post-only crosses.
    MakerOnly,
    /// Any other business rejection (post-only cross, margin, ...). Python: roll back reserved
    /// nonces AND hard-refresh (consumption is ambiguous; refresh is the source of truth).
    Other,
}

/// Classify a reject message in the same order as the Python handler for nonce/quota handling
/// (quota → 429 → nonce → business rejection), with maker-only split out from generic business
/// rejections for clearer logs.
/// The quota test excludes the benign "quota remained"/"didn't use quota" informational
/// strings, matching `"quota" in err and "remained" not in err and "didn't use" not in err`.
pub fn classify_reject(msg: &str) -> RejectKind {
    let m = msg.to_lowercase();
    if m.contains("quota") && !m.contains("remained") && !m.contains("didn't use") {
        RejectKind::Quota
    } else if m.contains("429") || m.contains("too many") {
        RejectKind::RateLimit
    } else if m.contains("nonce") {
        RejectKind::Nonce
    } else if m.contains("maker-only api key")
        || m.contains("maker only api key")
        || m.contains("0ms delay transactions")
    {
        RejectKind::MakerOnly
    } else {
        RejectKind::Other
    }
}

/// True for transient errors (429 / nonce / quota) that should trigger a
/// backoff rather than the circuit breaker (`_is_transient_error`).
pub fn is_transient_error(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("429")
        || m.contains("too many")
        || (m.contains("not enough") && m.contains("quota"))
        || m.contains("invalid nonce")
}

// --- RateLimiter ------------------------------------------------------------

/// Adaptive rate limiter holding the sliding-op window, last-send timestamp,
/// volume-quota state, and the global 429 backoff escalation state.
///
/// Timekeeping uses [`Instant`] (monotonic), matching Python's `time.monotonic()`.
/// The `last_send` field is `None` until the first recorded send, equivalent to
/// the Python `_last_send_time = 0.0` initial value (a far-past instant).
#[derive(Debug)]
pub struct RateLimiter {
    /// Monotonic instants of each op sent within (or just outside) the window.
    window: VecDeque<Instant>,
    /// Instant of the most recent send (`_last_send_time`); `None` == never sent.
    last_send: Option<Instant>,

    /// Instant until which the global 429 backoff is active (`_global_backoff_until`).
    global_backoff_until: Option<Instant>,
    /// Consecutive-429 escalation level (`_global_backoff_consecutive`).
    backoff_level: u32,
    /// Consecutive successful writes since the last 429 (`_consecutive_successes`).
    consecutive_successes: u32,
    /// Instant of the most recent backoff trigger (`_last_backoff_trigger_time`).
    last_backoff_trigger: Option<Instant>,

    /// Latest known `volume_quota_remaining`; `None` until first API response.
    quota: Option<i64>,

    /// Base quote-update threshold in bps (`QUOTE_UPDATE_THRESHOLD_BPS`).
    default_quote_update_threshold_bps: f64,
    /// Minimum interval between any two sends, seconds (`_RL_MIN_SEND_INTERVAL`).
    min_send_interval: f64,
}

impl RateLimiter {
    /// Build a limiter.
    ///
    /// * `default_quote_update_threshold_bps` — base bps for the adaptive
    ///   threshold (config `default_quote_update_threshold_bps`).
    /// * `rate_limit_send_interval` — normal min send-interval floor in seconds
    ///   (config `rate_limit_send_interval`, default 0.1).
    pub fn new(default_quote_update_threshold_bps: f64, rate_limit_send_interval: f64) -> Self {
        Self {
            window: VecDeque::new(),
            last_send: None,
            global_backoff_until: None,
            backoff_level: 0,
            consecutive_successes: 0,
            last_backoff_trigger: None,
            quota: None,
            default_quote_update_threshold_bps,
            min_send_interval: rate_limit_send_interval,
        }
    }

    // -- Quota accessors -----------------------------------------------------

    /// Current `volume_quota_remaining`, if known.
    #[inline]
    pub fn quota(&self) -> Option<i64> {
        self.quota
    }

    /// Set `volume_quota_remaining` directly (e.g. forced to 0 on a quota error).
    #[inline]
    pub fn set_quota(&mut self, value: Option<i64>) {
        self.quota = value;
    }

    /// Parse and store a quota value from an exchange response
    /// (`_update_volume_quota`). `None` / `"?"` / unparseable leaves it unchanged.
    pub fn update_volume_quota(&mut self, raw: Option<&str>) {
        match raw {
            None => {}
            Some(s) if s == "?" => {}
            Some(s) => {
                if let Ok(v) = s.trim().parse::<i64>() {
                    self.quota = Some(v);
                }
            }
        }
    }

    /// Convenience: store a numeric quota value directly (`_update_volume_quota`
    /// path where the response already carries an integer).
    #[inline]
    pub fn update_volume_quota_i64(&mut self, value: i64) {
        self.quota = Some(value);
    }

    // -- Sliding window (pure) ----------------------------------------------

    /// Evict ops older than the rolling window; return the current op count
    /// (`_prune_op_window`). Pure aside from mutating the window deque.
    pub fn prune_op_window(&mut self) -> usize {
        self.prune_op_window_at(Instant::now())
    }

    /// `prune_op_window` against an explicit `now` (testable, no clock read).
    pub fn prune_op_window_at(&mut self, now: Instant) -> usize {
        let cutoff = now - Duration::from_secs_f64(RL_WINDOW_SECONDS);
        while let Some(&front) = self.window.front() {
            if front < cutoff {
                self.window.pop_front();
            } else {
                break;
            }
        }
        self.window.len()
    }

    /// How many ops can be sent right now within the window (`_ops_available`).
    pub fn ops_available(&mut self) -> usize {
        self.ops_available_at(Instant::now())
    }

    /// `ops_available` against an explicit `now`.
    pub fn ops_available_at(&mut self, now: Instant) -> usize {
        RL_OPS_PER_WINDOW.saturating_sub(self.prune_op_window_at(now))
    }

    /// Seconds until `n` ops become available; 0.0 if already available
    /// (`_time_until_ops_free`).
    pub fn time_until_ops_free(&mut self, n: usize) -> f64 {
        self.time_until_ops_free_at(n, Instant::now())
    }

    /// `time_until_ops_free` against an explicit `now`.
    pub fn time_until_ops_free_at(&mut self, n: usize, now: Instant) -> f64 {
        self.prune_op_window_at(now);
        let len = self.window.len();
        // `if not _op_timestamps or len + n <= budget: return 0.0`
        if len == 0 || len + n <= RL_OPS_PER_WINDOW {
            return 0.0;
        }
        // idx = len - (budget - n); Python uses signed arithmetic and guards <0.
        // Here `len + n > budget` ⇒ `len > budget - n` ⇒ idx >= 1, so the
        // `idx < 0` branch is unreachable, but we mirror the clamp for safety.
        let budget_minus_n = RL_OPS_PER_WINDOW as i64 - n as i64;
        let mut idx = len as i64 - budget_minus_n;
        if idx < 0 {
            return 0.0;
        }
        if idx >= len as i64 {
            idx = len as i64 - 1;
        }
        let expires_at = self.window[idx as usize] + Duration::from_secs_f64(RL_WINDOW_SECONDS);
        if expires_at <= now {
            0.0
        } else {
            (expires_at - now).as_secs_f64()
        }
    }

    /// Record `count` ops sent at the current instant (`_record_ops_sent`).
    pub fn record_ops_sent(&mut self, count: usize) {
        self.record_ops_sent_at(count, Instant::now());
    }

    /// `record_ops_sent` at an explicit `now`.
    pub fn record_ops_sent_at(&mut self, count: usize, now: Instant) {
        for _ in 0..count {
            self.window.push_back(now);
        }
        self.last_send = Some(now);
    }

    // -- Quota pacing (pure) -------------------------------------------------

    /// Pacing multiplier from `volume_quota_remaining` (`_quota_pace_multiplier`).
    /// `1.0` full speed, `1.5`/`3.0` slower, `+inf` ⇒ wait for the free slot.
    /// Unknown quota (`None`) is treated as full speed, matching the Python
    /// `is None or >= HIGH` guard.
    pub fn quota_pace_multiplier(&self) -> f64 {
        match self.quota {
            None => 1.0,
            Some(q) if q >= RL_QUOTA_HIGH => 1.0,
            Some(q) if q >= RL_QUOTA_MEDIUM => 1.5,
            Some(q) if q >= RL_QUOTA_LOW => 3.0,
            Some(_) => f64::INFINITY,
        }
    }

    /// Quote-update threshold (bps) scaled by quota pressure
    /// (`_adaptive_threshold_bps`). Multipliers: base / 2.0 / 3.5 / 5.0.
    pub fn adaptive_threshold_bps(&self) -> f64 {
        quota_adaptive_threshold_bps(self.default_quote_update_threshold_bps, self.quota)
    }

    // -- Global 429 backoff --------------------------------------------------

    /// Trigger / escalate the global backoff after a 429 (`_trigger_global_backoff`).
    /// Duration = `min(15 * 2^(level-1), 120)` seconds.
    pub fn trigger_global_backoff(&mut self) {
        self.trigger_global_backoff_at(Instant::now());
    }

    /// `trigger_global_backoff` at an explicit `now`. Returns the backoff
    /// duration in seconds.
    pub fn trigger_global_backoff_at(&mut self, now: Instant) -> f64 {
        self.backoff_level += 1;
        self.consecutive_successes = 0;
        self.last_backoff_trigger = Some(now);
        let duration = (RL_BACKOFF_BASE * 2f64.powi(self.backoff_level as i32 - 1)).min(RL_BACKOFF_MAX);
        self.global_backoff_until = Some(now + Duration::from_secs_f64(duration));
        duration
    }

    /// Record a successful write; reset escalation after enough successes or a
    /// 5-minute decay (`_reset_global_backoff`).
    pub fn reset_global_backoff(&mut self) {
        self.reset_global_backoff_at(Instant::now());
    }

    /// `reset_global_backoff` at an explicit `now`.
    pub fn reset_global_backoff_at(&mut self, now: Instant) {
        self.consecutive_successes += 1;
        if self.backoff_level > 0 {
            // Time-based decay: auto-reset if no 429 for 5 minutes.
            if let Some(trig) = self.last_backoff_trigger {
                if (now - trig).as_secs_f64() > BACKOFF_DECAY_SECS {
                    self.backoff_level = 0;
                    self.consecutive_successes = 0;
                    return;
                }
            }
            if self.consecutive_successes >= RL_BACKOFF_RESET_AFTER {
                self.backoff_level = 0;
                self.consecutive_successes = 0;
            }
        }
    }

    /// Current escalation level (for diagnostics / tests).
    #[inline]
    pub fn backoff_level(&self) -> u32 {
        self.backoff_level
    }

    /// Whether a global backoff is currently active at `now`.
    #[inline]
    pub fn in_global_backoff_at(&self, now: Instant) -> bool {
        matches!(self.global_backoff_until, Some(until) if now < until)
    }

    // -- The async gate ------------------------------------------------------

    /// Adaptive rate-limit gate (`_wait_for_write_slot`). Returns `true` if it
    /// is OK to proceed, `false` to skip this cycle.
    ///
    /// Four phases, executed in order:
    /// 1. Global 429 backoff — if within 2 s of expiry, sleep it out; otherwise skip.
    /// 2. Sliding-window capacity (40 ops / 60 s) — skip if it needs >30 s, else wait.
    /// 3. Minimum send-interval floor (0.1 s normal / 0.5 s cancel-only).
    /// 4. Volume-quota pacing (skipped for cancel-only batches).
    pub async fn write_slot(&mut self, op_count: usize, cancel_only: bool) -> bool {
        let now = Instant::now();

        // Phase 1: Global 429 backoff.
        if let Some(until) = self.global_backoff_until {
            if now < until {
                let remaining = (until - now).as_secs_f64();
                if remaining <= BACKOFF_SLEEP_THRESHOLD {
                    sleep_secs(remaining).await;
                } else {
                    return false;
                }
            }
        }

        // Phase 2: Sliding-window capacity.
        let avail = self.ops_available_at(Instant::now());
        if avail < op_count {
            let wait_time = self.time_until_ops_free_at(op_count, Instant::now());
            if wait_time > WINDOW_SKIP_THRESHOLD {
                return false;
            }
            if wait_time > 0.0 {
                sleep_secs(wait_time).await;
            }
        }

        // Phase 3: Minimum send-interval floor.
        let floor = if cancel_only {
            RL_CANCEL_MIN_INTERVAL
        } else {
            self.min_send_interval
        };
        let elapsed = self.elapsed_since_last_send(Instant::now());
        if elapsed < floor {
            sleep_secs(floor - elapsed).await;
        }

        // Phase 4: Volume-quota pacing (skip for cancel-only batches).
        if !cancel_only {
            let mult = self.quota_pace_multiplier();
            if mult.is_infinite() {
                // Quota critically low — wait for the free 15 s slot.
                let since_last = self.elapsed_since_last_send(Instant::now());
                if since_last < RL_FREE_SLOT_INTERVAL {
                    sleep_secs(RL_FREE_SLOT_INTERVAL - since_last).await;
                }
            } else if mult > 1.0 {
                // Stretch the interval to slow down.
                let extra = floor * (mult - 1.0);
                let elapsed2 = self.elapsed_since_last_send(Instant::now());
                if elapsed2 < floor + extra {
                    sleep_secs(floor + extra - elapsed2).await;
                }
            }
        }

        true
    }

    /// Seconds since the last recorded send. With no prior send this returns a
    /// huge value, matching Python's `_last_send_time = 0.0` (far in the past)
    /// so the very first cycle is never throttled by the interval floors.
    #[inline]
    fn elapsed_since_last_send(&self, now: Instant) -> f64 {
        match self.last_send {
            Some(t) => (now - t).as_secs_f64(),
            None => f64::MAX,
        }
    }
}

/// Sleep for `secs` seconds via tokio, ignoring non-positive / non-finite values.
#[inline]
async fn sleep_secs(secs: f64) {
    if secs.is_finite() && secs > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(secs)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter() -> RateLimiter {
        RateLimiter::new(8.0, 0.1)
    }

    #[test]
    fn quota_error_classification() {
        assert!(is_quota_error("Not enough volume quota remaining"));
        assert!(is_quota_error("QUOTA EXHAUSTED"));
        assert!(!is_quota_error("invalid nonce"));
        assert!(!is_quota_error("not enough margin"));
    }

    #[test]
    fn reject_classification_matches_python() {
        use RejectKind::*;
        // Quota exhaustion: contains "quota", not "remained"/"didn't use".
        assert_eq!(classify_reject("Not enough volume quota remaining"), Quota);
        assert_eq!(classify_reject("volume quota exhausted"), Quota);
        assert_eq!(classify_reject("not enough quota"), Quota);
        // Benign quota info ("remained"/"didn't use") must NOT be treated as quota → falls to Other.
        assert_eq!(classify_reject("quota remained: 5"), Other);
        assert_eq!(classify_reject("you didn't use your quota"), Other);
        // 429 / rate limit.
        assert_eq!(classify_reject("HTTP 429 Too Many Requests"), RateLimit);
        assert_eq!(classify_reject("too many requests"), RateLimit);
        // Nonce desync (only if not quota/429 first).
        assert_eq!(classify_reject("invalid nonce"), Nonce);
        assert_eq!(classify_reject("nonce too low: expected 7"), Nonce);
        // Maker-only key restrictions are business rejections, but classified distinctly for logs.
        assert_eq!(
            classify_reject("maker-only api key can only send 0ms delay transactions"),
            MakerOnly
        );
        assert_eq!(classify_reject("maker only api key"), MakerOnly);
        // Generic business rejections.
        assert_eq!(classify_reject("post only order would cross the book"), Other);
        assert_eq!(classify_reject("insufficient margin"), Other);
        // Empty message → caller substitutes "code=<n>" (Python `message or f"code={code}"`):
        // a 429 with no message must still classify as RateLimit, not Other.
        assert_eq!(classify_reject("code=429"), RateLimit);
        // An empty-message nonce reject (code=21104) has no "nonce" substring → Other, which
        // matches Python's generic branch (rollback+refresh) for that case.
        assert_eq!(classify_reject("code=21104"), Other);
    }

    #[test]
    fn transient_error_classification() {
        assert!(is_transient_error("HTTP 429 Too Many Requests"));
        assert!(is_transient_error("too many requests"));
        assert!(is_transient_error("not enough volume quota"));
        assert!(is_transient_error("invalid nonce: expected 5"));
        assert!(!is_transient_error("insufficient balance"));
    }

    #[test]
    fn quota_pace_multiplier_bands() {
        let mut rl = limiter();
        // None -> full speed.
        assert_eq!(rl.quota_pace_multiplier(), 1.0);
        rl.set_quota(Some(500));
        assert_eq!(rl.quota_pace_multiplier(), 1.0);
        rl.set_quota(Some(499));
        assert_eq!(rl.quota_pace_multiplier(), 1.5);
        rl.set_quota(Some(50));
        assert_eq!(rl.quota_pace_multiplier(), 1.5);
        rl.set_quota(Some(49));
        assert_eq!(rl.quota_pace_multiplier(), 3.0);
        rl.set_quota(Some(10));
        assert_eq!(rl.quota_pace_multiplier(), 3.0);
        rl.set_quota(Some(9));
        assert!(rl.quota_pace_multiplier().is_infinite());
        rl.set_quota(Some(0));
        assert!(rl.quota_pace_multiplier().is_infinite());
    }

    #[test]
    fn adaptive_threshold_bands() {
        let mut rl = limiter(); // base = 8.0
        assert_eq!(rl.adaptive_threshold_bps(), 8.0); // None
        rl.set_quota(Some(500));
        assert_eq!(rl.adaptive_threshold_bps(), 8.0);
        rl.set_quota(Some(499));
        assert_eq!(rl.adaptive_threshold_bps(), 16.0); // 8 * 2.0
        rl.set_quota(Some(50));
        assert_eq!(rl.adaptive_threshold_bps(), 16.0);
        rl.set_quota(Some(49));
        assert_eq!(rl.adaptive_threshold_bps(), 28.0); // 8 * 3.5
        rl.set_quota(Some(10));
        assert_eq!(rl.adaptive_threshold_bps(), 28.0);
        rl.set_quota(Some(9));
        assert_eq!(rl.adaptive_threshold_bps(), 40.0); // 8 * 5.0
        rl.set_quota(Some(0));
        assert_eq!(rl.adaptive_threshold_bps(), 40.0);
    }

    #[test]
    fn ops_available_and_pruning() {
        let mut rl = limiter();
        let t0 = Instant::now();
        assert_eq!(rl.ops_available_at(t0), RL_OPS_PER_WINDOW);
        rl.record_ops_sent_at(4, t0);
        assert_eq!(rl.ops_available_at(t0), RL_OPS_PER_WINDOW - 4);
        // Fill the window.
        rl.record_ops_sent_at(36, t0);
        assert_eq!(rl.ops_available_at(t0), 0);
        // After 60s + epsilon, everything prunes.
        let later = t0 + Duration::from_secs_f64(RL_WINDOW_SECONDS + 0.001);
        assert_eq!(rl.ops_available_at(later), RL_OPS_PER_WINDOW);
    }

    #[test]
    fn time_until_ops_free_zero_when_room() {
        let mut rl = limiter();
        let t0 = Instant::now();
        rl.record_ops_sent_at(10, t0);
        // 10 used, asking for 4 => 14 <= 40 => 0.0.
        assert_eq!(rl.time_until_ops_free_at(4, t0), 0.0);
    }

    #[test]
    fn time_until_ops_free_waits_for_oldest() {
        let mut rl = limiter();
        let t0 = Instant::now();
        // Fill to exactly 40, all at t0.
        rl.record_ops_sent_at(40, t0);
        // Need 4 ops: idx = 40 - (40 - 4) = 4; the 4th-oldest expires at t0+60.
        // Querying at t0 ⇒ ~60s wait.
        let w = rl.time_until_ops_free_at(4, t0);
        assert!((w - RL_WINDOW_SECONDS).abs() < 1e-6, "got {w}");
        // 39 used, need 4 => 43 > 40 => idx = 39 - 36 = 3 (4th oldest at t0).
        let mut rl2 = limiter();
        rl2.record_ops_sent_at(39, t0);
        let w2 = rl2.time_until_ops_free_at(4, t0);
        assert!((w2 - RL_WINDOW_SECONDS).abs() < 1e-6, "got {w2}");
    }

    #[test]
    fn backoff_escalation_durations() {
        let mut rl = limiter();
        let t0 = Instant::now();
        assert_eq!(rl.trigger_global_backoff_at(t0), 15.0); // 15 * 2^0
        assert_eq!(rl.backoff_level(), 1);
        assert_eq!(rl.trigger_global_backoff_at(t0), 30.0); // 15 * 2^1
        assert_eq!(rl.trigger_global_backoff_at(t0), 60.0); // 15 * 2^2
        assert_eq!(rl.trigger_global_backoff_at(t0), 120.0); // 15 * 2^3 = 120 (cap)
        assert_eq!(rl.trigger_global_backoff_at(t0), 120.0); // 15 * 2^4 = 240 -> cap 120
        assert!(rl.in_global_backoff_at(t0));
    }

    #[test]
    fn backoff_reset_after_two_successes() {
        let mut rl = limiter();
        let t0 = Instant::now();
        rl.trigger_global_backoff_at(t0);
        assert_eq!(rl.backoff_level(), 1);
        rl.reset_global_backoff_at(t0); // 1 success, not enough
        assert_eq!(rl.backoff_level(), 1);
        rl.reset_global_backoff_at(t0); // 2 successes -> reset
        assert_eq!(rl.backoff_level(), 0);
    }

    #[test]
    fn backoff_auto_decay_after_5min() {
        let mut rl = limiter();
        let t0 = Instant::now();
        rl.trigger_global_backoff_at(t0);
        rl.trigger_global_backoff_at(t0); // level 2
        assert_eq!(rl.backoff_level(), 2);
        // First success > 5min later auto-resets regardless of count.
        let later = t0 + Duration::from_secs_f64(BACKOFF_DECAY_SECS + 1.0);
        rl.reset_global_backoff_at(later);
        assert_eq!(rl.backoff_level(), 0);
    }

    #[test]
    fn update_volume_quota_parsing() {
        let mut rl = limiter();
        rl.update_volume_quota(None);
        assert_eq!(rl.quota(), None);
        rl.update_volume_quota(Some("?"));
        assert_eq!(rl.quota(), None);
        rl.update_volume_quota(Some("not-a-number"));
        assert_eq!(rl.quota(), None);
        rl.update_volume_quota(Some("123"));
        assert_eq!(rl.quota(), Some(123));
        rl.update_volume_quota(Some(" 45 "));
        assert_eq!(rl.quota(), Some(45));
    }

    // NOTE: these async tests exercise only the non-sleeping (skip / fast)
    // paths, so they run instantly under a plain runtime and do NOT require
    // tokio's `test-util` feature (which `features = ["full"]` does not pull in).
    #[tokio::test]
    async fn write_slot_skips_during_long_backoff() {
        let mut rl = limiter();
        rl.trigger_global_backoff(); // 15s backoff (> 2s threshold)
        // Far from expiry => skip.
        assert!(!rl.write_slot(4, false).await);
    }

    #[tokio::test]
    async fn write_slot_skips_when_window_needs_too_long() {
        let mut rl = limiter();
        let now = Instant::now();
        rl.record_ops_sent_at(40, now); // window full, ~60s to free
        // Needs ~60s > 30s skip threshold => skip.
        assert!(!rl.write_slot(4, false).await);
    }

    #[tokio::test]
    async fn write_slot_ok_first_cycle() {
        let mut rl = limiter();
        // No sends yet, no backoff, empty window => proceed immediately.
        assert!(rl.write_slot(4, false).await);
    }
}
