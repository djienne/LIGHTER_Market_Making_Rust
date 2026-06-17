//! Risk controller / circuit breaker.
//!
//! Ported from `lighter_MM/market_maker_v2.py::RiskController` (~1259-1352) and
//! `RiskState` (~908).
//!
//! Behavior mirror:
//! - `consecutive_rejections` tracks back-to-back order rejections;
//!   `record_rejection(reason)` increments it and triggers a pause once the
//!   count reaches `max_consecutive_order_rejections`.
//! - `record_success()` resets the rejection counter.
//! - `trigger_pause(reason)` sets a pause deadline `cooldown_sec` in the future
//!   (only ever extends, never shortens an existing deadline).
//! - `is_paused()` reports whether the pause deadline is still in the future.
//! - `maybe_recover(websocket_healthy)` clears a pause after the cooldown
//!   elapses, but only if the last reconcile was OK and the websocket is healthy.
//! - `mark_reconcile(ok, reason)` tracks the consecutive reconcile-failure
//!   streak (`mismatch_streak`) and the last reconcile outcome.
//!
//! This struct is `&mut self` driven and is expected to live behind a `Mutex`
//! (the Python version mutates a shared `RiskState` while the event loop holds
//! the relevant locks). All time accounting uses `std::time::Instant`, the Rust
//! analogue of Python's `time.monotonic()`.

use std::time::{Duration, Instant};

/// Circuit breaker and reconciliation health controller.
///
/// Constructor mirrors the two tunables the Python reference reads from globals:
/// `MAX_CONSECUTIVE_ORDER_REJECTIONS` and `CIRCUIT_BREAKER_COOLDOWN_SEC`
/// (see `crate::config::Safety::{max_consecutive_order_rejections,
/// circuit_breaker_cooldown_sec}`).
#[derive(Debug, Clone)]
pub struct RiskController {
    // ---- tunables (Python module-level constants) ----
    max_consecutive_order_rejections: u32,
    circuit_breaker_cooldown_sec: f64,

    // ---- mutable state (Python RiskState) ----
    /// Number of back-to-back order rejections since the last success.
    consecutive_rejections: u32,
    /// Pause deadline. `None` mirrors Python's `paused_until == 0.0` sentinel
    /// (no pause has ever been armed). `Some(t)` is an absolute deadline.
    paused_until: Option<Instant>,
    /// Human-readable reason for the current/last pause.
    pause_reason: String,
    /// Whether the last reconcile pass succeeded.
    last_reconcile_ok: bool,
    /// Reason string from the last reconcile pass.
    last_reconcile_reason: String,
    /// Timestamp of the last reconcile pass (`None` until first reconcile).
    last_reconcile_time: Option<Instant>,
    /// Consecutive reconcile-mismatch streak (resets to 0 on a successful pass).
    mismatch_streak: u32,
    /// Whether the orchestrator has already issued the cancel-all triggered by
    /// the current pause. Reset whenever a new pause is armed or cleared.
    pause_cancel_done: bool,
}

impl RiskController {
    /// Construct a new controller.
    ///
    /// `max_consecutive_order_rejections`: rejection count at which the circuit
    /// breaker trips. A value of `0` disables rejection-driven pauses (mirrors
    /// the Python `threshold > 0` guard).
    ///
    /// `circuit_breaker_cooldown_sec`: pause duration in seconds. Negative
    /// values are clamped to `0.0` (mirrors `max(0.0, COOLDOWN)`).
    pub fn new(max_consecutive_order_rejections: u32, circuit_breaker_cooldown_sec: f64) -> Self {
        Self {
            max_consecutive_order_rejections,
            circuit_breaker_cooldown_sec,
            consecutive_rejections: 0,
            paused_until: None,
            pause_reason: String::new(),
            last_reconcile_ok: true,
            last_reconcile_reason: String::new(),
            last_reconcile_time: None,
            mismatch_streak: 0,
            pause_cancel_done: false,
        }
    }

    /// Reset the consecutive-rejection counter (Python `record_success`).
    pub fn record_success(&mut self) {
        self.consecutive_rejections = 0;
    }

    /// Record an order rejection (Python `record_rejection`).
    ///
    /// Increments `consecutive_rejections`; if the configured threshold is
    /// positive and the count reaches it, trips the circuit breaker.
    pub fn record_rejection(&mut self, reason: &str) {
        self.consecutive_rejections += 1;
        let threshold = self.max_consecutive_order_rejections;
        if threshold > 0 && self.consecutive_rejections >= threshold {
            let msg = format!(
                "circuit_breaker: {} consecutive rejections ({})",
                self.consecutive_rejections, reason
            );
            self.trigger_pause(&msg);
        }
    }

    /// Arm (or extend) a trading pause (Python `trigger_pause`).
    ///
    /// The new deadline is `now + max(0, cooldown)`. The stored deadline is only
    /// ever extended forward, never pulled in (matching `if until > paused_until`).
    pub fn trigger_pause(&mut self, reason: &str) {
        let cooldown = self.circuit_breaker_cooldown_sec.max(0.0);
        let until = Instant::now() + Duration::from_secs_f64(cooldown);
        match self.paused_until {
            Some(prev) if until <= prev => {}
            _ => self.paused_until = Some(until),
        }
        self.pause_reason = reason.to_string();
        self.pause_cancel_done = false;
        tracing::error!(
            "Trading paused: {} (cooldown {:.1}s)",
            reason,
            self.circuit_breaker_cooldown_sec
        );
    }

    /// Whether trading is currently paused (Python `is_paused`).
    ///
    /// True while `now < paused_until`. With no pause armed (`paused_until ==
    /// None`, i.e. Python's `0.0`), this is always false.
    pub fn is_paused(&self) -> bool {
        match self.paused_until {
            Some(until) => Instant::now() < until,
            None => false,
        }
    }

    /// Attempt to clear a pause after its cooldown elapses (Python `maybe_recover`).
    ///
    /// Returns `true` when trading may proceed:
    /// - still paused -> `false`
    /// - never paused (`paused_until` unset) -> `true`
    /// - last reconcile failed or websocket unhealthy -> `false` (stay parked
    ///   past the deadline until conditions are healthy)
    /// - otherwise clears the pause, resets the rejection counter, and returns `true`.
    pub fn maybe_recover(&mut self, websocket_healthy: bool) -> bool {
        if self.is_paused() {
            return false;
        }
        // Python: `if self._state.paused_until <= 0: return True`
        // No pause was ever armed -> nothing to recover from.
        if self.paused_until.is_none() {
            return true;
        }
        if !self.last_reconcile_ok || !websocket_healthy {
            return false;
        }
        self.paused_until = None;
        self.pause_reason = String::new();
        self.consecutive_rejections = 0;
        self.pause_cancel_done = false;
        tracing::info!("Trading resumed after circuit-breaker cooldown.");
        true
    }

    /// Record the outcome of a reconcile pass (Python `mark_reconcile`).
    ///
    /// On success the mismatch streak resets to 0; on failure it increments.
    pub fn mark_reconcile(&mut self, ok: bool, reason: &str) {
        self.last_reconcile_ok = ok;
        self.last_reconcile_reason = reason.to_string();
        self.last_reconcile_time = Some(Instant::now());
        self.mismatch_streak = if ok { 0 } else { self.mismatch_streak + 1 };
    }

    // ---- accessors ----

    /// Current consecutive reconcile-failure streak (Python `mismatch_streak`).
    pub fn mismatch_streak(&self) -> u32 {
        self.mismatch_streak
    }

    /// Reason for the current/last pause (Python `pause_reason`). Empty when not paused.
    pub fn pause_reason(&self) -> &str {
        &self.pause_reason
    }

    /// Current consecutive order-rejection count.
    pub fn consecutive_rejections(&self) -> u32 {
        self.consecutive_rejections
    }

    /// Whether the last reconcile pass succeeded (Python `last_reconcile_ok`).
    pub fn last_reconcile_ok(&self) -> bool {
        self.last_reconcile_ok
    }

    /// Reason from the last reconcile pass (Python `last_reconcile_reason`).
    pub fn last_reconcile_reason(&self) -> &str {
        &self.last_reconcile_reason
    }

    /// Instant of the last reconcile pass, or `None` if none has run yet.
    pub fn last_reconcile_time(&self) -> Option<Instant> {
        self.last_reconcile_time
    }

    /// Whether the cancel-all triggered by the current pause has been issued
    /// (Python `pause_cancel_done` getter).
    pub fn pause_cancel_done(&self) -> bool {
        self.pause_cancel_done
    }

    /// Set the pause-cancel-done flag (Python `pause_cancel_done` setter).
    pub fn set_pause_cancel_done(&mut self, value: bool) {
        self.pause_cancel_done = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn threshold_trips_pause() {
        // threshold = 3, long cooldown so the pause stays armed during the test.
        let mut rc = RiskController::new(3, 60.0);
        assert!(!rc.is_paused());

        rc.record_rejection("rate_limited");
        assert_eq!(rc.consecutive_rejections(), 1);
        assert!(!rc.is_paused());

        rc.record_rejection("rate_limited");
        assert_eq!(rc.consecutive_rejections(), 2);
        assert!(!rc.is_paused());

        rc.record_rejection("rate_limited");
        assert_eq!(rc.consecutive_rejections(), 3);
        assert!(rc.is_paused());
        // Reason format must match the Python f-string exactly.
        assert_eq!(
            rc.pause_reason(),
            "circuit_breaker: 3 consecutive rejections (rate_limited)"
        );
    }

    #[test]
    fn zero_threshold_never_trips() {
        // threshold = 0 disables rejection-driven pauses (Python `threshold > 0`).
        let mut rc = RiskController::new(0, 60.0);
        for _ in 0..10 {
            rc.record_rejection("nope");
        }
        assert_eq!(rc.consecutive_rejections(), 10);
        assert!(!rc.is_paused());
    }

    #[test]
    fn success_resets_counter() {
        let mut rc = RiskController::new(5, 60.0);
        rc.record_rejection("x");
        rc.record_rejection("x");
        assert_eq!(rc.consecutive_rejections(), 2);
        rc.record_success();
        assert_eq!(rc.consecutive_rejections(), 0);
        assert!(!rc.is_paused());
    }

    #[test]
    fn cooldown_elapses_then_recover() {
        // Tiny cooldown so the deadline passes essentially immediately.
        let mut rc = RiskController::new(1, 0.0);
        rc.record_rejection("boom");
        // cooldown == 0.0 -> deadline is `now`, so `now < deadline` is false.
        assert!(!rc.is_paused());

        // Healthy reconcile + healthy ws -> recover clears the pause.
        rc.mark_reconcile(true, "");
        assert!(rc.maybe_recover(true));
        assert_eq!(rc.consecutive_rejections(), 0);
        assert_eq!(rc.pause_reason(), "");
        // A subsequent recover with no pause armed returns true (paused_until unset).
        assert!(rc.maybe_recover(true));
    }

    #[test]
    fn recover_blocked_while_paused() {
        let mut rc = RiskController::new(1, 60.0);
        rc.record_rejection("boom");
        assert!(rc.is_paused());
        // Still within cooldown -> cannot recover even if everything is healthy.
        rc.mark_reconcile(true, "");
        assert!(!rc.maybe_recover(true));
        assert!(rc.is_paused());
    }

    #[test]
    fn recover_blocked_when_unhealthy() {
        // Cooldown elapsed (0s) but ws unhealthy / reconcile failed -> stay parked.
        let mut rc = RiskController::new(1, 0.0);
        rc.record_rejection("boom");
        rc.mark_reconcile(true, "");
        // unhealthy websocket blocks recovery
        assert!(!rc.maybe_recover(false));

        // reconcile failure blocks recovery even with healthy ws
        rc.mark_reconcile(false, "mismatch");
        assert!(!rc.maybe_recover(true));
    }

    #[test]
    fn mismatch_streak_inc_and_reset() {
        let mut rc = RiskController::new(5, 60.0);
        assert_eq!(rc.mismatch_streak(), 0);
        assert!(rc.last_reconcile_ok());

        rc.mark_reconcile(false, "a");
        assert_eq!(rc.mismatch_streak(), 1);
        assert!(!rc.last_reconcile_ok());
        assert_eq!(rc.last_reconcile_reason(), "a");

        rc.mark_reconcile(false, "b");
        assert_eq!(rc.mismatch_streak(), 2);

        rc.mark_reconcile(true, "");
        assert_eq!(rc.mismatch_streak(), 0);
        assert!(rc.last_reconcile_ok());
        assert!(rc.last_reconcile_time().is_some());
    }

    #[test]
    fn trigger_pause_only_extends() {
        let mut rc = RiskController::new(5, 60.0);
        rc.trigger_pause("first");
        let first_deadline = rc.paused_until.expect("armed");
        // A shorter cooldown should NOT pull the deadline in (Python `if until > paused_until`).
        rc.circuit_breaker_cooldown_sec = 0.0;
        rc.trigger_pause("second");
        let second_deadline = rc.paused_until.expect("still armed");
        assert_eq!(first_deadline, second_deadline);
        // Reason still updates even when the deadline does not move.
        assert_eq!(rc.pause_reason(), "second");
    }

    #[test]
    fn pause_cancel_done_toggles() {
        let mut rc = RiskController::new(1, 60.0);
        assert!(!rc.pause_cancel_done());
        rc.set_pause_cancel_done(true);
        assert!(rc.pause_cancel_done());
        // Arming a new pause resets the flag.
        rc.trigger_pause("again");
        assert!(!rc.pause_cancel_done());
    }

    #[test]
    fn manual_pause_then_recover_after_cooldown() {
        // End-to-end: manual trigger, wait out a short real cooldown, recover.
        let mut rc = RiskController::new(5, 0.05);
        rc.trigger_pause("manual");
        assert!(rc.is_paused());
        std::thread::sleep(Duration::from_millis(80));
        assert!(!rc.is_paused());
        rc.mark_reconcile(true, "");
        assert!(rc.maybe_recover(true));
        assert!(!rc.is_paused());
    }
}
