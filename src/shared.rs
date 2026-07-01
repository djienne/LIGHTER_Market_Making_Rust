//! Lock-free cross-task shared state (ported from `standx`).
//!
//! All hot-path scalars shared between the synchronous market-data task and async
//! background tasks live here as cache-line-aligned atomics. f64 is stored via
//! `to_bits`/`from_bits` in an `AtomicU64`. Reads are ~1ns and never block.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[inline]
fn load_f64(a: &AtomicU64, ord: Ordering) -> f64 {
    f64::from_bits(a.load(ord))
}
#[inline]
fn store_f64(a: &AtomicU64, v: f64, ord: Ordering) {
    a.store(v.to_bits(), ord);
}

/// Binance OBI alpha feed (mirrors `binance_obi.SharedAlpha`). Written by the depth task,
/// read lock-free in the hot path's quote step. Cache-line aligned.
#[repr(align(64))]
pub struct SharedAlpha {
    alpha_bits: AtomicU64,
    sample_count: AtomicU64,
    last_update_ms: AtomicU64,
    min_samples: u64,
}

impl SharedAlpha {
    pub fn new(min_samples: usize) -> Self {
        Self {
            alpha_bits: AtomicU64::new(0),
            sample_count: AtomicU64::new(0),
            last_update_ms: AtomicU64::new(0),
            min_samples: min_samples as u64,
        }
    }

    /// Store a fresh alpha (depth task). Payload stores are Relaxed; the timestamp and
    /// sample-count stores are the Release "commit" (readers Acquire-load one of them
    /// before touching the payload — no-op on x86, required ordering on ARM).
    pub fn update(&self, alpha: f64) {
        store_f64(&self.alpha_bits, alpha, Ordering::Relaxed);
        self.last_update_ms.store(now_ms(), Ordering::Release);
        self.sample_count.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub fn alpha(&self) -> f64 {
        load_f64(&self.alpha_bits, Ordering::Relaxed)
    }

    #[inline]
    pub fn sample_count(&self) -> u64 {
        self.sample_count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn age_ms(&self) -> u64 {
        let last = self.last_update_ms.load(Ordering::Acquire);
        if last == 0 {
            u64::MAX
        } else {
            now_ms().saturating_sub(last)
        }
    }

    #[inline]
    pub fn warmed_up(&self) -> bool {
        self.sample_count() >= self.min_samples
    }

    #[inline]
    pub fn is_stale(&self, threshold_ms: u64) -> bool {
        let last = self.last_update_ms.load(Ordering::Acquire);
        last == 0 || now_ms().saturating_sub(last) > threshold_ms
    }

    /// Reset on a Binance feed reconnect / sequence-gap re-snapshot (Python `SharedAlpha.reset`):
    /// zero the sample count + timestamp so `warmed_up`→false and `is_stale`→true, forcing the
    /// hot path to drop the external-alpha override until the feed re-warms (no stale-alpha leak).
    #[inline]
    pub fn reset(&self) {
        store_f64(&self.alpha_bits, 0.0, Ordering::Relaxed);
        self.last_update_ms.store(0, Ordering::Relaxed);
        self.sample_count.store(0, Ordering::Release);
    }

    /// The value to inject as override, or None if not usable (stale or cold).
    #[inline]
    pub fn usable_alpha(&self, stale_ms: u64) -> Option<f64> {
        if self.warmed_up() && !self.is_stale(stale_ms) {
            Some(self.alpha())
        } else {
            None
        }
    }
}

/// Binance best bid/offer (mirrors `SharedBBO`). Reference/sanity use.
#[repr(align(64))]
pub struct SharedBbo {
    best_bid: AtomicU64,
    best_ask: AtomicU64,
    bid_qty: AtomicU64,
    ask_qty: AtomicU64,
    sample_count: AtomicU64,
    last_update_ms: AtomicU64,
    min_samples: u64,
}

impl SharedBbo {
    pub fn new(min_samples: usize) -> Self {
        Self {
            best_bid: AtomicU64::new(0),
            best_ask: AtomicU64::new(0),
            bid_qty: AtomicU64::new(0),
            ask_qty: AtomicU64::new(0),
            sample_count: AtomicU64::new(0),
            last_update_ms: AtomicU64::new(0),
            min_samples: min_samples as u64,
        }
    }

    pub fn update(&self, bid: f64, ask: f64, bid_qty: f64, ask_qty: f64) {
        store_f64(&self.best_bid, bid, Ordering::Relaxed);
        store_f64(&self.best_ask, ask, Ordering::Relaxed);
        store_f64(&self.bid_qty, bid_qty, Ordering::Relaxed);
        store_f64(&self.ask_qty, ask_qty, Ordering::Relaxed);
        // Release "commit" fields; readers Acquire-load them before the payload.
        self.last_update_ms.store(now_ms(), Ordering::Release);
        self.sample_count.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub fn best_bid(&self) -> f64 {
        load_f64(&self.best_bid, Ordering::Relaxed)
    }
    #[inline]
    pub fn best_ask(&self) -> f64 {
        load_f64(&self.best_ask, Ordering::Relaxed)
    }
    #[inline]
    pub fn mid(&self) -> f64 {
        (self.best_bid() + self.best_ask()) * 0.5
    }
    #[inline]
    pub fn warmed_up(&self) -> bool {
        self.sample_count() >= self.min_samples
    }
    #[inline]
    pub fn sample_count(&self) -> u64 {
        self.sample_count.load(Ordering::Acquire)
    }
    #[inline]
    pub fn age_ms(&self) -> u64 {
        let last = self.last_update_ms.load(Ordering::Acquire);
        if last == 0 {
            u64::MAX
        } else {
            now_ms().saturating_sub(last)
        }
    }
    #[inline]
    pub fn is_stale(&self, threshold_ms: u64) -> bool {
        let last = self.last_update_ms.load(Ordering::Acquire);
        last == 0 || now_ms().saturating_sub(last) > threshold_ms
    }
    /// Reset on a Binance bookTicker reconnect (Python `SharedBBO.reset`): zero values + count
    /// + timestamp so `warmed_up`/freshness gate downstream consumers until the feed re-warms.
    #[inline]
    pub fn reset(&self) {
        store_f64(&self.best_bid, 0.0, Ordering::Relaxed);
        store_f64(&self.best_ask, 0.0, Ordering::Relaxed);
        store_f64(&self.bid_qty, 0.0, Ordering::Relaxed);
        store_f64(&self.ask_qty, 0.0, Ordering::Relaxed);
        self.last_update_ms.store(0, Ordering::Relaxed);
        self.sample_count.store(0, Ordering::Release);
    }
}

/// Position in base units (signed). Written by the account_all task, read in hot path.
#[repr(align(64))]
pub struct SharedPosition {
    bits: AtomicU64,
    last_update_ms: AtomicU64,
}

impl Default for SharedPosition {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedPosition {
    pub fn new() -> Self {
        Self {
            bits: AtomicU64::new(0),
            last_update_ms: AtomicU64::new(0),
        }
    }
    pub fn set(&self, pos: f64) {
        store_f64(&self.bits, pos, Ordering::Release);
        self.last_update_ms.store(now_ms(), Ordering::Relaxed);
    }
    #[inline]
    pub fn get(&self) -> f64 {
        load_f64(&self.bits, Ordering::Acquire)
    }
    #[inline]
    pub fn age_ms(&self) -> u64 {
        let last = self.last_update_ms.load(Ordering::Relaxed);
        if last == 0 {
            u64::MAX
        } else {
            now_ms().saturating_sub(last)
        }
    }
}

/// Capital-derived hot-path params recomputed by cold tasks (user_stats / mid changes):
/// `base_amount`, `max_position_dollar`, `available_capital`, and the live volume quota.
#[repr(align(64))]
pub struct Derived {
    base_amount: AtomicU64,
    max_pos_usd: AtomicU64,
    available_capital: AtomicU64,
    quota_remaining: AtomicU64, // i64 reinterpreted; u64::MAX == unknown
    /// Wall-clock ms of the last market-data (order-book) update. The hot task stamps it on
    /// every book message; the sender reads its age as a "market-data feed healthy" proxy
    /// (Python `check_websocket_health`) when deciding whether to resume after a pause. 0 == none.
    last_md_ms: AtomicU64,
    /// Latest Lighter book mid (hot task, every tick). PnL marks against THIS (same venue —
    /// no cross-exchange basis), falling back to the Binance BBO only when it is stale.
    mid_bits: AtomicU64,
}

impl Default for Derived {
    fn default() -> Self {
        Self::new()
    }
}

impl Derived {
    pub fn new() -> Self {
        Self {
            base_amount: AtomicU64::new(0),
            max_pos_usd: AtomicU64::new(0),
            available_capital: AtomicU64::new(0),
            quota_remaining: AtomicU64::new(u64::MAX),
            last_md_ms: AtomicU64::new(0),
            mid_bits: AtomicU64::new(0),
        }
    }

    /// Stamp the current time as the last market-data update (hot task, every book msg).
    #[inline]
    pub fn set_md_now(&self) {
        self.last_md_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// Publish the latest Lighter mid (hot task, every tick). Freshness = `md_age_ms`.
    #[inline]
    pub fn set_mid(&self, mid: f64) {
        store_f64(&self.mid_bits, mid, Ordering::Relaxed);
    }

    /// Latest Lighter mid; 0.0 until the first book tick.
    #[inline]
    pub fn mid(&self) -> f64 {
        load_f64(&self.mid_bits, Ordering::Relaxed)
    }

    /// Age in ms since the last market-data update; `u64::MAX` if none yet (Python WS-health).
    #[inline]
    pub fn md_age_ms(&self) -> u64 {
        let last = self.last_md_ms.load(Ordering::Relaxed);
        if last == 0 {
            u64::MAX
        } else {
            now_ms().saturating_sub(last)
        }
    }
    pub fn set_base_amount(&self, v: f64) {
        store_f64(&self.base_amount, v, Ordering::Relaxed);
    }
    pub fn set_max_pos_usd(&self, v: f64) {
        store_f64(&self.max_pos_usd, v, Ordering::Relaxed);
    }
    pub fn set_capital(&self, v: f64) {
        store_f64(&self.available_capital, v, Ordering::Relaxed);
    }
    #[inline]
    pub fn base_amount(&self) -> f64 {
        load_f64(&self.base_amount, Ordering::Relaxed)
    }
    #[inline]
    pub fn max_pos_usd(&self) -> f64 {
        load_f64(&self.max_pos_usd, Ordering::Relaxed)
    }
    #[inline]
    pub fn capital(&self) -> f64 {
        load_f64(&self.available_capital, Ordering::Relaxed)
    }
    pub fn set_quota(&self, q: Option<i64>) {
        let v = q.map(|x| x as u64).unwrap_or(u64::MAX);
        self.quota_remaining.store(v, Ordering::Relaxed);
    }
    #[inline]
    pub fn quota(&self) -> Option<i64> {
        let v = self.quota_remaining.load(Ordering::Relaxed);
        if v == u64::MAX {
            None
        } else {
            Some(v as i64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alpha_warmup_and_value() {
        let a = SharedAlpha::new(3);
        assert!(!a.warmed_up());
        a.update(1.5);
        a.update(2.5);
        assert!(!a.warmed_up());
        a.update(3.5);
        assert!(a.warmed_up());
        assert!((a.alpha() - 3.5).abs() < 1e-12);
        assert!(!a.is_stale(60_000));
        assert_eq!(a.usable_alpha(60_000), Some(3.5));
    }

    #[test]
    fn position_roundtrip() {
        let p = SharedPosition::new();
        p.set(-0.0025);
        assert!((p.get() + 0.0025).abs() < 1e-12);
    }

    #[test]
    fn derived_quota() {
        let d = Derived::new();
        assert_eq!(d.quota(), None);
        d.set_quota(Some(42));
        assert_eq!(d.quota(), Some(42));
        d.set_base_amount(0.0002);
        assert!((d.base_amount() - 0.0002).abs() < 1e-12);
    }

    #[test]
    fn md_freshness() {
        let d = Derived::new();
        assert_eq!(d.md_age_ms(), u64::MAX); // none yet
        d.set_md_now();
        assert!(d.md_age_ms() < 1_000); // just stamped
    }
}
