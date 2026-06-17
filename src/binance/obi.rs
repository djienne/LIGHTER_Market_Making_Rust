//! Binance order-book-imbalance alpha calculator — port of `binance_obi.py` OBI logic.
//! Reuses the parity-verified `LocalBook` + `RollingStats`; the imbalance formula and
//! z-score are identical to the Lighter vol_obi engine, so this matches by construction.

use crate::book::local_book::LocalBook;
use crate::shared::SharedAlpha;
use crate::strategy::rolling::RollingStats;
use std::sync::Arc;

pub struct BinanceObi {
    book: LocalBook,
    imb_stats: RollingStats,
    looking_depth: f64,
    last_update_id: i64,
    prev_u: i64,
    shared: Arc<SharedAlpha>,
}

impl BinanceObi {
    pub fn new(window: usize, looking_depth: f64, shared: Arc<SharedAlpha>) -> Self {
        Self {
            book: LocalBook::new(),
            imb_stats: RollingStats::new(window),
            looking_depth,
            last_update_id: 0,
            prev_u: 0,
            shared,
        }
    }

    pub fn reset(&mut self) {
        self.book.reset();
        self.imb_stats.clear();
        self.last_update_id = 0;
        self.prev_u = 0;
        // Also clear the published alpha so a stale OBI value cannot leak across a reconnect /
        // re-snapshot (Python `BinanceDiffDepthClient._reset` calls `SharedAlpha.reset`).
        self.shared.reset();
    }

    pub fn last_update_id(&self) -> i64 {
        self.last_update_id
    }
    pub fn prev_u(&self) -> i64 {
        self.prev_u
    }

    pub fn apply_snapshot(&mut self, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>, last_update_id: i64) {
        self.book.apply_snapshot(bids, asks);
        self.last_update_id = last_update_id;
        self.prev_u = 0; // set by first diff
    }

    /// Apply a depth diff and record `u` as the new sequence cursor.
    pub fn apply_diff(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)], u: i64) {
        self.book.apply_delta(bids, asks);
        self.prev_u = u;
    }

    /// Compute imbalance z-score on the current book and publish to SharedAlpha.
    /// Mirrors `_update_alpha`: skips when one-sided / crossed / non-finite mid.
    pub fn update_alpha(&mut self) {
        let (best_bid, best_ask) = match (self.book.best_bid(), self.book.best_ask()) {
            (Some(b), Some(a)) => (b, a),
            _ => return,
        };
        let mid = (best_bid + best_ask) * 0.5;
        if !mid.is_finite() || mid <= 0.0 || best_ask <= best_bid {
            return;
        }
        let lower = mid * (1.0 - self.looking_depth);
        let upper = mid * (1.0 + self.looking_depth);
        let imbalance = self.book.bids.sum_sizes_from(lower) - self.book.asks.sum_sizes_to(upper);
        self.imb_stats.push(imbalance);
        let alpha = self.imb_stats.zscore(imbalance);
        self.shared.update(alpha);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishes_alpha_after_warmup() {
        let shared = Arc::new(SharedAlpha::new(3));
        let mut obi = BinanceObi::new(100, 0.025, shared.clone());
        obi.apply_snapshot(vec![(99.0, 1.0), (100.0, 2.0)], vec![(101.0, 1.0), (102.0, 2.0)], 10);
        // vary imbalance across updates so std > 0 and zscore is meaningful
        for i in 0..5 {
            obi.apply_diff(&[(100.0, 2.0 + i as f64)], &[(101.0, 1.0)], 11 + i);
            obi.update_alpha();
        }
        assert!(shared.warmed_up());
        // alpha is finite
        assert!(shared.alpha().is_finite());
    }
}
