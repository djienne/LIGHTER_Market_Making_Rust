//! Live fill accounting: signed-position VWAP + realized PnL estimation.
//!
//! Ported exactly from `lighter_MM/market_maker_v2.py`:
//!   - `_apply_live_fill_accounting` (~2133-2187)
//!   - `_live_fill_*` module globals (~799-811)
//!
//! This is observability only: the exchange portfolio value remains
//! authoritative. We maintain a local estimate of signed position size, the
//! entry VWAP, and a cumulative realized-PnL figure (net of maker fees).

use crate::types::Side;

/// Matches Python `utils.EPSILON = 1e-9`.
const EPSILON: f64 = 1e-9;

/// Result of applying a single fill to the accounting state.
///
/// Mirrors the Python return tuple
/// `(position_after_est, realized_delta, realized_cumulative, entry_vwap_after, fee_usd)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FillResult {
    /// Signed position size after this fill (positive = long, negative = short).
    pub position_after: f64,
    /// Realized PnL change attributed to this fill. Starts at `-fee_usd` and
    /// gains the closed-leg PnL when the fill reduces/flips the position.
    pub realized_delta: f64,
    /// Cumulative realized PnL after applying `realized_delta`.
    pub realized_cumulative: f64,
    /// Entry VWAP after this fill (0.0 when flat).
    pub entry_vwap_after: f64,
    /// Absolute maker fee in USD charged for this fill (`|price * size * rate|`).
    pub fee_usd: f64,
}

/// Local live-fill accounting state.
///
/// State fields correspond to the Python globals:
///   - `position_size`  <- `_live_fill_position_size`  (signed)
///   - `entry_vwap`     <- `_live_fill_entry_vwap`
///   - `realized_pnl_cumulative` <- `_live_fill_realized_pnl`
///   - `fill_count`     <- `_live_fill_count`
///   - `volume_usd`     <- `_live_volume_usd`
#[derive(Debug, Clone)]
pub struct FillAccounting {
    maker_fee_rate: f64,
    position_size: f64,
    entry_vwap: f64,
    realized_pnl_cumulative: f64,
    fill_count: u64,
    volume_usd: f64,
}

impl Default for FillAccounting {
    fn default() -> Self {
        Self::new(0.0)
    }
}

impl FillAccounting {
    /// Construct fresh accounting with the given maker fee rate (default 0.0).
    pub fn new(maker_fee_rate: f64) -> Self {
        Self {
            maker_fee_rate,
            position_size: 0.0,
            entry_vwap: 0.0,
            realized_pnl_cumulative: 0.0,
            fill_count: 0,
            volume_usd: 0.0,
        }
    }

    /// Restore accounting from a persisted snapshot (position, vwap, cumulative
    /// realized PnL). Fee rate is still required to compute fees on new fills.
    /// `fill_count` and `volume_usd` start at zero, mirroring how the Python
    /// process rebuilds those counters per session.
    pub fn from_snapshot(
        maker_fee_rate: f64,
        position: f64,
        vwap: f64,
        realized_cum: f64,
    ) -> Self {
        Self {
            maker_fee_rate,
            position_size: position,
            entry_vwap: vwap,
            realized_pnl_cumulative: realized_cum,
            fill_count: 0,
            volume_usd: 0.0,
        }
    }

    /// Apply a single fill and return the resulting accounting deltas.
    ///
    /// Direct port of `_apply_live_fill_accounting`. `side` is the side of *our*
    /// fill (Buy increases the signed position, Sell decreases it).
    pub fn apply(&mut self, side: Side, price: f64, size: f64) -> FillResult {
        // signed_fill = size if side == "buy" else -size
        let signed_fill = match side {
            Side::Buy => size,
            Side::Sell => -size,
        };
        // fee_usd = abs(price * size * MAKER_FEE_RATE)
        let fee_usd = (price * size * self.maker_fee_rate).abs();

        let pos = self.position_size;
        let vwap = self.entry_vwap;
        let mut realized_delta = -fee_usd;

        let new_pos;
        let new_vwap;

        if pos.abs() < EPSILON {
            // Flat -> open at price.
            new_pos = signed_fill;
            new_vwap = if new_pos.abs() >= EPSILON { price } else { 0.0 };
        } else if pos * signed_fill > 0.0 {
            // Same-sign add -> weighted-average VWAP.
            let new_abs = pos.abs() + signed_fill.abs();
            new_pos = pos + signed_fill;
            new_vwap = ((pos.abs() * vwap) + (signed_fill.abs() * price)) / new_abs;
        } else {
            // Opposite side -> realize closed leg, then flatten/flip.
            let closing_size = pos.abs().min(signed_fill.abs());
            if pos > 0.0 && side == Side::Sell {
                // Long exit.
                realized_delta += (price - vwap) * closing_size;
            } else if pos < 0.0 && side == Side::Buy {
                // Short exit.
                realized_delta += (vwap - price) * closing_size;
            }

            let candidate = pos + signed_fill;
            if candidate.abs() < EPSILON {
                new_pos = 0.0;
                new_vwap = 0.0;
            } else if pos * candidate > 0.0 {
                // Still same side: keep old vwap.
                new_pos = candidate;
                new_vwap = vwap;
            } else {
                // Flipped to the opposite side: vwap resets to fill price.
                new_pos = candidate;
                new_vwap = price;
            }
        }

        self.position_size = new_pos;
        self.entry_vwap = new_vwap;
        self.realized_pnl_cumulative += realized_delta;
        self.fill_count += 1;
        self.volume_usd += (price * size).abs();

        FillResult {
            position_after: new_pos,
            realized_delta,
            realized_cumulative: self.realized_pnl_cumulative,
            entry_vwap_after: new_vwap,
            fee_usd,
        }
    }

    // --- Getters ---

    /// Configured maker fee rate.
    #[inline]
    pub fn maker_fee_rate(&self) -> f64 {
        self.maker_fee_rate
    }

    /// Current signed position size (positive long, negative short).
    #[inline]
    pub fn position_size(&self) -> f64 {
        self.position_size
    }

    /// Current entry VWAP (0.0 when flat).
    #[inline]
    pub fn entry_vwap(&self) -> f64 {
        self.entry_vwap
    }

    /// Cumulative realized PnL (net of maker fees).
    #[inline]
    pub fn realized_pnl_cumulative(&self) -> f64 {
        self.realized_pnl_cumulative
    }

    /// Number of fills applied this session.
    #[inline]
    pub fn fill_count(&self) -> u64 {
        self.fill_count
    }

    /// Cumulative notional volume in USD (`sum |price * size|`).
    #[inline]
    pub fn volume_usd(&self) -> f64 {
        self.volume_usd
    }

    /// Snapshot of the persistable accounting state
    /// `(position_size, entry_vwap, realized_pnl_cumulative)`.
    pub fn snapshot(&self) -> FillSnapshot {
        FillSnapshot {
            position_size: self.position_size,
            entry_vwap: self.entry_vwap,
            realized_pnl_cumulative: self.realized_pnl_cumulative,
        }
    }
}

/// Persistable subset of accounting state.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FillSnapshot {
    pub position_size: f64,
    pub entry_vwap: f64,
    pub realized_pnl_cumulative: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOL: f64 = 1e-12;

    fn approx(a: f64, b: f64) {
        assert!((a - b).abs() < TOL, "expected {b}, got {a}");
    }

    #[test]
    fn open_long_zero_fee() {
        let mut acc = FillAccounting::new(0.0);
        let r = acc.apply(Side::Buy, 100.0, 2.0);
        approx(r.position_after, 2.0);
        approx(r.realized_delta, 0.0); // no fee, no realization on open
        approx(r.realized_cumulative, 0.0);
        approx(r.entry_vwap_after, 100.0);
        approx(r.fee_usd, 0.0);
        approx(acc.volume_usd(), 200.0);
        assert_eq!(acc.fill_count(), 1);
    }

    #[test]
    fn open_long_with_fee() {
        // maker_fee_rate 0.00004; fee = |100*2*0.00004| = 0.008
        let mut acc = FillAccounting::new(0.00004);
        let r = acc.apply(Side::Buy, 100.0, 2.0);
        approx(r.fee_usd, 0.008);
        approx(r.realized_delta, -0.008);
        approx(r.realized_cumulative, -0.008);
        approx(r.position_after, 2.0);
        approx(r.entry_vwap_after, 100.0);
    }

    #[test]
    fn add_long_weighted_vwap() {
        let mut acc = FillAccounting::new(0.0);
        acc.apply(Side::Buy, 100.0, 2.0); // pos 2 @ 100
        let r = acc.apply(Side::Buy, 110.0, 2.0); // add 2 @ 110
        // new_vwap = (2*100 + 2*110)/4 = 420/4 = 105
        approx(r.position_after, 4.0);
        approx(r.entry_vwap_after, 105.0);
        approx(r.realized_delta, 0.0);
        approx(r.realized_cumulative, 0.0);

        // Uneven add: add 6 @ 120 -> (4*105 + 6*120)/10 = (420+720)/10 = 114
        let r2 = acc.apply(Side::Buy, 120.0, 6.0);
        approx(r2.position_after, 10.0);
        approx(r2.entry_vwap_after, 114.0);
    }

    #[test]
    fn partial_close_long_profit_sign() {
        let mut acc = FillAccounting::new(0.0);
        acc.apply(Side::Buy, 100.0, 4.0); // long 4 @ 100
        let r = acc.apply(Side::Sell, 110.0, 1.0); // close 1 @ 110
        // long exit: (price - vwap)*closing = (110-100)*1 = +10
        approx(r.realized_delta, 10.0);
        approx(r.realized_cumulative, 10.0);
        approx(r.position_after, 3.0);
        approx(r.entry_vwap_after, 100.0); // vwap unchanged, still long
    }

    #[test]
    fn partial_close_long_loss_sign() {
        let mut acc = FillAccounting::new(0.0);
        acc.apply(Side::Buy, 100.0, 4.0);
        let r = acc.apply(Side::Sell, 90.0, 2.0); // sell below entry -> loss
        approx(r.realized_delta, -20.0); // (90-100)*2
        approx(r.realized_cumulative, -20.0);
        approx(r.position_after, 2.0);
        approx(r.entry_vwap_after, 100.0);
    }

    #[test]
    fn full_close_long_to_flat() {
        let mut acc = FillAccounting::new(0.0);
        acc.apply(Side::Buy, 100.0, 3.0);
        let r = acc.apply(Side::Sell, 105.0, 3.0); // close all
        approx(r.realized_delta, 15.0); // (105-100)*3
        approx(r.realized_cumulative, 15.0);
        approx(r.position_after, 0.0);
        approx(r.entry_vwap_after, 0.0);
        assert!(acc.position_size().abs() < EPSILON);
    }

    #[test]
    fn flip_long_to_short() {
        let mut acc = FillAccounting::new(0.0);
        acc.apply(Side::Buy, 100.0, 2.0); // long 2 @ 100
        // sell 5 @ 110: closes 2 (realize (110-100)*2 = +20), flips to short 3 @ 110
        let r = acc.apply(Side::Sell, 110.0, 5.0);
        approx(r.realized_delta, 20.0);
        approx(r.realized_cumulative, 20.0);
        approx(r.position_after, -3.0);
        approx(r.entry_vwap_after, 110.0); // new short vwap = fill price
    }

    #[test]
    fn short_exit_profit_sign() {
        let mut acc = FillAccounting::new(0.0);
        acc.apply(Side::Sell, 100.0, 3.0); // short 3 @ 100
        approx(acc.entry_vwap(), 100.0);
        approx(acc.position_size(), -3.0);
        // buy 1 @ 90 -> short exit profit (vwap - price)*1 = (100-90)*1 = +10
        let r = acc.apply(Side::Buy, 90.0, 1.0);
        approx(r.realized_delta, 10.0);
        approx(r.position_after, -2.0);
        approx(r.entry_vwap_after, 100.0);
    }

    #[test]
    fn flip_short_to_long() {
        let mut acc = FillAccounting::new(0.0);
        acc.apply(Side::Sell, 100.0, 2.0); // short 2 @ 100
        // buy 5 @ 90: closes 2 (realize (100-90)*2 = +20), flips long 3 @ 90
        let r = acc.apply(Side::Buy, 90.0, 5.0);
        approx(r.realized_delta, 20.0);
        approx(r.position_after, 3.0);
        approx(r.entry_vwap_after, 90.0);
    }

    #[test]
    fn from_snapshot_and_snapshot_roundtrip() {
        let acc = FillAccounting::from_snapshot(0.00004, -5.0, 250.0, 12.5);
        approx(acc.position_size(), -5.0);
        approx(acc.entry_vwap(), 250.0);
        approx(acc.realized_pnl_cumulative(), 12.5);
        approx(acc.maker_fee_rate(), 0.00004);
        let snap = acc.snapshot();
        approx(snap.position_size, -5.0);
        approx(snap.entry_vwap, 250.0);
        approx(snap.realized_pnl_cumulative, 12.5);
    }

    #[test]
    fn close_with_fee_combines_realization_and_fee() {
        let mut acc = FillAccounting::new(0.00004);
        acc.apply(Side::Buy, 100.0, 4.0); // open long, fee on this fill only affects cumulative
        let cum_after_open = acc.realized_pnl_cumulative();
        approx(cum_after_open, -(100.0 * 4.0 * 0.00004)); // -0.016
        let r = acc.apply(Side::Sell, 110.0, 2.0);
        // fee = |110*2*0.00004| = 0.0088 ; realized = -0.0088 + (110-100)*2 = 19.9912
        approx(r.fee_usd, 0.0088);
        approx(r.realized_delta, 20.0 - 0.0088);
        approx(r.realized_cumulative, cum_after_open + (20.0 - 0.0088));
    }
}
