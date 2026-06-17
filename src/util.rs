//! Numeric helpers with strict Python parity.
//!
//! Critical (codex review): Python `round()` is banker's rounding (ties-to-even), whereas
//! `f64::round` is ties-away-from-zero. `_to_raw_price`/`_to_raw_amount` do
//! `int(round(value / tick))`, so raw integer encoding MUST use ties-to-even or signatures
//! will occasionally differ by one tick. We use `f64::round_ties_even` (Rust >= 1.77).

/// Banker's rounding to nearest integer (matches Python `round`).
#[inline]
pub fn py_round(x: f64) -> f64 {
    x.round_ties_even()
}

/// Convert a human price/amount to the exchange raw integer (`int(round(value/tick))`).
#[inline]
pub fn to_raw(value: f64, tick: f64) -> i64 {
    py_round(value / tick) as i64
}

/// Snap a price down to the tick grid (bids): `floor(price/tick)*tick`.
#[inline]
pub fn floor_to_tick(price: f64, tick: f64) -> f64 {
    (price / tick).floor() * tick
}

/// Snap a price up to the tick grid (asks): `ceil(price/tick)*tick`.
#[inline]
pub fn ceil_to_tick(price: f64, tick: f64) -> f64 {
    (price / tick).ceil() * tick
}

/// Price change in basis points; zero-safe. Mirrors `price_change_bps_fast`.
#[inline]
pub fn price_change_bps(old_price: f64, new_price: f64) -> f64 {
    if old_price <= 0.0 {
        return 1e18; // inf sentinel (matches pyx)
    }
    ((new_price - old_price).abs() / old_price) * 10_000.0
}

/// Dynamic max position in USD. Mirrors `dynamic_max_position_fast`:
/// `raw = capital*leverage - 2*num_levels*base_amount*mid`, then `*0.9`, floored at 0.
#[inline]
pub fn dynamic_max_position(
    mid: f64,
    capital: f64,
    leverage: i32,
    base_amount: f64,
    num_levels: i32,
) -> f64 {
    if capital <= 0.0 || mid <= 0.0 {
        return 0.0;
    }
    let mut raw = capital * leverage as f64;
    if base_amount > 0.0 {
        raw -= 2.0 * num_levels as f64 * base_amount * mid;
    }
    if raw < 0.0 {
        return 0.0;
    }
    raw * 0.9
}

#[inline]
pub fn clamp(x: f64, lo: f64, hi: f64) -> f64 {
    if x < lo {
        lo
    } else if x > hi {
        hi
    } else {
        x
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banker_rounding() {
        // ties to even
        assert_eq!(py_round(0.5), 0.0);
        assert_eq!(py_round(1.5), 2.0);
        assert_eq!(py_round(2.5), 2.0);
        assert_eq!(py_round(-0.5), 0.0);
        assert_eq!(py_round(-1.5), -2.0);
        assert_eq!(py_round(2.4), 2.0);
        assert_eq!(py_round(2.6), 3.0);
    }

    #[test]
    fn raw_conversion() {
        assert_eq!(to_raw(100.0, 0.1), 1000);
        assert_eq!(to_raw(1.00005, 0.0001), 10000); // 10000.5 -> ties even -> 10000
        assert_eq!(to_raw(1.00015, 0.0001), 10002); // 10001.5 -> ties even -> 10002
    }

    #[test]
    fn tick_snap() {
        assert!((floor_to_tick(100.27, 0.1) - 100.2).abs() < 1e-9);
        assert!((ceil_to_tick(100.21, 0.1) - 100.3).abs() < 1e-9);
    }

    #[test]
    fn bps_and_maxpos() {
        assert!((price_change_bps(100.0, 100.1) - 10.0).abs() < 1e-6);
        assert_eq!(price_change_bps(0.0, 1.0), 1e18);
        // capital 1000, lev 2 => 2000; reserve 2*2*0.0002*50000 = 40; (2000-40)*0.9 = 1764
        let mp = dynamic_max_position(50000.0, 1000.0, 2, 0.0002, 2);
        assert!((mp - 1764.0).abs() < 1e-6);
    }
}
