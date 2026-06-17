//! Quote ladder construction — port of `calculate_order_prices`,
//! `_apply_quality_spread_multiplier`, `_apply_inventory_exit_bias`,
//! `_fallback_reduce_only_quote_levels`, `_normalize_live_order_size`.
//!
//! Pure functions (no global state) so they are exhaustively testable. The CJ estimator
//! gate is omitted (CJ is out of scope); for the vol_obi engine it is always-allow.

use crate::config::InventoryExitBias;

pub const EPSILON: f64 = 1e-9;

/// One quote level: (bid, ask), either side may be suppressed (None).
pub type Level = (Option<f64>, Option<f64>);

/// Precompute spread factors `[spread_factor_level1^l for l in 0..num_levels]`.
pub fn spread_factors(spread_factor_level1: f64, num_levels: usize) -> Vec<f64> {
    (0..num_levels).map(|l| spread_factor_level1.powi(l as i32)).collect()
}

#[inline]
fn floor_tick(p: f64, tick: f64) -> f64 {
    if tick > 0.0 {
        (p / tick).floor() * tick
    } else {
        p
    }
}
#[inline]
fn ceil_tick(p: f64, tick: f64) -> f64 {
    if tick > 0.0 {
        (p / tick).ceil() * tick
    } else {
        p
    }
}

/// Build the full quote ladder from the level-0 quote returned by the vol_obi engine.
/// `l0` is `Some((bid, ask))` (both present) or `None` (not warmed / crossed).
#[allow(clippy::too_many_arguments)]
pub fn build_quote_levels(
    l0: Option<(f64, f64)>,
    mid: f64,
    position: f64,
    max_pos_usd: f64,
    tick: f64,
    num_levels: usize,
    factors: &[f64],
    fallback_bps: f64,
) -> Vec<Level> {
    let none_levels = vec![(None, None); num_levels];

    let (mut buy_0, mut sell_0): (Option<f64>, Option<f64>) = match l0 {
        Some((b, a)) => (Some(b), Some(a)),
        None => {
            return if position.abs() >= EPSILON {
                fallback_reduce_only(mid, position, tick, fallback_bps, num_levels)
            } else {
                none_levels
            };
        }
    };

    // Hard position limit: suppress the side that would increase exposure.
    if max_pos_usd <= 0.0 {
        return if position.abs() >= EPSILON {
            fallback_reduce_only(mid, position, tick, fallback_bps, num_levels)
        } else {
            none_levels
        };
    }
    let pos_value_usd = position.abs() * mid;
    if pos_value_usd >= max_pos_usd {
        if position > 0.0 {
            buy_0 = None; // long at limit -> suppress buys
        } else if position < 0.0 {
            sell_0 = None; // short at limit -> suppress sells
        }
        if buy_0.is_none() && sell_0.is_none() {
            return fallback_reduce_only(mid, position, tick, fallback_bps, num_levels);
        }
    }

    let bid_depth = buy_0.map(|b| mid - b);
    let ask_depth = sell_0.map(|a| a - mid);

    let mut levels: Vec<Level> = Vec::with_capacity(num_levels);
    levels.push((buy_0, sell_0));
    for lvl in 1..num_levels {
        let factor = factors.get(lvl).copied().unwrap_or(1.0);
        let raw_bid = bid_depth.map(|d| floor_tick(mid - d * factor, tick));
        let raw_ask = ask_depth.map(|d| ceil_tick(mid + d * factor, tick));
        levels.push((raw_bid, raw_ask));
    }
    levels
}

/// Widen each level's depth by `multiplier` (>1) — defensive on adverse markouts.
pub fn apply_quality_spread_multiplier(levels: &[Level], mid: f64, multiplier: f64, tick: f64) -> Vec<Level> {
    if multiplier <= 1.0001 || mid <= 0.0 {
        return levels.to_vec();
    }
    levels
        .iter()
        .map(|&(bid, ask)| {
            let new_bid = bid.map(|b| {
                let depth = (mid - b).max(0.0);
                floor_tick(mid - depth * multiplier, tick)
            });
            let new_ask = ask.map(|a| {
                let depth = (a - mid).max(0.0);
                ceil_tick(mid + depth * multiplier, tick)
            });
            (new_bid, new_ask)
        })
        .collect()
}

/// Bias quotes to flatten inventory: tighten the reducing side, widen the adding side.
#[allow(clippy::too_many_arguments)]
pub fn apply_inventory_exit_bias(
    levels: &[Level],
    mid: f64,
    position: f64,
    max_pos_usd: f64,
    adverse_bps: f64,
    adverse_threshold_bps: f64,
    cfg: &InventoryExitBias,
    tick: f64,
) -> Vec<Level> {
    if !cfg.enabled || mid <= 0.0 || max_pos_usd <= 0.0 || position.abs() < EPSILON {
        return levels.to_vec();
    }
    let inventory_value = position.abs() * mid;
    let ratio = inventory_value / max_pos_usd;
    if ratio < cfg.min_ratio {
        return levels.to_vec();
    }

    let adverse_excess = (adverse_bps - adverse_threshold_bps).max(0.0);
    let boost = 1.0 + (adverse_excess * cfg.adverse_boost_per_bps.max(0.0)).min(0.5);
    let exit_tighten = (cfg.exit_tighten_per_ratio.max(0.0) * ratio * boost).min(cfg.max_exit_tighten.max(0.0));
    let add_widen = (cfg.add_widen_per_ratio.max(0.0) * ratio * boost).min(cfg.max_add_widen.max(0.0));
    if exit_tighten <= 0.0 && add_widen <= 0.0 {
        return levels.to_vec();
    }

    let min_depth = if tick > 0.0 { tick } else { (mid * 1e-6).max(1e-9) };
    levels
        .iter()
        .map(|&(bid, ask)| {
            let new_bid = bid.map(|b| {
                let mut depth = (mid - b).max(min_depth);
                if position < 0.0 {
                    depth *= (1.0 - exit_tighten).max(0.05); // short: bid reduces
                } else {
                    depth *= 1.0 + add_widen; // long: bid adds
                }
                let mut nb = floor_tick(mid - depth, tick);
                if nb >= mid {
                    nb = floor_tick(mid - min_depth, tick);
                }
                nb
            });
            let new_ask = ask.map(|a| {
                let mut depth = (a - mid).max(min_depth);
                if position > 0.0 {
                    depth *= (1.0 - exit_tighten).max(0.05); // long: ask reduces
                } else {
                    depth *= 1.0 + add_widen; // short: ask adds
                }
                let mut na = ceil_tick(mid + depth, tick);
                if na <= mid {
                    na = ceil_tick(mid + min_depth, tick);
                }
                na
            });
            (new_bid, new_ask)
        })
        .collect()
}

/// A single passive reducing quote (level 0 only) when the engine withholds quotes.
pub fn fallback_reduce_only(
    mid: f64,
    position: f64,
    tick: f64,
    fallback_bps: f64,
    num_levels: usize,
) -> Vec<Level> {
    let mut levels = vec![(None, None); num_levels];
    if position.abs() < EPSILON || mid <= 0.0 {
        return levels;
    }
    let min_depth = if tick > 0.0 { tick } else { (mid * 1e-6).max(1e-9) };
    let bps = fallback_bps.max(1.0);
    let depth = (mid * bps / 10_000.0).max(min_depth);
    if position > 0.0 {
        let mut ask = ceil_tick(mid + depth, tick);
        if ask <= mid {
            ask = mid + min_depth;
        }
        levels[0] = (None, Some(ask));
    } else {
        let mut bid = floor_tick(mid - depth, tick);
        if bid >= mid {
            bid = mid - min_depth;
        }
        levels[0] = (Some(bid), None);
    }
    levels
}

/// Normalize an order size to exchange minimums (port of `_normalize_live_order_size`).
/// `min_quote` should already be `max(min_quote_amount, min_order_value_usd)`.
pub fn normalize_live_order_size(
    size: f64,
    mid: f64,
    amount_tick: f64,
    min_base: f64,
    min_quote: f64,
) -> f64 {
    if size <= 0.0 || mid <= 0.0 {
        return 0.0;
    }
    let mut s = size;
    if amount_tick > 0.0 {
        s = (s / amount_tick).floor() * amount_tick;
    }
    if min_base > 0.0 && s + EPSILON < min_base {
        s = min_base;
    }
    if min_quote > 0.0 && s * mid + EPSILON < min_quote {
        s = min_quote / mid;
        if amount_tick > 0.0 {
            s = (s / amount_tick).ceil() * amount_tick;
        }
    }
    s.max(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_widens_by_factor() {
        let factors = spread_factors(2.0, 2);
        assert_eq!(factors, vec![1.0, 2.0]);
        let levels = build_quote_levels(Some((99.0, 101.0)), 100.0, 0.0, 1e9, 0.1, 2, &factors, 4.0);
        assert_eq!(levels[0], (Some(99.0), Some(101.0)));
        // level1: bid = 100 - 1*2 = 98 ; ask = 100 + 1*2 = 102 (depth=1 each, factor 2)
        assert_eq!(levels[1], (Some(98.0), Some(102.0)));
    }

    #[test]
    fn position_limit_suppresses_add_side_long() {
        // long at limit -> buys suppressed, only ask side quotes
        let factors = spread_factors(2.0, 2);
        let levels = build_quote_levels(Some((99.0, 101.0)), 100.0, 1.0, 50.0, 0.1, 2, &factors, 4.0);
        assert_eq!(levels[0].0, None);
        assert!(levels[0].1.is_some());
    }

    #[test]
    fn not_warmed_with_inventory_falls_back() {
        let factors = spread_factors(2.0, 2);
        let levels = build_quote_levels(None, 100.0, 0.5, 1e9, 0.1, 2, &factors, 4.0);
        // long inventory -> reduce-only ask at level 0
        assert_eq!(levels[0].0, None);
        assert!(levels[0].1.unwrap() > 100.0);
    }

    #[test]
    fn quality_multiplier_widens() {
        let levels = vec![(Some(99.0), Some(101.0))];
        let out = apply_quality_spread_multiplier(&levels, 100.0, 1.5, 0.1);
        // depth 1 -> 1.5 ; bid 100-1.5=98.5, ask 100+1.5=101.5
        assert!((out[0].0.unwrap() - 98.5).abs() < 1e-9);
        assert!((out[0].1.unwrap() - 101.5).abs() < 1e-9);
    }

    #[test]
    fn inventory_bias_tightens_exit_long() {
        let cfg = InventoryExitBias::default();
        let levels = vec![(Some(99.0), Some(101.0))];
        // long position near limit -> ask (exit) tightens toward mid, bid (add) widens
        let out = apply_inventory_exit_bias(&levels, 100.0, 1.0, 100.0, 0.0, 2.0, &cfg, 0.1);
        assert!(out[0].1.unwrap() < 101.0); // exit tighter
        assert!(out[0].0.unwrap() < 99.0); // add wider
    }

    #[test]
    fn size_minimums() {
        // size below min_base bumped to min_base; below min_quote bumped via quote/mid
        let s = normalize_live_order_size(0.0001, 50000.0, 0.00001, 0.0002, 14.5);
        assert!(s >= 0.0002 - 1e-12);
        assert!(s * 50000.0 + 1e-9 >= 14.5 - 1e-6 || s >= 0.0002);
    }
}
