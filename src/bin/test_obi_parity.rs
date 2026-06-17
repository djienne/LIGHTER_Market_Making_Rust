//! P1 gate: run a fully deterministic scenario through the Rust VolObiCalculator and
//! print volatility / alpha / quote. A twin Python script feeds the SAME scenario through
//! the compiled `_vol_obi_fast` Cython engine; the outputs must match to ~1e-9.
//!
//! Scenario is integer-driven (no transcendentals) so both languages produce identical f64.

use lighter_mm::book::local_book::LocalBook;
use lighter_mm::strategy::vol_obi::{VolObiCalculator, VolObiConfig};

const N: i64 = 300;
const TICK: f64 = 0.01;
const MAX_POS_USD: f64 = 10_000.0;
const POSITION: f64 = 0.5;

fn mid_at(t: i64) -> f64 {
    100.0 + ((t % 21 - 10) as f64) * 0.05
}

fn build_book(t: i64) -> LocalBook {
    let mid = mid_at(t);
    let bids = vec![
        (mid - 0.1, 1.0 + (t % 5) as f64),
        (mid - 0.2, 2.0),
        (mid - 0.3, 3.0),
    ];
    let asks = vec![
        (mid + 0.1, 1.0),
        (mid + 0.2, 2.0 + (t % 3) as f64),
        (mid + 0.3, 3.0),
    ];
    let mut b = LocalBook::new();
    b.apply_snapshot(bids, asks);
    b
}

fn main() {
    let cfg = VolObiConfig {
        window_steps: 6000,
        step_ns: 100_000_000,
        vol_to_half_spread: 60.0,
        min_half_spread_bps: 4.0,
        c1_ticks: 40.0,
        c1: 0.0,
        skew: 0.1,
        looking_depth: 0.025,
        min_warmup_samples: 100,
    };
    let mut calc = VolObiCalculator::new(&cfg, TICK, MAX_POS_USD);

    let mut last_mid = 0.0;
    for t in 0..N {
        let book = build_book(t);
        let mid = mid_at(t);
        calc.on_book_update(mid, &book.bids, &book.asks);
        last_mid = mid;
    }

    let (bid, ask) = calc.quote(last_mid, POSITION).unwrap_or((f64::NAN, f64::NAN));
    println!("RUST_VOL {:.17e}", calc.volatility());
    println!("RUST_ALPHA {:.17e}", calc.alpha());
    println!("RUST_BID {:.17e}", bid);
    println!("RUST_ASK {:.17e}", ask);
}
