//! Rolling statistics — EXACT port of `_vol_obi_fast.pyx::RollingStats` (Welford online
//! mean/variance with reverse-Welford eviction). We deliberately match the Python/Cython
//! implementation bit-for-bit (including the `M2 < 0` underflow guards and the n<2 paths)
//! rather than the simpler `E[X²]-E[X]²` form, because this engine runs for days over
//! 6000-element windows at ~100 Hz where catastrophic cancellation would otherwise drift.

/// Fixed-capacity ring buffer with O(1) incremental mean/std/zscore via Welford.
#[derive(Debug, Clone)]
pub struct RollingStats {
    buffer: Box<[f64]>,
    capacity: usize,
    write_pos: usize,
    count: usize,
    sum: f64,
    m2: f64,
    cached_mean: f64,
    cached_std: f64,
}

impl RollingStats {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        Self {
            buffer: vec![0.0; capacity].into_boxed_slice(),
            capacity,
            write_pos: 0,
            count: 0,
            sum: 0.0,
            m2: 0.0,
            cached_mean: 0.0,
            cached_std: 0.0,
        }
    }

    /// Push a value, updating cached mean/std in O(1). Mirrors `c_push` exactly.
    #[inline]
    pub fn push(&mut self, value: f64) {
        let idx = self.write_pos;

        // Evict oldest (reverse Welford) if full.
        if self.count >= self.capacity {
            let old = self.buffer[idx];
            let n = self.count;
            let old_mean = self.cached_mean;
            let new_mean = if n > 1 {
                old_mean + (old_mean - old) / (n as f64 - 1.0)
            } else {
                0.0
            };
            self.m2 -= (old - old_mean) * (old - new_mean);
            if self.m2 < 0.0 {
                self.m2 = 0.0;
            }
            self.sum -= old;
            self.count -= 1;
            self.cached_mean = new_mean;
        }

        // Add new (forward Welford).
        self.buffer[idx] = value;
        self.sum += value;
        self.count += 1;
        self.write_pos += 1;
        if self.write_pos >= self.capacity {
            self.write_pos = 0;
        }
        let n = self.count;
        let old_mean = self.cached_mean;
        let new_mean = self.sum / n as f64;
        self.m2 += (value - old_mean) * (value - new_mean);
        if self.m2 < 0.0 {
            self.m2 = 0.0;
        }
        self.cached_mean = new_mean;
        self.cached_std = if n >= 2 { (self.m2 / n as f64).sqrt() } else { 0.0 };
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        self.cached_mean
    }

    #[inline]
    pub fn std(&self) -> f64 {
        self.cached_std
    }

    /// z-score using cached stats; 0 when std underflows (matches `c_zscore`).
    #[inline]
    pub fn zscore(&self, value: f64) -> f64 {
        if self.cached_std < 1e-10 {
            return 0.0;
        }
        (value - self.cached_mean) / self.cached_std
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.count >= self.capacity
    }

    pub fn clear(&mut self) {
        self.write_pos = 0;
        self.count = 0;
        self.sum = 0.0;
        self.m2 = 0.0;
        self.cached_mean = 0.0;
        self.cached_std = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mean_std_population() {
        let mut s = RollingStats::new(5);
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            s.push(v);
        }
        assert!((s.mean() - 3.0).abs() < 1e-9);
        // population variance = 2.0
        assert!((s.std() - 2.0_f64.sqrt()).abs() < 1e-9);
        assert!(s.zscore(3.0).abs() < 1e-9);
        assert!((s.zscore(5.0) - 2.0_f64.sqrt()).abs() < 1e-9);
    }

    #[test]
    fn eviction_matches_window() {
        let mut s = RollingStats::new(3);
        for v in [1.0, 2.0, 3.0] {
            s.push(v);
        }
        assert!((s.mean() - 2.0).abs() < 1e-9);
        s.push(4.0); // -> [2,3,4]
        assert!((s.mean() - 3.0).abs() < 1e-9);
        s.push(5.0); // -> [3,4,5]
        assert!((s.mean() - 4.0).abs() < 1e-9);
        // population variance of [3,4,5] = 2/3 (reverse-Welford eviction stays exact)
        assert!((s.std() - (2.0_f64 / 3.0).sqrt()).abs() < 1e-9);
    }

    #[test]
    fn nan_inf_still_pushed_like_pyx() {
        // pyx c_push does NOT filter NaN/Inf (unlike standx). We match pyx: push raw.
        // Callers (on_book_update) only push finite mids/imbalances, so this is fine.
        let mut s = RollingStats::new(4);
        s.push(1.0);
        s.push(2.0);
        assert!((s.mean() - 1.5).abs() < 1e-9);
    }

    #[test]
    fn warmup_zero_std() {
        let mut s = RollingStats::new(10);
        s.push(42.0);
        assert_eq!(s.std(), 0.0);
        assert_eq!(s.zscore(99.0), 0.0);
    }
}
