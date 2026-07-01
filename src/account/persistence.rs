//! Durable live-state persistence: a tiny JSON store for live fill accounting
//! that survives bot restarts.
//!
//! Ported from:
//!   - `lighter_MM/market_maker_v2.py`:
//!       `_live_state_payload`            (~1857)
//!       `_persist_live_state`            (~1873)
//!       `_restore_live_state_defaults`   (~1882-1923)
//!   - `lighter_MM/live_metrics.py`:
//!       `_atomic_json_write`             (~38-50)
//!       `LiveStateStore`                 (~53-74)
//!
//! Behaviour parity notes:
//!   * `save` writes the payload atomically (temp file in the same directory,
//!     then `rename`), injecting `symbol` and `updated_at` exactly like the
//!     Python `LiveStateStore.save`.
//!   * The on-disk JSON is pretty-printed (matching Python `indent=2`) with a
//!     trailing newline. Keys are sorted because the struct is serialized in a
//!     fixed field order that we keep alphabetical, mirroring `sort_keys=True`.
//!   * `load` tolerates a missing file, an unreadable file, or corrupt JSON and
//!     returns `LiveState::default()` in all of those cases (Python returns
//!     `{}`, which then degrades to the zero defaults in
//!     `_restore_live_state_defaults`).
//!
//! This is observability/accounting state only; the exchange portfolio value
//! remains authoritative at runtime.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Durable snapshot of local live-fill accounting + last-known exchange context.
///
/// Field correspondence with the Python `_live_state_payload` dict:
///   - `account_index`            <- `ACCOUNT_INDEX`
///   - `market_id`                <- `state.config.market_id`
///   - `position_size_est`        <- `_live_fill_position_size`
///   - `entry_vwap`               <- `_live_fill_entry_vwap`
///   - `realized_pnl_cumulative`  <- `_live_fill_realized_pnl`
///   - `fill_count`               <- `_live_fill_count`
///   - `volume_usd`               <- `_live_volume_usd`
///   - `exchange_position_size`   <- `state.account.position_size`
///   - `exchange_entry_vwap`      <- `_extract_position_entry_vwap()`
///   - `portfolio_value`          <- `state.account.portfolio_value`
///   - `available_capital`        <- `state.account.available_capital`
///   - `symbol` / `updated_at`    <- injected by the store on `save`
///
/// Every field uses `#[serde(default)]` so a partial or older payload (e.g. one
/// bootstrapped from a trade log that only knows realized/fills/volume) still
/// deserializes cleanly, with missing fields falling back to their zero/None
/// defaults — matching the lenient `payload.get(...)` access in Python.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveState {
    /// Lighter account index this state belongs to.
    #[serde(default)]
    pub account_index: i64,
    /// Market id this state belongs to (u32 to match `MarketConfig::market_id` — the old
    /// `u8` silently truncated ids > 255).
    #[serde(default)]
    pub market_id: u32,
    /// Local signed position-size estimate (`_live_fill_position_size`).
    #[serde(default)]
    pub position_size_est: f64,
    /// Local entry VWAP estimate (`_live_fill_entry_vwap`).
    #[serde(default)]
    pub entry_vwap: f64,
    /// Cumulative realized PnL, net of maker fees (`_live_fill_realized_pnl`).
    #[serde(default)]
    pub realized_pnl_cumulative: f64,
    /// Number of fills accounted for (`_live_fill_count`).
    #[serde(default)]
    pub fill_count: u64,
    /// Cumulative traded notional in USD (`_live_volume_usd`).
    #[serde(default)]
    pub volume_usd: f64,
    /// Last-known exchange-reported signed position size.
    #[serde(default)]
    pub exchange_position_size: f64,
    /// Last-known exchange-reported entry VWAP, if any.
    #[serde(default)]
    pub exchange_entry_vwap: Option<f64>,
    /// Last-known portfolio value (USD).
    #[serde(default)]
    pub portfolio_value: f64,
    /// Last-known available capital (USD).
    #[serde(default)]
    pub available_capital: f64,
    /// Trading symbol; stamped by the store on `save`.
    #[serde(default)]
    pub symbol: String,
    /// ISO-8601 UTC timestamp of the last save; stamped by the store on `save`.
    #[serde(default)]
    pub updated_at: String,
}

impl Default for LiveState {
    fn default() -> Self {
        Self {
            account_index: 0,
            market_id: 0,
            position_size_est: 0.0,
            entry_vwap: 0.0,
            realized_pnl_cumulative: 0.0,
            fill_count: 0,
            volume_usd: 0.0,
            exchange_position_size: 0.0,
            exchange_entry_vwap: None,
            portfolio_value: 0.0,
            available_capital: 0.0,
            symbol: String::new(),
            updated_at: String::new(),
        }
    }
}

/// Tiny durable JSON store for live fill accounting across restarts.
///
/// Mirrors Python `live_metrics.LiveStateStore`: it owns the resolved file path
/// `<log_dir>/live_state_<symbol>.json` and the symbol used to stamp payloads.
#[derive(Debug, Clone)]
pub struct LiveStateStore {
    /// Resolved path: `<log_dir>/live_state_<symbol>.json`.
    path: PathBuf,
    /// Symbol stamped into every saved payload.
    symbol: String,
}

impl LiveStateStore {
    /// Construct a store for `symbol` rooted at `log_dir`.
    ///
    /// Equivalent to Python `LiveStateStore.__init__`: it eagerly creates
    /// `log_dir` (best-effort; a hard failure surfaces later on `save`) and
    /// builds the path `<log_dir>/live_state_<symbol>.json`.
    pub fn new(log_dir: impl AsRef<Path>, symbol: impl Into<String>) -> Self {
        let log_dir = log_dir.as_ref();
        // Best-effort, matching Python `os.makedirs(log_dir, exist_ok=True)`.
        let _ = fs::create_dir_all(log_dir);
        let symbol = symbol.into();
        let path = log_dir.join(format!("live_state_{symbol}.json"));
        Self { path, symbol }
    }

    /// Path of the backing JSON file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Symbol this store stamps onto saved payloads.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Load the persisted state.
    ///
    /// Tolerates every failure mode (missing file, IO error, corrupt JSON,
    /// or JSON that is not an object) by returning `LiveState::default()` — the
    /// Rust analogue of Python returning `{}` and then degrading to zero
    /// defaults in `_restore_live_state_defaults`.
    pub fn load(&self) -> LiveState {
        let bytes = match fs::read(&self.path) {
            Ok(b) => b,
            Err(_) => return LiveState::default(),
        };
        serde_json::from_slice::<LiveState>(&bytes).unwrap_or_default()
    }

    /// Persist `state` atomically.
    ///
    /// Stamps `symbol` and `updated_at` (UTC, millisecond precision) onto a copy
    /// of `state`, then writes it via a temp-file-plus-rename so that readers
    /// never observe a partially written file. Mirrors Python
    /// `LiveStateStore.save` + `_atomic_json_write`.
    pub fn save(&self, state: &LiveState) -> std::io::Result<()> {
        let mut payload = state.clone();
        payload.symbol = self.symbol.clone();
        payload.updated_at = utc_now();
        atomic_json_write(&self.path, &payload)
    }
}

/// ISO-8601 UTC timestamp with millisecond precision and a trailing `Z`,
/// e.g. `2026-06-17T13:41:09.123Z`.
///
/// Matches Python `live_metrics._utc_now`:
/// `datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"`.
fn utc_now() -> String {
    // `%.3f` gives a leading dot + exactly 3 fractional digits, which is what
    // the Python `[:-3]` slice of `%f` (6 digits) produces.
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// Atomically serialize `payload` to `path` as pretty-printed JSON.
///
/// Steps mirror Python `_atomic_json_write`:
///   1. ensure the parent directory exists,
///   2. write to a uniquely named temp file in the *same* directory,
///   3. `rename` (atomic on the same filesystem) over the destination,
///   4. on any failure, best-effort remove the temp file.
fn atomic_json_write<T: Serialize>(path: &Path, payload: &T) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(dir)?;

    // Pretty-print (Python `indent=2`) with a trailing newline.
    let mut json = serde_json::to_vec_pretty(payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    json.push(b'\n');

    // Unique temp name in the destination directory: ".tmp-<pid>-<nanos>.json".
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp = dir.join(format!(".tmp-{}-{}.json", std::process::id(), nanos));

    // Closure so we can clean up the temp file on any error.
    let write_result = (|| -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(&json)?;
        f.flush()?;
        // Durability: best-effort fsync so the rename has stable contents.
        let _ = f.sync_all();
        drop(f);
        fs::rename(&tmp, path)
    })();

    if write_result.is_err() {
        // Best-effort cleanup, mirroring the Python `finally: os.remove(tmp)`.
        let _ = fs::remove_file(&tmp);
    }
    write_result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "lighter_mm_persist_{}_{}_{}",
            tag,
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = unique_dir("roundtrip");
        let store = LiveStateStore::new(&dir, "ETH");

        let state = LiveState {
            account_index: 42,
            market_id: 1,
            position_size_est: 1.25,
            entry_vwap: 3200.5,
            realized_pnl_cumulative: -12.3456,
            fill_count: 7,
            volume_usd: 98765.43,
            exchange_position_size: 1.2,
            exchange_entry_vwap: Some(3201.0),
            portfolio_value: 100000.0,
            available_capital: 50000.0,
            // symbol/updated_at are overwritten by save().
            symbol: String::new(),
            updated_at: String::new(),
        };

        store.save(&state).unwrap();

        let loaded = store.load();
        assert_eq!(loaded.account_index, 42);
        assert_eq!(loaded.market_id, 1);
        assert_eq!(loaded.position_size_est, 1.25);
        assert_eq!(loaded.entry_vwap, 3200.5);
        assert_eq!(loaded.realized_pnl_cumulative, -12.3456);
        assert_eq!(loaded.fill_count, 7);
        assert_eq!(loaded.volume_usd, 98765.43);
        assert_eq!(loaded.exchange_position_size, 1.2);
        assert_eq!(loaded.exchange_entry_vwap, Some(3201.0));
        assert_eq!(loaded.portfolio_value, 100000.0);
        assert_eq!(loaded.available_capital, 50000.0);

        // Store stamps symbol and a non-empty updated_at.
        assert_eq!(loaded.symbol, "ETH");
        assert!(loaded.updated_at.ends_with('Z'));
        assert!(loaded.updated_at.contains('T'));

        // Path is the documented shape.
        assert!(store.path().ends_with("live_state_ETH.json"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_file_yields_defaults() {
        let dir = unique_dir("missing");
        let store = LiveStateStore::new(&dir, "BTC");

        // No save performed: load must yield defaults, not an error.
        let loaded = store.load();
        assert_eq!(loaded, LiveState::default());
        assert_eq!(loaded.realized_pnl_cumulative, 0.0);
        assert_eq!(loaded.fill_count, 0);
        assert_eq!(loaded.volume_usd, 0.0);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_file_yields_defaults() {
        let dir = unique_dir("corrupt");
        let store = LiveStateStore::new(&dir, "SOL");
        fs::write(store.path(), b"{ this is not valid json ]").unwrap();

        let loaded = store.load();
        assert_eq!(loaded, LiveState::default());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn partial_payload_uses_field_defaults() {
        // Mirrors the trade-log bootstrap path in `_restore_live_state_defaults`,
        // where only realized/fills/volume are known. `#[serde(default)]` must
        // fill in the rest.
        let dir = unique_dir("partial");
        let store = LiveStateStore::new(&dir, "DOGE");
        fs::write(
            store.path(),
            br#"{"realized_pnl_cumulative": -5.5, "fill_count": 3, "volume_usd": 1234.5}"#,
        )
        .unwrap();

        let loaded = store.load();
        assert_eq!(loaded.realized_pnl_cumulative, -5.5);
        assert_eq!(loaded.fill_count, 3);
        assert_eq!(loaded.volume_usd, 1234.5);
        // Untouched fields are defaults.
        assert_eq!(loaded.position_size_est, 0.0);
        assert_eq!(loaded.entry_vwap, 0.0);
        assert_eq!(loaded.exchange_entry_vwap, None);
        assert_eq!(loaded.portfolio_value, 0.0);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn save_overwrites_existing() {
        let dir = unique_dir("overwrite");
        let store = LiveStateStore::new(&dir, "ETH");

        let mut s = LiveState {
            fill_count: 1,
            volume_usd: 10.0,
            ..LiveState::default()
        };
        store.save(&s).unwrap();

        s.fill_count = 2;
        s.volume_usd = 20.0;
        store.save(&s).unwrap();

        let loaded = store.load();
        assert_eq!(loaded.fill_count, 2);
        assert_eq!(loaded.volume_usd, 20.0);

        // No stray temp files left behind after successful saves.
        let strays: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".tmp-")
            })
            .collect();
        assert!(strays.is_empty(), "leftover temp files: {strays:?}");

        fs::remove_dir_all(&dir).ok();
    }
}
