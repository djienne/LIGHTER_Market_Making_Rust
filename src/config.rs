//! Configuration — serde models for `config.json` (same schema as the Python bot) plus
//! `.env` credentials. Unknown keys are ignored; missing keys use Python defaults.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

fn d_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub trading: Trading,
    #[serde(default)]
    pub performance: Performance,
    #[serde(default)]
    pub websocket: WebsocketCfg,
    #[serde(default)]
    pub safety: Safety,
    #[serde(default)]
    pub pnl: PnlCfg,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Trading {
    pub leverage: i32,
    pub margin_mode: String,
    pub levels_per_side: usize,
    pub base_amount: f64,
    pub capital_usage_percent: f64,
    pub default_quote_update_threshold_bps: f64,
    pub spread_factor_level1: f64,
    pub order_timeout_seconds: f64,
    pub position_value_threshold_usd: f64,
    pub min_order_value_usd: f64,
    pub maker_fee_rate: f64,
    pub quote_engine: String,
    pub live_quality: LiveQuality,
    pub inventory_exit_bias: InventoryExitBias,
    pub vol_obi: VolObiCfg,
    pub alpha: AlphaCfg,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VolObiCfg {
    pub window_steps: usize,
    pub step_ns: i64,
    pub vol_to_half_spread: f64,
    pub min_half_spread_bps: f64,
    pub c1_ticks: f64,
    pub skew: f64,
    pub looking_depth: f64,
    pub min_warmup_samples: i64,
    pub warmup_seconds: f64,
}

impl Default for VolObiCfg {
    fn default() -> Self {
        Self {
            window_steps: 6000,
            step_ns: 100_000_000,
            vol_to_half_spread: 0.8,
            min_half_spread_bps: 2.0,
            c1_ticks: 160.0,
            skew: 1.0,
            looking_depth: 0.025,
            min_warmup_samples: 100,
            warmup_seconds: 600.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AlphaCfg {
    pub source: String,
    pub stale_seconds: f64,
    pub window_size: usize,
    pub min_samples: usize,
    pub looking_depth: f64,
    pub bbo_min_samples: usize,
    pub bbo_stale_seconds: f64,
    pub depth_snapshot_limit: usize,
}

impl Default for AlphaCfg {
    fn default() -> Self {
        Self {
            source: "binance".into(),
            stale_seconds: 5.0,
            window_size: 6000,
            min_samples: 150,
            looking_depth: 0.025,
            bbo_min_samples: 10,
            bbo_stale_seconds: 5.0,
            depth_snapshot_limit: 1000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LiveQuality {
    pub markout_horizons_sec: Vec<f64>,
    pub window_seconds: f64,
    pub adaptive_enabled: bool,
    pub adaptive_horizon_sec: f64,
    pub adverse_threshold_bps: f64,
    pub spread_widen_per_adverse_bps: f64,
    pub max_spread_multiplier: f64,
    pub size_reduce_per_adverse_bps: f64,
    pub min_size_multiplier: f64,
    pub metrics_flush_seconds: f64,
}

impl Default for LiveQuality {
    fn default() -> Self {
        Self {
            markout_horizons_sec: vec![5.0, 30.0, 60.0],
            window_seconds: 3600.0,
            adaptive_enabled: true,
            adaptive_horizon_sec: 30.0,
            adverse_threshold_bps: 2.0,
            spread_widen_per_adverse_bps: 0.05,
            max_spread_multiplier: 1.5,
            size_reduce_per_adverse_bps: 0.06,
            min_size_multiplier: 0.55,
            metrics_flush_seconds: 10.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct InventoryExitBias {
    pub enabled: bool,
    pub min_ratio: f64,
    pub exit_tighten_per_ratio: f64,
    pub add_widen_per_ratio: f64,
    pub max_exit_tighten: f64,
    pub max_add_widen: f64,
    pub adverse_boost_per_bps: f64,
}

impl Default for InventoryExitBias {
    fn default() -> Self {
        Self {
            enabled: true,
            min_ratio: 0.05,
            exit_tighten_per_ratio: 0.45,
            add_widen_per_ratio: 0.75,
            max_exit_tighten: 0.35,
            max_add_widen: 0.65,
            adverse_boost_per_bps: 0.03,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Performance {
    pub min_loop_interval: f64,
    pub rate_limit_send_interval: f64,
}

impl Default for Performance {
    fn default() -> Self {
        Self {
            min_loop_interval: 0.1,
            rate_limit_send_interval: 0.15,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebsocketCfg {
    pub ping_interval: f64,
    pub recv_timeout: f64,
    pub account_recv_timeout: f64,
    pub reconnect_base_delay: f64,
    pub reconnect_max_delay: f64,
}

impl Default for WebsocketCfg {
    fn default() -> Self {
        Self {
            ping_interval: 20.0,
            recv_timeout: 30.0,
            account_recv_timeout: 1800.0,
            reconnect_base_delay: 5.0,
            reconnect_max_delay: 60.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Safety {
    pub stale_order_poller_interval_sec: f64,
    pub stale_order_debounce_count: u32,
    pub max_consecutive_order_rejections: u32,
    pub circuit_breaker_cooldown_sec: f64,
    pub order_reconcile_timeout_sec: f64,
    pub max_live_orders_per_market: usize,
    pub panic_close_on_startup: bool,
    pub panic_close_on_shutdown: bool,
}

impl Default for Safety {
    fn default() -> Self {
        Self {
            stale_order_poller_interval_sec: 3.0,
            stale_order_debounce_count: 2,
            max_consecutive_order_rejections: 5,
            circuit_breaker_cooldown_sec: 60.0,
            order_reconcile_timeout_sec: 2.0,
            max_live_orders_per_market: 4,
            panic_close_on_startup: false,
            panic_close_on_shutdown: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PnlCfg {
    pub enabled: bool,
    pub snapshot_interval_seconds: f64,
    pub persist_dir: String,
    pub include_unattributed_account_fills: bool,
}

impl Default for PnlCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            snapshot_interval_seconds: 60.0,
            persist_dir: "logs".to_string(),
            include_unattributed_account_fills: false,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config = serde_json::from_str(&s).context("parsing config json")?;
        Ok(cfg)
    }
}

/// Credentials + runtime identity from `.env` / environment.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub api_key_private_key: String,
    pub api_key_index: i32,
    pub account_index: i64,
    pub wallet_address: String,
    pub market_symbol: String,
}

impl Credentials {
    pub fn from_env() -> Result<Self> {
        let _ = d_true; // keep helper referenced
        Ok(Self {
            api_key_private_key: std::env::var("API_KEY_PRIVATE_KEY")
                .context("API_KEY_PRIVATE_KEY missing")?,
            api_key_index: std::env::var("API_KEY_INDEX")
                .unwrap_or_else(|_| "0".into())
                .trim()
                .parse()
                .context("API_KEY_INDEX")?,
            account_index: std::env::var("ACCOUNT_INDEX")
                .unwrap_or_else(|_| "0".into())
                .trim()
                .parse()
                .context("ACCOUNT_INDEX")?,
            wallet_address: std::env::var("WALLET_ADDRESS").unwrap_or_default(),
            market_symbol: std::env::var("MARKET_SYMBOL").unwrap_or_else(|_| "BTC".into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_repo_config() {
        // The ported config.json must parse with all sections.
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.json");
        if path.exists() {
            let cfg = Config::load(&path).expect("config.json parses");
            assert_eq!(cfg.trading.quote_engine, "vol_obi");
            assert!(cfg.trading.levels_per_side >= 1);
        }
    }
}
