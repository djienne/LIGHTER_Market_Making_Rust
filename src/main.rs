//! lighter-mm entry point.
//!
//! Usage:
//!   lighter-mm [--symbol BTC] [--config config.json] [--shadow|--live]
//!
//! `--shadow` (default) runs the full hot path against live market data WITHOUT sending
//! any orders — the safe way to verify the pipeline. `--live` enables real trading
//! (requires credentials in .env and is intentionally gated).

use anyhow::Result;
use lighter_mm::config::{Config, Credentials};
use lighter_mm::orchestrator::{App, Mode};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    lighter_mm::logging::init();
    // Log panics through tracing (location + message) so a crash is visible in the bot log even
    // though we unwind (release `panic = "unwind"`) rather than abort. The unwinding then resolves
    // the panicking task's JoinHandle, letting `run()` perform the shutdown cancel-all.
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "<unknown>".into());
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".into());
        tracing::error!("PANIC at {loc}: {msg}");
    }));
    let _ = dotenvy::dotenv();

    let mut symbol: Option<String> = None;
    let mut config_path = PathBuf::from("config.json");
    let mut mode = Mode::Shadow;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--symbol" => symbol = args.next(),
            "--config" => {
                if let Some(p) = args.next() {
                    config_path = PathBuf::from(p);
                }
            }
            "--shadow" => mode = Mode::Shadow,
            "--live" => mode = Mode::Live,
            other => tracing::warn!("ignoring unknown arg: {other}"),
        }
    }

    let config = Config::load(&config_path)
        .unwrap_or_else(|e| {
            tracing::warn!("config load failed ({e}); using defaults");
            serde_json::from_str("{}").unwrap()
        });

    let mut creds = Credentials::from_env().unwrap_or_else(|_| Credentials {
        api_key_private_key: String::new(),
        api_key_index: 0,
        account_index: 0,
        wallet_address: String::new(),
        market_symbol: "BTC".into(),
    });
    if let Some(s) = symbol {
        creds.market_symbol = s;
    }

    tracing::info!("lighter-mm starting: symbol={} mode={:?}", creds.market_symbol, mode);
    let app = App::bootstrap(config, creds).await?;
    app.run(mode).await
}
