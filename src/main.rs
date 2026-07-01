//! lighter-mm entry point.
//!
//! Usage:
//!   lighter-mm [--symbol BTC] [--config config.json] [--dry-run|--live]
//!
//! `--dry-run` (default) runs the full hot path against live market data WITHOUT sending
//! any orders — the safe way to verify the pipeline. `--live` enables real trading
//! (requires credentials in .env and is intentionally gated). `--shadow` is a deprecated
//! alias for `--dry-run`.

use anyhow::Result;
use lighter_mm::config::{Config, Credentials};
use lighter_mm::orchestrator::{App, Mode};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    // Guard must live until exit: it owns the non-blocking log worker (flushes on drop).
    let _log_guard = lighter_mm::logging::init();
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
    let mut mode = Mode::DryRun;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--symbol" => symbol = args.next(),
            "--config" => {
                if let Some(p) = args.next() {
                    config_path = PathBuf::from(p);
                }
            }
            "--dry-run" => mode = Mode::DryRun,
            "--shadow" => {
                tracing::warn!("--shadow is deprecated; use --dry-run");
                mode = Mode::DryRun;
            }
            "--live" => mode = Mode::Live,
            other => tracing::warn!("ignoring unknown arg: {other}"),
        }
    }

    // A live bot must NEVER start on silent zero-defaults (leverage 0, sizes 0, spreads 0).
    // Dry-run tolerates only a MISSING file; a malformed file aborts in both modes.
    let config = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            if mode == Mode::Live || config_path.exists() {
                tracing::error!("config {} unusable: {e:#}", config_path.display());
                return Err(e.context(format!("config {} unusable", config_path.display())));
            }
            tracing::warn!(
                "config {} missing; dry-run continues on built-in defaults",
                config_path.display()
            );
            serde_json::from_str("{}").unwrap()
        }
    };
    if mode == Mode::Live {
        config.validate_live()?;
    }

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

    let run_id = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    tracing::info!(
        "lighter-mm starting: run_id={} pid={} symbol={} mode={:?} config={} cwd={} account_index={} api_key_index={}",
        run_id,
        std::process::id(),
        creds.market_symbol,
        mode,
        config_path.display(),
        std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "<unknown>".into()),
        creds.account_index,
        creds.api_key_index,
    );
    let app = App::bootstrap(config, creds).await?;
    app.run(mode).await
}
