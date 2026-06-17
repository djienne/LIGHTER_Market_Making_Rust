//! Tracing-based logging init. Honors RUST_LOG, defaults to info.
use tracing_subscriber::{fmt, EnvFilter};

pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .try_init();
}
