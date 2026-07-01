//! Tracing-based logging init. Honors RUST_LOG, defaults to info.
//!
//! Uses a NON-BLOCKING writer (dedicated worker thread, lossy on overflow) so a stalled
//! stdout consumer (e.g. a backed-up Docker log driver) can never block the hot market-data
//! task mid-tick. The returned guard must be held for the process lifetime — dropping it
//! flushes and stops the worker.

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, EnvFilter};

#[must_use = "hold the guard for the process lifetime or logs stop flushing"]
pub fn init() -> Option<WorkerGuard> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    let ok = fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_writer(writer)
        .try_init()
        .is_ok();
    ok.then_some(guard)
}
