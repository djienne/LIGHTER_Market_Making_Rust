//! Append-only buffered CSV trade logger.
//!
//! Port of `lighter_MM/trade_log.py`. Records every fill (dry-run and live) to
//! `logs/trades_{symbol}.csv`.
//!
//! [`TradeLogger::log_fill`] is O(1) — it appends a fully-formatted row to an
//! in-memory buffer under a [`parking_lot::Mutex`]. Actual disk I/O happens only
//! in [`TradeLogger::flush`] (called periodically from the main loop and on
//! shutdown). On write failure, rows are prepended back into the buffer so they
//! can be retried on the next flush.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use chrono::{SecondsFormat, Utc};
use parking_lot::Mutex;

/// The exact CSV header, in order. Mirrors `_HEADER` in `trade_log.py`.
///
/// NOTE: column index 8 is `available_capital` (verified from the Python source;
/// the task brief loosely calls it `capital`). The file is the source of truth.
pub const HEADER: [&str; 21] = [
    "timestamp",
    "symbol",
    "side",
    "price",
    "size",
    "level",
    "position_after",
    "realized_pnl",
    "available_capital",
    "portfolio_value",
    "simulated",
    "notional_usd",
    "fee_usd",
    "entry_vwap_after",
    "realized_pnl_cumulative",
    "mid_at_fill",
    "spread_capture_bps",
    "inventory_after_usd",
    "client_order_index",
    "exchange_order_index",
    "fill_source",
];

/// One fill to be logged.
///
/// Required fields are bare; optional fields are `Option<_>` and serialize to an
/// empty string when `None`. Construct via [`TradeRow::default`] and override the
/// fields you need, or build one explicitly.
///
/// `notional_usd == None` is special-cased: the logger substitutes `price * size`
/// (matching the Python `notional = price * size if notional_usd is None`).
#[derive(Debug, Clone, Default)]
pub struct TradeRow {
    /// Optional fill timestamp. When absent, the logger stamps current UTC time.
    pub timestamp: Option<String>,
    /// "buy" / "sell" (see [`crate::types::Side::as_str`]).
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub level: i64,
    pub position_after: f64,
    pub realized_pnl: f64,
    pub available_capital: f64,
    pub portfolio_value: f64,
    pub simulated: bool,
    pub notional_usd: Option<f64>,
    pub fee_usd: Option<f64>,
    pub entry_vwap_after: Option<f64>,
    pub realized_pnl_cumulative: Option<f64>,
    pub mid_at_fill: Option<f64>,
    pub spread_capture_bps: Option<f64>,
    pub inventory_after_usd: Option<f64>,
    /// Free-form id; serialized verbatim. Empty string => empty cell.
    pub client_order_index: Option<String>,
    /// Free-form id; serialized verbatim. Empty string => empty cell.
    pub exchange_order_index: Option<String>,
    pub fill_source: String,
}

/// Buffered, append-only CSV trade log. Thread-safe.
pub struct TradeLogger {
    path: PathBuf,
    symbol: String,
    /// Pre-formatted rows awaiting flush. Each inner `Vec` is the 21 cells.
    buffer: Mutex<Vec<Vec<String>>>,
    /// Serializes header init + flush appends so concurrent writers don't
    /// interleave file I/O (mirrors Python's `_write_lock`).
    write_lock: Mutex<()>,
}

impl TradeLogger {
    /// Create a logger writing to `{log_dir}/trades_{symbol}.csv`.
    ///
    /// Creates `log_dir` if needed and ensures the CSV header is present.
    pub fn new(log_dir: impl AsRef<Path>, symbol: impl Into<String>) -> std::io::Result<Self> {
        let log_dir = log_dir.as_ref();
        fs::create_dir_all(log_dir)?;
        let symbol = symbol.into();
        let path = log_dir.join(format!("trades_{symbol}.csv"));
        let logger = Self {
            path,
            symbol,
            buffer: Mutex::new(Vec::new()),
            write_lock: Mutex::new(()),
        };
        logger.ensure_header()?;
        Ok(logger)
    }

    /// Absolute/relative path of the underlying CSV file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Symbol this logger is bound to.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Write the CSV header if the file is missing/empty, or migrate an existing
    /// file whose first row isn't the canonical header by re-padding columns.
    fn ensure_header(&self) -> std::io::Result<()> {
        let _guard = self.write_lock.lock();
        if self.path.exists() && fs::metadata(&self.path)?.len() > 0 {
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_path(&self.path)?;
            let mut rows: Vec<Vec<String>> = Vec::new();
            for rec in rdr.records() {
                let rec = rec?;
                rows.push(rec.iter().map(|s| s.to_string()).collect());
            }
            if let Some(first) = rows.first() {
                if first.as_slice() == HEADER {
                    return Ok(());
                }
                if !first.is_empty() {
                    // Re-pad legacy rows to the canonical column count, write a
                    // fresh header, then the (padded) data rows. Mirrors Python.
                    let mut padded: Vec<Vec<String>> = Vec::with_capacity(rows.len().saturating_sub(1));
                    for row in rows.iter().skip(1) {
                        let mut r = row.clone();
                        if r.len() < HEADER.len() {
                            r.resize(HEADER.len(), String::new());
                        }
                        padded.push(r);
                    }
                    let mut wtr = csv::WriterBuilder::new().from_path(&self.path)?;
                    wtr.write_record(HEADER)?;
                    for row in padded {
                        wtr.write_record(&row)?;
                    }
                    wtr.flush()?;
                    return Ok(());
                }
            }
        }
        let mut wtr = csv::WriterBuilder::new().from_path(&self.path)?;
        wtr.write_record(HEADER)?;
        wtr.flush()?;
        Ok(())
    }

    /// Buffer one fill row (no disk I/O). O(1) amortized.
    pub fn log_fill(&self, row: TradeRow) {
        let cells = Self::format_row(&self.symbol, &row);
        self.buffer.lock().push(cells);
    }

    /// Format a [`TradeRow`] into the 21 string cells, matching the Python
    /// `f"{...}"` specifiers exactly.
    fn format_row(symbol: &str, row: &TradeRow) -> Vec<String> {
        let ts = row
            .timestamp
            .clone()
            .unwrap_or_else(Self::timestamp_now);
        // notional = price * size if notional_usd is None else notional_usd
        let notional = row.notional_usd.unwrap_or(row.price * row.size);
        vec![
            ts,
            symbol.to_string(),
            row.side.clone(),
            fmt_g(row.price, 10),
            format!("{:.6}", row.size),
            row.level.to_string(),
            format!("{:.6}", row.position_after),
            format!("{:.4}", row.realized_pnl),
            format!("{:.2}", row.available_capital),
            format!("{:.2}", row.portfolio_value),
            // Python: str(simulated).lower() -> "true" / "false"
            if row.simulated { "true".to_string() } else { "false".to_string() },
            // notional is never None here (defaulted to price*size above).
            format!("{:.6}", notional),
            opt(row.fee_usd, |v| format!("{:.8}", v)),
            opt(row.entry_vwap_after, |v| fmt_g(v, 10)),
            opt(row.realized_pnl_cumulative, |v| format!("{:.6}", v)),
            opt(row.mid_at_fill, |v| fmt_g(v, 10)),
            opt(row.spread_capture_bps, |v| format!("{:.4}", v)),
            opt(row.inventory_after_usd, |v| format!("{:.6}", v)),
            row.client_order_index.clone().unwrap_or_default(),
            row.exchange_order_index.clone().unwrap_or_default(),
            row.fill_source.clone(),
        ]
    }

    /// UTC timestamp formatted as `%Y-%m-%dT%H:%M:%S.%fffZ` truncated to
    /// milliseconds — matches Python `strftime("...%f")[:-3] + "Z"`.
    fn timestamp_now() -> String {
        Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    }

    /// Append buffered rows to disk. On I/O failure, rows are prepended back into
    /// the buffer for retry and the error is returned.
    pub fn flush(&self) -> std::io::Result<()> {
        let rows = {
            let mut buf = self.buffer.lock();
            if buf.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buf)
        };

        match self.write_rows(&rows) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Prepend (preserving order) for retry on next flush.
                let mut buf = self.buffer.lock();
                let mut combined = rows;
                combined.append(&mut buf);
                *buf = combined;
                Err(e)
            }
        }
    }

    fn write_rows(&self, rows: &[Vec<String>]) -> std::io::Result<()> {
        let _guard = self.write_lock.lock();
        // Build the CSV payload in memory (like Python's io.StringIO), then
        // append in a single write so a partial write can't corrupt a row.
        let mut wtr = csv::WriterBuilder::new().from_writer(Vec::new());
        for row in rows {
            wtr.write_record(row)?;
        }
        wtr.flush()?;
        let bytes = wtr
            .into_inner()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        let mut file = OpenOptions::new().create(true).append(true).open(&self.path)?;
        file.write_all(&bytes)?;
        Ok(())
    }

    /// Delete the trade log and reset (used on capital reset). Re-writes header.
    pub fn clear(&self) -> std::io::Result<()> {
        self.buffer.lock().clear();
        {
            let _guard = self.write_lock.lock();
            if self.path.exists() {
                fs::remove_file(&self.path)?;
            }
        }
        self.ensure_header()
    }

    /// Number of rows currently buffered (not yet flushed). Mainly for tests.
    pub fn buffered_len(&self) -> usize {
        self.buffer.lock().len()
    }
}

/// Serialize an optional value: `None` -> empty string, `Some(v)` -> `f(v)`.
#[inline]
fn opt<F: Fn(f64) -> String>(v: Option<f64>, f: F) -> String {
    v.map(f).unwrap_or_default()
}

/// Format a float like Python's `f"{x:.<prec>g}"` (general format with `prec`
/// significant digits): pick fixed vs. scientific notation by exponent, strip
/// trailing zeros and a trailing decimal point.
///
/// Python's `g` rules (prec >= 1):
///   exp = floor(log10(|x|)); if -4 <= exp < prec use fixed with (prec-1-exp)
///   decimals, else scientific with (prec-1) decimals; then strip trailing zeros.
fn fmt_g(x: f64, prec: usize) -> String {
    if x == 0.0 {
        // Python: "0" (sign of -0.0 is dropped by %g for plain zero).
        return "0".to_string();
    }
    if !x.is_finite() {
        // %g would render inf/nan; preserve Rust's textual form.
        return format!("{x}");
    }
    let prec = prec.max(1);
    let exp = x.abs().log10().floor() as i64;

    if exp < -4 || exp >= prec as i64 {
        // Scientific: mantissa with (prec-1) decimals, then strip, then exponent.
        let s = format!("{:.*e}", prec - 1, x);
        strip_sci(&s)
    } else {
        // Fixed: (prec - 1 - exp) decimals.
        let decimals = (prec as i64 - 1 - exp).max(0) as usize;
        let s = format!("{x:.decimals$}");
        strip_fixed(&s)
    }
}

/// Strip trailing zeros (and a dangling decimal point) from a fixed-notation string.
fn strip_fixed(s: &str) -> String {
    if s.contains('.') {
        let t = s.trim_end_matches('0');
        let t = t.trim_end_matches('.');
        t.to_string()
    } else {
        s.to_string()
    }
}

/// Normalize Rust's `{:e}` output to Python `%g` style: strip trailing zeros in
/// the mantissa and format the exponent with a sign and >= 2 digits.
fn strip_sci(s: &str) -> String {
    let (mantissa, exp) = match s.split_once('e') {
        Some((m, e)) => (m, e),
        None => return s.to_string(),
    };
    let mantissa = strip_fixed(mantissa);
    let (sign, digits) = if let Some(rest) = exp.strip_prefix('-') {
        ('-', rest)
    } else if let Some(rest) = exp.strip_prefix('+') {
        ('+', rest)
    } else {
        ('+', exp)
    };
    let digits = if digits.len() < 2 {
        format!("{digits:0>2}")
    } else {
        digits.to_string()
    };
    format!("{mantissa}e{sign}{digits}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("trade_log_test_{tag}_{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn read_csv(path: &Path) -> Vec<Vec<String>> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(path)
            .unwrap();
        rdr.records()
            .map(|r| r.unwrap().iter().map(|s| s.to_string()).collect())
            .collect()
    }

    #[test]
    fn fmt_g_matches_python() {
        // Python f"{x:.10g}" reference values.
        assert_eq!(fmt_g(100.0, 10), "100");
        assert_eq!(fmt_g(100.25, 10), "100.25");
        assert_eq!(fmt_g(0.0, 10), "0");
        assert_eq!(fmt_g(1234.5678, 10), "1234.5678");
        assert_eq!(fmt_g(0.0001, 10), "0.0001");
        // 10 sig figs, no trailing zeros
        assert_eq!(fmt_g(3.14159265358979, 10), "3.141592654");
        // very small -> scientific (exp < -4)
        assert_eq!(fmt_g(0.00001234, 10), "1.234e-05");
        // large -> scientific (exp >= prec)
        assert_eq!(fmt_g(1.0e12, 10), "1e+12");
        assert_eq!(fmt_g(123456789012.0, 10), "1.23456789e+11");
        // negative
        assert_eq!(fmt_g(-0.5, 10), "-0.5");
    }

    #[test]
    fn fixed_specifiers_match_python() {
        // size .6f, pnl .4f, capital .2f, fee .8f
        assert_eq!(format!("{:.6}", 1.5f64), "1.500000");
        assert_eq!(format!("{:.4}", -12.3f64), "-12.3000");
        assert_eq!(format!("{:.2}", 1000.0f64), "1000.00");
        assert_eq!(format!("{:.8}", 0.0001f64), "0.00010000");
    }

    #[test]
    fn header_written_on_new_file() {
        let dir = temp_dir("header");
        let logger = TradeLogger::new(&dir, "BTC").unwrap();
        assert!(logger.path().ends_with("trades_BTC.csv"));
        let rows = read_csv(logger.path());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], HEADER.to_vec());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn log_then_flush_roundtrip() {
        let dir = temp_dir("roundtrip");
        let logger = TradeLogger::new(&dir, "ETH").unwrap();

        logger.log_fill(TradeRow {
            timestamp: None,
            side: "buy".to_string(),
            price: 100.25,
            size: 0.5,
            level: 0,
            position_after: 0.5,
            realized_pnl: 0.0,
            available_capital: 1000.0,
            portfolio_value: 1000.0,
            simulated: true,
            notional_usd: None, // -> price*size = 50.125
            fee_usd: Some(0.012345),
            entry_vwap_after: Some(100.25),
            realized_pnl_cumulative: Some(0.0),
            mid_at_fill: Some(100.3),
            spread_capture_bps: Some(2.5),
            inventory_after_usd: Some(50.125),
            client_order_index: Some("c-1".to_string()),
            exchange_order_index: None,
            fill_source: "ws".to_string(),
        });
        logger.log_fill(TradeRow {
            side: "sell".to_string(),
            price: 101.0,
            size: 0.25,
            level: 1,
            position_after: 0.25,
            realized_pnl: 1.2345,
            available_capital: 1001.0,
            portfolio_value: 1001.5,
            simulated: false,
            ..Default::default()
        });

        assert_eq!(logger.buffered_len(), 2);
        logger.flush().unwrap();
        assert_eq!(logger.buffered_len(), 0);

        // Second flush with empty buffer is a no-op.
        logger.flush().unwrap();

        let rows = read_csv(logger.path());
        // header + 2 data rows
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], HEADER.to_vec());
        assert_eq!(rows[0].len(), 21);

        // --- row 1 sample fields ---
        let r1 = &rows[1];
        assert_eq!(r1.len(), 21);
        assert_eq!(r1[1], "ETH"); // symbol
        assert_eq!(r1[2], "buy"); // side
        assert_eq!(r1[3], "100.25"); // price (.10g)
        assert_eq!(r1[4], "0.500000"); // size (.6f)
        assert_eq!(r1[5], "0"); // level
        assert_eq!(r1[8], "1000.00"); // available_capital (.2f)
        assert_eq!(r1[10], "true"); // simulated
        assert_eq!(r1[11], "50.125000"); // notional = price*size (.6f)
        assert_eq!(r1[12], "0.01234500"); // fee (.8f)
        assert_eq!(r1[18], "c-1"); // client_order_index
        assert_eq!(r1[19], ""); // exchange_order_index None -> empty
        assert_eq!(r1[20], "ws"); // fill_source
        // timestamp ends with Z and has a millisecond component
        assert!(r1[0].ends_with('Z'), "ts={}", r1[0]);
        assert!(r1[0].contains('.'), "ts={}", r1[0]);

        // --- row 2: optionals default to empty, simulated false ---
        let r2 = &rows[2];
        assert_eq!(r2[2], "sell");
        assert_eq!(r2[10], "false");
        assert_eq!(r2[12], ""); // fee_usd None
        assert_eq!(r2[13], ""); // entry_vwap_after None
        assert_eq!(r2[18], ""); // client_order_index None
        assert_eq!(r2[20], ""); // fill_source default empty
        // notional defaulted to price*size = 101.0 * 0.25 = 25.25
        assert_eq!(r2[11], "25.250000");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn flush_appends_without_duplicating_header() {
        let dir = temp_dir("append");
        let logger = TradeLogger::new(&dir, "SOL").unwrap();
        logger.log_fill(TradeRow { side: "buy".into(), price: 1.0, size: 1.0, ..Default::default() });
        logger.flush().unwrap();
        logger.log_fill(TradeRow { side: "sell".into(), price: 2.0, size: 1.0, ..Default::default() });
        logger.flush().unwrap();
        let rows = read_csv(logger.path());
        assert_eq!(rows.len(), 3); // 1 header + 2 data
        assert_eq!(rows[0], HEADER.to_vec());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopen_existing_file_keeps_header_once() {
        let dir = temp_dir("reopen");
        {
            let logger = TradeLogger::new(&dir, "XRP").unwrap();
            logger.log_fill(TradeRow { side: "buy".into(), price: 1.0, size: 1.0, ..Default::default() });
            logger.flush().unwrap();
        }
        // Re-open: should NOT add a second header.
        let logger = TradeLogger::new(&dir, "XRP").unwrap();
        logger.log_fill(TradeRow { side: "sell".into(), price: 2.0, size: 1.0, ..Default::default() });
        logger.flush().unwrap();
        let rows = read_csv(logger.path());
        assert_eq!(rows.len(), 3); // 1 header + 2 data
        assert_eq!(rows[0], HEADER.to_vec());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn clear_resets_to_header_only() {
        let dir = temp_dir("clear");
        let logger = TradeLogger::new(&dir, "DOGE").unwrap();
        logger.log_fill(TradeRow { side: "buy".into(), price: 1.0, size: 1.0, ..Default::default() });
        logger.flush().unwrap();
        logger.clear().unwrap();
        let rows = read_csv(logger.path());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], HEADER.to_vec());
        assert_eq!(logger.buffered_len(), 0);
        fs::remove_dir_all(&dir).ok();
    }
}
