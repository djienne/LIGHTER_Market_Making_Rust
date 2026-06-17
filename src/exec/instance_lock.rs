//! Single-instance guard for LIVE trading.
//!
//! Lighter nonces are sequential **per (account_index, api_key_index)**. If two bots run
//! against the same pair they each keep their own optimistic nonce counter, drift apart, and
//! produce a self-sustaining `invalid nonce` cascade — *and* they double-place / mutually
//! cancel each other's orders (each sees the other's as "orphans"). This was the dominant
//! cause of the first smoke test's instability (two Rust instances were accidentally live at
//! once). This guard makes that impossible for processes on the same host: a non-blocking
//! `flock` on a per-(account, api-key) lockfile. Acquired once at LIVE startup and held for
//! the whole process lifetime (the fd's lock is released automatically on exit/crash).
//!
//! NOTE: an `flock` only coordinates processes that can see the same inode. It does NOT
//! protect against a *containerized* bot (e.g. the production `lighter-mm` docker image) using
//! the same credentials — that remains an operator responsibility (see README / startup WARN).

use anyhow::{bail, Context, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

/// Holds the locked file for the process lifetime. Dropping it (or process exit) releases the
/// `flock`.
pub struct InstanceLock {
    _file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl InstanceLock {
    /// Acquire the exclusive per-(account, api-key) lock, or fail loudly if another live
    /// instance on this host already holds it.
    pub fn acquire(account_index: i64, api_key_index: i32) -> Result<Self> {
        let path =
            std::env::temp_dir().join(format!("lighter-mm-acct{account_index}-key{api_key_index}.lock"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening instance lock {}", path.display()))?;

        // Non-blocking exclusive lock — fails immediately if a live process holds it.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if matches!(err.raw_os_error(), Some(libc::EWOULDBLOCK)) {
                let other = std::fs::read_to_string(&path).unwrap_or_default();
                bail!(
                    "another lighter-mm instance is ALREADY RUNNING for account_index={account_index} \
                     api_key_index={api_key_index} (lock {}{}). Two bots on the same (account, api-key) \
                     share ONE exchange nonce sequence and WILL corrupt each other ('invalid nonce' \
                     cascade) and double-place orders. Refusing to start.",
                    path.display(),
                    if other.trim().is_empty() { String::new() } else { format!(", held by pid {}", other.trim()) },
                );
            }
            return Err(anyhow::Error::new(err).context(format!("flock {}", path.display())));
        }

        // Best-effort: stamp our pid for operator diagnostics (we hold the lock, so this is safe).
        {
            let mut f = &file;
            let _ = f.set_len(0);
            let _ = f.write_all(format!("{}\n", std::process::id()).as_bytes());
            let _ = f.flush();
        }
        Ok(Self { _file: file, path })
    }
}
