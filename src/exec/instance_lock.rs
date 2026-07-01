//! Single-instance guard for LIVE trading.
//!
//! Lighter nonces are sequential **per (account_index, api_key_index)**. If two bots run
//! against the same pair they each keep their own optimistic nonce counter, drift apart, and
//! produce a self-sustaining `invalid nonce` cascade — *and* they double-place / mutually
//! cancel each other's orders (each sees the other's as "orphans"). This was the dominant
//! cause of the first smoke test's instability (two Rust instances were accidentally live at
//! once). This guard makes that impossible for processes on the same host: an exclusive lock
//! on a per-(account, api-key) lockfile — `flock` on unix, share-mode-0 exclusive open on
//! Windows. Acquired once at LIVE startup and held for the whole process lifetime (the OS
//! releases the lock automatically on exit/crash).
//!
//! NOTE: the lock only coordinates processes on the same host/namespace. It does NOT protect
//! against a *containerized* bot (e.g. the production `lighter-mm` docker image) using the
//! same credentials — that remains an operator responsibility (see README / startup WARN).

use anyhow::Result;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Holds the locked file for the process lifetime. Dropping it (or process exit) releases
/// the lock.
pub struct InstanceLock {
    _file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

fn already_running_msg(account_index: i64, api_key_index: i32, detail: &str) -> String {
    format!(
        "another lighter-mm instance is ALREADY RUNNING for account_index={account_index} \
         api_key_index={api_key_index} ({detail}). Two bots on the same (account, api-key) \
         share ONE exchange nonce sequence and WILL corrupt each other ('invalid nonce' \
         cascade) and double-place orders. Refusing to start."
    )
}

impl InstanceLock {
    /// Acquire the exclusive per-(account, api-key) lock, or fail loudly if another live
    /// instance on this host already holds it.
    pub fn acquire(account_index: i64, api_key_index: i32) -> Result<Self> {
        let path = std::env::temp_dir()
            .join(format!("lighter-mm-acct{account_index}-key{api_key_index}.lock"));
        let file = open_locked(&path, account_index, api_key_index)?;

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

/// Unix: `flock(LOCK_EX | LOCK_NB)` — fails immediately if a live process holds it.
#[cfg(unix)]
fn open_locked(path: &Path, account_index: i64, api_key_index: i32) -> Result<File> {
    use anyhow::Context;
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening instance lock {}", path.display()))?;

    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if matches!(err.raw_os_error(), Some(libc::EWOULDBLOCK)) {
            let other = std::fs::read_to_string(path).unwrap_or_default();
            let detail = if other.trim().is_empty() {
                format!("lock {}", path.display())
            } else {
                format!("lock {}, held by pid {}", path.display(), other.trim())
            };
            anyhow::bail!(already_running_msg(account_index, api_key_index, &detail));
        }
        return Err(anyhow::Error::new(err).context(format!("flock {}", path.display())));
    }
    Ok(file)
}

/// Windows: exclusive open with share_mode(0) — a second open fails with
/// ERROR_SHARING_VIOLATION while the first handle is alive; the OS releases it on exit.
#[cfg(windows)]
fn open_locked(path: &Path, account_index: i64, api_key_index: i32) -> Result<File> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    const ERROR_SHARING_VIOLATION: i32 = 32;
    match OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .open(path)
    {
        Ok(f) => Ok(f),
        Err(e) if e.raw_os_error() == Some(ERROR_SHARING_VIOLATION) => {
            anyhow::bail!(already_running_msg(
                account_index,
                api_key_index,
                &format!("lock {}", path.display())
            ));
        }
        Err(e) => {
            Err(anyhow::Error::new(e).context(format!("opening instance lock {}", path.display())))
        }
    }
}
