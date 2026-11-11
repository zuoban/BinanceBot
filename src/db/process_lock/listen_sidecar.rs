#[cfg(unix)]
use super::{
    effective_uid, same_inode, try_flock_nonblocking, try_flock_shared_nonblocking,
    validate_database_and_sidecars, validate_regular_file_ownership_and_mode, verify_open_file,
};
use super::{
    ensure_not_sqlite_uri, remove_existing_non_dir_sidecar, resolve_existing_sidecar_path,
    resolve_sidecar_path_creating_parent,
};
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Records the active bound HTTP listener in `<db>-listen` while holding an
/// exclusive kernel `flock` so `healthcheck` ignores crash-stale sidecars.
#[must_use = "dropping ActiveListenGuard releases and unlinks the active listener sidecar"]
pub struct ActiveListenGuard {
    inner: Option<(std::fs::File, PathBuf)>,
}

#[cfg(unix)]
fn lock_and_verify_listen_file(file: &std::fs::File, path: &Path) -> Result<()> {
    let meta = verify_open_file(file, effective_uid(), "active listener sidecar")?;
    try_flock_nonblocking(file)
        .with_context(|| format!("Failed to lock active listener sidecar {:?}", path))?;
    let lmeta = std::fs::symlink_metadata(path)
        .with_context(|| format!("Active listener sidecar {:?} disappeared", path))?;
    anyhow::ensure!(
        same_inode(&meta, &lmeta),
        "Active listener sidecar {:?} was replaced during creation",
        path
    );
    Ok(())
}

impl ActiveListenGuard {
    pub fn record(db_path: &Path, addr: SocketAddr) -> Result<Self> {
        use std::io::Write;

        if db_path == Path::new(":memory:") {
            return Ok(Self { inner: None });
        }
        ensure_not_sqlite_uri(db_path)?;
        let path = resolve_sidecar_path_creating_parent(db_path, "-listen")?;
        let stage = resolve_sidecar_path_creating_parent(db_path, "-listen.tmp")?;
        remove_existing_non_dir_sidecar(&stage)?;
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
        }
        let mut file = opts
            .open(&stage)
            .with_context(|| format!("Failed to stage active listener sidecar {:?}", stage))?;
        let publish = (|| -> Result<()> {
            #[cfg(unix)]
            lock_and_verify_listen_file(&file, &stage)?;
            file.write_all(addr.to_string().as_bytes())
                .with_context(|| format!("Failed to write active listener sidecar {:?}", stage))?;
            remove_existing_non_dir_sidecar(&path)?;
            std::fs::rename(&stage, &path)
                .with_context(|| format!("Failed to publish active listener sidecar {:?}", path))?;
            #[cfg(unix)]
            {
                let fmeta =
                    verify_open_file(&file, effective_uid(), "published active listener sidecar")?;
                let lmeta = std::fs::symlink_metadata(&path)?;
                anyhow::ensure!(
                    same_inode(&fmeta, &lmeta),
                    "Active listener sidecar {:?} was replaced during publish",
                    path
                );
            }
            Ok(())
        })();
        if let Err(err) = publish {
            #[cfg(unix)]
            if let Ok(fmeta) = file.metadata() {
                for candidate in [&stage, &path] {
                    if let Ok(cmeta) = std::fs::symlink_metadata(candidate) {
                        if same_inode(&fmeta, &cmeta) {
                            let _ = std::fs::remove_file(candidate);
                        }
                    }
                }
            }
            #[cfg(not(unix))]
            for candidate in [&stage, &path] {
                let _ = std::fs::remove_file(candidate);
            }
            return Err(err);
        }
        Ok(Self {
            inner: Some((file, path)),
        })
    }
}

impl Drop for ActiveListenGuard {
    fn drop(&mut self) {
        if let Some((file, path)) = self.inner.take() {
            #[cfg(unix)]
            if let (Ok(fmeta), Ok(lmeta)) = (file.metadata(), std::fs::symlink_metadata(&path)) {
                if same_inode(&fmeta, &lmeta) {
                    let _ = std::fs::remove_file(&path);
                }
            }
            #[cfg(not(unix))]
            let _ = std::fs::remove_file(&path);
            drop(file);
        }
    }
}

fn parse_listen_file(file: std::fs::File) -> Option<SocketAddr> {
    use std::io::Read;
    let mut buf = String::new();
    file.take(128).read_to_string(&mut buf).ok()?;
    buf.trim().parse::<SocketAddr>().ok()
}

#[cfg(unix)]
pub fn read_active_listen_addr(db_path: &Path) -> Option<SocketAddr> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    if db_path == Path::new(":memory:") || db_path.to_string_lossy().starts_with("file:") {
        return None;
    }
    let path = resolve_existing_sidecar_path(db_path, "-listen")?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(&path)
        .ok()?;
    let meta = file.metadata().ok()?;
    if validate_regular_file_ownership_and_mode(&meta, effective_uid()).is_err() || meta.len() > 128
    {
        return None;
    }
    let Err(err) = try_flock_shared_nonblocking(&file) else {
        unsafe {
            let _ = libc::flock(file.as_raw_fd(), libc::LOCK_UN);
        }
        return None;
    };
    let raw = err.raw_os_error();
    if err.kind() != std::io::ErrorKind::WouldBlock
        && raw != Some(libc::EWOULDBLOCK)
        && raw != Some(libc::EAGAIN)
    {
        return None;
    }
    parse_listen_file(file)
}

#[cfg(not(unix))]
pub fn read_active_listen_addr(db_path: &Path) -> Option<SocketAddr> {
    if db_path == Path::new(":memory:") || db_path.to_string_lossy().starts_with("file:") {
        return None;
    }
    let path = resolve_existing_sidecar_path(db_path, "-listen")?;
    let meta = std::fs::symlink_metadata(&path).ok()?;
    if !meta.file_type().is_file() || meta.len() > 128 {
        return None;
    }
    parse_listen_file(std::fs::File::open(&path).ok()?)
}

pub fn read_saved_server_config(db_path: &Path) -> Option<crate::config::ServerConfig> {
    if db_path == Path::new(":memory:") || db_path.to_string_lossy().starts_with("file:") {
        return None;
    }
    #[cfg(unix)]
    let open_path = {
        use std::os::unix::fs::MetadataExt;
        let before = std::fs::symlink_metadata(db_path).ok()?;
        validate_database_and_sidecars(db_path).ok()?;
        let canonical = db_path.canonicalize().ok()?;
        let after = std::fs::symlink_metadata(&canonical).ok()?;
        if !after.file_type().is_file()
            || (before.dev(), before.ino()) != (after.dev(), after.ino())
        {
            return None;
        }
        validate_database_and_sidecars(&canonical).ok()?;
        canonical
    };
    #[cfg(not(unix))]
    let open_path = {
        if !std::fs::symlink_metadata(db_path).is_ok_and(|m| m.file_type().is_file()) {
            return None;
        }
        db_path.canonicalize().ok()?
    };
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let conn = Connection::open_with_flags(open_path, flags).ok()?;
    let _ = conn.busy_timeout(Duration::from_millis(500));
    let sql = "SELECT config_json FROM app_config WHERE id = 1 AND length(config_json) <= 1048576";
    let raw: String = conn.query_row(sql, [], |r| r.get(0)).ok()?;
    let mut val: serde_json::Value = serde_json::from_str(&raw).ok()?;
    serde_json::from_value(val.get_mut("server")?.take()).ok()
}
