use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

mod listen_sidecar;

pub use listen_sidecar::{read_active_listen_addr, read_saved_server_config, ActiveListenGuard};

/// A persistent sidecar lock shared by the server and offline maintenance.
/// The file is deliberately not removed: unlinking it can split the lock domain.
#[cfg(unix)]
#[must_use = "dropping this guard immediately releases the lock or barrier"]
pub struct DatabaseProcessLock {
    _file: std::fs::File,
}

fn invalid_input(msg: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, msg)
}

#[cfg(unix)]
fn permission_denied(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::PermissionDenied, msg.into())
}

pub(super) fn ensure_not_sqlite_uri(path: &Path) -> Result<()> {
    anyhow::ensure!(
        !path.to_string_lossy().starts_with("file:"),
        "SQLite URI database paths are not supported"
    );
    Ok(())
}

fn parent_or_dot(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn ensure_parent_dir(path: &Path) -> Result<&Path> {
    let parent = parent_or_dot(path);
    if parent != Path::new(".") {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(parent)
            .with_context(|| format!("Failed to create database directory: {:?}", parent))?;
    }
    Ok(parent)
}

fn sidecar_in_dir(dir: &Path, path: &Path, suffix: &str) -> Option<PathBuf> {
    let mut name = path.file_name()?.to_os_string();
    name.push(suffix);
    Some(dir.join(name))
}

pub(super) fn resolve_sidecar_path_creating_parent(path: &Path, suffix: &str) -> Result<PathBuf> {
    let parent = ensure_parent_dir(path)?
        .canonicalize()
        .context("Failed to resolve database directory")?;
    sidecar_in_dir(&parent, path, suffix).context("Database path must name a file")
}

pub(super) fn resolve_existing_sidecar_path(path: &Path, suffix: &str) -> Option<PathBuf> {
    sidecar_in_dir(&parent_or_dot(path).canonicalize().ok()?, path, suffix)
}

pub(super) fn remove_existing_non_dir_sidecar(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => {
            anyhow::bail!("Active listener sidecar {:?} must not be a directory", path);
        }
        Ok(_) => match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).context("Failed to remove listener sidecar"),
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).context("Failed to inspect listener sidecar"),
    }
}

#[cfg(unix)]
fn validate_lock_target(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("Failed to inspect database path"),
    };
    anyhow::ensure!(
        !metadata.file_type().is_symlink(),
        "Database path {:?} must not be a symbolic link",
        path
    );
    if !metadata.is_file() {
        return Err(invalid_input("Database/lock path must be a regular file").into());
    }
    anyhow::ensure!(
        metadata.nlink() == 1,
        "Database files with multiple hard links are not supported; use a single database path"
    );
    Ok(())
}

#[cfg(unix)]
pub(super) fn verify_open_file(
    file: &std::fs::File,
    uid: u32,
    label: &str,
) -> Result<std::fs::Metadata> {
    let meta = file
        .metadata()
        .with_context(|| format!("Failed to inspect {label}"))?;
    validate_regular_file_ownership_and_mode(&meta, uid)
        .with_context(|| format!("{label} failed ownership or mode verification"))?;
    Ok(meta)
}

#[cfg(unix)]
fn open_and_verify_lock_file(lock_path: &Path, uid: u32) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut opts = std::fs::OpenOptions::new();
    opts.read(true).write(true).create(true).truncate(false);
    opts.mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    let file = opts
        .open(lock_path)
        .with_context(|| format!("Failed to open database process lock {:?}", lock_path))?;
    verify_open_file(&file, uid, "database process lock")?;
    Ok(file)
}

#[cfg(unix)]
pub(super) fn same_inode(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    !b.file_type().is_symlink() && (a.dev(), a.ino()) == (b.dev(), b.ino())
}

#[cfg(unix)]
pub(super) fn try_flock_nonblocking(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
pub(super) fn try_flock_shared_nonblocking(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
impl DatabaseProcessLock {
    pub fn acquire(path: &Path) -> Result<Option<Self>> {
        if path == Path::new(":memory:") {
            return Ok(None);
        }
        ensure_not_sqlite_uri(path)?;
        validate_lock_target(path)?;
        let lock_path = resolve_sidecar_path_creating_parent(path, ".process-lock")?;
        let expected_uid = effective_uid();
        const RETRIES: usize = 3;
        let mut locked_file = None;
        for attempt in 0..=RETRIES {
            let file = open_and_verify_lock_file(&lock_path, expected_uid)?;
            if let Err(error) = try_flock_nonblocking(&file) {
                anyhow::bail!(
                    "Cannot acquire database process lock {:?}: {}. Stop the running bot or migration first",
                    lock_path,
                    error
                );
            }
            let fmeta = file
                .metadata()
                .context("Failed to inspect acquired database process lock")?;
            match std::fs::symlink_metadata(&lock_path) {
                Ok(lmeta) if same_inode(&fmeta, &lmeta) => {
                    verify_open_file(&file, expected_uid, "acquired database process lock")?;
                    locked_file = Some(file);
                    break;
                }
                Ok(lmeta) if lmeta.file_type().is_symlink() => {
                    anyhow::bail!(
                        "Database process lock {:?} must not be a symbolic link",
                        lock_path
                    );
                }
                Ok(_) if attempt < RETRIES => {}
                Ok(_) => {
                    anyhow::bail!(
                        "Database process lock {:?} was replaced concurrently during acquisition",
                        lock_path
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && attempt < RETRIES => {}
                Err(e) => return Err(e).context("Failed to verify database process lock"),
            }
        }
        let file = locked_file.context("Failed to acquire verified database process lock")?;
        // Re-verify target after acquiring the process lock to close pre-lock TOCTOU races.
        validate_lock_target(path)?;
        for suffix in ["-listen", "-listen.tmp"] {
            remove_existing_non_dir_sidecar(&resolve_sidecar_path_creating_parent(path, suffix)?)?;
        }
        Ok(Some(Self { _file: file }))
    }
}

#[cfg(not(unix))]
#[must_use = "dropping this guard immediately releases the lock or barrier"]
pub struct DatabaseProcessLock;

#[cfg(not(unix))]
impl DatabaseProcessLock {
    pub fn acquire(path: &Path) -> Result<Option<Self>> {
        if path != Path::new(":memory:") {
            ensure_not_sqlite_uri(path)?;
            tracing::warn!(
                "Database process locking is only enforced on Unix; ensure no other bot instance shares {:?}",
                path
            );
            for suffix in ["-listen", "-listen.tmp"] {
                let sidecar = resolve_sidecar_path_creating_parent(path, suffix)?;
                remove_existing_non_dir_sidecar(&sidecar)?;
            }
        }
        Ok(None)
    }
}

#[cfg(unix)]
pub(super) fn effective_uid() -> u32 {
    unsafe { libc::geteuid() as u32 }
}

#[cfg(unix)]
pub(super) fn validate_regular_file_ownership_and_mode(
    metadata: &std::fs::Metadata,
    expected_uid: u32,
) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if !metadata.is_file() {
        return Err(invalid_input("Database/lock path must be a regular file"));
    }
    if metadata.nlink() != 1 {
        return Err(invalid_input(
            "SQLite database and sidecar files must not have multiple hard links",
        ));
    }
    if metadata.uid() != expected_uid {
        return Err(permission_denied(format!(
            "file owner {} differs from service effective user {}",
            metadata.uid(),
            expected_uid
        )));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(permission_denied(
            "remove group and other permissions before starting",
        ));
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn validate_database_file(path: &Path, expected_uid: u32) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(invalid_input(
            "SQLite database and sidecar paths must not be symbolic links",
        ));
    }
    validate_regular_file_ownership_and_mode(&metadata, expected_uid)
}

#[cfg(unix)]
pub(super) fn validate_database_and_sidecars(path: &Path) -> Result<()> {
    const HINT: &str = "ensure it is owned by the service user, remove group and other permissions and avoid symbolic links";
    let expected_uid = effective_uid();
    validate_database_file(path, expected_uid)
        .with_context(|| format!("Insecure SQLite database path {:?}; {HINT}", path))?;
    // SQLite resolves parent symlinks before choosing sidecar names.
    let canonical = path
        .canonicalize()
        .context("Failed to resolve database path")?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut name = canonical.as_os_str().to_os_string();
        name.push(suffix);
        let sidecar = Path::new(&name);
        match validate_database_file(sidecar, expected_uid) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("Insecure SQLite sidecar {:?}; {HINT}", sidecar));
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn prepare_database_path(path: &Path) -> Result<(u64, u64)> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    ensure_parent_dir(path)?;
    // Pre-create the database privately so SQLite also creates its WAL and
    // shared-memory files with private permissions, without changing umask.
    if let Err(error) = std::fs::symlink_metadata(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(error).with_context(|| format!("Failed to inspect {:?}", path));
        }
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
        {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("Failed to prepare SQLite database at {:?}", path));
            }
        }
    }
    // Inspect without opening existing files: closing a descriptor can
    // release another SQLite connection's process-wide POSIX locks.
    validate_database_and_sidecars(path)?;
    let meta =
        std::fs::symlink_metadata(path).with_context(|| format!("Failed to inspect {:?}", path))?;
    Ok((meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
pub(super) fn prepare_database_path(path: &Path) -> Result<()> {
    ensure_parent_dir(path)?;
    Ok(())
}
