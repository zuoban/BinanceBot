use anyhow::{Context, Result};
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

/// A persistent sidecar lock shared by the server and offline maintenance.
/// The file is deliberately not removed: unlinking it can split the lock domain.
#[cfg(unix)]
#[must_use = "dropping this guard immediately releases the lock or barrier"]
pub struct DatabaseProcessLock {
    _file: std::fs::File,
}

#[cfg(unix)]
fn validate_lock_target(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                !metadata.file_type().is_symlink(),
                "Database path {:?} must not be a symbolic link",
                path
            );
            if metadata.is_file() {
                anyhow::ensure!(
                    metadata.nlink() == 1,
                    "Database files with multiple hard links are not supported; use a single database path"
                );
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("Failed to inspect database path"),
    }
}

#[cfg(unix)]
fn resolve_lock_file_path(path: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::DirBuilderExt;

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)
        .with_context(|| format!("Failed to create database directory {:?}", parent))?;
    let parent = parent
        .canonicalize()
        .context("Failed to resolve database directory")?;
    let name = path.file_name().context("Database path must name a file")?;
    let mut lock_name = name.to_os_string();
    lock_name.push(".process-lock");
    Ok(parent.join(lock_name))
}

#[cfg(unix)]
fn open_and_verify_lock_file(lock_path: &Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(lock_path)
        .with_context(|| format!("Failed to open database process lock {:?}", lock_path))?;
    let metadata = file
        .metadata()
        .context("Failed to inspect database process lock")?;
    anyhow::ensure!(
        metadata.is_file()
            && validate_regular_file_ownership_and_mode(&metadata, effective_uid()).is_ok(),
        "Database process lock must be a private, owned regular file with one link"
    );
    Ok(file)
}

#[cfg(unix)]
impl DatabaseProcessLock {
    pub fn acquire(path: &Path) -> Result<Option<Self>> {
        use std::os::fd::AsRawFd;

        if path == Path::new(":memory:") {
            return Ok(None);
        }
        anyhow::ensure!(
            !path.to_string_lossy().starts_with("file:"),
            "SQLite URI database paths are not supported"
        );
        validate_lock_target(path)?;
        let lock_path = resolve_lock_file_path(path)?;
        let file = open_and_verify_lock_file(&lock_path)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            anyhow::bail!(
                "Cannot acquire database process lock {:?}: {}. Stop the running bot or migration first",
                lock_path,
                error
            );
        }
        // Re-verify target after acquiring the process lock to close pre-lock TOCTOU races.
        validate_lock_target(path)?;
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
            tracing::warn!(
                "Database process locking is only enforced on Unix; ensure no other bot instance shares {:?}",
                path
            );
        }
        Ok(None)
    }
}

#[cfg(unix)]
pub(super) fn effective_uid() -> u32 {
    let uid = unsafe { libc::geteuid() };
    uid as u32
}

#[cfg(unix)]
pub(super) fn validate_regular_file_ownership_and_mode(
    metadata: &std::fs::Metadata,
    expected_uid: u32,
) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if metadata.is_file() && metadata.nlink() != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "SQLite database and sidecar files must not have multiple hard links",
        ));
    }
    if metadata.uid() != expected_uid {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "file owner {} differs from service effective user {}",
                metadata.uid(),
                expected_uid
            ),
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "remove group and other permissions before starting",
        ));
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn validate_database_file(path: &Path, expected_uid: u32) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "SQLite database and sidecar paths must not be symbolic links",
        ));
    }
    validate_regular_file_ownership_and_mode(&metadata, expected_uid)
}

#[cfg(unix)]
pub(super) fn validate_database_and_sidecars(path: &Path) -> Result<()> {
    let expected_uid = effective_uid();
    let validate = |target: &Path| validate_database_file(target, expected_uid);
    validate(path).with_context(|| {
        format!(
            "Insecure SQLite database path {:?}; ensure it is owned by the service user, remove group and other permissions and avoid symbolic links",
            path
        )
    })?;
    // SQLite resolves parent symlinks before choosing sidecar names.
    let canonical = path
        .canonicalize()
        .context("Failed to resolve database path")?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut name = canonical.as_os_str().to_os_string();
        name.push(suffix);
        let sidecar = Path::new(&name);
        match validate(sidecar) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "Insecure SQLite sidecar {:?}; ensure it is owned by the service user, remove group and other permissions and avoid symbolic links",
                        sidecar
                    )
                })
            }
        }
    }
    Ok(())
}

pub(super) fn prepare_database_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
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

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        // Pre-create the database privately so SQLite also creates its WAL and
        // shared-memory files with private permissions, without changing umask.
        match std::fs::symlink_metadata(path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                {
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("Failed to prepare SQLite database at {:?}", path)
                        })
                    }
                }
            }
            Err(error) => {
                return Err(error).with_context(|| format!("Failed to inspect {:?}", path))
            }
        }
        // Inspect without opening existing files: closing a descriptor can
        // release another SQLite connection's process-wide POSIX locks.
        validate_database_and_sidecars(path)?;
    }
    Ok(())
}
