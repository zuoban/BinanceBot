use anyhow::{Context, Result};
use std::path::Path;

/// A persistent sidecar lock shared by the server and offline maintenance.
/// The file is deliberately not removed: unlinking it can split the lock domain.
#[cfg(unix)]
pub struct DatabaseProcessLock {
    _file: std::fs::File,
}

#[cfg(unix)]
impl DatabaseProcessLock {
    pub fn acquire(path: &Path) -> Result<Option<Self>> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};

        if path == Path::new(":memory:") {
            return Ok(None);
        }
        anyhow::ensure!(
            !path.to_string_lossy().starts_with("file:"),
            "SQLite URI database paths are not supported"
        );
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() => anyhow::ensure!(
                metadata.nlink() == 1,
                "Database files with multiple hard links are not supported; use a single database path"
            ),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("Failed to inspect database path"),
        }
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
        let lock_path = parent.join(lock_name);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)
            .with_context(|| format!("Failed to open database process lock {:?}", lock_path))?;
        let metadata = file
            .metadata()
            .context("Failed to inspect database process lock")?;
        anyhow::ensure!(
            metadata.is_file()
                && metadata.uid() == unsafe { libc::geteuid() } as u32
                && metadata.mode() & 0o077 == 0
                && metadata.nlink() == 1,
            "Database process lock must be a private, owned regular file with one link"
        );
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            anyhow::bail!("Cannot acquire database process lock {:?}: {}. Stop the running bot or migration first", lock_path, error);
        }
        Ok(Some(Self { _file: file }))
    }
}

#[cfg(not(unix))]
pub struct DatabaseProcessLock;

#[cfg(not(unix))]
impl DatabaseProcessLock {
    pub fn acquire(_path: &Path) -> Result<Option<Self>> {
        Ok(None)
    }
}
