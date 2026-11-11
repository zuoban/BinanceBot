use std::path::{Path, PathBuf};
use uuid::Uuid;

pub(crate) struct TempTestDir(PathBuf);

impl TempTestDir {
    pub(crate) fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temporary test directory");
        Self(path)
    }

    pub(crate) fn join(&self, name: impl AsRef<Path>) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempTestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
