use std::path::{Path, PathBuf};
use uuid::Uuid;

#[must_use = "dropping TempTestDir immediately deletes the temporary directory"]
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

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempTestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn print_latency_percentiles(label: &str, samples: &mut [f64]) {
    if samples.is_empty() {
        return;
    }
    samples.sort_by(f64::total_cmp);
    let quantile = |percent: usize| {
        let index = samples
            .len()
            .saturating_mul(percent)
            .div_ceil(100)
            .saturating_sub(1)
            .min(samples.len() - 1);
        samples[index]
    };
    println!(
        "{label}: n={} p50={:.3} p95={:.3} p99={:.3} max={:.3}",
        samples.len(),
        quantile(50),
        quantile(95),
        quantile(99),
        samples[samples.len() - 1],
    );
}
