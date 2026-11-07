use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub(super) const FAILURE_COOLDOWN: Duration = Duration::from_secs(2);
// Match market-cache waiting: ordinary reconciliation yields after 100 ms
// rather than queuing behind a slow HTTP loader. Critical refreshes detach it.
pub(super) const ENTRY_WAIT: Duration = Duration::from_millis(100);

pub(super) struct CacheEntry<T> {
    pub snapshot: Option<T>,
    pub failed_at: Option<Instant>,
}

impl<T> Default for CacheEntry<T> {
    fn default() -> Self {
        Self {
            snapshot: None,
            failed_at: None,
        }
    }
}

type Entry<T> = Arc<Mutex<CacheEntry<T>>>;
type CacheEntryMap<T> = HashMap<[u8; 32], (u64, Entry<T>)>;

pub(super) struct CacheEntries<T> {
    entries: Mutex<CacheEntryMap<T>>,
}
impl<T> Default for CacheEntries<T> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<T> CacheEntries<T> {
    // A critical refresh detaches shared loaders without representing a fill.
    // Concurrent critical refreshes must not invalidate one another.
    pub(super) async fn detach(&self, key: [u8; 32]) -> u64 {
        let mut entries = self.entries.lock().await;
        let (epoch, entry) = entries.entry(key).or_default();
        *entry = Default::default();
        *epoch
    }

    pub(super) async fn detach_if_current(&self, key: [u8; 32], expected: u64) -> bool {
        let mut entries = self.entries.lock().await;
        let (epoch, entry) = entries.entry(key).or_default();
        if *epoch != expected {
            return false;
        }
        *entry = Default::default();
        true
    }

    pub(super) async fn refresh_critical<R, F, Fut>(&self, key: [u8; 32], mut fetch: F) -> Result<R>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<R>>,
    {
        loop {
            let epoch = self.detach(key).await;
            let result = fetch().await;
            let current = self.detach_if_current(key, epoch).await;
            let value = result?;
            if current {
                return Ok(value);
            }
        }
    }

    pub(super) async fn entry(&self, key: [u8; 32]) -> (u64, Entry<T>) {
        let mut entries = self.entries.lock().await;
        let (epoch, entry) = entries.entry(key).or_default();
        (*epoch, entry.clone())
    }

    // Retire the entry without waiting for an in-flight loader holding its lock.
    pub(super) async fn retire(&self, key: [u8; 32]) -> u64 {
        let mut entries = self.entries.lock().await;
        let (epoch, entry) = entries.entry(key).or_default();
        *epoch = epoch.wrapping_add(1);
        *entry = Default::default();
        *epoch
    }

    pub(super) async fn is_current(&self, key: [u8; 32], epoch: u64, entry: &Entry<T>) -> bool {
        self.entries
            .lock()
            .await
            .get(&key)
            .is_some_and(|(e, current)| *e == epoch && Arc::ptr_eq(current, entry))
    }

    pub(super) async fn get_or_fetch<R, Fresh, Fetch, Fut, Project>(
        &self,
        key: [u8; 32],
        label: &str,
        mut is_fresh: Fresh,
        mut fetch: Fetch,
        mut project: Project,
    ) -> Result<(u64, Entry<T>, R)>
    where
        Fresh: FnMut(&T) -> bool,
        Fetch: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
        Project: FnMut(&T) -> R,
    {
        loop {
            let (epoch, entry) = self.entry(key).await;
            let mut cached = tokio::time::timeout(ENTRY_WAIT, entry.lock())
                .await
                .map_err(|_| anyhow!("shared {label} request is still in flight"))?;
            if !self.is_current(key, epoch, &entry).await {
                continue;
            }
            if cached
                .failed_at
                .is_some_and(|at| at.elapsed() < FAILURE_COOLDOWN)
            {
                return Err(anyhow!("shared {label} request is cooling down"));
            }
            if !cached.snapshot.as_ref().is_some_and(&mut is_fresh) {
                // Do not serve an expired or invalidated snapshot after a failure.
                cached.snapshot = None;
                let fetched = fetch().await;
                if !self.is_current(key, epoch, &entry).await {
                    continue;
                }
                let snapshot = match fetched {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        cached.failed_at = Some(Instant::now());
                        return Err(error);
                    }
                };
                cached.failed_at = None;
                cached.snapshot = Some(snapshot);
            }
            let Some(snapshot) = cached.snapshot.as_ref() else {
                continue;
            };
            let projected = project(snapshot);
            drop(cached);
            if self.is_current(key, epoch, &entry).await {
                return Ok((epoch, entry, projected));
            }
        }
    }
}
