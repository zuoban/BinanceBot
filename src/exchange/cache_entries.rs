use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
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
    next_epoch: AtomicU64,
    entries: std::sync::RwLock<CacheEntryMap<T>>,
}
impl<T> Default for CacheEntries<T> {
    fn default() -> Self {
        Self {
            next_epoch: AtomicU64::new(0),
            entries: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

impl<T> CacheEntries<T> {
    // A critical refresh detaches shared loaders without representing a fill.
    // Concurrent critical refreshes must not invalidate one another.
    fn detach_sync(&self, key: [u8; 32]) -> u64 {
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let (epoch, entry) = entries.entry(key).or_insert_with(|| {
            (
                self.next_epoch.fetch_add(1, Ordering::Relaxed),
                Default::default(),
            )
        });
        *entry = Default::default();
        let current_epoch = *epoch;
        if entries.len() > 256 {
            entries.retain(|k, (_, entry)| *k == key || Arc::strong_count(entry) > 1);
        }
        current_epoch
    }

    fn detach_if_current_sync(&self, key: [u8; 32], expected: u64) -> bool {
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let Some((epoch, entry)) = entries.get_mut(&key) else {
            return false;
        };
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
            let epoch = self.detach_sync(key);
            let result = fetch().await;
            let current = self.detach_if_current_sync(key, epoch);
            let value = result?;
            if current {
                return Ok(value);
            }
        }
    }

    fn entry_sync(&self, key: [u8; 32]) -> (u64, Entry<T>) {
        if let Some((epoch, entry)) = self
            .entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
        {
            return (*epoch, entry.clone());
        }
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let (epoch, entry) = entries.entry(key).or_insert_with(|| {
            (
                self.next_epoch.fetch_add(1, Ordering::Relaxed),
                Default::default(),
            )
        });
        let result = (*epoch, entry.clone());
        if entries.len() > 256 {
            entries.retain(|k, (_, entry)| *k == key || Arc::strong_count(entry) > 1);
        }
        result
    }

    #[cfg(test)]
    pub(super) async fn entry(&self, key: [u8; 32]) -> (u64, Entry<T>) {
        self.entry_sync(key)
    }

    // Retire the entry without waiting for an in-flight loader holding its lock.
    pub(super) async fn retire(&self, key: [u8; 32]) -> u64 {
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let next = self.next_epoch.fetch_add(1, Ordering::Relaxed);
        let (epoch, entry) = entries
            .entry(key)
            .or_insert_with(|| (next, Default::default()));
        *epoch = next;
        *entry = Default::default();
        let retired_epoch = *epoch;
        if entries.len() > 256 {
            entries.retain(|k, (_, entry)| *k == key || Arc::strong_count(entry) > 1);
        }
        retired_epoch
    }

    fn is_current_sync(&self, key: [u8; 32], epoch: u64, entry: &Entry<T>) -> bool {
        self.entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .is_some_and(|(e, current)| *e == epoch && Arc::ptr_eq(current, entry))
    }

    pub(super) async fn is_current(&self, key: [u8; 32], epoch: u64, entry: &Entry<T>) -> bool {
        self.is_current_sync(key, epoch, entry)
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
            let (epoch, entry) = self.entry_sync(key);
            let mut cached = tokio::time::timeout(ENTRY_WAIT, entry.lock())
                .await
                .map_err(|_| anyhow!("shared {label} request is still in flight"))?;
            if !self.is_current_sync(key, epoch, &entry) {
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
                if !self.is_current_sync(key, epoch, &entry) {
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
            if self.is_current_sync(key, epoch, &entry) {
                return Ok((epoch, entry, projected));
            }
        }
    }
}
