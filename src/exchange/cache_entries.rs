use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

type Entry<T> = Arc<Mutex<Option<T>>>;

pub(super) struct CacheEntries<T> {
    entries: Mutex<HashMap<[u8; 32], (u64, Entry<T>)>>,
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
}
