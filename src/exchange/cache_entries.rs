use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

type Entry<T> = Arc<Mutex<Option<T>>>;

pub(super) struct CacheEntries<T> {
    entries: Mutex<HashMap<[u8; 32], Entry<T>>>,
}

impl<T> Default for CacheEntries<T> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<T> CacheEntries<T> {
    pub(super) async fn entry(&self, key: [u8; 32]) -> Entry<T> {
        let mut entries = self.entries.lock().await;
        entries.entry(key).or_default().clone()
    }

    pub(super) async fn invalidate(&self, key: [u8; 32]) {
        let entry = self.entries.lock().await.get(&key).cloned();
        if let Some(entry) = entry {
            *entry.lock().await = None;
        }
    }
}
