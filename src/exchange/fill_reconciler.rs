use super::*;
use std::collections::HashMap;

#[derive(serde::Deserialize)]
pub(super) struct ExecutionFill {
    pub(super) id: i64,
    #[serde(rename = "orderId")]
    pub(super) order_id: i64,
    pub(super) qty: Decimal,
    #[serde(flatten)]
    pub(super) trade: BinanceUserTrade,
}

pub(super) struct FillProgress {
    pub(super) expected_qty: Decimal,
    pub(super) from_id: Option<i64>,
    pub(super) restarted: bool,
    pub(super) fills: HashMap<i64, ExecutionFill>,
    pub(super) quantity: Decimal,
    pub(super) advances: u64,
}

impl FillProgress {
    pub(super) fn new(expected_qty: Decimal) -> Self {
        Self {
            expected_qty,
            from_id: None,
            restarted: false,
            fills: Default::default(),
            quantity: Decimal::ZERO,
            advances: 0,
        }
    }
}

const FILL_WAITER_ACTIVE_LEASE: Duration = Duration::from_secs(3 * 60);

pub(super) struct FillCacheEntry {
    pub(super) progress: Arc<tokio::sync::Mutex<FillProgress>>,
    pub(super) last_used: Instant,
    pub(super) stalled_attempts: u8,
}

type FillCacheMap = HashMap<(String, i64), FillCacheEntry>;
type FillProgressSlot = (
    Arc<tokio::sync::Mutex<FillProgress>>,
    tokio::sync::OwnedMutexGuard<FillProgress>,
);

pub(super) struct FillCacheCleanup<'a> {
    pub(super) cache: &'a std::sync::Mutex<FillCacheMap>,
    pub(super) key: (String, i64),
    pub(super) shared: Arc<tokio::sync::Mutex<FillProgress>>,
    pub(super) progress: tokio::sync::OwnedMutexGuard<FillProgress>,
    pub(super) initial_advances: u64,
    pub(super) failed_query: bool,
}

impl Drop for FillCacheCleanup<'_> {
    fn drop(&mut self) {
        // Keep the progress lock until accounting and removal are complete.
        // Acquisition also happens under the cache lock, so a concurrent caller
        // cannot acquire an orphaned entry while it is being removed.
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = cache.get_mut(&self.key) {
            if !Arc::ptr_eq(&entry.progress, &self.shared) {
                return;
            }
            if self.progress.from_id.is_none() || self.progress.fills.is_empty() {
                cache.remove(&self.key);
            } else if self.failed_query && self.progress.advances == self.initial_advances {
                entry.stalled_attempts = entry.stalled_attempts.saturating_add(1);
            } else if self.progress.advances != self.initial_advances {
                entry.stalled_attempts = 0;
            }
        }
    }
}

fn accumulate_fill_page(
    progress: &mut FillProgress,
    order_id: i64,
    expected_qty: Decimal,
    page: Vec<ExecutionFill>,
) -> Result<Option<Vec<BinanceUserTrade>>> {
    let page_len = page.len();
    if page_len > 1000 {
        return Err(anyhow!("Unexpected trade page size for order {}", order_id));
    }
    let mut max_id = None;
    let mut added = HashMap::<i64, ExecutionFill>::new();
    let mut quantity = progress.quantity;
    for fill in page {
        if fill.order_id != order_id || fill.id < 0 || fill.qty <= Decimal::ZERO {
            return Err(anyhow!("Invalid trade returned for order {}", order_id));
        }
        max_id = Some(max_id.map_or(fill.id, |id: i64| id.max(fill.id)));
        if let Some(existing) = progress.fills.get(&fill.id).or_else(|| added.get(&fill.id)) {
            if existing.qty != fill.qty
                || existing.trade.realized_pnl != fill.trade.realized_pnl
                || existing.trade.commission != fill.trade.commission
                || existing.trade.commission_asset != fill.trade.commission_asset
                || existing.trade.maker != fill.trade.maker
            {
                return Err(anyhow!(
                    "Conflicting trade {} for order {}",
                    fill.id,
                    order_id
                ));
            }
        } else {
            quantity = quantity
                .checked_add(fill.qty)
                .ok_or_else(|| anyhow!("Trade quantity overflow for order {}", order_id))?;
            added.insert(fill.id, fill);
        }
    }
    if quantity > expected_qty {
        return Err(anyhow!(
            "Trade quantity exceeds executed quantity for order {}",
            order_id
        ));
    }
    if progress.fills.len() + added.len() > 1_000_000 {
        *progress = FillProgress::new(expected_qty);
        return Err(anyhow!(
            "Order {} exceeds the one million fill reconciliation limit",
            order_id
        ));
    }
    if quantity == expected_qty {
        progress.fills.extend(added);
        let result = std::mem::take(&mut progress.fills)
            .into_values()
            .map(|fill| fill.trade)
            .collect();
        return Ok(Some(result));
    }
    let next = if !progress.restarted {
        Some(0)
    } else if page_len < 1000 {
        *progress = FillProgress::new(expected_qty);
        return Err(anyhow!(
            "Incomplete trades for order {}: {} of {}",
            order_id,
            quantity,
            expected_qty
        ));
    } else {
        let next = max_id
            .and_then(|id| id.checked_add(1))
            .ok_or_else(|| anyhow!("Invalid trade cursor for order {}", order_id))?;
        if progress.from_id.is_some_and(|cursor| next <= cursor) {
            return Err(anyhow!(
                "Trade cursor did not advance for order {}",
                order_id
            ));
        }
        Some(next)
    };
    progress.fills.extend(added);
    progress.quantity = quantity;
    progress.from_id = next;
    progress.restarted = true;
    progress.advances = progress.advances.saturating_add(1);
    Ok(None)
}

impl BinanceFuturesClient {
    pub(crate) fn note_pnl_reconcile_backlog(&self, pending: usize) {
        if pending == 0 {
            let cache = self.fill_progress.lock().unwrap_or_else(|e| e.into_inner());
            if cache.is_empty()
                && self
                    .fill_waiters
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty()
            {
                self.fill_retention_seconds.store(600, Ordering::Relaxed);
            }
            return;
        }
        // Keep a conservative lease of three records per minute even when the
        // bounded compensation pump advances faster. Each record may need an
        // order lookup and three 10-second requests; allow retries and slack.
        // A later decrease in backlog must not shorten an existing lease.
        let batches = u64::try_from(pending).unwrap_or(u64::MAX).saturating_add(2) / 3;
        let seconds = batches.saturating_mul(360).saturating_add(120).max(600);
        self.fill_retention_seconds
            .fetch_max(seconds, Ordering::Relaxed);
    }

    fn sync_fill_clear_generation(
        &self,
        cache: &mut FillCacheMap,
        symbol: &str,
        clear_revision: u64,
    ) -> Result<()> {
        let mut generations = self
            .fill_generations
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !generations.contains_key(symbol) && generations.len() >= 1024 {
            return Err(anyhow!("Trade reconciliation symbol limit reached"));
        }
        let generation = generations
            .entry(symbol.to_owned())
            .or_insert(clear_revision);
        if clear_revision < *generation {
            return Err(anyhow!("Trade history was cleared during reconciliation"));
        }
        if clear_revision > *generation {
            *generation = clear_revision;
            cache.retain(|(cached_symbol, _), _| cached_symbol != symbol);
            self.fill_waiters
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|((waiting_symbol, _), _)| waiting_symbol != symbol);
        }
        Ok(())
    }

    fn admit_or_evict_fill_slot(
        &self,
        cache: &mut FillCacheMap,
        order_id: i64,
        key: &(String, i64),
    ) -> Result<()> {
        let mut waiters = self.fill_waiters.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        waiters.retain(|(_, last_seen)| {
            now.duration_since(*last_seen)
                < Duration::from_secs(self.fill_retention_seconds.load(Ordering::Relaxed))
        });
        if let Some((_, last_seen)) = waiters.iter_mut().find(|(waiting, _)| waiting == key) {
            *last_seen = now;
        } else if cache.len() >= 8 || !waiters.is_empty() {
            if waiters.len() >= 1024 {
                if let Some(index) = waiters.iter().position(|(_, last_seen)| {
                    now.duration_since(*last_seen) >= FILL_WAITER_ACTIVE_LEASE
                }) {
                    waiters.remove(index);
                } else {
                    return Err(anyhow!("Trade reconciliation waiting queue is full"));
                }
            }
            waiters.push_back((key.clone(), now));
        }
        // Keep FIFO positions for slow backlog scans, but let a recently
        // polled waiter pass an inactive head. Three minutes tolerates
        // normal one-minute reconciliation ticks and a missed tick.
        let active = waiters
            .iter()
            .position(|(_, last_seen)| now.duration_since(*last_seen) < FILL_WAITER_ACTIVE_LEASE);
        let turn = active.is_none_or(|index| &waiters[index].0 == key);
        if turn && active.is_some() && cache.len() >= 8 {
            // Preserve healthy cursors while releasing repeated failures
            // and cursors abandoned before the fill lookup can run.
            let retention = Duration::from_secs(
                self.fill_retention_seconds
                    .load(Ordering::Relaxed)
                    .max(2 * 60 * 60),
            );
            let victim = cache
                .iter()
                .filter(|(_, entry)| {
                    (entry.stalled_attempts >= 3 || entry.last_used.elapsed() >= retention)
                        && Arc::strong_count(&entry.progress) == 1
                })
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(victim_key, _)| victim_key.clone());
            if let Some(victim) = victim {
                cache.remove(&victim);
            }
        }
        if cache.len() >= 8 || !turn {
            return Err(anyhow!(
                "Trade reconciliation for order {} is waiting for a progress slot",
                order_id
            ));
        }
        if let Some(index) = active {
            waiters.remove(index);
        }
        Ok(())
    }

    fn acquire_fill_progress_slot(
        &self,
        symbol: &str,
        order_id: i64,
        expected_qty: Decimal,
        clear_revision: u64,
        key: &(String, i64),
    ) -> Result<FillProgressSlot> {
        let mut cache = self.fill_progress.lock().unwrap_or_else(|e| e.into_inner());
        self.sync_fill_clear_generation(&mut cache, symbol, clear_revision)?;
        if !cache.contains_key(key) {
            self.admit_or_evict_fill_slot(&mut cache, order_id, key)?;
        }
        let entry = cache.entry(key.clone()).or_insert_with(|| FillCacheEntry {
            progress: Arc::new(tokio::sync::Mutex::new(FillProgress::new(expected_qty))),
            last_used: Instant::now(),
            stalled_attempts: 0,
        });
        let shared = Arc::clone(&entry.progress);
        let mut progress = Arc::clone(&shared).try_lock_owned().map_err(|_| {
            anyhow!(
                "Trade reconciliation already in progress for order {}",
                order_id
            )
        })?;
        if progress.expected_qty != expected_qty {
            *progress = FillProgress::new(expected_qty);
        }
        Ok((shared, progress))
    }

    /// Binance execution records contain the exchange-calculated realized PnL and fee.
    pub async fn get_user_trades(
        &self,
        symbol: &str,
        order_id: i64,
        expected_qty: Decimal,
        clear_revision: u64,
    ) -> Result<Vec<BinanceUserTrade>> {
        if expected_qty <= Decimal::ZERO {
            return Err(anyhow!("Invalid executed quantity for order {}", order_id));
        }
        let key = (symbol.to_owned(), order_id);
        let (shared, progress) =
            self.acquire_fill_progress_slot(symbol, order_id, expected_qty, clear_revision, &key)?;
        let initial_advances = progress.advances;
        let mut cleanup = FillCacheCleanup {
            cache: &self.fill_progress,
            key: key.clone(),
            shared: Arc::clone(&shared),
            progress,
            initial_advances,
            failed_query: false,
        };
        // Limit work on the live fill path. The reconciliation timer resumes the
        // saved cursor, including after a local budget or HTTP error.
        for _ in 0..3 {
            self.reserve(5, RequestClass::PnlReconciliation)?;
            let cursor = cleanup
                .progress
                .from_id
                .map(|id| format!("&fromId={}", id))
                .unwrap_or_default();
            let url = self.signed_url(
                "/fapi/v1/userTrades",
                &format!(
                    "symbol={}&orderId={}&limit=1000{}",
                    symbol, order_id, cursor
                ),
            );
            let resp = match self.send(self.client.get(&url)).await {
                Ok(resp) => resp,
                Err(error) => {
                    cleanup.failed_query = true;
                    return Err(error.into());
                }
            };
            if !resp.status().is_success() {
                cleanup.failed_query = true;
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                self.maybe_resync_on_invalid_timestamp(&body).await;
                return Err(anyhow!(
                    "Failed to fetch trades for order {}: {} - {}",
                    order_id,
                    status,
                    body
                ));
            }
            let page: Vec<ExecutionFill> =
                match resp.json().await.map_err(reqwest::Error::without_url) {
                    Ok(page) => page,
                    Err(error) => {
                        cleanup.failed_query = true;
                        return Err(error.into());
                    }
                };
            if self
                .fill_generations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(symbol)
                .copied()
                != Some(clear_revision)
            {
                return Err(anyhow!("Trade history was cleared during reconciliation"));
            }
            // From here until the next await, validation either fails or advances.
            cleanup.failed_query = true;
            if let Some(trades) =
                accumulate_fill_page(&mut cleanup.progress, order_id, expected_qty, page)?
            {
                let mut cache = self.fill_progress.lock().unwrap_or_else(|e| e.into_inner());
                if cache
                    .get(&key)
                    .is_some_and(|entry| Arc::ptr_eq(&entry.progress, &shared))
                {
                    cache.remove(&key);
                }
                return Ok(trades);
            }
            cleanup.failed_query = false;
            if let Some(entry) = self
                .fill_progress
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_mut(&key)
            {
                if Arc::ptr_eq(&entry.progress, &shared) {
                    entry.last_used = Instant::now();
                    entry.stalled_attempts = 0;
                }
            }
        }
        Err(anyhow!(
            "Trade reconciliation for order {} will continue later",
            order_id
        ))
    }
}
