use super::cache_entries::{CacheEntries, ENTRY_WAIT};
use super::model::ONE_WAY_POSITION_ERROR;
use super::{BinanceFuturesClient, BinancePositionRisk};
use anyhow::Result;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const MAX_AGE: Duration = Duration::from_secs(1);

#[derive(Clone)]
enum CachedSymbolPosition {
    OneWay(Option<BinancePositionRisk>, Instant),
    InvalidMode,
}

struct PositionSnapshot {
    positions: HashMap<String, CachedSymbolPosition>,
    received_instant: Instant,
}

impl PositionSnapshot {
    fn normalized_key(symbol: &str) -> std::borrow::Cow<'_, str> {
        if symbol.bytes().any(|b| b.is_ascii_lowercase()) {
            std::borrow::Cow::Owned(symbol.to_ascii_uppercase())
        } else {
            std::borrow::Cow::Borrowed(symbol)
        }
    }

    fn from_vec(list: Vec<BinancePositionRisk>, received_instant: Instant) -> Self {
        let mut positions = HashMap::with_capacity(list.len());
        for position in list {
            let valid_side = position
                .position_side
                .as_deref()
                .is_none_or(|side| side == "BOTH");
            let key = position.symbol.to_ascii_uppercase();
            match positions.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(if valid_side {
                        CachedSymbolPosition::OneWay(Some(position), received_instant)
                    } else {
                        CachedSymbolPosition::InvalidMode
                    });
                }
                Entry::Occupied(mut entry) => {
                    entry.insert(CachedSymbolPosition::InvalidMode);
                }
            }
        }
        Self {
            positions,
            received_instant,
        }
    }

    #[cfg(test)]
    fn lookup(&self, symbol: &str) -> Result<Option<BinancePositionRisk>> {
        Ok(self.lookup_cached(symbol)?.and_then(|(pos, _)| pos))
    }

    fn lookup_cached(
        &self,
        symbol: &str,
    ) -> Result<Option<(Option<BinancePositionRisk>, Instant)>> {
        let key = Self::normalized_key(symbol);
        match self.positions.get(key.as_ref()) {
            None => Ok(None),
            Some(CachedSymbolPosition::OneWay(position, at)) => Ok(Some((position.clone(), *at))),
            Some(CachedSymbolPosition::InvalidMode) => anyhow::bail!(ONE_WAY_POSITION_ERROR),
        }
    }

    fn insert_cached(&mut self, symbol: &str, value: CachedSymbolPosition) {
        match self.positions.entry(symbol.to_ascii_uppercase()) {
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
            Entry::Occupied(mut entry) if matches!(value, CachedSymbolPosition::InvalidMode) => {
                entry.insert(CachedSymbolPosition::InvalidMode);
            }
            Entry::Occupied(_) => {}
        }
    }
}

#[derive(Default)]
pub(crate) struct PositionCache {
    entries: CacheEntries<[u8; 32], PositionSnapshot>,
}

impl PositionCache {
    pub(crate) fn invalidate(&self, client: &BinanceFuturesClient) {
        self.entries.retire(&client.account_cache_key());
    }

    pub(crate) async fn refresh_critical(
        &self,
        client: &BinanceFuturesClient,
        symbol: &str,
    ) -> Result<(Option<BinancePositionRisk>, Instant)> {
        let key = client.account_cache_key();
        self.entries
            .refresh_critical(&key, || async {
                let position = client.get_position(symbol).await;
                let received_at = Instant::now();
                Ok((position?, received_at))
            })
            .await
    }

    pub(crate) async fn get(
        &self,
        client: &BinanceFuturesClient,
        symbol: &str,
    ) -> Result<(Option<BinancePositionRisk>, Instant)> {
        let key = client.account_cache_key();
        loop {
            let (epoch, entry, (bulk_instant, resolved)) = self
                .entries
                .get_or_fetch(
                    &key,
                    "position",
                    |snapshot| snapshot.received_instant.elapsed() < MAX_AGE,
                    || async {
                        let positions = client.get_all_positions().await?;
                        Ok(PositionSnapshot::from_vec(positions, Instant::now()))
                    },
                    |snapshot| (snapshot.received_instant, snapshot.lookup_cached(symbol)),
                )
                .await?;
            if let Some((position, received_instant)) = resolved? {
                return Ok((position, received_instant));
            }

            // A missing symbol is unknown, not a flat position. Query the
            // symbol-specific endpoint on Normal budget and cache the result.
            let requested_at = Instant::now();
            let fetched = client.get_position_for_periodic_sync(symbol).await;
            let received_instant = Instant::now();
            if !self.entries.is_current(&key, epoch, &entry) {
                continue;
            }
            let cached_value = match &fetched {
                Ok(position) => Some(CachedSymbolPosition::OneWay(position.clone(), requested_at)),
                Err(err) if err.to_string() == ONE_WAY_POSITION_ERROR => {
                    Some(CachedSymbolPosition::InvalidMode)
                }
                Err(_) => None,
            };
            if let Some(value) = cached_value {
                if let Ok(mut cached) = tokio::time::timeout(ENTRY_WAIT, entry.lock()).await {
                    if self.entries.is_current(&key, epoch, &entry) {
                        if let Some(snapshot) = cached.snapshot.as_mut() {
                            if snapshot.received_instant == bulk_instant
                                && requested_at.elapsed() < MAX_AGE
                            {
                                snapshot.insert_cached(symbol, value);
                            }
                        }
                    }
                }
            }
            let position = fetched?;
            if !self.entries.is_current(&key, epoch, &entry) {
                continue;
            }
            return Ok((position, received_instant));
        }
    }
}

#[cfg(test)]
mod tests;
