use super::cache_entries::CacheEntries;
use super::model::ONE_WAY_POSITION_ERROR;
use super::{BinanceFuturesClient, BinancePositionRisk};
use anyhow::Result;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const MAX_AGE: Duration = Duration::from_secs(1);

struct PositionSnapshot {
    positions: HashMap<String, Option<BinancePositionRisk>>,
    received_instant: Instant,
}

impl PositionSnapshot {
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
                    entry.insert(valid_side.then_some(position));
                }
                Entry::Occupied(mut entry) => {
                    entry.insert(None);
                }
            }
        }
        Self {
            positions,
            received_instant,
        }
    }

    fn lookup(&self, symbol: &str) -> Result<Option<BinancePositionRisk>> {
        let key: std::borrow::Cow<'_, str> = if symbol.bytes().any(|b| b.is_ascii_lowercase()) {
            std::borrow::Cow::Owned(symbol.to_ascii_uppercase())
        } else {
            std::borrow::Cow::Borrowed(symbol)
        };
        match self.positions.get(key.as_ref()) {
            None => Ok(None),
            Some(Some(position)) => Ok(Some(position.clone())),
            Some(None) => anyhow::bail!(ONE_WAY_POSITION_ERROR),
        }
    }
}

#[derive(Default)]
pub(crate) struct PositionCache {
    entries: CacheEntries<[u8; 32], PositionSnapshot>,
}

impl PositionCache {
    pub(crate) async fn invalidate(&self, client: &BinanceFuturesClient) {
        self.entries.retire(&client.account_cache_key()).await;
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
            let (epoch, entry, (position, received_instant)) = self
                .entries
                .get_or_fetch(
                    &key,
                    "position",
                    |snapshot| snapshot.received_instant.elapsed() < MAX_AGE,
                    || async {
                        let positions = client.get_all_positions().await?;
                        Ok(PositionSnapshot::from_vec(positions, Instant::now()))
                    },
                    |snapshot| (snapshot.lookup(symbol), snapshot.received_instant),
                )
                .await?;
            let position = position?;
            if position.is_some() {
                return Ok((position, received_instant));
            }

            // A missing symbol is unknown, not a flat position. Retry the original
            // symbol-specific endpoint before allowing reconciliation to succeed.
            let position = client.get_position(symbol).await;
            let received_instant = Instant::now();
            if self.entries.is_current(&key, epoch, &entry).await {
                return Ok((position?, received_instant));
            }
        }
    }
}

#[cfg(test)]
#[path = "position_cache_tests.rs"]
mod tests;
