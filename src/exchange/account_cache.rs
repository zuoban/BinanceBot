use super::cache_entries::CacheEntries;
use super::{BinanceAccountInfoResponse, BinanceFuturesClient};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Bound reuse independently of the configured polling period: cached balances
// must never be treated as a fresh exchange response indefinitely.
const MAX_AGE: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct AccountSnapshot {
    pub(crate) account: Arc<BinanceAccountInfoResponse>,
    pub(crate) received_at: DateTime<Utc>,
    pub(crate) received_instant: Instant,
}

#[derive(Default)]
pub(crate) struct AccountCache {
    entries: CacheEntries<[u8; 32], AccountSnapshot>,
}

impl AccountCache {
    pub(crate) async fn get(&self, client: &BinanceFuturesClient) -> Result<AccountSnapshot> {
        self.get_inner(client, false).await
    }

    pub(crate) async fn refresh(&self, client: &BinanceFuturesClient) -> Result<AccountSnapshot> {
        self.get_inner(client, true).await
    }

    pub(crate) async fn refresh_critical(
        &self,
        client: &BinanceFuturesClient,
    ) -> Result<AccountSnapshot> {
        let key = client.account_cache_key();
        self.entries
            .refresh_critical(&key, || async {
                let account = client.get_account_critical().await?;
                Ok(AccountSnapshot {
                    account: Arc::new(account),
                    received_at: Utc::now(),
                    received_instant: Instant::now(),
                })
            })
            .await
    }

    pub(crate) fn invalidate(&self, client: &BinanceFuturesClient) {
        self.entries.retire(&client.account_cache_key());
    }

    async fn get_inner(
        &self,
        client: &BinanceFuturesClient,
        force: bool,
    ) -> Result<AccountSnapshot> {
        let key = client.account_cache_key();
        let (_, _, snapshot) = self
            .entries
            .get_or_fetch(
                &key,
                "account",
                |snapshot| !force && snapshot.received_instant.elapsed() < MAX_AGE,
                || async {
                    let account = client.get_account().await?;
                    Ok(AccountSnapshot {
                        account: Arc::new(account),
                        received_at: Utc::now(),
                        received_instant: Instant::now(),
                    })
                },
                Clone::clone,
            )
            .await?;
        Ok(snapshot)
    }
}

#[cfg(test)]
#[path = "account_cache_tests.rs"]
mod tests;
