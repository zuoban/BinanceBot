use super::cache_entries::{CacheEntries, ENTRY_WAIT, FAILURE_COOLDOWN};
use super::{BinanceAccountInfoResponse, BinanceFuturesClient};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Bound reuse independently of the configured polling period: cached balances
// must never be treated as a fresh exchange response indefinitely.
const MAX_AGE: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct AccountSnapshot {
    pub account: Arc<BinanceAccountInfoResponse>,
    pub received_at: DateTime<Utc>,
    pub received_instant: Instant,
}

#[derive(Default)]
pub(crate) struct AccountCache {
    entries: CacheEntries<AccountSnapshot>,
}

impl AccountCache {
    pub async fn get(&self, client: &BinanceFuturesClient) -> Result<AccountSnapshot> {
        self.get_inner(client, false).await
    }

    pub async fn refresh(&self, client: &BinanceFuturesClient) -> Result<AccountSnapshot> {
        self.get_inner(client, true).await
    }

    pub async fn refresh_critical(&self, client: &BinanceFuturesClient) -> Result<AccountSnapshot> {
        let key = client.account_cache_key();
        self.entries
            .refresh_critical(key, || async {
                let account = client.get_account_critical().await?;
                Ok(AccountSnapshot {
                    account: Arc::new(account),
                    received_at: Utc::now(),
                    received_instant: Instant::now(),
                })
            })
            .await
    }

    pub async fn invalidate(&self, client: &BinanceFuturesClient) {
        self.entries.retire(client.account_cache_key()).await;
    }

    async fn get_inner(
        &self,
        client: &BinanceFuturesClient,
        force: bool,
    ) -> Result<AccountSnapshot> {
        let key = client.account_cache_key();
        loop {
            let (epoch, entry) = self.entries.entry(key).await;
            let mut cached = tokio::time::timeout(ENTRY_WAIT, entry.lock())
                .await
                .map_err(|_| anyhow!("shared account request is still in flight"))?;
            if !self.entries.is_current(key, epoch, &entry).await {
                continue;
            }
            if cached
                .failed_at
                .is_some_and(|at| at.elapsed() < FAILURE_COOLDOWN)
            {
                return Err(anyhow!("shared account request is cooling down"));
            }
            if let Some(snapshot) = cached.snapshot.as_ref().filter(|_| !force) {
                if snapshot.received_instant.elapsed() < MAX_AGE {
                    return Ok(snapshot.clone());
                }
            }
            // Do not serve an expired or invalidated snapshot after a failure.
            cached.snapshot = None;
            let account = client.get_account().await;
            if !self.entries.is_current(key, epoch, &entry).await {
                continue;
            }
            let account = match account {
                Ok(account) => account,
                Err(error) => {
                    cached.failed_at = Some(Instant::now());
                    return Err(error);
                }
            };
            let snapshot = AccountSnapshot {
                account: Arc::new(account),
                received_at: Utc::now(),
                received_instant: Instant::now(),
            };
            cached.failed_at = None;
            cached.snapshot = Some(snapshot.clone());
            return Ok(snapshot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use axum::{http::StatusCode, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    async fn server() -> (
        String,
        Arc<AtomicUsize>,
        Arc<AtomicBool>,
        tokio::task::JoinHandle<()>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let app = Router::new().route(
            "/fapi/v2/account",
            get({
                let calls = calls.clone();
                let fail = fail.clone();
                move || {
                    let calls = calls.clone();
                    let fail = fail.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        if fail.load(Ordering::SeqCst) {
                            return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
                        }
                        (
                            StatusCode::OK,
                            Json(json!({
                                "totalWalletBalance": "10",
                                "totalMarginBalance": "10",
                                "totalUnrealizedProfit": "0",
                                "availableBalance": "8",
                                "assets": [{
                                    "asset": "USDT", "walletBalance": "10",
                                    "unrealizedProfit": "0", "marginBalance": "10",
                                    "availableBalance": "8"
                                }]
                            })),
                        )
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, calls, fail, task)
    }

    #[tokio::test]
    async fn concurrent_pairs_share_one_fresh_account_response() {
        let (url, calls, _, server) = server().await;
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let cache = AccountCache::default();
        let snapshots = futures_util::future::join_all((0..20).map(|_| cache.get(&client))).await;
        let first = snapshots[0].as_ref().unwrap();
        for result in &snapshots {
            let snapshot = result.as_ref().unwrap();
            assert!(Arc::ptr_eq(&first.account, &snapshot.account));
            assert_eq!(first.received_at, snapshot.received_at);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn critical_refresh_waits_for_inflight_snapshot_then_fetches_again() {
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route(
            "/fapi/v2/account",
            get({
                let calls = calls.clone();
                let started = started.clone();
                let release = release.clone();
                move || {
                    let calls = calls.clone();
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        let number = calls.fetch_add(1, Ordering::SeqCst);
                        if number == 0 {
                            started.notify_one();
                            release.notified().await;
                        }
                        let balance = if number == 0 { "8" } else { "3" };
                        Json(json!({
                            "totalWalletBalance": "10", "totalMarginBalance": "10",
                            "totalUnrealizedProfit": "0", "availableBalance": balance,
                            "assets": [{"asset": "USDT", "walletBalance": "10",
                                "unrealizedProfit": "0", "marginBalance": "10",
                                "availableBalance": balance}]
                        }))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let cache = AccountCache::default();
        let (initial, refreshed) = tokio::join!(cache.get(&client), async {
            started.notified().await;
            release.notify_one();
            cache.refresh(&client).await
        });
        assert_eq!(
            initial.unwrap().account.assets[0].available_balance,
            rust_decimal_macros::dec!(8)
        );
        assert_eq!(
            refreshed.unwrap().account.assets[0].available_balance,
            rust_decimal_macros::dec!(3)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            cache.get(&client).await.unwrap().account.assets[0].available_balance,
            rust_decimal_macros::dec!(3)
        );
        cache.invalidate(&client).await;
        cache.get(&client).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn credentials_endpoint_and_recv_window_are_isolated() {
        let (url, calls, _, server_task) = server().await;
        let (other_url, other_calls, _, other_server) = server().await;
        let config = AppConfig::default().exchange;
        let mut changed_key = config.clone();
        changed_key.api_key = "another-key".into();
        let mut changed_secret = config.clone();
        changed_secret.api_secret = "another-secret".into();
        let mut changed_window = config.clone();
        changed_window.recv_window += 1;
        let cache = AccountCache::default();
        for config in [&config, &changed_key, &changed_secret, &changed_window] {
            cache
                .get(&BinanceFuturesClient::with_base_url(config, url.clone()))
                .await
                .unwrap();
        }
        cache
            .get(&BinanceFuturesClient::with_base_url(&config, other_url))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(other_calls.load(Ordering::SeqCst), 1);
        server_task.abort();
        other_server.abort();
    }

    #[tokio::test]
    async fn expired_snapshot_is_not_served_when_refresh_fails() {
        let (url, calls, fail, server) = server().await;
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let cache = AccountCache::default();
        cache.get(&client).await.unwrap();
        let (_epoch, entry) = cache.entries.entry(client.account_cache_key()).await;
        entry
            .lock()
            .await
            .snapshot
            .as_mut()
            .unwrap()
            .received_instant = Instant::now() - MAX_AGE;
        fail.store(true, Ordering::SeqCst);
        let failed = futures_util::future::join_all((0..20).map(|_| cache.get(&client))).await;
        assert!(failed.iter().all(Result::is_err));
        assert!(entry.lock().await.snapshot.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        fail.store(false, Ordering::SeqCst);
        assert!(cache.get(&client).await.is_err());
        assert!(cache.refresh(&client).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        tokio::time::sleep(FAILURE_COOLDOWN).await;
        cache.get(&client).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn critical_refresh_bypasses_failure_cooldown_and_locked_entry() {
        let (url, calls, fail, server) = server().await;
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let cache = AccountCache::default();
        fail.store(true, Ordering::SeqCst);
        assert!(cache.get(&client).await.is_err());
        fail.store(false, Ordering::SeqCst);
        assert!(cache.refresh(&client).await.is_err());
        let (_, entry) = cache.entries.entry(client.account_cache_key()).await;
        let held = entry.lock().await;
        let critical =
            tokio::time::timeout(Duration::from_secs(1), cache.refresh_critical(&client))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            critical.account.available_balance,
            rust_decimal::Decimal::from(8)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(held);
        cache.get(&client).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn ordinary_wait_is_bounded_and_retired_failure_cannot_poison_new_generation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route("/fapi/v2/account", get({
            let calls = calls.clone();
            let started = started.clone();
            let release = release.clone();
            move || {
                let calls = calls.clone();
                let started = started.clone();
                let release = release.clone();
                async move {
                    let old = calls.fetch_add(1, Ordering::SeqCst) == 0;
                    if old {
                        started.notify_one();
                        release.notified().await;
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
                    }
                    let balance = "2";
                    (StatusCode::OK, Json(json!({"totalWalletBalance": balance, "totalMarginBalance": balance,
                        "totalUnrealizedProfit": "0", "availableBalance": balance,
                        "assets": [{"asset": "USDC", "walletBalance": balance,
                            "unrealizedProfit": "0", "marginBalance": balance, "availableBalance": balance}]})))
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Arc::new(BinanceFuturesClient::with_base_url(
            &AppConfig::default().exchange,
            url,
        ));
        let cache = Arc::new(AccountCache::default());
        let old_load = tokio::spawn({
            let client = client.clone();
            let cache = cache.clone();
            async move { cache.get(&client).await.unwrap() }
        });
        started.notified().await;
        let waiting = futures_util::future::join_all((0..20).map(|_| cache.get(&client)));
        let waiting = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap();
        assert!(waiting.iter().all(Result::is_err));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(1), cache.invalidate(&client))
            .await
            .unwrap();
        let fresh = tokio::time::timeout(Duration::from_secs(1), cache.get(&client))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fresh.account.available_balance,
            rust_decimal::Decimal::from(2)
        );
        release.notify_one();
        let old_result = old_load.await.unwrap();
        assert_eq!(
            old_result.account.available_balance,
            rust_decimal::Decimal::from(2)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn concurrent_invalidation_forces_critical_refresh_to_retry_and_yield_fresh_balance() {
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route("/fapi/v2/account", get({
            let calls = calls.clone();
            let started = started.clone();
            let release = release.clone();
            move || {
                let calls = calls.clone();
                let started = started.clone();
                let release = release.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        started.notify_one();
                        release.notified().await;
                    }
                    let balance = if call == 0 { "10" } else { "2" };
                    Json(json!({"totalWalletBalance": balance, "totalMarginBalance": balance,
                        "totalUnrealizedProfit": "0", "availableBalance": balance,
                        "assets": [{"asset": "USDC", "walletBalance": balance,
                            "unrealizedProfit": "0", "marginBalance": balance, "availableBalance": balance}]}))
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Arc::new(BinanceFuturesClient::with_base_url(
            &AppConfig::default().exchange,
            url,
        ));
        let cache = Arc::new(AccountCache::default());
        let critical_task = tokio::spawn({
            let client = client.clone();
            let cache = cache.clone();
            async move { cache.refresh_critical(&client).await.unwrap() }
        });
        started.notified().await;
        // Sibling pair fills an order while the first pair's request is in flight.
        cache.invalidate(&client).await;
        release.notify_one();
        let snapshot = critical_task.await.unwrap();
        assert_eq!(
            snapshot.account.available_balance,
            rust_decimal::Decimal::from(2)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn concurrent_critical_account_refreshes_do_not_invalidate_each_other() {
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let app = Router::new().route(
            "/fapi/v2/account",
            get({
                let calls = calls.clone();
                let barrier = barrier.clone();
                move || {
                    let calls = calls.clone();
                    let barrier = barrier.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        barrier.wait().await;
                        Json(json!({"totalWalletBalance":"2", "totalMarginBalance":"2",
                        "totalUnrealizedProfit":"0", "availableBalance":"2", "assets":[
                            {"asset":"USDC", "walletBalance":"2", "marginBalance":"2",
                             "availableBalance":"2", "unrealizedProfit":"0"}]}))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Arc::new(BinanceFuturesClient::with_base_url(
            &AppConfig::default().exchange,
            url,
        ));
        let cache = Arc::new(AccountCache::default());
        let task = tokio::spawn({
            let client = client.clone();
            let cache = cache.clone();
            async move {
                tokio::join!(
                    cache.refresh_critical(&client),
                    cache.refresh_critical(&client)
                )
            }
        });
        tokio::time::timeout(Duration::from_secs(2), barrier.wait())
            .await
            .unwrap();
        let (first, second) = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(first.is_ok() && second.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }
}
