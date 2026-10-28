use super::{BinanceAccountInfoResponse, BinanceFuturesClient};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

// Bound reuse independently of the configured polling period: cached balances
// must never be treated as a fresh exchange response indefinitely.
const MAX_AGE: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct AccountSnapshot {
    pub account: Arc<BinanceAccountInfoResponse>,
    pub received_at: DateTime<Utc>,
    pub received_instant: Instant,
}

type AccountEntry = Arc<Mutex<Option<AccountSnapshot>>>;

#[derive(Default)]
pub(crate) struct AccountCache {
    entries: Mutex<HashMap<[u8; 32], AccountEntry>>,
}

impl AccountCache {
    pub async fn get(&self, client: &BinanceFuturesClient) -> Result<AccountSnapshot> {
        self.get_inner(client, false).await
    }

    pub async fn refresh(&self, client: &BinanceFuturesClient) -> Result<AccountSnapshot> {
        self.get_inner(client, true).await
    }

    pub async fn invalidate(&self, client: &BinanceFuturesClient) {
        let entry = self
            .entries
            .lock()
            .await
            .get(&client.account_cache_key())
            .cloned();
        if let Some(entry) = entry {
            *entry.lock().await = None;
        }
    }

    async fn get_inner(
        &self,
        client: &BinanceFuturesClient,
        force: bool,
    ) -> Result<AccountSnapshot> {
        let entry = {
            let mut entries = self.entries.lock().await;
            entries
                .entry(client.account_cache_key())
                .or_default()
                .clone()
        };
        let mut cached = entry.lock().await;
        if let Some(snapshot) = cached.as_ref().filter(|_| !force) {
            if snapshot.received_instant.elapsed() < MAX_AGE {
                return Ok(snapshot.clone());
            }
        }
        // Do not serve an expired snapshot if the exchange request fails.
        *cached = None;
        let account = client.get_account().await?;
        let snapshot = AccountSnapshot {
            account: Arc::new(account),
            received_at: Utc::now(),
            received_instant: Instant::now(),
        };
        *cached = Some(snapshot.clone());
        Ok(snapshot)
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
        let entry = cache
            .entries
            .lock()
            .await
            .get(&client.account_cache_key())
            .unwrap()
            .clone();
        entry.lock().await.as_mut().unwrap().received_instant = Instant::now() - MAX_AGE;
        fail.store(true, Ordering::SeqCst);
        assert!(cache.get(&client).await.is_err());
        assert!(entry.lock().await.is_none());
        fail.store(false, Ordering::SeqCst);
        cache.get(&client).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }
}
