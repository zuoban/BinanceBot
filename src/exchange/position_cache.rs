use super::cache_entries::CacheEntries;
use super::{BinanceFuturesClient, BinancePositionRisk};
use anyhow::Result;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const MAX_AGE: Duration = Duration::from_secs(1);
const ONE_WAY_POSITION_ERROR: &str = "策略仅支持币安单向持仓模式，请先将账户设置为单向持仓";

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
        let key = symbol.to_ascii_uppercase();
        match self.positions.get(&key) {
            None => Ok(None),
            Some(Some(position)) => Ok(Some(position.clone())),
            Some(None) => anyhow::bail!(ONE_WAY_POSITION_ERROR),
        }
    }
}

#[derive(Default)]
pub(crate) struct PositionCache {
    entries: CacheEntries<PositionSnapshot>,
}

impl PositionCache {
    pub async fn invalidate(&self, client: &BinanceFuturesClient) {
        self.entries.retire(client.account_cache_key()).await;
    }

    pub async fn refresh_critical(
        &self,
        client: &BinanceFuturesClient,
        symbol: &str,
    ) -> Result<(Option<BinancePositionRisk>, Instant)> {
        let key = client.account_cache_key();
        self.entries
            .refresh_critical(key, || async {
                let position = client.get_position(symbol).await;
                let received_at = Instant::now();
                Ok((position?, received_at))
            })
            .await
    }

    pub async fn get(
        &self,
        client: &BinanceFuturesClient,
        symbol: &str,
    ) -> Result<(Option<BinancePositionRisk>, Instant)> {
        let key = client.account_cache_key();
        loop {
            let (epoch, entry, (position, received_instant)) = self
                .entries
                .get_or_fetch(
                    key,
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
            if self.entries.is_current(key, epoch, &entry).await {
                return Ok((position?, received_instant));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::cache_entries::FAILURE_COOLDOWN;
    use super::*;
    use crate::config::AppConfig;
    use axum::{extract::Query, http::StatusCode, routing::get, Json, Router};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    async fn server() -> (
        String,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        // 0 = normal, 1 = error, 2 = missing, 3 = hedge, 4 = bulk missing.
        let mode = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/fapi/v2/positionRisk",
            get({
                let calls = calls.clone();
                let mode = mode.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let calls = calls.clone();
                    let mode = mode.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let current_mode = mode.load(Ordering::SeqCst);
                        if current_mode == 1 {
                            return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
                        }
                        let position = |symbol: &str, side: &str| {
                            json!({
                                "symbol": symbol, "positionSide": side, "positionAmt": "0",
                                "entryPrice": "0", "markPrice": "100", "unRealizedProfit": "0",
                                "liquidationPrice": "0", "leverage": "20"
                            })
                        };
                        let rows: Vec<Value> = if current_mode == 2
                            || (current_mode == 4 && !query.contains_key("symbol"))
                        {
                            vec![]
                        } else if current_mode == 3 {
                            vec![position("SOLUSDC", "LONG"), position("SOLUSDC", "SHORT")]
                        } else if let Some(symbol) = query.get("symbol") {
                            vec![position(symbol, "BOTH")]
                        } else {
                            vec![position("SOLUSDC", "BOTH"), position("BTCUSDT", "BOTH")]
                        };
                        (StatusCode::OK, Json(json!(rows)))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, calls, mode, task)
    }

    #[test]
    fn position_index_keeps_duplicates_and_hedge_errors_scoped_to_each_symbol() {
        let row = |symbol: &str, side: &str| {
            serde_json::from_value(json!({
                "symbol": symbol, "positionSide": side, "positionAmt": "0",
                "entryPrice": "0", "markPrice": "100", "unRealizedProfit": "0",
                "liquidationPrice": "0", "leverage": "20"
            }))
            .unwrap()
        };
        let snapshot = PositionSnapshot::from_vec(
            vec![
                row("SOLUSDC", "BOTH"),
                row("solusdc", "BOTH"),
                row("ETHUSDC", "LONG"),
                row("BTCUSDT", "BOTH"),
            ],
            Instant::now(),
        );
        assert!(snapshot.lookup("SOLUSDC").is_err());
        assert!(snapshot.lookup("ethusdc").is_err());
        assert_eq!(
            snapshot.lookup("btcusdt").unwrap().unwrap().symbol,
            "BTCUSDT"
        );
        assert!(snapshot.lookup("BNBUSDC").unwrap().is_none());
    }

    #[tokio::test]
    async fn concurrent_symbols_share_bulk_request_and_credentials_are_isolated() {
        let (url, calls, _, server_task) = server().await;
        let (other_url, other_calls, _, other_server) = server().await;
        let config = AppConfig::default().exchange;
        let client = BinanceFuturesClient::with_base_url(&config, url.clone());
        let cache = PositionCache::default();
        let (sol, btc) = tokio::join!(cache.get(&client, "SOLUSDC"), cache.get(&client, "BTCUSDT"));
        assert_eq!(sol.unwrap().0.unwrap().symbol, "SOLUSDC");
        assert_eq!(btc.unwrap().0.unwrap().symbol, "BTCUSDT");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut other = config.clone();
        other.api_secret = "other".into();
        let other = BinanceFuturesClient::with_base_url(&other, url);
        cache.get(&other, "SOLUSDC").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let other_endpoint = BinanceFuturesClient::with_base_url(&config, other_url);
        cache.get(&other_endpoint, "SOLUSDC").await.unwrap();
        assert_eq!(other_calls.load(Ordering::SeqCst), 1);
        server_task.abort();
        other_server.abort();
    }

    #[tokio::test]
    async fn missing_positions_use_symbol_fallback_and_hedge_is_rejected() {
        let (url, calls, mode, server) = server().await;
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let cache = PositionCache::default();
        mode.store(2, Ordering::SeqCst);
        assert!(cache.get(&client, "SOLUSDC").await.unwrap().0.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        cache.invalidate(&client).await;
        mode.store(4, Ordering::SeqCst);
        assert!(cache.get(&client, "SOLUSDC").await.unwrap().0.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        cache.invalidate(&client).await;
        mode.store(3, Ordering::SeqCst);
        assert!(cache.get(&client, "SOLUSDC").await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        server.abort();
    }

    #[tokio::test]
    async fn expired_positions_are_not_served_after_failure_and_can_recover() {
        let (url, calls, mode, server) = server().await;
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let cache = PositionCache::default();
        cache.get(&client, "SOLUSDC").await.unwrap();
        let (_epoch, entry) = cache.entries.entry(client.account_cache_key()).await;
        entry
            .lock()
            .await
            .snapshot
            .as_mut()
            .unwrap()
            .received_instant = Instant::now() - MAX_AGE;
        mode.store(1, Ordering::SeqCst);
        let failed =
            futures_util::future::join_all((0..20).map(|_| cache.get(&client, "SOLUSDC"))).await;
        assert!(failed.iter().all(Result::is_err));
        assert!(entry.lock().await.snapshot.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        mode.store(0, Ordering::SeqCst);
        assert!(cache.get(&client, "BTCUSDT").await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        tokio::time::sleep(FAILURE_COOLDOWN).await;
        assert!(cache.get(&client, "SOLUSDC").await.unwrap().0.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn critical_refresh_bypasses_failure_cooldown_and_locked_entry() {
        let (url, calls, mode, server) = server().await;
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let cache = PositionCache::default();
        mode.store(1, Ordering::SeqCst);
        assert!(cache.get(&client, "SOLUSDC").await.is_err());
        mode.store(0, Ordering::SeqCst);
        assert!(cache.get(&client, "SOLUSDC").await.is_err());
        let (_, entry) = cache.entries.entry(client.account_cache_key()).await;
        let held = entry.lock().await;
        let critical = tokio::time::timeout(
            Duration::from_secs(1),
            cache.refresh_critical(&client, "SOLUSDC"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(critical.0.unwrap().symbol, "SOLUSDC");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(held);
        cache.get(&client, "SOLUSDC").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn ordinary_wait_is_bounded_and_retired_failure_cannot_poison_new_generation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route(
            "/fapi/v2/positionRisk",
            get({
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
                        (
                            StatusCode::OK,
                            Json(json!([{
                                "symbol": "SOLUSDC", "positionSide": "BOTH",
                                "positionAmt": "1",
                                "entryPrice": "100", "markPrice": "100", "unRealizedProfit": "0",
                                "liquidationPrice": "0", "leverage": "20"
                            }])),
                        )
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
        let cache = Arc::new(PositionCache::default());
        let old_load = tokio::spawn({
            let client = client.clone();
            let cache = cache.clone();
            async move {
                cache
                    .get(&client, "SOLUSDC")
                    .await
                    .unwrap()
                    .0
                    .unwrap()
                    .position_amt
            }
        });
        started.notified().await;
        let waiting =
            futures_util::future::join_all((0..20).map(|_| cache.get(&client, "BTCUSDT")));
        let waiting = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap();
        assert!(waiting.iter().all(Result::is_err));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(1), cache.invalidate(&client))
            .await
            .unwrap();
        let fresh = tokio::time::timeout(Duration::from_secs(1), cache.get(&client, "SOLUSDC"))
            .await
            .unwrap()
            .unwrap()
            .0
            .unwrap();
        assert_eq!(fresh.position_amt, rust_decimal::Decimal::ONE);
        release.notify_one();
        assert_eq!(old_load.await.unwrap(), rust_decimal::Decimal::ONE);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            cache
                .get(&client, "SOLUSDC")
                .await
                .unwrap()
                .0
                .unwrap()
                .position_amt,
            rust_decimal::Decimal::ONE
        );
        server.abort();
    }

    #[tokio::test]
    async fn critical_position_retries_when_another_pair_invalidates_in_flight() {
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route(
            "/fapi/v2/positionRisk",
            get({
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
                        }
                        Json(json!([{"symbol":"SOLUSDC", "positionSide":"BOTH",
                        "positionAmt":if old { "0" } else { "1" }, "entryPrice":"100",
                        "markPrice":"100", "unRealizedProfit":"0", "liquidationPrice":"0",
                        "leverage":"20"}]))
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
        let cache = Arc::new(PositionCache::default());
        let critical = tokio::spawn({
            let client = client.clone();
            let cache = cache.clone();
            async move { cache.refresh_critical(&client, "SOLUSDC").await.unwrap() }
        });
        started.notified().await;
        cache.invalidate(&client).await;
        release.notify_one();
        assert_eq!(
            critical.await.unwrap().0.unwrap().position_amt,
            rust_decimal::Decimal::ONE
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn concurrent_critical_position_refreshes_do_not_invalidate_each_other() {
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let app = Router::new().route(
            "/fapi/v2/positionRisk",
            get({
                let calls = calls.clone();
                let barrier = barrier.clone();
                move || {
                    let calls = calls.clone();
                    let barrier = barrier.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        barrier.wait().await;
                        Json(
                            json!([{"symbol":"SOLUSDC", "positionSide":"BOTH", "positionAmt":"1",
                        "entryPrice":"100", "markPrice":"100", "unRealizedProfit":"0",
                        "liquidationPrice":"0", "leverage":"20"}]),
                        )
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
        let cache = Arc::new(PositionCache::default());
        let task = tokio::spawn({
            let client = client.clone();
            let cache = cache.clone();
            async move {
                tokio::join!(
                    cache.refresh_critical(&client, "SOLUSDC"),
                    cache.refresh_critical(&client, "SOLUSDC")
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
