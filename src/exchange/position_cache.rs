use super::client::one_way_position_ref;
use super::{BinanceFuturesClient, BinancePositionRisk};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const MAX_AGE: Duration = Duration::from_secs(1);

struct PositionSnapshot {
    positions: Vec<BinancePositionRisk>,
    received_instant: Instant,
}

#[derive(Default)]
pub(crate) struct PositionCache {
    entries: Mutex<HashMap<[u8; 32], Arc<Mutex<Option<PositionSnapshot>>>>>,
}

impl PositionCache {
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

    pub async fn get(
        &self,
        client: &BinanceFuturesClient,
        symbol: &str,
    ) -> Result<(Option<BinancePositionRisk>, Instant)> {
        let entry = {
            let mut entries = self.entries.lock().await;
            entries
                .entry(client.account_cache_key())
                .or_default()
                .clone()
        };
        let mut cached = entry.lock().await;
        if cached
            .as_ref()
            .is_none_or(|snapshot| snapshot.received_instant.elapsed() >= MAX_AGE)
        {
            *cached = None;
            let positions = client.get_all_positions().await?;
            *cached = Some(PositionSnapshot {
                positions,
                received_instant: Instant::now(),
            });
        }
        let snapshot = cached.as_ref().expect("position snapshot just loaded");
        let position = one_way_position_ref(&snapshot.positions, symbol)?.cloned();
        let received_instant = snapshot.received_instant;
        drop(cached);
        if position.is_some() {
            return Ok((position, received_instant));
        }

        // A missing symbol is unknown, not a flat position. Retry the original
        // symbol-specific endpoint before allowing reconciliation to succeed.
        let position = client.get_position(symbol).await?;
        Ok((position, Instant::now()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use axum::{extract::Query, http::StatusCode, routing::get, Json, Router};
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        let entry = cache
            .entries
            .lock()
            .await
            .get(&client.account_cache_key())
            .unwrap()
            .clone();
        entry.lock().await.as_mut().unwrap().received_instant = Instant::now() - MAX_AGE;
        mode.store(1, Ordering::SeqCst);
        assert!(cache.get(&client, "SOLUSDC").await.is_err());
        assert!(entry.lock().await.is_none());
        mode.store(0, Ordering::SeqCst);
        assert!(cache.get(&client, "SOLUSDC").await.unwrap().0.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }
}
