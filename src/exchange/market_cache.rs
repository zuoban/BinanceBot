use super::BinanceFuturesClient;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const DEMAND_WINDOW: Duration = Duration::from_secs(10);
const BULK_THRESHOLD: usize = 10;
const SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(3);
const FAILURE_COOLDOWN: Duration = Duration::from_secs(2);
const ENTRY_WAIT: Duration = Duration::from_millis(100);

#[derive(Default)]
struct MarketEntry {
    demand: HashMap<String, Instant>,
    snapshot: Option<Snapshot>,
    failed_at: Option<Instant>,
}

struct Snapshot {
    requested: Instant,
    requested_at: DateTime<Utc>,
    prices: HashMap<String, Decimal>,
}

#[derive(Default)]
pub(crate) struct MarketCache {
    entries: Mutex<HashMap<String, Arc<Mutex<MarketEntry>>>>,
}

impl MarketCache {
    pub async fn mark_price(
        &self,
        client: &BinanceFuturesClient,
        symbol: &str,
    ) -> Result<(Decimal, DateTime<Utc>)> {
        let entry = {
            let mut entries = self.entries.lock().await;
            entries
                .entry(client.market_cache_key().to_owned())
                .or_default()
                .clone()
        };
        // A slow shared HTTP request must not stall every engine's reconciliation.
        let mut entry = tokio::time::timeout(ENTRY_WAIT, entry.lock())
            .await
            .map_err(|_| anyhow!("shared mark-price request is still in flight"))?;
        let now = Instant::now();
        entry
            .demand
            .retain(|_, at| now.duration_since(*at) < DEMAND_WINDOW);
        entry.demand.insert(symbol.to_owned(), now);
        if entry.demand.len() < BULK_THRESHOLD {
            drop(entry);
            let requested_at = Utc::now();
            return Ok((
                client.get_mark_price_for_fallback(symbol).await?,
                requested_at,
            ));
        }

        if entry
            .snapshot
            .as_ref()
            .is_none_or(|snapshot| snapshot.requested.elapsed() >= SNAPSHOT_MAX_AGE)
        {
            if entry
                .failed_at
                .is_some_and(|at| at.elapsed() < FAILURE_COOLDOWN)
            {
                return Err(anyhow!("shared mark-price request is cooling down"));
            }
            entry.snapshot = None;
            let requested = Instant::now();
            let requested_at = Utc::now();
            match client.get_all_mark_prices_for_fallback().await {
                Ok(prices) => {
                    entry.failed_at = None;
                    entry.snapshot = Some(Snapshot {
                        requested,
                        requested_at,
                        prices: prices
                            .into_iter()
                            .map(|price| (price.symbol, price.mark_price))
                            .collect(),
                    });
                }
                Err(error) => {
                    entry.failed_at = Some(Instant::now());
                    return Err(error);
                }
            }
        }
        let snapshot = entry.snapshot.as_ref().expect("snapshot populated");
        if let Some(price) = snapshot.prices.get(symbol) {
            return Ok((*price, snapshot.requested_at));
        }
        drop(entry);
        let requested_at = Utc::now();
        Ok((
            client.get_mark_price_for_fallback(symbol).await?,
            requested_at,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use axum::{extract::Query, routing::get, Json, Router};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[tokio::test]
    async fn bulk_mark_requests_are_shared_and_failures_are_cooled_down() {
        let bulk = Arc::new(AtomicUsize::new(0));
        let single = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let app = Router::new().route("/fapi/v1/premiumIndex", get({
            let bulk = bulk.clone();
            let single = single.clone();
            let fail = fail.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let bulk = bulk.clone();
                let single = single.clone();
                let fail = fail.clone();
                async move {
                    if let Some(symbol) = query.get("symbol") {
                        single.fetch_add(1, Ordering::SeqCst);
                        (axum::http::StatusCode::OK, Json(serde_json::json!({"symbol": symbol, "markPrice": "100"})))
                    } else {
                        bulk.fetch_add(1, Ordering::SeqCst);
                        if fail.load(Ordering::SeqCst) {
                            (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({})))
                        } else {
                            let rows: Vec<_> = (0..20).map(|i| serde_json::json!({"symbol": format!("PAIR{i}"), "markPrice": "101"})).collect();
                            (axum::http::StatusCode::OK, Json(serde_json::json!(rows)))
                        }
                    }
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let other_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other_url = format!("http://{}", other_listener.local_addr().unwrap());
        let other_app = app.clone();
        let other_server =
            tokio::spawn(async move { axum::serve(other_listener, other_app).await.unwrap() });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client =
            BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url.clone());
        let cache = MarketCache::default();
        for i in 0..9 {
            cache
                .mark_price(&client, &format!("PAIR{i}"))
                .await
                .unwrap();
        }
        let results = futures_util::future::join_all((9..20).map(|i| {
            let symbol = format!("PAIR{i}");
            let cache = &cache;
            let client = &client;
            async move { cache.mark_price(client, &symbol).await }
        }))
        .await;
        let first_time = results[0].as_ref().unwrap().1;
        assert!(results.iter().all(|result| {
            let (price, time) = result.as_ref().unwrap();
            *price == Decimal::from(101) && *time == first_time
        }));
        assert_eq!(single.load(Ordering::SeqCst), 9);
        assert_eq!(bulk.load(Ordering::SeqCst), 1);
        // Missing symbols fall back individually; a different endpoint cannot reuse this snapshot.
        assert_eq!(
            cache.mark_price(&client, "PAIR20").await.unwrap().0,
            Decimal::from(100)
        );
        let other_client =
            BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, other_url);
        assert_eq!(
            cache.mark_price(&other_client, "PAIR0").await.unwrap().0,
            Decimal::from(100)
        );
        assert_eq!(single.load(Ordering::SeqCst), 11);

        let entry = cache.entries.lock().await.get(&url).unwrap().clone();
        entry.lock().await.snapshot = None;
        fail.store(true, Ordering::SeqCst);
        let failed = futures_util::future::join_all((0..10).map(|i| {
            let symbol = format!("PAIR{i}");
            let cache = &cache;
            let client = &client;
            async move { cache.mark_price(client, &symbol).await }
        }))
        .await;
        assert!(failed.iter().all(Result::is_err));
        assert_eq!(bulk.load(Ordering::SeqCst), 2);
        assert_eq!(single.load(Ordering::SeqCst), 11);
        let held = entry.lock().await;
        let started = Instant::now();
        assert!(cache.mark_price(&client, "PAIR0").await.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
        server.abort();
        other_server.abort();
    }
}
