use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const COINBASE_RATES_URL: &str = "https://api.coinbase.com/v2/exchange-rates";
const CACHE_TTL: Duration = Duration::from_secs(15 * 60);
const RETRY_DELAY: Duration = Duration::from_secs(60);
const MAX_DISPLAY_AGE: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CnyRate {
    pub asset: String,
    pub cny_per_unit: Decimal,
    pub updated_at: DateTime<Utc>,
}

#[derive(Default)]
struct CacheEntry {
    rate: Option<CnyRate>,
    fetched_at: Option<Instant>,
    attempted_at: Option<Instant>,
}

pub struct CnyRateCache {
    client: reqwest::Client,
    endpoint: String,
    entries: Mutex<HashMap<String, CacheEntry>>,
}

#[derive(Deserialize)]
struct CoinbaseResponse {
    data: CoinbaseRates,
}

#[derive(Deserialize)]
struct CoinbaseRates {
    currency: String,
    rates: HashMap<String, String>,
}

impl CnyRateCache {
    pub fn new() -> Self {
        Self::with_endpoint(COINBASE_RATES_URL.to_string())
    }

    fn with_endpoint(endpoint: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("Failed to build exchange-rate HTTP client"),
            endpoint,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, asset: &str) -> Option<CnyRate> {
        let entries = self.entries.lock().await;
        let entry = entries.get(asset)?;
        if entry.fetched_at?.elapsed() > MAX_DISPLAY_AGE {
            return None;
        }
        entry.rate.clone()
    }

    pub async fn refresh_if_due(&self, asset: &str) -> Result<()> {
        // These are the quote assets supported by the public rate endpoint.
        if !matches!(asset, "USDC" | "USDT") {
            return Ok(());
        }

        {
            let mut entries = self.entries.lock().await;
            let entry = entries.entry(asset.to_string()).or_default();
            if entry
                .fetched_at
                .is_some_and(|time| time.elapsed() < CACHE_TTL)
                || entry
                    .attempted_at
                    .is_some_and(|time| time.elapsed() < RETRY_DELAY)
            {
                return Ok(());
            }
            // Set this before the request so concurrent refreshes share one attempt.
            entry.attempted_at = Some(Instant::now());
        }

        let response = self
            .client
            .get(&self.endpoint)
            .query(&[("currency", asset)])
            .send()
            .await?
            .error_for_status()?
            .json::<CoinbaseResponse>()
            .await?;
        if response.data.currency != asset {
            bail!("Exchange-rate response currency does not match {}", asset);
        }
        let raw = response
            .data
            .rates
            .get("CNY")
            .ok_or_else(|| anyhow!("CNY rate is missing for {}", asset))?;
        let rate = Decimal::from_str(raw).context("Invalid CNY exchange rate")?;
        if rate <= Decimal::ZERO {
            bail!("CNY exchange rate must be positive");
        }

        let mut entries = self.entries.lock().await;
        let entry = entries.entry(asset.to_string()).or_default();
        entry.rate = Some(CnyRate {
            asset: asset.to_string(),
            cny_per_unit: rate,
            updated_at: Utc::now(),
        });
        entry.fetched_at = Some(Instant::now());
        Ok(())
    }
}

impl Default for CnyRateCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::get, Json, Router};
    use rust_decimal_macros::dec;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn fetches_rate_once_and_reuses_cache() {
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/rates",
                get(|State(calls): State<Arc<AtomicUsize>>| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "data": {"currency": "USDC", "rates": {"CNY": "6.710078"}}
                    }))
                }),
            )
            .with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/rates", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let cache = CnyRateCache::with_endpoint(endpoint);
        cache.refresh_if_due("USDC").await.unwrap();
        cache.refresh_if_due("USDC").await.unwrap();
        cache.refresh_if_due("FDUSD").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            cache.get("USDC").await.unwrap().cny_per_unit,
            dec!(6.710078)
        );
        assert!(cache.get("FDUSD").await.is_none());

        {
            let mut entries = cache.entries.lock().await;
            let entry = entries.get_mut("USDC").unwrap();
            entry.fetched_at = Some(Instant::now() - CACHE_TTL - Duration::from_secs(1));
            entry.attempted_at = Some(Instant::now() - RETRY_DELAY - Duration::from_secs(1));
        }
        cache.refresh_if_due("USDC").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        cache
            .entries
            .lock()
            .await
            .get_mut("USDC")
            .unwrap()
            .fetched_at = Some(Instant::now() - MAX_DISPLAY_AGE - Duration::from_secs(1));
        assert!(cache.get("USDC").await.is_none());

        server.abort();
    }
}
