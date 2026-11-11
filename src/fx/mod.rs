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

pub(crate) struct CnyRateCache {
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
    rates: CoinbaseRateMap,
}

#[derive(Deserialize)]
struct CoinbaseRateMap {
    #[serde(rename = "CNY")]
    cny: Option<String>,
}

impl CnyRateCache {
    pub(crate) fn new() -> Self {
        Self::with_endpoint(COINBASE_RATES_URL.to_string())
    }

    fn with_endpoint(endpoint: String) -> Self {
        Self {
            client: crate::exchange::client::shared_http_client(),
            endpoint,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn get(&self, asset: &str) -> Option<CnyRate> {
        let entries = self.entries.lock().await;
        let entry = entries.get(asset)?;
        if entry.fetched_at?.elapsed() > MAX_DISPLAY_AGE {
            return None;
        }
        entry.rate.clone()
    }

    pub(crate) async fn refresh_if_due(&self, asset: &str) -> Result<()> {
        // These are the quote assets supported by the public rate endpoint.
        if !matches!(asset, "USDC" | "USDT") {
            return Ok(());
        }

        {
            let mut entries = self.entries.lock().await;
            if entries.get(asset).is_some_and(|entry| {
                entry
                    .fetched_at
                    .is_some_and(|time| time.elapsed() < CACHE_TTL)
                    || entry
                        .attempted_at
                        .is_some_and(|time| time.elapsed() < RETRY_DELAY)
            }) {
                return Ok(());
            }
            // Set this before the request so concurrent refreshes share one attempt.
            if let Some(entry) = entries.get_mut(asset) {
                entry.attempted_at = Some(Instant::now());
            } else {
                entries.entry(asset.to_string()).or_default().attempted_at = Some(Instant::now());
            }
        }

        let resp = self
            .client
            .get(&self.endpoint)
            .query(&[("currency", asset)])
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(reqwest::Error::without_url)?
            .error_for_status()
            .map_err(reqwest::Error::without_url)?;
        let response =
            crate::exchange::client::read_bounded_json::<CoinbaseResponse>(resp, 256 * 1024)
                .await?;
        if response.data.currency != asset {
            bail!("Exchange-rate response currency does not match {}", asset);
        }
        let raw = response
            .data
            .rates
            .cny
            .as_deref()
            .ok_or_else(|| anyhow!("CNY rate is missing for {}", asset))?;
        let rate = Decimal::from_str(raw).context("Invalid CNY exchange rate")?;
        if rate <= Decimal::ZERO {
            bail!("CNY exchange rate must be positive");
        }

        let mut entries = self.entries.lock().await;
        let entry = if let Some(entry) = entries.get_mut(asset) {
            entry
        } else {
            entries.entry(asset.to_string()).or_default()
        };
        entry.rate = Some(CnyRate {
            asset: response.data.currency,
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
mod tests;
