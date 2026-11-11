use super::cache_entries::CacheEntries;
use super::BinanceFuturesClient;
use crate::sync_ext::MutexUnpoisonedExt;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const DEMAND_WINDOW: Duration = Duration::from_secs(10);
const BULK_THRESHOLD: usize = 10;
const SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(3);

struct MarketCacheSnapshot {
    requested: Instant,
    requested_at: DateTime<Utc>,
    prices: HashMap<String, Decimal>,
}

#[derive(Default)]
pub(crate) struct MarketCache {
    demand: std::sync::Mutex<HashMap<String, HashMap<String, Instant>>>,
    entries: CacheEntries<String, MarketCacheSnapshot>,
}

impl MarketCache {
    fn record_demand(&self, endpoint: &str, symbol: &str) -> usize {
        let now = Instant::now();
        let mut endpoints = self.demand.lock_unpoisoned();
        if endpoints.len() > 64 && !endpoints.contains_key(endpoint) {
            endpoints.retain(|_, symbols| {
                symbols.retain(|_, at| now.duration_since(*at) < DEMAND_WINDOW);
                !symbols.is_empty()
            });
        }
        let symbols = if endpoints.contains_key(endpoint) {
            endpoints.get_mut(endpoint).expect("checked key")
        } else {
            endpoints.entry(endpoint.to_owned()).or_default()
        };
        symbols.retain(|_, at| now.duration_since(*at) < DEMAND_WINDOW);
        if let Some(at) = symbols.get_mut(symbol) {
            *at = now;
        } else {
            symbols.insert(symbol.to_owned(), now);
        }
        symbols.len()
    }

    pub(crate) async fn mark_price(
        &self,
        client: &BinanceFuturesClient,
        symbol: &str,
    ) -> Result<(Decimal, DateTime<Utc>)> {
        let key = client.market_cache_key();
        if self.record_demand(key, symbol) < BULK_THRESHOLD {
            let requested_at = Utc::now();
            return Ok((
                client.get_mark_price_for_fallback(symbol).await?,
                requested_at,
            ));
        }
        let (_, _, (price, requested_at)) = self
            .entries
            .get_or_fetch(
                key,
                "mark-price",
                |snapshot| snapshot.requested.elapsed() < SNAPSHOT_MAX_AGE,
                || async {
                    let requested = Instant::now();
                    let requested_at = Utc::now();
                    let prices = client.get_all_mark_prices_for_fallback().await?;
                    Ok(MarketCacheSnapshot {
                        requested,
                        requested_at,
                        prices: prices
                            .into_iter()
                            .map(|price| (price.symbol, price.mark_price))
                            .collect(),
                    })
                },
                |snapshot| (snapshot.prices.get(symbol).copied(), snapshot.requested_at),
            )
            .await?;
        if let Some(price) = price {
            return Ok((price, requested_at));
        }
        let requested_at = Utc::now();
        Ok((
            client.get_mark_price_for_fallback(symbol).await?,
            requested_at,
        ))
    }
}

#[cfg(test)]
mod tests;
