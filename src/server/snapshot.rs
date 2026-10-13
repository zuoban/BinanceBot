use super::AppState;
use crate::config::AppConfig;
use crate::db::Database;
use crate::types::{BotSnapshot, HourlyTradeStats, OrderSide, PriceHistory, TradingMode};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::warn;

const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(800);
const TRADE_CACHE_TTL: Duration = Duration::from_secs(5);
const PRICE_CACHE_TTL: Duration = Duration::from_secs(60);
const QUERY_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, PartialEq, Eq)]
struct SnapshotScope {
    symbol: String,
    mode: TradingMode,
    is_testnet: bool,
}

impl SnapshotScope {
    fn from_config(config: &AppConfig) -> Self {
        Self {
            symbol: config.exchange.symbol.clone(),
            mode: TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet),
            is_testnet: config.exchange.is_testnet,
        }
    }
}

#[derive(PartialEq, Eq)]
struct TradeKey {
    symbol: String,
    mode: TradingMode,
    hour: i64,
    revision: u64,
}

#[derive(PartialEq, Eq)]
struct PriceKey {
    symbol: String,
    is_testnet: bool,
    hour: i64,
    revision: u64,
}

struct CachedQuery<K, V> {
    key: K,
    value: Option<V>,
    queried_at: DateTime<Utc>,
    loaded_at: Instant,
}

impl<K: PartialEq, V> CachedQuery<K, V> {
    fn reusable(&self, key: &K, now: DateTime<Utc>, ttl: Duration) -> bool {
        let ttl = if self.value.is_some() {
            ttl
        } else {
            QUERY_RETRY_DELAY
        };
        self.key == *key
            && self.loaded_at.elapsed() < ttl
            && now
                .signed_duration_since(self.queried_at)
                .to_std()
                .is_ok_and(|age| age < ttl)
    }

    fn new(key: K, value: Option<V>, queried_at: DateTime<Utc>) -> Self {
        Self {
            key,
            value,
            queried_at,
            loaded_at: Instant::now(),
        }
    }
}

#[derive(Default)]
struct ChartCache {
    trades: Option<CachedQuery<TradeKey, HourlyTradeStats>>,
    prices: Option<CachedQuery<PriceKey, PriceHistory>>,
}

#[derive(Default)]
pub(super) struct SnapshotCache {
    charts: Mutex<ChartCache>,
}

struct SnapshotCharts {
    scope: SnapshotScope,
    revisions: (u64, u64),
    trades: Option<HourlyTradeStats>,
    prices: Option<PriceHistory>,
}

impl SnapshotCache {
    async fn get(
        &self,
        db: &Arc<Database>,
        scope: SnapshotScope,
        now: DateTime<Utc>,
    ) -> SnapshotCharts {
        // Serialize only dashboard readers; trading never takes this mutex.
        let mut cache = self.charts.lock().await;
        let revisions = db.snapshot_data_revisions();
        let hour = now.timestamp().div_euclid(3600);
        let trade_key = TradeKey {
            symbol: scope.symbol.clone(),
            mode: scope.mode,
            hour,
            revision: revisions.0,
        };
        let price_key = PriceKey {
            symbol: scope.symbol.clone(),
            is_testnet: scope.is_testnet,
            hour,
            revision: revisions.1,
        };
        let load_trades = cache
            .trades
            .as_ref()
            .is_none_or(|entry| !entry.reusable(&trade_key, now, TRADE_CACHE_TTL));
        let load_prices = cache
            .prices
            .as_ref()
            .is_none_or(|entry| !entry.reusable(&price_key, now, PRICE_CACHE_TTL));
        if load_trades || load_prices {
            let query_scope = scope.clone();
            let result = db
                .run_blocking(move |db| {
                    Ok((
                        load_trades.then(|| {
                            db.get_hourly_trade_stats(&query_scope.symbol, query_scope.mode, now)
                        }),
                        load_prices.then(|| {
                            db.get_price_history(&query_scope.symbol, query_scope.is_testnet, now)
                        }),
                    ))
                })
                .await;
            let (trades, prices) = match result {
                Ok(result) => result,
                Err(error) => {
                    warn!("Snapshot database worker failed: {}", error);
                    (None, None)
                }
            };
            if load_trades {
                let value = trades.and_then(|result| {
                    result
                        .map_err(|error| warn!("Failed to load hourly trade counts: {}", error))
                        .ok()
                });
                cache.trades = Some(CachedQuery::new(trade_key, value, now));
            }
            if load_prices {
                let value = prices.and_then(|result| {
                    result
                        .map_err(|error| warn!("Failed to load price history: {}", error))
                        .ok()
                });
                cache.prices = Some(CachedQuery::new(price_key, value, now));
            }
        }
        SnapshotCharts {
            scope,
            revisions,
            trades: cache.trades.as_ref().and_then(|entry| entry.value.clone()),
            prices: cache.prices.as_ref().and_then(|entry| entry.value.clone()),
        }
    }
}

impl AppState {
    pub async fn snapshot(&self) -> BotSnapshot {
        self.snapshot_at(Utc::now()).await
    }

    async fn snapshot_at(&self, now: DateTime<Utc>) -> BotSnapshot {
        // A trade commit, cleanup, or scope transition during a database read
        // must not combine old chart rows with the new in-memory state.
        for _ in 0..3 {
            let scope = SnapshotScope::from_config(&*self.config.read().await);
            let charts = self.snapshot_cache.get(&self.db, scope, now).await;
            if let Some(snapshot) = self.collect_snapshot(Some(charts), now).await {
                return snapshot;
            }
        }
        // Keep status, orders and logs updating under continuous writes. Charts
        // that cannot be validated will be retried on the next snapshot.
        self.collect_snapshot(None, now)
            .await
            .expect("An uncached snapshot has no scope or revision to invalidate")
    }

    async fn collect_snapshot(
        &self,
        charts: Option<SnapshotCharts>,
        now: DateTime<Utc>,
    ) -> Option<BotSnapshot> {
        let _scope = self.snapshot_scope_lock.read().await;
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        if charts.as_ref().is_some_and(|charts| {
            charts.scope != SnapshotScope::from_config(&config)
                || charts.revisions != self.db.snapshot_data_revisions()
        }) {
            return None;
        }
        let status = *self.status.read().await;
        let ticker = self.ticker.read().await.clone();
        let mut stats = self.stats.read().await.clone();
        let position = self.position.read().await.clone();
        let account = self.account.read().await.clone();
        let account_cny_rate = self.cny_rates.get(&account.asset).await;
        let mut active_orders: Vec<_> = self.active_orders.read().await.values().cloned().collect();
        active_orders.sort_by_key(|order| std::cmp::Reverse(order.price));
        stats.active_buy_orders = active_orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .count();
        stats.active_sell_orders = active_orders
            .iter()
            .filter(|order| order.side == OrderSide::Sell)
            .count();
        stats.uptime_secs = (now - stats.start_time).num_seconds().max(0) as u64;
        let recent_trades = self
            .recent_trades
            .read()
            .await
            .iter()
            .rev()
            .take(50)
            .cloned()
            .collect();
        let recent_logs = self
            .recent_logs
            .read()
            .await
            .iter()
            .rev()
            .take(50)
            .cloned()
            .collect();
        let (hourly_trade_stats, price_history) = charts
            .map(|charts| (charts.trades, charts.prices))
            .unwrap_or_default();
        Some(BotSnapshot {
            status,
            dry_run: config.exchange.dry_run,
            symbol: config.exchange.symbol.clone(),
            ticker,
            stats,
            position,
            account,
            account_cny_rate,
            grid_config: config.summary(),
            active_orders,
            recent_trades,
            hourly_trade_stats,
            price_history,
            recent_logs,
        })
    }

    pub(crate) async fn broadcast_snapshot(&self) -> BotSnapshot {
        // Cleanup uses this same path, preventing a previously generated frame
        // from being published after the cleanup response's fresh snapshot.
        let _publish = self.snapshot_publish_lock.lock().await;
        let snapshot = self.snapshot().await;
        if let Ok(json) =
            serde_json::to_string(&serde_json::json!({"type": "snapshot", "data": snapshot}))
        {
            let _ = self.ws_broadcast_tx.send(json);
        }
        snapshot
    }

    pub async fn run_snapshot_broadcast(self: Arc<Self>) {
        let mut timer = tokio::time::interval(SNAPSHOT_INTERVAL);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if self.ws_broadcast_tx.receiver_count() > 0 {
                self.broadcast_snapshot().await;
            }
        }
    }
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
