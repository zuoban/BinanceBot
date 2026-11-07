use super::AppState;
use crate::config::AppConfig;
use crate::db::Database;
use crate::types::{
    AccountInfo, BotSnapshot, BotStatus, HourlyTradeStats, OrderSide, OverviewPairView,
    PairSummary, SummaryPosition, SummaryStats, SummaryTicker, TodayTradeCounts, TradeRecord,
    TradingMode,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::warn;

const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(800);
const TRADE_CACHE_TTL: Duration = Duration::from_secs(5);
const QUERY_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Serialize)]
pub(crate) struct AccountSnapshot {
    mode: TradingMode,
    snapshot: AccountSnapshotData,
}

#[derive(Serialize)]
struct AccountSnapshotData {
    symbol: String,
    status: BotStatus,
    ticker: AccountTicker,
    position: AccountPosition,
    account: AccountInfo,
    account_cny_rate: Option<crate::fx::CnyRate>,
}

#[derive(Serialize)]
struct AccountTicker {
    mark_price: Decimal,
    mark_update_time: DateTime<Utc>,
}

#[derive(Serialize)]
struct AccountPosition {
    size: Decimal,
    unrealized_pnl: Decimal,
}

#[derive(Serialize)]
pub(crate) struct AccountFrame {
    pub generated_at: DateTime<Utc>,
    pub pairs: Vec<AccountSnapshot>,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub(crate) enum AccountMessage {
    Accounts(AccountFrame),
}

#[derive(Clone, PartialEq, Eq)]
struct SnapshotScope {
    symbol: String,
    mode: TradingMode,
}

impl SnapshotScope {
    fn from_config(config: &AppConfig) -> Self {
        Self {
            symbol: config.exchange.symbol.clone(),
            mode: config.exchange.trading_mode(),
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
pub(super) struct SnapshotCache {
    trades: Mutex<Option<CachedQuery<TradeKey, HourlyTradeStats>>>,
}

struct SnapshotCharts {
    scope: SnapshotScope,
    revision: u64,
    trades: Option<HourlyTradeStats>,
}

impl SnapshotCache {
    async fn get(
        &self,
        db: &Arc<Database>,
        scope: SnapshotScope,
        now: DateTime<Utc>,
    ) -> SnapshotCharts {
        // Serialize only dashboard readers; trading never takes this mutex.
        let mut cache = self.trades.lock().await;
        let revision = db.symbol_trade_revision(&scope.symbol);
        let key = TradeKey {
            symbol: scope.symbol.clone(),
            mode: scope.mode,
            hour: now.timestamp().div_euclid(3600),
            revision,
        };
        if cache
            .as_ref()
            .is_none_or(|entry| !entry.reusable(&key, now, TRADE_CACHE_TTL))
        {
            let query_scope = scope.clone();
            let value = db
                .run_blocking(move |db| {
                    db.get_hourly_trade_stats(&query_scope.symbol, query_scope.mode, now)
                })
                .await
                .map_err(|error| warn!("Failed to load hourly trade counts: {}", error))
                .ok();
            *cache = Some(CachedQuery::new(key, value, now));
        }
        SnapshotCharts {
            scope,
            revision,
            trades: cache.as_ref().and_then(|entry| entry.value.clone()),
        }
    }
}

impl AppState {
    pub(crate) async fn account_snapshot(&self) -> AccountSnapshot {
        // Match the full snapshot's lock order so a scope transition or trade
        // commit cannot expose a partially updated account view.
        let _scope = self.snapshot_scope_lock.read().await;
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let status = *self.status.read().await;
        let ticker = {
            let ticker = self.ticker.read().await;
            AccountTicker {
                mark_price: ticker.mark_price,
                mark_update_time: ticker.mark_update_time,
            }
        };
        let (position, account) = {
            let position = self.position.read().await;
            let local_account = self.account.read().await;
            let account = self.account_view(
                &config.exchange.symbol,
                config.exchange.trading_mode(),
                &position,
                &local_account,
            );
            (
                AccountPosition {
                    size: position.size,
                    unrealized_pnl: position.unrealized_pnl,
                },
                account,
            )
        };
        let account_cny_rate = self.cny_rates.get(&account.asset).await;
        AccountSnapshot {
            mode: config.exchange.trading_mode(),
            snapshot: AccountSnapshotData {
                symbol: config.exchange.symbol.clone(),
                status,
                ticker,
                position,
                account,
                account_cny_rate,
            },
        }
    }

    pub(crate) async fn overview_snapshot(
        &self,
        now: DateTime<Utc>,
    ) -> (OverviewPairView, Vec<TradeRecord>) {
        for _ in 0..3 {
            let scope = SnapshotScope::from_config(&*self.config.read().await);
            let charts = self.snapshot_cache.get(&self.db, scope, now).await;
            if let Some(summary) = self.collect_overview(Some(charts)).await {
                return summary;
            }
        }
        self.collect_overview(None)
            .await
            .expect("An uncached summary has no scope or revision to invalidate")
    }

    async fn collect_overview(
        &self,
        charts: Option<SnapshotCharts>,
    ) -> Option<(OverviewPairView, Vec<TradeRecord>)> {
        let _scope = self.snapshot_scope_lock.read().await;
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let mode = config.exchange.trading_mode();
        if charts.as_ref().is_some_and(|charts| {
            charts.scope != SnapshotScope::from_config(&config)
                || charts.revision != self.db.symbol_trade_revision(&charts.scope.symbol)
        }) {
            return None;
        }
        let status = *self.status.read().await;
        let ticker = {
            let ticker = self.ticker.read().await;
            SummaryTicker {
                mark_price: ticker.mark_price,
                mark_update_time: ticker.mark_update_time,
            }
        };
        let (position, account) = {
            let position = self.position.read().await;
            let local_account = self.account.read().await;
            let account =
                self.account_view(&config.exchange.symbol, mode, &position, &local_account);
            (
                SummaryPosition {
                    size: position.size,
                    entry_price: position.entry_price,
                    mark_price: position.mark_price,
                    unrealized_pnl: position.unrealized_pnl,
                },
                account,
            )
        };
        let account_cny_rate = self.cny_rates.get(&account.asset).await;
        let orders = self.active_orders.read().await;
        let active_buy_orders = orders
            .values()
            .filter(|order| order.side == OrderSide::Buy)
            .count();
        let active_sell_orders = orders.len() - active_buy_orders;
        drop(orders);
        let stats = SummaryStats {
            total_realized_pnl: self.stats.read().await.total_realized_pnl,
            active_buy_orders,
            active_sell_orders,
        };
        let today_trade_counts = charts.and_then(|charts| charts.trades).map(|stats| {
            let (buy, sell) = stats.buckets.iter().fold((0, 0), |(buy, sell), bucket| {
                (buy + bucket.buy_count, sell + bucket.sell_count)
            });
            TodayTradeCounts {
                window_start: stats.window_start,
                window_end: stats.window_end,
                buy,
                sell,
            }
        });
        let trades = self.recent_trades.read().await;
        let mut recent: Vec<_> = trades
            .iter()
            .filter(|trade| trade.mode == mode && trade.symbol == config.exchange.symbol)
            .collect();
        recent.sort_unstable_by(|a, b| {
            b.timestamp
                .cmp(&a.timestamp)
                .then_with(|| b.trade_id.cmp(&a.trade_id))
        });
        let recent = recent.into_iter().take(20).cloned().collect();
        Some((
            OverviewPairView {
                scoped: self.pair_scope().is_some(),
                mode,
                snapshot: PairSummary {
                    symbol: config.exchange.symbol.clone(),
                    status,
                    mode,
                    grid_config: config.summary(),
                    ticker,
                    position,
                    stats,
                    today_trade_counts,
                    account,
                    account_cny_rate,
                },
            },
            recent,
        ))
    }

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
                || charts.revision != self.db.symbol_trade_revision(&charts.scope.symbol)
        }) {
            return None;
        }
        let status = *self.status.read().await;
        let ticker = self.ticker.read().await.clone();
        let mut stats = self.stats.read().await.clone();
        let (position, account) = {
            // Valuation updates take position -> account without the commit lock.
            // Keep both reads together until the shared valuation is published.
            let position = self.position.read().await;
            let local_account = self.account.read().await;
            let account = self.account_view(
                &config.exchange.symbol,
                config.exchange.trading_mode(),
                &position,
                &local_account,
            );
            (position.clone(), account)
        };
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
        let hourly_trade_stats = charts.and_then(|charts| charts.trades);
        Some(BotSnapshot {
            status,
            mode: config.exchange.trading_mode(),
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
