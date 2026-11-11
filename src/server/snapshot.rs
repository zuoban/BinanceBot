use super::AppState;
use crate::config::AppConfig;
use crate::db::Database;
use crate::types::{
    AccountInfo, BotSnapshot, BotStatus, GridOrder, HourlyTradeStats, OrderSide, OverviewPairView,
    PairSummary, PositionInfo, SummaryPosition, SummaryStats, SummaryTicker, TodayTradeCounts,
    TradeRecord, TradingMode,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::warn;

const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(800);
const SNAPSHOT_IDLE_HEARTBEAT: Duration = Duration::from_secs(3);
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

pub(super) struct LastSnapshotBroadcast {
    sent_at: Instant,
    receivers: usize,
    frame: Arc<str>,
}

fn snapshot_frame_unchanged(previous: &str, current: &str) -> bool {
    const NEEDLE: &str = "\"uptime_secs\":";
    let (Some(prev_pos), Some(curr_pos)) = (previous.find(NEEDLE), current.find(NEEDLE)) else {
        return previous == current;
    };
    if prev_pos != curr_pos || previous[..prev_pos] != current[..curr_pos] {
        return false;
    }
    let prev_rest = &previous[prev_pos + NEEDLE.len()..];
    let curr_rest = &current[curr_pos + NEEDLE.len()..];
    let prev_digits = prev_rest.bytes().take_while(|b| b.is_ascii_digit()).count();
    let curr_digits = curr_rest.bytes().take_while(|b| b.is_ascii_digit()).count();
    prev_rest[prev_digits..] == curr_rest[curr_digits..]
}

fn count_active_order_sides<'a>(
    orders: impl ExactSizeIterator<Item = &'a GridOrder>,
) -> (usize, usize) {
    let total = orders.len();
    let buy = orders.filter(|order| order.side == OrderSide::Buy).count();
    (buy, total - buy)
}

fn sort_active_orders_desc(orders: &mut [GridOrder]) {
    orders.sort_by(|a, b| {
        b.price
            .cmp(&a.price)
            .then_with(|| a.client_order_id.cmp(&b.client_order_id))
    });
}

fn recent_scoped_trades<'a>(
    trades: impl Iterator<Item = &'a TradeRecord>,
    symbol: &str,
    mode: TradingMode,
) -> Vec<TradeRecord> {
    let mut recent: Vec<TradeRecord> = trades
        .filter(|trade| trade.mode == mode && trade.symbol == symbol)
        .cloned()
        .collect();
    recent.sort_unstable_by(|a, b| {
        b.timestamp
            .cmp(&a.timestamp)
            .then_with(|| b.trade_id.cmp(&a.trade_id))
    });
    recent.truncate(20);
    recent
}

struct SnapshotCharts {
    scope: SnapshotScope,
    revision: u64,
    trades: Option<HourlyTradeStats>,
}

impl SnapshotCharts {
    fn is_current(&self, config: &AppConfig, db: &Database) -> bool {
        self.scope == SnapshotScope::from_config(config)
            && self.revision == db.symbol_trade_revision(&self.scope.symbol)
    }
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
    async fn with_position_and_account<R>(
        &self,
        config: &AppConfig,
        project: impl FnOnce(&PositionInfo) -> R,
    ) -> (R, AccountInfo) {
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
        (project(&position), account)
    }

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
        let (position, account) = self
            .with_position_and_account(&config, |position| AccountPosition {
                size: position.size,
                unrealized_pnl: position.unrealized_pnl,
            })
            .await;
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

    async fn collect_with_charts<T, F, Fut>(&self, now: DateTime<Utc>, mut collect: F) -> T
    where
        F: FnMut(Option<SnapshotCharts>) -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        // A trade commit, cleanup, or scope transition during a database read
        // must not combine old chart rows with the new in-memory state.
        for _ in 0..3 {
            let scope = SnapshotScope::from_config(&*self.config.read().await);
            let charts = self.snapshot_cache.get(&self.db, scope, now).await;
            if let Some(value) = collect(Some(charts)).await {
                return value;
            }
        }
        // Keep status, orders and logs updating under continuous writes. Charts
        // that cannot be validated will be retried on the next snapshot.
        collect(None)
            .await
            .expect("An uncached snapshot has no scope or revision to invalidate")
    }

    pub(crate) async fn overview_snapshot(
        &self,
        now: DateTime<Utc>,
    ) -> (OverviewPairView, Vec<TradeRecord>) {
        self.collect_with_charts(now, |charts| self.collect_overview(charts))
            .await
    }

    async fn read_overview_pair_state(
        &self,
        charts: Option<&SnapshotCharts>,
    ) -> Option<(PairSummary, Vec<TradeRecord>)> {
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let mode = config.exchange.trading_mode();
        if charts.is_some_and(|charts| !charts.is_current(&config, &self.db)) {
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
        let (position, account) = self
            .with_position_and_account(&config, |position| SummaryPosition {
                size: position.size,
                entry_price: position.entry_price,
                mark_price: position.mark_price,
                unrealized_pnl: position.unrealized_pnl,
            })
            .await;
        let (active_buy_orders, active_sell_orders) =
            count_active_order_sides(self.active_orders.read().await.values());
        let stats = SummaryStats {
            total_realized_pnl: self.stats.read().await.total_realized_pnl,
            active_buy_orders,
            active_sell_orders,
        };
        let recent = recent_scoped_trades(
            self.recent_trades.read().await.iter(),
            &config.exchange.symbol,
            mode,
        );
        Some((
            PairSummary {
                symbol: config.exchange.symbol.clone(),
                status,
                mode,
                grid_config: config.summary(),
                ticker,
                position,
                stats,
                today_trade_counts: None,
                account,
                account_cny_rate: None,
            },
            recent,
        ))
    }

    async fn collect_overview(
        &self,
        charts: Option<SnapshotCharts>,
    ) -> Option<(OverviewPairView, Vec<TradeRecord>)> {
        let _scope = self.snapshot_scope_lock.read().await;
        let (mut summary, recent) = self.read_overview_pair_state(charts.as_ref()).await?;
        summary.account_cny_rate = self.cny_rates.get(&summary.account.asset).await;
        summary.today_trade_counts = charts.and_then(|charts| charts.trades).map(|stats| {
            let (buy, sell) = stats.total_counts();
            TodayTradeCounts {
                window_start: stats.window_start,
                window_end: stats.window_end,
                buy,
                sell,
            }
        });
        Some((
            OverviewPairView {
                scoped: self.pair_scope().is_some(),
                mode: summary.mode,
                snapshot: summary,
            },
            recent,
        ))
    }

    pub(crate) async fn sorted_active_orders(&self) -> Vec<GridOrder> {
        let mut orders: Vec<GridOrder> =
            self.active_orders.read().await.values().cloned().collect();
        sort_active_orders_desc(&mut orders);
        orders
    }

    pub async fn snapshot(&self) -> BotSnapshot {
        self.snapshot_at(Utc::now()).await
    }

    pub(crate) async fn snapshot_for_hourly(&self) -> BotSnapshot {
        let _scope = self.snapshot_scope_lock.read().await;
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let status = *self.status.read().await;
        let ticker = self.ticker.read().await.clone();
        let stats = self.stats.read().await.clone();
        let (position, account) = self
            .with_position_and_account(&config, PositionInfo::clone)
            .await;
        BotSnapshot {
            status,
            mode: config.exchange.trading_mode(),
            symbol: config.exchange.symbol.clone(),
            ticker,
            stats,
            position,
            account,
            account_cny_rate: None,
            grid_config: config.summary(),
            active_orders: Vec::new(),
            recent_trades: Vec::new(),
            hourly_trade_stats: None,
            recent_logs: Vec::new(),
        }
    }

    async fn snapshot_at(&self, now: DateTime<Utc>) -> BotSnapshot {
        self.collect_with_charts(now, |charts| self.collect_snapshot(charts, now))
            .await
    }

    async fn collect_snapshot(
        &self,
        charts: Option<SnapshotCharts>,
        now: DateTime<Utc>,
    ) -> Option<BotSnapshot> {
        let _scope = self.snapshot_scope_lock.read().await;
        let (mut snapshot, mut active_orders) = {
            let _commit = self.trade_commit_lock.lock().await;
            let config = self.config.read().await;
            if charts
                .as_ref()
                .is_some_and(|charts| !charts.is_current(&config, &self.db))
            {
                return None;
            }
            let status = *self.status.read().await;
            let ticker = self.ticker.read().await.clone();
            let stats = self.stats.read().await.clone();
            let (position, account) = self
                .with_position_and_account(&config, PositionInfo::clone)
                .await;
            let active_orders: Vec<GridOrder> =
                self.active_orders.read().await.values().cloned().collect();
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
            (
                BotSnapshot {
                    status,
                    mode: config.exchange.trading_mode(),
                    symbol: config.exchange.symbol.clone(),
                    ticker,
                    stats,
                    position,
                    account,
                    account_cny_rate: None,
                    grid_config: config.summary(),
                    active_orders: Vec::new(),
                    recent_trades,
                    hourly_trade_stats: None,
                    recent_logs,
                },
                active_orders,
            )
        };
        sort_active_orders_desc(&mut active_orders);
        snapshot.account_cny_rate = self.cny_rates.get(&snapshot.account.asset).await;
        let (buy, sell) = count_active_order_sides(active_orders.iter());
        snapshot.stats.active_buy_orders = buy;
        snapshot.stats.active_sell_orders = sell;
        snapshot.stats.uptime_secs = (now - snapshot.stats.start_time).num_seconds().max(0) as u64;
        snapshot.active_orders = active_orders;
        snapshot.hourly_trade_stats = charts.and_then(|charts| charts.trades);
        Some(snapshot)
    }

    pub(crate) async fn broadcast_snapshot(&self) -> BotSnapshot {
        self.publish_snapshot(true).await
    }

    async fn publish_snapshot(&self, force: bool) -> BotSnapshot {
        // Cleanup uses this same path, preventing a previously generated frame
        // from being published after the cleanup response's fresh snapshot.
        let mut last = self.snapshot_publish_lock.lock().await;
        let snapshot = self.snapshot().await;
        let receivers = self.ws_broadcast_tx.receiver_count();
        if receivers == 0 {
            *last = None;
            return snapshot;
        }
        if let Some(json) = crate::types::ws_frame_json("snapshot", &snapshot) {
            let skip = !force
                && last.as_ref().is_some_and(|prev| {
                    receivers <= prev.receivers
                        && prev.sent_at.elapsed() < SNAPSHOT_IDLE_HEARTBEAT
                        && snapshot_frame_unchanged(&prev.frame, &json)
                });
            if !skip {
                let _ = self.ws_broadcast_tx.send(Arc::clone(&json));
                *last = Some(LastSnapshotBroadcast {
                    sent_at: Instant::now(),
                    receivers,
                    frame: json,
                });
            }
        }
        snapshot
    }

    pub(crate) async fn run_snapshot_broadcast(self: Arc<Self>) {
        let mut timer = tokio::time::interval(SNAPSHOT_INTERVAL);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if self.ws_broadcast_tx.receiver_count() > 0 {
                self.publish_snapshot(false).await;
            } else {
                let mut last = self.snapshot_publish_lock.lock().await;
                if last.is_some() {
                    *last = None;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
