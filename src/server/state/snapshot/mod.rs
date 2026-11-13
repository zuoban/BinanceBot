use super::AppState;
use crate::config::AppConfig;
#[cfg(test)]
use crate::db::Database;
use crate::types::{
    AccountInfo, BotSnapshot, GridOrder, OrderSide, OverviewPairView, PairSummary, PositionInfo,
    SummaryPosition, SummaryStats, SummaryTicker, TodayTradeCounts, TradeRecord, TradingMode,
};
use chrono::{DateTime, Utc};
use std::future::Future;
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

mod cache;

pub(crate) use cache::{
    snapshot_frame_unchanged, sort_and_truncate_recent_trades, AccountFrame, AccountMessage,
    AccountSnapshot, SNAPSHOT_IDLE_HEARTBEAT,
};
use cache::{
    AccountPosition, AccountSnapshotData, AccountTicker, SnapshotCharts, SnapshotScope,
    SNAPSHOT_INTERVAL,
};
pub(in crate::server::state) use cache::{LastSnapshotBroadcast, ResyncFrame, SnapshotCache};

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
    sort_and_truncate_recent_trades(&mut recent, 20);
    recent
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
        let (mode, mut snapshot) = {
            let _commit = self.trade_commit_lock.lock().await;
            let config = self.config.read().await;
            let status = *self.status.read().await;
            let symbol = config.exchange.symbol.clone();
            let account_scope = config.exchange.account_scope();
            let sync_interval_secs = config.exchange.sync_interval_secs;
            let mode = config.exchange.trading_mode();
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
            (
                mode,
                AccountSnapshotData {
                    symbol,
                    status,
                    account_scope,
                    sync_interval_secs,
                    ticker,
                    position,
                    account,
                    account_cny_rate: None,
                },
            )
        };
        snapshot.account_cny_rate = self.cny_rates.get(&snapshot.account.asset).await;
        AccountSnapshot { mode, snapshot }
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
                account_scope: config.exchange.account_scope(),
                sync_interval_secs: config.exchange.sync_interval_secs,
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

    async fn read_base_bot_snapshot(
        &self,
        config: &AppConfig,
        generated_at: DateTime<Utc>,
    ) -> BotSnapshot {
        let status = *self.status.read().await;
        let ticker = self.ticker.read().await.clone();
        let stats = self.stats.read().await.clone();
        let (position, account) = self
            .with_position_and_account(config, PositionInfo::clone)
            .await;
        BotSnapshot {
            generated_at,
            status,
            mode: config.exchange.trading_mode(),
            symbol: config.exchange.symbol.clone(),
            account_scope: config.exchange.account_scope(),
            sync_interval_secs: config.exchange.sync_interval_secs,
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

    pub(crate) async fn snapshot_for_hourly(&self) -> BotSnapshot {
        let _scope = self.snapshot_scope_lock.read().await;
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        self.read_base_bot_snapshot(&config, Utc::now()).await
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
            let mut base = self.read_base_bot_snapshot(&config, now).await;
            let active_orders: Vec<GridOrder> =
                self.active_orders.read().await.values().cloned().collect();
            base.recent_trades = self
                .recent_trades
                .read()
                .await
                .iter()
                .rev()
                .filter(|t| t.symbol == base.symbol && t.mode == base.mode)
                .take(50)
                .cloned()
                .collect();
            (base, active_orders)
        };
        // Logs are not part of the trade-commit consistency boundary.
        snapshot.recent_logs = self
            .recent_logs
            .read()
            .await
            .iter()
            .rev()
            .take(50)
            .cloned()
            .collect();
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

    /// Snapshot frame for clients that lagged behind the broadcast. Clients that
    /// lag together share one frame instead of each rebuilding the snapshot.
    pub(crate) async fn resync_snapshot_frame(&self) -> Option<Arc<str>> {
        let mut cached = self.resync_frame.lock().await;
        if let Some(frame) = cached.as_ref().and_then(ResyncFrame::fresh) {
            return Some(frame);
        }
        let frame = crate::types::ws_frame_json("snapshot", &self.snapshot().await)?;
        *cached = Some(ResyncFrame::new(Arc::clone(&frame)));
        Some(frame)
    }

    pub(crate) async fn broadcast_snapshot(&self) -> BotSnapshot {
        // A forced publish follows a state change; lagged clients must not be
        // resynced from a frame generated before it.
        *self.resync_frame.lock().await = None;
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
mod tests;
