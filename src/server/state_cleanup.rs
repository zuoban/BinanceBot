use super::state_trades::{build_grid_stats, recent_trade_cache, RECENT_TRADE_LIMIT};
use super::AppState;
use crate::types::{BotSnapshot, GridStats};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::error;

impl AppState {
    pub async fn refresh_current_scope(&self) {
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let symbol = config.exchange.symbol.clone();
        let mode = config.exchange.trading_mode();
        drop(config);
        let start_time = self.stats.read().await.start_time;
        let query_symbol = symbol.clone();
        let result = self
            .db
            .run_blocking(move |db| {
                let saved_stats = db.load_scoped_stats(&query_symbol, mode)?;
                let pnl_totals = db.get_pnl_totals(&query_symbol, mode)?;
                let trades = db.get_recent_trades_for(&query_symbol, mode, RECENT_TRADE_LIMIT)?;
                Ok((
                    build_grid_stats(saved_stats, Some(pnl_totals)),
                    recent_trade_cache(trades),
                ))
            })
            .await;
        match result {
            Ok((mut loaded_stats, loaded_trades)) => {
                loaded_stats.start_time = start_time;
                self.stats_dirty.store(false, Ordering::SeqCst);
                *self.stats.write().await = loaded_stats;
                *self.recent_trades.write().await = loaded_trades;
            }
            Err(e) => {
                error!("Failed to load scoped statistics: {}", e);
                self.recent_trades.write().await.clear();
                let cleared = GridStats {
                    start_time,
                    ..GridStats::default()
                };
                *self.stats.write().await = cleared;
                self.stats_dirty.store(true, Ordering::SeqCst);
            }
        }
    }

    pub(crate) async fn clear_trade_history(self: &Arc<Self>) -> anyhow::Result<usize> {
        Self::clear_trade_history_for(std::slice::from_ref(self)).await
    }

    pub(crate) async fn clear_symbol_trade_history(
        self: &Arc<Self>,
        symbol: String,
    ) -> anyhow::Result<(usize, BotSnapshot)> {
        let state = Arc::clone(self);
        tokio::spawn(async move {
            let deleted = {
                let _commit = state.trade_commit_lock.lock().await;
                let db_symbol = symbol.clone();
                let deleted = state
                    .db
                    .run_blocking(move |db| db.clear_trade_history_for_symbol(&db_symbol))
                    .await?;
                let config = state.config.read().await;
                if config.exchange.symbol == symbol {
                    state.reset_trade_history_cache().await;
                }
                deleted
            };
            let snapshot = state.broadcast_snapshot().await;
            Ok((deleted, snapshot))
        })
        .await?
    }

    pub(crate) async fn clear_trade_history_for(states: &[Arc<Self>]) -> anyhow::Result<usize> {
        Ok(
            Self::clear_selected_database_inner(states, true, false, None, false)
                .await?
                .0,
        )
    }

    pub(crate) async fn clear_selected_database_for(
        states: &[Arc<Self>],
        trades: bool,
        snapshots: bool,
        membership_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> anyhow::Result<(usize, usize)> {
        Self::clear_selected_database_inner(states, trades, snapshots, membership_guard, true).await
    }

    async fn clear_selected_database_inner(
        states: &[Arc<Self>],
        trades: bool,
        snapshots: bool,
        membership_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
        broadcast: bool,
    ) -> anyhow::Result<(usize, usize)> {
        let states = states.to_vec();
        anyhow::ensure!(!states.is_empty(), "没有运行中的交易对");
        tokio::spawn(async move {
            let _guard = membership_guard;
            let deleted = {
                let first = &states[0];
                let _commit = first.trade_commit_lock.lock().await;
                let deleted = first
                    .db
                    .run_blocking(move |db| db.clear_selected_database(trades, snapshots))
                    .await?;
                if trades {
                    for state in &states {
                        state.reset_trade_history_cache().await;
                    }
                }
                deleted
            };
            if trades && broadcast {
                for state in &states {
                    state.broadcast_snapshot().await;
                }
            }
            Ok(deleted)
        })
        .await?
    }

    async fn reset_trade_history_cache(&self) {
        self.stats_dirty.store(false, Ordering::SeqCst);
        let mut stats = self.stats.write().await;
        *stats = GridStats {
            start_time: stats.start_time,
            uptime_secs: stats.uptime_secs,
            active_buy_orders: stats.active_buy_orders,
            active_sell_orders: stats.active_sell_orders,
            ..GridStats::default()
        };
        self.recent_trades.write().await.clear();
    }
}
