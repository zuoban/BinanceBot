use super::state_trades::{
    build_grid_stats, preserve_runtime_grid_stats, recent_trade_cache, RECENT_TRADE_LIMIT,
};
use super::AppState;
use crate::types::{BotSnapshot, GridStats};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::error;

// Routed requests and detached cleanup share exclusion until the last owner finishes.
pub(crate) type MembershipGuard = Arc<tokio::sync::OwnedMutexGuard<()>>;

impl AppState {
    pub async fn refresh_current_scope(&self) {
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let symbol = config.exchange.symbol.clone();
        let mode = config.exchange.trading_mode();
        drop(config);
        let cached_stats = self.stats.read().await.clone();
        let query_symbol = symbol.clone();
        let result = self
            .db
            .run_blocking(move |db| {
                let rev = db.symbol_trade_revision(&query_symbol);
                let saved_stats = db.load_scoped_stats(&query_symbol, mode)?;
                let pnl_totals = db.get_pnl_totals(&query_symbol, mode)?;
                let trades = db.get_recent_trades_for(&query_symbol, mode, RECENT_TRADE_LIMIT)?;
                Ok((
                    build_grid_stats(saved_stats, Some(pnl_totals)),
                    recent_trade_cache(trades),
                    rev,
                ))
            })
            .await;
        match result {
            Ok((mut loaded_stats, loaded_trades, rev)) => {
                preserve_runtime_grid_stats(&mut loaded_stats, &cached_stats);
                self.stats_dirty.store(false, Ordering::SeqCst);
                self.recent_trades_dirty.store(false, Ordering::SeqCst);
                *self.stats.write().await = loaded_stats;
                *self.recent_trades.write().await = loaded_trades;
                self.recent_trades_revision.store(rev, Ordering::Release);
            }
            Err(e) => {
                error!("Failed to load scoped statistics: {}", e);
                self.recent_trades.write().await.clear();
                let mut cleared = GridStats::default();
                preserve_runtime_grid_stats(&mut cleared, &cached_stats);
                *self.stats.write().await = cleared;
                self.stats_dirty.store(true, Ordering::SeqCst);
                self.recent_trades_dirty.store(true, Ordering::SeqCst);
            }
        }
    }

    pub(crate) async fn clear_trade_history(self: &Arc<Self>) -> anyhow::Result<usize> {
        Self::clear_trade_history_for(std::slice::from_ref(self), None).await
    }

    pub(crate) async fn clear_symbol_trade_history(
        self: &Arc<Self>,
        symbol: String,
        membership_guard: Option<MembershipGuard>,
    ) -> anyhow::Result<(usize, BotSnapshot)> {
        let state = Arc::clone(self);
        tokio::spawn(async move {
            let _membership = membership_guard;
            let deleted = {
                let _commit = state.trade_commit_lock.lock().await;
                let db_symbol = symbol.clone();
                let deleted = state
                    .db
                    .run_blocking(move |db| db.clear_trade_history_for_symbol(&db_symbol))
                    .await?;
                let current_symbol = {
                    let config = state.config.read().await;
                    config.exchange.symbol.clone()
                };
                if current_symbol == symbol {
                    state.reset_trade_history_cache().await;
                }
                deleted
            };
            let snapshot = state.broadcast_snapshot().await;
            Ok((deleted, snapshot))
        })
        .await?
    }

    pub(crate) async fn clear_trade_history_for(
        states: &[Arc<Self>],
        membership_guard: Option<MembershipGuard>,
    ) -> anyhow::Result<usize> {
        Ok(
            Self::clear_selected_database_inner(states, true, false, membership_guard, false)
                .await?
                .0,
        )
    }

    pub(crate) async fn clear_selected_database_for(
        states: &[Arc<Self>],
        trades: bool,
        snapshots: bool,
        membership_guard: Option<MembershipGuard>,
    ) -> anyhow::Result<(usize, usize)> {
        Self::clear_selected_database_inner(states, trades, snapshots, membership_guard, true).await
    }

    async fn clear_selected_database_inner(
        states: &[Arc<Self>],
        trades: bool,
        snapshots: bool,
        membership_guard: Option<MembershipGuard>,
        broadcast: bool,
    ) -> anyhow::Result<(usize, usize)> {
        let states = states.to_vec();
        anyhow::ensure!(!states.is_empty(), "没有运行中的交易对");
        tokio::spawn(async move {
            let _guard = membership_guard;
            let deleted = {
                let mut locks: Vec<Arc<tokio::sync::Mutex<()>>> = states
                    .iter()
                    .map(|state| Arc::clone(&state.trade_commit_lock))
                    .collect();
                locks.sort_by_key(|lock| Arc::as_ptr(lock) as usize);
                locks.dedup_by_key(|lock| Arc::as_ptr(lock) as usize);
                let mut commit_guards = Vec::with_capacity(locks.len());
                for lock in locks {
                    commit_guards.push(lock.lock_owned().await);
                }
                let first = &states[0];
                let deleted = first
                    .db
                    .run_blocking(move |db| db.clear_selected_database(trades, snapshots))
                    .await?;
                if trades {
                    for state in &states {
                        state.reset_trade_history_cache().await;
                    }
                }
                drop(commit_guards);
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
        let symbol = self.config.read().await.exchange.symbol.clone();
        self.stats_dirty.store(false, Ordering::SeqCst);
        self.recent_trades_dirty.store(false, Ordering::SeqCst);
        let mut stats = self.stats.write().await;
        let mut cleared = GridStats::default();
        preserve_runtime_grid_stats(&mut cleared, &stats);
        *stats = cleared;
        self.recent_trades.write().await.clear();
        self.recent_trades_revision
            .store(self.db.symbol_trade_revision(&symbol), Ordering::Release);
    }
}
