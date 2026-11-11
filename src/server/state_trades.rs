use super::AppState;
use crate::telegram::TradeNotificationContext;
use crate::types::{
    AccountInfo, BotSnapshot, GridDirection, GridOrder, GridStats, HourlyTradeStats, PositionInfo,
    TradeRecord, TradingMode,
};
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, warn};

pub(super) const RECENT_TRADE_LIMIT: usize = 200;

// Oldest first; trades with equal timestamps retain their database insertion order.
fn cache_recent_trade(trades: &mut VecDeque<TradeRecord>, trade: TradeRecord) {
    let index = trades.partition_point(|existing| existing.timestamp <= trade.timestamp);
    trades.insert(index, trade);
    if trades.len() > RECENT_TRADE_LIMIT {
        trades.pop_front();
    }
}

pub(super) fn recent_trade_cache(trades: Vec<TradeRecord>) -> VecDeque<TradeRecord> {
    let mut cache = VecDeque::with_capacity(RECENT_TRADE_LIMIT);
    // The database returns newest first, with later rowids first for equal timestamps.
    for trade in trades.into_iter().rev() {
        cache_recent_trade(&mut cache, trade);
    }
    cache
}

pub(super) fn build_grid_stats(
    scoped: Option<(usize, usize, Decimal, Decimal)>,
    pnl: Option<(Decimal, Decimal, usize)>,
) -> GridStats {
    let mut stats = GridStats::default();
    if let Some((total_trades, cycles, profit, volume)) = scoped {
        stats.total_trades = total_trades;
        stats.completed_cycles = cycles;
        stats.total_realized_profit = profit;
        stats.total_volume_usdc = volume;
    }
    if let Some((realized_pnl, commission, pending)) = pnl {
        stats.total_realized_pnl = realized_pnl;
        stats.total_commission = commission;
        stats.pending_pnl_trades = pending;
    }
    stats
}

fn updated_stats_for_trade(
    current: &GridStats,
    trade: &TradeRecord,
    cycle_profit: Option<Decimal>,
) -> GridStats {
    let mut updated = current.clone();
    updated.total_trades = updated.total_trades.saturating_add(1);
    updated.total_volume_usdc = updated
        .total_volume_usdc
        .checked_add(trade.amount_usdc)
        .unwrap_or(Decimal::MAX);
    if let Some(profit) = cycle_profit {
        updated.completed_cycles = updated.completed_cycles.saturating_add(1);
        updated.total_realized_profit = updated.total_realized_profit.saturating_add(profit);
    }
    if trade.pnl_verified {
        updated.total_realized_pnl = updated
            .total_realized_pnl
            .saturating_add(trade.realized_pnl);
        updated.total_commission = updated.total_commission.saturating_add(trade.commission);
    } else {
        updated.pending_pnl_trades = updated.pending_pnl_trades.saturating_add(1);
    }
    updated
}

impl AppState {
    pub(crate) fn account_view(
        &self,
        symbol: &str,
        mode: TradingMode,
        position: &PositionInfo,
        local: &AccountInfo,
    ) -> AccountInfo {
        if mode != TradingMode::Paper {
            return local.clone();
        }
        if self.paper_restore_failed.load(Ordering::SeqCst) {
            return AccountInfo::default();
        }
        match self.db.shared_paper_account(symbol, position, local) {
            Ok(account) => account,
            Err(error) => {
                error!(%symbol, %error, "Could not restore shared paper account");
                self.paper_restore_failed.store(true, Ordering::SeqCst);
                AccountInfo::default()
            }
        }
    }

    pub(crate) async fn current_account(&self) -> AccountInfo {
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let position = self.position.read().await;
        let local = self.account.read().await;
        self.account_view(
            &config.exchange.symbol,
            config.exchange.trading_mode(),
            &position,
            &local,
        )
    }

    pub async fn record_trade(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
    ) -> bool {
        self.record_trade_with_paper_state(
            trade,
            cycle_profit,
            pair_intent,
            consumed_pair_source,
            None,
        )
        .await
    }

    pub(crate) async fn record_trade_with_paper_state(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
        paper_state: Option<(PositionInfo, AccountInfo)>,
    ) -> bool {
        if paper_state.is_some() {
            let state = Arc::clone(self);
            let order_id = trade.client_order_id.clone();
            return match tokio::spawn(async move {
                let recorded = state
                    .record_trade_inner(
                        trade,
                        cycle_profit,
                        pair_intent,
                        consumed_pair_source,
                        paper_state,
                    )
                    .await;
                if recorded {
                    state.active_orders.write().await.remove(&order_id);
                } else if let Err(error) = state.pause_trading().await {
                    error!("Could not persist paused bot status: {}", error);
                }
                recorded
            })
            .await
            {
                Ok(recorded) => recorded,
                Err(error) => {
                    error!("Paper fill commit task failed: {}", error);
                    false
                }
            };
        }
        self.record_trade_inner(
            trade,
            cycle_profit,
            pair_intent,
            consumed_pair_source,
            paper_state,
        )
        .await
    }

    pub(crate) async fn has_recorded_paper_order(
        self: &Arc<Self>,
        order_id: String,
        mode: TradingMode,
    ) -> anyhow::Result<bool> {
        let _commit = self.trade_commit_lock.lock().await;
        self.db
            .run_blocking(move |db| db.has_recorded_order(&order_id, mode))
            .await
    }

    async fn commit_trade_to_db_and_caches(
        &self,
        trade: &TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
        paper_state: Option<(PositionInfo, AccountInfo)>,
        notify_trade: bool,
    ) -> Result<(bool, Option<HourlyTradeStats>), ()> {
        let _commit = self.trade_commit_lock.lock().await;
        let updated = updated_stats_for_trade(&*self.stats.read().await, trade, cycle_profit);
        let stored_trade = trade.clone();
        let stored_stats = updated.clone();
        let stored_paper_state = paper_state.clone();
        let notification_stats = match self
            .db
            .run_blocking(move |db| {
                let inserted = db.insert_trade_with_paper_recovery(
                    &stored_trade,
                    &stored_stats,
                    pair_intent.as_ref(),
                    consumed_pair_source.as_deref(),
                    stored_paper_state.as_ref(),
                )?;
                let stats = if inserted && notify_trade {
                    db.get_hourly_trade_stats(
                        &stored_trade.symbol,
                        stored_trade.mode,
                        stored_trade.timestamp,
                    )
                    .map_err(|error| warn!("Could not load Telegram trade counts: {}", error))
                    .ok()
                } else {
                    None
                };
                Ok((inserted, stats))
            })
            .await
        {
            Ok((true, stats)) => {
                *self.stats.write().await = updated;
                stats
            }
            Ok((false, _)) => return Ok((false, None)),
            Err(e) => {
                error!("Failed to persist trade and grid stats to SQLite database: {e}");
                return Err(());
            }
        };
        if let Some((position, account)) = paper_state {
            let mut current_position = self.position.write().await;
            let mut current_account = self.account.write().await;
            *current_position = position;
            *current_account = account;
            self.account_view(
                &trade.symbol,
                trade.mode,
                &current_position,
                &current_account,
            );
        }
        // Keep the cache update in the same critical section as persistence
        // so cleanup cannot be followed by an old trade reappearing here.
        cache_recent_trade(&mut *self.recent_trades.write().await, trade.clone());
        Ok((true, notification_stats))
    }

    async fn record_trade_inner(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
        paper_state: Option<(PositionInfo, AccountInfo)>,
    ) -> bool {
        let (current_symbol, current_mode, telegram, direction) = {
            let cfg = self.config.read().await;
            (
                cfg.exchange.symbol.clone(),
                cfg.exchange.trading_mode(),
                cfg.telegram.clone(),
                cfg.grid.direction,
            )
        };
        if trade.symbol != current_symbol || trade.mode != current_mode {
            error!(
                "Trade scope differs from the active strategy: {}",
                trade.trade_id
            );
            return false;
        }
        let notify_trade =
            telegram.enabled && (trade.pnl_verified || trade.side != direction.closing_side());
        let Ok((inserted, notification_stats)) = self
            .commit_trade_to_db_and_caches(
                &trade,
                cycle_profit,
                pair_intent,
                consumed_pair_source,
                paper_state,
                notify_trade,
            )
            .await
        else {
            return false;
        };
        if !inserted {
            return true;
        }
        if self.ws_broadcast_tx.receiver_count() > 0 {
            if let Some(json) = crate::types::ws_frame_json("trade", &trade) {
                let _ = self.ws_broadcast_tx.send(json);
            }
        }
        if notify_trade {
            self.spawn_trade_telegram_notification(telegram, trade, notification_stats, direction)
                .await;
        }
        true
    }

    async fn trade_notification_stats(&self, trade: &TradeRecord) -> Option<HourlyTradeStats> {
        let symbol = trade.symbol.clone();
        let mode = trade.mode;
        let timestamp = trade.timestamp;
        match self
            .db
            .run_blocking(move |db| db.get_hourly_trade_stats(&symbol, mode, timestamp))
            .await
        {
            Ok(stats) => Some(stats),
            Err(error) => {
                warn!("Could not load Telegram trade counts: {}", error);
                None
            }
        }
    }

    async fn spawn_trade_telegram_notification(
        self: &Arc<Self>,
        telegram: crate::config::TelegramConfig,
        trade: TradeRecord,
        notification_stats: Option<HourlyTradeStats>,
        direction: GridDirection,
    ) {
        let account = self.current_account().await;
        let position = self.position.read().await.clone();
        let ticker = self.ticker.read().await.clone();
        let cny_rate = self.cny_rates.get(&account.asset).await;
        let context = TradeNotificationContext::for_trade(
            &trade,
            notification_stats.as_ref(),
            &account,
            cny_rate.as_ref(),
            Utc::now(),
        )
        .with_position(&trade, &position, &ticker)
        .with_direction(direction);
        let notifier = self.telegram_notifier.clone();
        let state = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = notifier
                .send_trade_notification(&telegram, &trade, &context)
                .await
            {
                warn!("Failed to send Telegram trade notification: {}", err);
                state
                    .add_log("WARN", format!("Telegram 成交通知发送失败: {}", err))
                    .await;
            }
        });
    }

    pub async fn verify_trade_pnl(
        self: &Arc<Self>,
        trade: TradeRecord,
        expected_clear_revision: u64,
    ) -> anyhow::Result<()> {
        let commit = self.trade_commit_lock.clone().lock_owned().await;
        // Retirement closes admission even if an aborted worker is still polling.
        if self.is_retired() {
            return Ok(());
        }
        let state = self.clone();
        // Once admitted, finish publishing even if the requester stops.
        tokio::spawn(async move {
            if !state
                .verify_trade_pnl_inner(&trade, expected_clear_revision)
                .await?
            {
                return Ok(());
            }
            let notification = {
                let config = state.config.read().await;
                (config.telegram.enabled
                    && trade.symbol == config.exchange.symbol
                    && trade.mode == config.exchange.trading_mode()
                    && trade.side == config.grid.direction.closing_side())
                .then(|| (config.telegram.clone(), config.grid.direction))
            };
            // Account capture takes the same lock; finish publication before preparing it.
            drop(commit);
            if let Some((telegram, direction)) = notification {
                let notification_stats = state.trade_notification_stats(&trade).await;
                let mut trade = trade;
                trade.pnl_verified = true;
                state
                    .spawn_trade_telegram_notification(
                        telegram,
                        trade,
                        notification_stats,
                        direction,
                    )
                    .await;
            }
            Ok(())
        })
        .await?
    }

    async fn verify_trade_pnl_inner(
        &self,
        trade: &TradeRecord,
        expected_clear_revision: u64,
    ) -> anyhow::Result<bool> {
        let stored_trade = trade.clone();
        if !self
            .db
            .run_blocking(move |db| db.verify_trade_pnl(&stored_trade, expected_clear_revision))
            .await?
        {
            return Ok(false);
        }
        let config = self.config.read().await;
        let current_mode = config.exchange.trading_mode();
        let current_symbol = config.exchange.symbol.clone();
        drop(config);
        if trade.symbol == current_symbol && trade.mode == current_mode {
            let mut stats = self.stats.write().await;
            stats.total_realized_pnl = stats.total_realized_pnl.saturating_add(trade.realized_pnl);
            stats.total_commission = stats.total_commission.saturating_add(trade.commission);
            stats.pending_pnl_trades = stats.pending_pnl_trades.saturating_sub(1);
        }
        let mut recent = self.recent_trades.write().await;
        if let Some(existing) = recent
            .iter_mut()
            .find(|item| item.trade_id == trade.trade_id)
        {
            // Verification updates PnL fields only; SQLite retains the original timestamp.
            existing.realized_pnl = trade.realized_pnl;
            existing.commission = trade.commission;
            existing.is_maker = trade.is_maker;
            existing.pnl_verified = true;
        }
        Ok(true)
    }

    pub(crate) async fn drain_trade_commits(&self) {
        let _commit = self.trade_commit_lock.lock().await;
    }

    pub async fn refresh_current_scope(&self) {
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let symbol = config.exchange.symbol.clone();
        let mode = config.exchange.trading_mode();
        drop(config);
        let query_symbol = symbol.clone();
        let result = self
            .db
            .run_blocking(move |db| {
                Ok((
                    db.load_scoped_stats(&query_symbol, mode)?,
                    db.get_pnl_totals(&query_symbol, mode)?,
                    db.get_recent_trades_for(&query_symbol, mode, RECENT_TRADE_LIMIT)?,
                ))
            })
            .await;
        let (saved_stats, pnl_totals, trades) = match result {
            Ok(result) => result,
            Err(e) => {
                error!("Failed to load scoped statistics: {}", e);
                return;
            }
        };
        *self.stats.write().await = build_grid_stats(saved_stats, Some(pnl_totals));
        *self.recent_trades.write().await = recent_trade_cache(trades);
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
