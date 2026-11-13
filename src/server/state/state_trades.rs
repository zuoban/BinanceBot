use super::AppState;
use crate::db::OrderFillDbOutcome;
use crate::types::{
    AccountInfo, GridOrder, GridStats, HourlyTradeStats, PositionInfo, TradeRecord, TradingMode,
};
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
    pnl: Option<(Decimal, Decimal, usize, usize)>,
) -> GridStats {
    let mut stats = GridStats::default();
    if let Some((total_trades, cycles, profit, volume)) = scoped {
        stats.total_trades = total_trades;
        stats.completed_cycles = cycles;
        stats.total_realized_profit = profit;
        stats.total_volume_usdc = volume;
    }
    if let Some((realized_pnl, commission, non_quote_commission_trades, pending)) = pnl {
        stats.total_realized_pnl = realized_pnl;
        stats.total_commission = commission;
        stats.non_quote_commission_trades = non_quote_commission_trades;
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
        updated.total_commission = updated
            .total_commission
            .saturating_add(trade.quote_commission());
        if trade.has_non_quote_commission() {
            updated.non_quote_commission_trades =
                updated.non_quote_commission_trades.saturating_add(1);
        }
    } else {
        updated.pending_pnl_trades = updated.pending_pnl_trades.saturating_add(1);
    }
    updated
}

pub(super) fn preserve_runtime_grid_stats(loaded: &mut GridStats, cached: &GridStats) {
    loaded.start_time = cached.start_time;
    loaded.uptime_secs = cached.uptime_secs;
    loaded.active_buy_orders = cached.active_buy_orders;
    loaded.active_sell_orders = cached.active_sell_orders;
}

fn reload_durable_base_stats(
    db: &crate::db::Database,
    symbol: &str,
    mode: TradingMode,
    cached: &GridStats,
) -> anyhow::Result<GridStats> {
    let mut base = build_grid_stats(
        db.load_scoped_stats(symbol, mode)?,
        Some(db.get_pnl_rollup(symbol, mode)?),
    );
    preserve_runtime_grid_stats(&mut base, cached);
    Ok(base)
}

type PersistedTradeOutcome = (
    OrderFillDbOutcome,
    GridStats,
    Option<VecDeque<TradeRecord>>,
    Option<HourlyTradeStats>,
    u64,
);

struct PersistTradeInput<'a> {
    stored_trade: &'a TradeRecord,
    cached_stats: GridStats,
    stats_dirty: bool,
    trades_dirty: bool,
    cycle_profit: Option<Decimal>,
    pair_intent: Option<&'a GridOrder>,
    consumed_pair_source: Option<&'a str>,
    paper_state: Option<&'a (PositionInfo, AccountInfo)>,
    notify_trade: bool,
    dedup_client_order_id: bool,
}

fn persist_trade_and_stats_on_db(
    db: &crate::db::Database,
    input: PersistTradeInput<'_>,
) -> anyhow::Result<PersistedTradeOutcome> {
    let PersistTradeInput {
        stored_trade,
        cached_stats,
        stats_dirty,
        trades_dirty,
        cycle_profit,
        pair_intent,
        consumed_pair_source,
        paper_state,
        notify_trade,
        dedup_client_order_id,
    } = input;
    let base = if stats_dirty {
        reload_durable_base_stats(db, &stored_trade.symbol, stored_trade.mode, &cached_stats)?
    } else {
        cached_stats
    };
    let updated = updated_stats_for_trade(&base, stored_trade, cycle_profit);
    let status = db.insert_trade_with_paper_recovery_inner(
        stored_trade,
        &updated,
        pair_intent,
        consumed_pair_source,
        paper_state,
        dedup_client_order_id,
    )?;
    if status == OrderFillDbOutcome::AlreadyRecorded {
        let revision = db.symbol_trade_revision(&stored_trade.symbol);
        return Ok((status, base, None, None, revision));
    }
    let inserted = status == OrderFillDbOutcome::Inserted;
    // Never fail after commit: paper_accounts and PaperLedger have already advanced.
    let reloaded_trades = if trades_dirty {
        match db.get_recent_trades_for(&stored_trade.symbol, stored_trade.mode, RECENT_TRADE_LIMIT)
        {
            Ok(trades) => Some(recent_trade_cache(trades)),
            Err(err) => {
                warn!("Could not reload recent trades after trade commit: {err}");
                None
            }
        }
    } else {
        None
    };
    let hourly = (inserted && notify_trade)
        .then(|| {
            db.get_hourly_trade_stats(
                &stored_trade.symbol,
                stored_trade.mode,
                stored_trade.timestamp,
            )
            .map_err(|error| warn!("Could not load Telegram trade counts: {}", error))
            .ok()
        })
        .flatten();
    let revision = db.symbol_trade_revision(&stored_trade.symbol);
    Ok((
        status,
        if inserted { updated } else { base },
        reloaded_trades,
        hourly,
        revision,
    ))
}

impl AppState {
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
        self.record_trade_with_options(
            trade,
            cycle_profit,
            pair_intent,
            consumed_pair_source,
            paper_state,
            false,
        )
        .await
        .is_some()
    }

    pub(crate) async fn record_order_fill_with_paper_state(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
        paper_state: Option<(PositionInfo, AccountInfo)>,
        dedup_client_order_id: bool,
    ) -> Option<OrderFillDbOutcome> {
        self.record_trade_with_options(
            trade,
            cycle_profit,
            pair_intent,
            consumed_pair_source,
            paper_state,
            dedup_client_order_id,
        )
        .await
    }

    async fn record_trade_with_options(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
        paper_state: Option<(PositionInfo, AccountInfo)>,
        dedup_client_order_id: bool,
    ) -> Option<OrderFillDbOutcome> {
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
                        dedup_client_order_id,
                    )
                    .await;
                if recorded.is_some() {
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
                    None
                }
            };
        }
        self.record_trade_inner(
            trade,
            cycle_profit,
            pair_intent,
            consumed_pair_source,
            paper_state,
            dedup_client_order_id,
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
        (notify_trade, dedup_client_order_id): (bool, bool),
    ) -> Result<(OrderFillDbOutcome, Option<HourlyTradeStats>), ()> {
        let _commit = self.trade_commit_lock.lock().await;
        let stats_dirty = self.stats_dirty.load(Ordering::SeqCst);
        let trades_dirty = self.recent_trades_dirty.load(Ordering::SeqCst);
        let cached_stats = self.stats.read().await.clone();
        let stored_trade = trade.clone();
        let stored_paper = paper_state.clone();
        let persisted = self
            .db
            .run_blocking(move |db| {
                persist_trade_and_stats_on_db(
                    db,
                    PersistTradeInput {
                        stored_trade: &stored_trade,
                        cached_stats,
                        stats_dirty,
                        trades_dirty,
                        cycle_profit,
                        pair_intent: pair_intent.as_ref(),
                        consumed_pair_source: consumed_pair_source.as_deref(),
                        paper_state: stored_paper.as_ref(),
                        notify_trade,
                        dedup_client_order_id,
                    },
                )
            })
            .await;
        let outcome = persisted.map_err(|e| {
            error!("Failed to persist trade and grid stats to SQLite database: {e}");
        })?;
        Ok(self
            .apply_persisted_trade_outcome(trade, paper_state, outcome)
            .await)
    }

    async fn apply_persisted_trade_outcome(
        &self,
        trade: &TradeRecord,
        paper_state: Option<(PositionInfo, AccountInfo)>,
        outcome: PersistedTradeOutcome,
    ) -> (OrderFillDbOutcome, Option<HourlyTradeStats>) {
        let (status, next_stats, reloaded_trades, notification_stats, revision) = outcome;
        if status == OrderFillDbOutcome::AlreadyRecorded {
            return (status, None);
        }
        let had_reloaded_trades = reloaded_trades.is_some();
        if let Some(reloaded) = reloaded_trades {
            *self.recent_trades.write().await = reloaded;
            self.recent_trades_dirty.store(false, Ordering::SeqCst);
            self.recent_trades_revision
                .store(revision, Ordering::Release);
        }
        *self.stats.write().await = next_stats;
        self.stats_dirty.store(false, Ordering::SeqCst);
        if status != OrderFillDbOutcome::Inserted {
            return (status, None);
        }
        if let Some((position, account)) = paper_state {
            self.apply_committed_paper_state(&trade.symbol, trade.mode, position, account)
                .await;
        }
        if !had_reloaded_trades {
            cache_recent_trade(&mut *self.recent_trades.write().await, trade.clone());
            self.recent_trades_revision
                .store(revision, Ordering::Release);
        }
        (OrderFillDbOutcome::Inserted, notification_stats)
    }

    async fn apply_committed_paper_state(
        &self,
        symbol: &str,
        mode: TradingMode,
        position: PositionInfo,
        account: AccountInfo,
    ) {
        let mut current_position = self.position.write().await;
        let mut current_account = self.account.write().await;
        *current_position = position;
        *current_account = account;
        self.account_view(symbol, mode, &current_position, &current_account);
    }

    async fn record_trade_inner(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
        paper_state: Option<(PositionInfo, AccountInfo)>,
        dedup_client_order_id: bool,
    ) -> Option<OrderFillDbOutcome> {
        let (current_symbol, current_mode, channels, direction, sync_interval_secs) = {
            let cfg = self.config.read().await;
            (
                cfg.exchange.symbol.clone(),
                cfg.exchange.trading_mode(),
                super::NotificationChannels::from_app(&cfg),
                cfg.grid.direction,
                cfg.exchange.sync_interval_secs,
            )
        };
        if trade.symbol != current_symbol || trade.mode != current_mode {
            error!(
                "Trade scope differs from the active strategy: {}",
                trade.trade_id
            );
            return None;
        }
        let notify_trade = channels.any_enabled()
            && !trade.is_reverse_trade()
            && (trade.pnl_verified || trade.side != direction.closing_side());
        let Ok((status, notification_stats)) = self
            .commit_trade_to_db_and_caches(
                &trade,
                cycle_profit,
                pair_intent,
                consumed_pair_source,
                paper_state,
                (notify_trade, dedup_client_order_id),
            )
            .await
        else {
            return None;
        };
        if status != OrderFillDbOutcome::Inserted {
            return Some(status);
        }
        if self.ws_broadcast_tx.receiver_count() > 0 {
            if let Some(json) = crate::types::ws_frame_json("trade", &trade) {
                let _ = self.ws_broadcast_tx.send(json);
            }
        }
        if notify_trade {
            self.spawn_trade_notifications(
                channels,
                trade,
                notification_stats,
                direction,
                sync_interval_secs,
            )
            .await;
        }
        Some(OrderFillDbOutcome::Inserted)
    }

    pub async fn verify_trade_pnl(
        self: &Arc<Self>,
        trade: TradeRecord,
        expected_clear_revision: u64,
    ) -> anyhow::Result<()> {
        self.verify_trades_pnl_batch(vec![trade], expected_clear_revision)
            .await
    }

    pub async fn verify_trades_pnl_batch(
        self: &Arc<Self>,
        trades: Vec<TradeRecord>,
        expected_clear_revision: u64,
    ) -> anyhow::Result<()> {
        if trades.is_empty() {
            return Ok(());
        }
        let commit = self.trade_commit_lock.clone().lock_owned().await;
        // Retirement closes admission even if an aborted worker is still polling.
        if self.is_retired() {
            return Ok(());
        }
        let state = self.clone();
        // Once admitted, finish publishing even if the requester stops.
        tokio::spawn(async move {
            let verified = state
                .verify_trades_pnl_batch_inner(trades, expected_clear_revision)
                .await?;
            if verified.is_empty() {
                return Ok(());
            }
            // Account capture takes the same lock; finish publication before preparing it.
            drop(commit);
            for trade in verified {
                let notification = state.closing_trade_notification_target(&trade).await;
                if let Some(target) = notification {
                    state.notify_verified_closing_trade(trade, target).await;
                }
            }
            Ok(())
        })
        .await?
    }

    async fn verify_trades_pnl_batch_inner(
        &self,
        trades: Vec<TradeRecord>,
        expected_clear_revision: u64,
    ) -> anyhow::Result<Vec<TradeRecord>> {
        let verified = self
            .db
            .run_blocking(move |db| db.verify_trades_pnl_batch(&trades, expected_clear_revision))
            .await?;
        if verified.is_empty() {
            return Ok(Vec::new());
        }
        let config = self.config.read().await;
        let current_mode = config.exchange.trading_mode();
        let current_symbol = config.exchange.symbol.clone();
        drop(config);
        if !self.stats_dirty.load(Ordering::SeqCst) {
            let mut stats = self.stats.write().await;
            for trade in &verified {
                if trade.symbol == current_symbol && trade.mode == current_mode {
                    stats.total_realized_pnl =
                        stats.total_realized_pnl.saturating_add(trade.realized_pnl);
                    stats.total_commission = stats
                        .total_commission
                        .saturating_add(trade.quote_commission());
                    if trade.has_non_quote_commission() {
                        stats.non_quote_commission_trades =
                            stats.non_quote_commission_trades.saturating_add(1);
                    }
                    stats.pending_pnl_trades = stats.pending_pnl_trades.saturating_sub(1);
                }
            }
        }
        let mut recent = self.recent_trades.write().await;
        let mut updated_symbol = None;
        for trade in &verified {
            if let Some(existing) = recent
                .iter_mut()
                .find(|item| item.trade_id == trade.trade_id)
            {
                // Verification updates PnL fields only; SQLite retains the original timestamp.
                existing.realized_pnl = trade.realized_pnl;
                existing.commission = trade.commission;
                existing.commission_asset = trade.commission_asset.clone();
                existing.is_maker = trade.is_maker;
                existing.pnl_verified = true;
                updated_symbol = Some(trade.symbol.as_str());
            }
        }
        if let Some(symbol) = updated_symbol {
            self.recent_trades_revision
                .store(self.db.symbol_trade_revision(symbol), Ordering::Release);
        }
        Ok(verified)
    }

    pub(crate) async fn drain_trade_commits(&self) {
        let _commit = self.trade_commit_lock.lock().await;
    }
}
