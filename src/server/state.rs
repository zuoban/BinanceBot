use crate::config::AppConfig;
use crate::db::Database;
use crate::fx::CnyRateCache;
use crate::strategy::precision::SymbolRules;
use crate::telegram::{
    StrategyEvent, StrategyNotification, TelegramNotifier, TradeNotificationContext,
};
use crate::types::*;
use anyhow::Context;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Mutex, OwnedRwLockReadGuard, RwLock};
use tracing::{error, warn};

#[path = "snapshot.rs"]
mod snapshot;

/// Coordinates the filled-position limit across all currently configured strategies.
pub(crate) struct GlobalPositionLimit {
    pub(crate) states: Arc<RwLock<Vec<Arc<AppState>>>>,
    lock: Mutex<()>,
    committed_config: std::sync::RwLock<Option<crate::config::GlobalConfig>>,
    triggered: AtomicBool,
    blocked: AtomicBool,
    pub(crate) enabled: AtomicBool,
    pub(crate) membership_ready: AtomicBool,
}

impl GlobalPositionLimit {
    pub(crate) fn new(states: Arc<RwLock<Vec<Arc<AppState>>>>) -> Arc<Self> {
        Arc::new(Self {
            states,
            lock: Mutex::new(()),
            committed_config: std::sync::RwLock::new(None),
            triggered: AtomicBool::new(false),
            blocked: AtomicBool::new(false),
            enabled: AtomicBool::new(false),
            membership_ready: AtomicBool::new(false),
        })
    }

    async fn check_locked(&self) -> bool {
        let states = self.states.read().await.clone();
        let Some(first) = states.first() else {
            return self.enabled.load(Ordering::SeqCst)
                && !self.membership_ready.load(Ordering::SeqCst);
        };
        let committed = self.committed_config.read().unwrap().clone();
        let mut config = first.config.read().await.clone();
        if let Some(ref committed) = committed {
            committed.apply_to(&mut config);
        }
        let limit = config
            .global_max_position_notional
            .filter(|v| *v > Decimal::ZERO);
        let margin_guard = config.pause_on_zero_available_margin;
        if limit.is_none() && !margin_guard {
            self.enabled.store(false, Ordering::SeqCst);
            self.blocked.store(false, Ordering::SeqCst);
            self.triggered.store(false, Ordering::SeqCst);
            for state in &states {
                state.queue_global_cancel();
            }
            return false;
        }
        self.enabled.store(true, Ordering::SeqCst);
        let mut total = Decimal::ZERO;
        let mut incomplete = !self.membership_ready.load(Ordering::SeqCst);
        let mut initialized_incomplete = false;
        let mut zero_margin = false;
        for state in &states {
            let initialized = state.global_position_initialized.load(Ordering::SeqCst);
            let mut member_incomplete =
                !initialized || !state.global_position_ready.load(Ordering::SeqCst);
            let mut config = state.config.read().await.clone();
            if let Some(ref committed) = committed {
                committed.apply_to(&mut config);
            }
            if margin_guard {
                let account = state.account.read().await.clone();
                let max_age = config
                    .exchange
                    .sync_interval_secs
                    .saturating_mul(3)
                    .max(30)
                    .min(i64::MAX as u64) as i64;
                let age = Utc::now().signed_duration_since(account.update_time);
                if account.asset.is_empty()
                    || (!config.exchange.dry_run
                        && (age < chrono::Duration::zero()
                            || age > chrono::Duration::seconds(max_age)))
                {
                    member_incomplete = true;
                } else if account.available_balance <= Decimal::ZERO {
                    zero_margin = true;
                }
            }
            if limit.is_none() {
                incomplete |= member_incomplete;
                initialized_incomplete |= initialized && member_incomplete;
                continue;
            }
            let position = state.position.read().await.clone();
            if !config.exchange.dry_run {
                let max_age = config
                    .exchange
                    .sync_interval_secs
                    .saturating_mul(3)
                    .max(30)
                    .min(i64::MAX as u64) as i64;
                let fresh = position.update_time.is_some_and(|time| {
                    let age = Utc::now().signed_duration_since(time);
                    age >= chrono::Duration::zero() && age <= chrono::Duration::seconds(max_age)
                });
                if !fresh {
                    member_incomplete = true;
                }
            }
            let ticker = state.ticker.read().await.clone();
            let max_age = config
                .exchange
                .sync_interval_secs
                .saturating_mul(3)
                .max(30)
                .min(i64::MAX as u64) as i64;
            let fresh = |time| {
                let age = Utc::now().signed_duration_since(time);
                age >= chrono::Duration::zero() && age <= chrono::Duration::seconds(max_age)
            };
            let price = if ticker.mark_price > Decimal::ZERO && fresh(ticker.mark_update_time) {
                ticker.mark_price
            } else if position.mark_price > Decimal::ZERO && position.update_time.is_some_and(fresh)
            {
                position.mark_price
            } else if ticker.last_price > Decimal::ZERO && fresh(ticker.update_time) {
                ticker.last_price
            } else {
                Decimal::ZERO
            };
            if !position.size.is_zero() && price <= Decimal::ZERO {
                member_incomplete = true;
            }
            if price > Decimal::ZERO {
                match position
                    .size
                    .abs()
                    .checked_mul(price)
                    .and_then(|v| total.checked_add(v))
                {
                    Some(value) => total = value,
                    None => member_incomplete = true,
                }
            }
            incomplete |= member_incomplete;
            initialized_incomplete |= initialized && member_incomplete;
        }
        let position_limit_reached = limit.is_some_and(|limit| total >= limit);
        let unsafe_now = initialized_incomplete || position_limit_reached || zero_margin;
        self.blocked.store(
            incomplete || position_limit_reached || zero_margin,
            Ordering::SeqCst,
        );
        let first_trigger = unsafe_now && !self.triggered.swap(true, Ordering::SeqCst);
        if !unsafe_now {
            self.triggered.store(false, Ordering::SeqCst);
        }
        if first_trigger {
            for state in &states {
                state.global_cancel_pending.store(true, Ordering::SeqCst);
            }
        }
        for state in &states {
            if unsafe_now && (first_trigger || *state.status.read().await == BotStatus::Running) {
                state.global_cancel_pending.store(true, Ordering::SeqCst);
                let reason = if zero_margin {
                    "可用保证金已达到零，需手动恢复".to_string()
                } else if incomplete {
                    "全局风控数据不可用或已过期，需手动恢复".to_string()
                } else {
                    format!(
                        "全局持仓名义价值 {} 已达到上限 {}，需手动恢复",
                        total,
                        limit.expect("position limit must exist")
                    )
                };
                if let Err(error) = Box::pin(state.pause_trading_with_reason(&reason)).await {
                    state
                        .add_log("ERROR", format!("全局风控暂停状态保存失败: {}", error))
                        .await;
                }
                state.add_log("WARN", reason).await;
            }
            state.queue_global_cancel();
        }
        incomplete || position_limit_reached || zero_margin
    }

    pub(crate) fn initialize_config(&self, config: crate::config::GlobalConfig) {
        self.enabled.store(
            config
                .global_max_position_notional
                .is_some_and(|v| v > Decimal::ZERO)
                || config.pause_on_zero_available_margin,
            Ordering::SeqCst,
        );
        *self.committed_config.write().unwrap() = Some(config);
    }

    pub(crate) async fn commit_config(&self, config: crate::config::GlobalConfig) {
        let _guard = self.lock.lock().await;
        self.initialize_config(config);
        self.check_locked().await;
    }

    pub(crate) async fn check(&self) -> bool {
        let _guard = self.lock.lock().await;
        self.check_locked().await
    }
}

const RECENT_TRADE_LIMIT: usize = 200;

// Oldest first; trades with equal timestamps retain their database insertion order.
fn cache_recent_trade(trades: &mut VecDeque<TradeRecord>, trade: TradeRecord) {
    let index = trades.partition_point(|existing| existing.timestamp <= trade.timestamp);
    trades.insert(index, trade);
    if trades.len() > RECENT_TRADE_LIMIT {
        trades.pop_front();
    }
}

fn recent_trade_cache(trades: Vec<TradeRecord>) -> VecDeque<TradeRecord> {
    let mut cache = VecDeque::with_capacity(RECENT_TRADE_LIMIT);
    // The database returns newest first, with later rowids first for equal timestamps.
    for trade in trades.into_iter().rev() {
        cache_recent_trade(&mut cache, trade);
    }
    cache
}

pub struct AppState {
    pub db: Arc<Database>,
    pub config: RwLock<AppConfig>,
    pub rules: RwLock<SymbolRules>,
    pub status: RwLock<BotStatus>,
    pub ticker: RwLock<TickerInfo>,
    pub stats: RwLock<GridStats>,
    pub position: RwLock<PositionInfo>,
    pub account: RwLock<AccountInfo>,
    pub cny_rates: Arc<CnyRateCache>,
    pub active_orders: RwLock<HashMap<String, GridOrder>>,
    pub recent_trades: RwLock<VecDeque<TradeRecord>>,
    pub recent_logs: RwLock<VecDeque<LogEntry>>,
    pub(crate) global_position_limit: std::sync::OnceLock<std::sync::Weak<GlobalPositionLimit>>,
    pub(crate) global_position_ready: AtomicBool,
    pub(crate) global_position_initialized: AtomicBool,
    pub(crate) global_cancel_pending: AtomicBool,
    pub(crate) global_cancel_queued: AtomicBool,
    pause_epoch: AtomicU64,
    shutdown_requested: AtomicBool,
    // Tracks the last non-maintenance status so internal cancellation does not send alerts.
    status_update_lock: Mutex<BotStatus>,
    trade_commit_lock: Arc<Mutex<()>>,
    /// A global configuration transition holds the writer until commit or rollback completes.
    pub(crate) order_submission_lock: Arc<RwLock<()>>,
    pair_scope: Option<String>,
    pub(crate) snapshot_scope_lock: RwLock<()>,
    snapshot_publish_lock: Mutex<()>,
    snapshot_cache: snapshot::SnapshotCache,
    login_failures: Mutex<VecDeque<Instant>>,
    setup_code: Option<String>,

    pub action_tx: mpsc::Sender<BotControlAction>,
    pub ws_broadcast_tx: broadcast::Sender<String>,
    telegram_notifier: TelegramNotifier,
}

impl AppState {
    pub fn new(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
    ) -> Arc<Self> {
        Self::new_with_rates(config, db, action_tx, Arc::new(CnyRateCache::new()))
    }

    pub(crate) fn new_with_rates(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        cny_rates: Arc<CnyRateCache>,
    ) -> Arc<Self> {
        Self::build(
            config,
            db,
            action_tx,
            None,
            Arc::new(Mutex::new(())),
            cny_rates,
        )
    }

    pub fn new_for_pair(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        trade_commit_lock: Arc<Mutex<()>>,
    ) -> Arc<Self> {
        Self::new_for_pair_with_rates(
            config,
            db,
            action_tx,
            trade_commit_lock,
            Arc::new(CnyRateCache::new()),
        )
    }

    pub(crate) fn new_for_pair_with_rates(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        trade_commit_lock: Arc<Mutex<()>>,
        cny_rates: Arc<CnyRateCache>,
    ) -> Arc<Self> {
        let symbol = config.exchange.symbol.clone();
        Self::build(
            config,
            db,
            action_tx,
            Some(symbol),
            trade_commit_lock,
            cny_rates,
        )
    }

    fn build(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        pair_scope: Option<String>,
        trade_commit_lock: Arc<Mutex<()>>,
        cny_rates: Arc<CnyRateCache>,
    ) -> Arc<Self> {
        let (ws_broadcast_tx, _) = broadcast::channel(100);
        let symbol = config.exchange.symbol.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let saved_status = db.load_pair_status(&symbol, mode).and_then(|status| {
            if status.is_some() || pair_scope.is_some() {
                Ok(status)
            } else {
                db.load_bot_status()
            }
        });
        let initial_status = match saved_status {
            Ok(Some(status)) => status,
            Ok(None) if config.exchange.dry_run => BotStatus::Running,
            Ok(None) => BotStatus::Paused,
            Err(error) => {
                warn!(
                    "Could not load saved bot status; starting paused: {}",
                    error
                );
                BotStatus::Paused
            }
        };
        let setup_code = if db.is_admin_password_set().unwrap_or(false) {
            None
        } else {
            Some(crate::auth::generate_setup_code())
        };

        let initial_account = if config.exchange.dry_run {
            AccountInfo {
                asset: quote_asset(&symbol).unwrap_or(&symbol).to_string(),
                total_wallet_balance: rust_decimal_macros::dec!(10000.0),
                available_balance: rust_decimal_macros::dec!(10000.0),
                margin_balance: rust_decimal_macros::dec!(10000.0),
                unrealized_profit: Decimal::ZERO,
                update_time: Utc::now(),
            }
        } else {
            AccountInfo::default()
        };

        // Preload recent trades from SQLite database
        let saved_trades = db
            .get_recent_trades_for(&symbol, mode, RECENT_TRADE_LIMIT)
            .unwrap_or_default();
        let recent_trades = recent_trade_cache(saved_trades);

        // Restore cumulative grid stats from SQLite database if available
        let mut initial_stats = GridStats::default();
        if let Ok(Some((total_trades, cycles, profit, volume))) =
            db.load_scoped_stats(&symbol, mode)
        {
            initial_stats.total_trades = total_trades;
            initial_stats.completed_cycles = cycles;
            initial_stats.total_realized_profit = profit;
            initial_stats.total_volume_usdc = volume;
        }
        match db.get_pnl_totals(&symbol, mode) {
            Ok((pnl, commission, pending)) => {
                initial_stats.total_realized_pnl = pnl;
                initial_stats.total_commission = commission;
                initial_stats.pending_pnl_trades = pending;
            }
            Err(e) => warn!("Failed to load realized PnL totals: {}", e),
        }

        Arc::new(Self {
            db,
            config: RwLock::new(config),
            rules: RwLock::new(SymbolRules::default()),
            status: RwLock::new(initial_status),
            ticker: RwLock::new(TickerInfo {
                symbol,
                ..Default::default()
            }),
            stats: RwLock::new(initial_stats),
            position: RwLock::new(PositionInfo::default()),
            account: RwLock::new(initial_account),
            cny_rates,
            active_orders: RwLock::new(HashMap::new()),
            recent_trades: RwLock::new(recent_trades),
            recent_logs: RwLock::new(VecDeque::with_capacity(300)),
            global_position_limit: std::sync::OnceLock::new(),
            global_position_ready: AtomicBool::new(false),
            global_position_initialized: AtomicBool::new(false),
            global_cancel_pending: AtomicBool::new(false),
            global_cancel_queued: AtomicBool::new(false),
            pause_epoch: AtomicU64::new(0),
            shutdown_requested: AtomicBool::new(false),
            status_update_lock: Mutex::new(initial_status),
            trade_commit_lock,
            order_submission_lock: Arc::new(RwLock::new(())),
            pair_scope,
            snapshot_scope_lock: RwLock::new(()),
            snapshot_publish_lock: Mutex::new(()),
            snapshot_cache: snapshot::SnapshotCache::default(),
            login_failures: Mutex::new(VecDeque::new()),
            setup_code,
            action_tx,
            ws_broadcast_tx,
            telegram_notifier: TelegramNotifier::new(
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .expect("Failed to build Telegram HTTP client"),
            ),
        })
    }

    pub async fn record_trade(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
    ) -> bool {
        // Persist the trade and its cumulative statistics together.
        let current_config = self.config.read().await;
        let current_symbol = current_config.exchange.symbol.clone();
        let current_mode = TradingMode::from_exchange(
            current_config.exchange.dry_run,
            current_config.exchange.is_testnet,
        );
        let telegram = current_config.telegram.clone();
        let direction = current_config.grid.direction;
        drop(current_config);
        if trade.symbol != current_symbol || trade.mode != current_mode {
            error!(
                "Trade scope differs from the active strategy: {}",
                trade.trade_id
            );
            return false;
        }
        let mut notification_stats = None;
        {
            let _commit = self.trade_commit_lock.lock().await;
            let mut updated = self.stats.read().await.clone();
            updated.total_trades += 1;
            updated.total_volume_usdc += trade.amount_usdc;
            if let Some(profit) = cycle_profit {
                updated.completed_cycles += 1;
                updated.total_realized_profit += profit;
            }
            if trade.pnl_verified {
                updated.total_realized_pnl += trade.realized_pnl;
                updated.total_commission += trade.commission;
            } else {
                updated.pending_pnl_trades += 1;
            }
            let stored_trade = trade.clone();
            let stored_stats = updated.clone();
            match self
                .db
                .run_blocking(move |db| {
                    db.insert_trade_with_recovery(
                        &stored_trade,
                        &stored_stats,
                        pair_intent.as_ref(),
                        consumed_pair_source.as_deref(),
                    )
                })
                .await
            {
                Ok(true) => *self.stats.write().await = updated,
                Ok(false) => return true,
                Err(e) => {
                    error!(
                        "Failed to persist trade and grid stats to SQLite database: {}",
                        e
                    );
                    return false;
                }
            }
            if telegram.enabled {
                let symbol = trade.symbol.clone();
                let mode = trade.mode;
                let timestamp = trade.timestamp;
                // Capture counts while commits are serialized, before another fill can advance them.
                match self
                    .db
                    .run_blocking(move |db| db.get_hourly_trade_stats(&symbol, mode, timestamp))
                    .await
                {
                    Ok(stats) => notification_stats = Some(stats),
                    Err(error) => warn!("Could not load Telegram trade counts: {}", error),
                }
            }

            // Keep the cache update in the same critical section as persistence
            // so cleanup cannot be followed by an old trade reappearing here.
            let mut trades = self.recent_trades.write().await;
            cache_recent_trade(&mut trades, trade.clone());
        }

        // Broadcast trade update event to WebSocket clients
        if let Ok(json) = serde_json::to_string(&serde_json::json!({
            "type": "trade",
            "data": trade
        })) {
            let _ = self.ws_broadcast_tx.send(json);
        }

        if telegram.enabled {
            let account = self.account.read().await.clone();
            let position = self.position.read().await.clone();
            let ticker = self.ticker.read().await.clone();
            let cny_rate = self.cny_rates.get(&account.asset).await;
            let context = TradeNotificationContext::for_trade(
                &trade,
                notification_stats.as_ref(),
                &account,
                position.unrealized_pnl,
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
        true
    }

    pub async fn verify_trade_pnl(&self, trade: TradeRecord) -> anyhow::Result<()> {
        let _commit = self.trade_commit_lock.lock().await;
        let stored_trade = trade.clone();
        if !self
            .db
            .run_blocking(move |db| db.verify_trade_pnl(&stored_trade))
            .await?
        {
            return Ok(());
        }
        let config = self.config.read().await;
        let current_mode =
            TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let current_symbol = config.exchange.symbol.clone();
        drop(config);
        if trade.symbol == current_symbol && trade.mode == current_mode {
            let mut stats = self.stats.write().await;
            stats.total_realized_pnl += trade.realized_pnl;
            stats.total_commission += trade.commission;
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
        Ok(())
    }

    pub async fn refresh_current_scope(&self) {
        let _commit = self.trade_commit_lock.lock().await;
        let config = self.config.read().await;
        let symbol = config.exchange.symbol.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
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
        let (saved_stats, (pnl, commission, pending), trades) = match result {
            Ok(result) => result,
            Err(e) => {
                error!("Failed to load scoped statistics: {}", e);
                return;
            }
        };
        let mut updated = GridStats::default();
        if let Some((trades, cycles, profit, volume)) = saved_stats {
            updated.total_trades = trades;
            updated.completed_cycles = cycles;
            updated.total_realized_profit = profit;
            updated.total_volume_usdc = volume;
        }
        updated.total_realized_pnl = pnl;
        updated.total_commission = commission;
        updated.pending_pnl_trades = pending;
        *self.stats.write().await = updated;
        *self.recent_trades.write().await = recent_trade_cache(trades);
    }

    pub async fn clear_trade_history(&self) -> anyhow::Result<usize> {
        let _commit = self.trade_commit_lock.lock().await;
        let deleted = self.db.run_blocking(Database::clear_trade_history).await?;
        self.reset_trade_history_cache().await;
        Ok(deleted)
    }

    pub async fn clear_trade_history_for(states: &[Arc<Self>]) -> anyhow::Result<usize> {
        let first = states.first().context("没有运行中的交易对")?;
        let _commit = first.trade_commit_lock.lock().await;
        let deleted = first.db.run_blocking(Database::clear_trade_history).await?;
        for state in states {
            state.reset_trade_history_cache().await;
        }
        Ok(deleted)
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

    pub fn pair_scope(&self) -> Option<&str> {
        self.pair_scope.as_deref()
    }

    pub async fn save_runtime_config(&self, config: AppConfig) -> anyhow::Result<()> {
        // An immediate HTTP pause cannot slip between saving a new mode and installing it.
        let _update = self.status_update_lock.lock().await;
        let status = *self.status.read().await;
        let scope = self.pair_scope.clone();
        let saved = config.clone();
        self.db
            .run_blocking(move |db| db.save_runtime_config(&saved, scope.as_deref(), status))
            .await?;
        *self.config.write().await = config;
        Ok(())
    }

    async fn persist_status(&self, status: BotStatus) -> anyhow::Result<()> {
        let config = self.config.read().await;
        let symbol = config.exchange.symbol.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let legacy = self.pair_scope.is_none();
        drop(config);
        self.db
            .run_blocking(move |db| db.save_runtime_status(&symbol, mode, status, legacy))
            .await
    }

    pub async fn add_log(&self, level: &str, message: impl Into<String>) {
        let entry = LogEntry {
            timestamp: Utc::now(),
            level: level.to_string(),
            message: message.into(),
        };

        let mut logs = self.recent_logs.write().await;
        if logs.len() >= 250 {
            logs.pop_front();
        }
        logs.push_back(entry.clone());

        // Also broadcast log event to WebSocket clients
        if let Ok(json) = serde_json::to_string(&serde_json::json!({
            "type": "log",
            "data": entry
        })) {
            let _ = self.ws_broadcast_tx.send(json);
        }
    }

    pub async fn run_cny_rate_refresh(self: Arc<Self>) {
        let mut timer = tokio::time::interval(Duration::from_secs(10));
        loop {
            timer.tick().await;
            let asset = self.account.read().await.asset.clone();
            if let Err(err) = self.cny_rates.refresh_if_due(&asset).await {
                warn!("Could not refresh {}/CNY exchange rate: {}", asset, err);
            }
        }
    }
}

impl AppState {
    pub async fn is_token_valid(&self, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        let token = token.to_string();
        self.db
            .run_blocking(move |db| db.is_session_valid(&token))
            .await
            .unwrap_or(false)
    }

    pub async fn create_session(&self, token: &str) -> anyhow::Result<()> {
        let token = token.to_string();
        self.db
            .run_blocking(move |db| db.create_session(&token))
            .await
    }

    pub async fn delete_session(&self, token: &str) -> anyhow::Result<()> {
        let token = token.to_string();
        self.db
            .run_blocking(move |db| db.delete_session(&token))
            .await
    }

    pub async fn clear_all_sessions(&self) -> anyhow::Result<()> {
        self.db.run_blocking(Database::clear_all_sessions).await
    }

    pub async fn begin_login_attempt(&self) -> bool {
        let mut failures = self.login_failures.lock().await;
        failures.retain(|time| time.elapsed() < Duration::from_secs(60));
        if failures.len() >= 10 {
            return false;
        }
        failures.push_back(Instant::now());
        true
    }

    pub async fn clear_login_attempts(&self) {
        self.login_failures.lock().await.clear();
    }

    pub async fn pause_trading(self: &Arc<Self>) -> anyhow::Result<()> {
        self.pause_trading_with_reason("策略运行异常，已暂停，请查看系统运行日志")
            .await
    }

    pub async fn pause_trading_with_reason(self: &Arc<Self>, reason: &str) -> anyhow::Result<()> {
        self.set_trading_status(BotStatus::Paused, Some(reason))
            .await
    }

    pub async fn resume_trading(self: &Arc<Self>) -> anyhow::Result<()> {
        self.set_trading_status(BotStatus::Running, Some("手动恢复策略"))
            .await
    }

    pub(crate) async fn pause_trading_silently(self: &Arc<Self>) -> anyhow::Result<()> {
        self.set_trading_status(BotStatus::Paused, None).await
    }

    pub(crate) async fn resume_trading_silently(self: &Arc<Self>) -> anyhow::Result<()> {
        self.set_trading_status(BotStatus::Running, None).await
    }

    async fn set_trading_status(
        self: &Arc<Self>,
        status: BotStatus,
        reason: Option<&str>,
    ) -> anyhow::Result<()> {
        let global = if status == BotStatus::Running {
            self.global_position_limit
                .get()
                .and_then(std::sync::Weak::upgrade)
        } else {
            None
        };
        let _global_guard = if let Some(ref coordinator) = global {
            let guard = coordinator.lock.lock().await;
            anyhow::ensure!(
                !coordinator.check_locked().await
                    && !self.global_cancel_pending.load(Ordering::SeqCst),
                "全局持仓限制已达到或持仓数据尚未就绪，无法恢复策略"
            );
            Some(guard)
        } else {
            None
        };
        if status == BotStatus::Paused {
            self.pause_epoch.fetch_add(1, Ordering::SeqCst);
        }
        let mut notification_status = self.status_update_lock.lock().await;
        anyhow::ensure!(
            status != BotStatus::Running || !self.is_shutting_down(),
            "程序正在退出，不能恢复策略"
        );
        let saved = if status == BotStatus::Paused {
            // An unsafe strategy must stop even if persistence fails.
            *self.status.write().await = status;
            self.persist_status(status).await
        } else {
            self.persist_status(status).await?;
            *self.status.write().await = status;
            Ok(())
        };
        if let Some(reason) = reason {
            if *notification_status != status {
                *notification_status = status;
                let event = if status == BotStatus::Paused {
                    StrategyEvent::Paused
                } else {
                    StrategyEvent::Resumed
                };
                self.notify_strategy_event(event, reason).await;
            }
        }
        saved
    }

    pub async fn notify_strategy_started(self: &Arc<Self>) {
        let _update = self.status_update_lock.lock().await;
        if !self.is_shutting_down() && *self.status.read().await == BotStatus::Running {
            self.notify_strategy_event(StrategyEvent::Started, "程序启动后开始运行策略")
                .await;
        }
    }

    async fn notify_strategy_event(self: &Arc<Self>, event: StrategyEvent, reason: &str) {
        let config = self.config.read().await;
        let telegram = config.telegram.clone();
        if !telegram.enabled {
            return;
        }
        let notification = StrategyNotification {
            event,
            symbol: config.exchange.symbol.clone(),
            mode: TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet),
            reason: reason.to_string(),
            timestamp: Utc::now(),
        };
        drop(config);
        let notifier = self.telegram_notifier.clone();
        let state = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = notifier
                .send_strategy_notification(&telegram, &notification)
                .await
            {
                warn!("Failed to send Telegram strategy notification: {}", err);
                state
                    .add_log("WARN", format!("Telegram 策略启停通知发送失败: {}", err))
                    .await;
            }
        });
    }

    pub fn pause_epoch(&self) -> u64 {
        self.pause_epoch.load(Ordering::SeqCst)
    }

    fn queue_global_cancel(self: &Arc<Self>) {
        if !self.global_cancel_pending.load(Ordering::SeqCst)
            || self.global_cancel_queued.swap(true, Ordering::SeqCst)
        {
            return;
        }
        let state = self.clone();
        tokio::spawn(async move {
            if state
                .action_tx
                .send(BotControlAction::GlobalRiskCancel)
                .await
                .is_err()
            {
                state.global_cancel_queued.store(false, Ordering::SeqCst);
            }
        });
    }

    pub(crate) fn try_order_submission(&self) -> Option<OwnedRwLockReadGuard<()>> {
        // Skip replenishment instead of waiting: the engine must stay available for
        // configuration actions and cancellation while the coordinator holds the writer.
        if self.global_cancel_pending.load(Ordering::SeqCst) {
            return None;
        }
        if self
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|global| {
                global.enabled.load(Ordering::SeqCst)
                    && (global.blocked.load(Ordering::SeqCst)
                        || !global.membership_ready.load(Ordering::SeqCst))
            })
        {
            return None;
        }
        self.order_submission_lock.clone().try_read_owned().ok()
    }

    pub fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.pause_epoch.fetch_add(1, Ordering::SeqCst);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    pub fn setup_code(&self) -> Option<&str> {
        self.setup_code.as_deref()
    }

    pub fn verify_setup_code(&self, code: &str) -> bool {
        self.setup_code
            .as_deref()
            .is_some_and(|expected| expected == code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn legacy_and_multi_pair_formats_preserve_pause_and_manual_resume() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let mut config = AppConfig::default();
        config.exchange.dry_run = false;
        config.exchange.api_key = "test-key".into();
        config.exchange.api_secret = "test-secret".into();
        config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
        db.save_config(&config).unwrap();
        db.save_bot_status(BotStatus::Running).unwrap();
        let strategy = config.strategy_configs().unwrap().remove(0);
        let (tx, _) = mpsc::channel(8);
        let multi =
            AppState::new_for_pair(strategy.clone(), db.clone(), tx, Arc::new(Mutex::new(())));
        multi.pause_trading().await.unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));

        db.save_config(&strategy).unwrap();
        let (tx, _) = mpsc::channel(8);
        let legacy = AppState::new(strategy.clone(), db.clone(), tx);
        assert_eq!(*legacy.status.read().await, BotStatus::Paused);
        legacy.resume_trading().await.unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Running)
        );

        db.save_config(&config).unwrap();
        let (tx, _) = mpsc::channel(8);
        let multi = AppState::new_for_pair(strategy, db.clone(), tx, Arc::new(Mutex::new(())));
        assert_eq!(*multi.status.read().await, BotStatus::Running);
        multi.pause_trading().await.unwrap();
        assert_eq!(*multi.status.read().await, BotStatus::Paused);
    }

    #[tokio::test]
    async fn a_pause_racing_a_mode_commit_is_persisted_in_the_new_mode() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let config = AppConfig {
            pairs: vec![crate::config::TradingPairConfig::new("SOLUSDC".into())],
            ..Default::default()
        };
        db.save_config(&config).unwrap();
        db.save_pair_status("SOLUSDC", TradingMode::Live, BotStatus::Running)
            .unwrap();
        let (tx, _) = mpsc::channel(8);
        let state = AppState::new_for_pair(
            config.strategy_configs().unwrap().remove(0),
            db.clone(),
            tx,
            Arc::new(Mutex::new(())),
        );
        let mut live = state.config.read().await.clone();
        live.exchange.dry_run = false;
        let (committed, paused) =
            tokio::join!(state.save_runtime_config(live), state.pause_trading());
        committed.unwrap();
        paused.unwrap();
        assert!(!state.config.read().await.exchange.dry_run);
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Paused)
        );
    }

    struct NotificationFixture {
        state: Arc<AppState>,
        requests: mpsc::UnboundedReceiver<serde_json::Value>,
        actions: mpsc::Receiver<BotControlAction>,
        url: String,
        server: tokio::task::JoinHandle<()>,
    }

    #[tokio::test]
    async fn global_position_limit_aggregates_and_blocks_resume_until_below_limit() {
        use rust_decimal_macros::dec;
        let first = notification_fixture(false, false).await;
        let second = notification_fixture(false, false).await;
        first
            .state
            .config
            .write()
            .await
            .global_max_position_notional = Some(dec!(100));
        second
            .state
            .config
            .write()
            .await
            .global_max_position_notional = Some(dec!(100));
        first.state.position.write().await.size = dec!(-2);
        first.state.position.write().await.mark_price = dec!(20);
        first.state.ticker.write().await.mark_price = dec!(30);
        first.state.ticker.write().await.mark_update_time = Utc::now();
        second.state.position.write().await.size = dec!(2);
        second.state.ticker.write().await.last_price = dec!(20);
        second.state.ticker.write().await.update_time = Utc::now();
        let states = Arc::new(RwLock::new(vec![first.state.clone(), second.state.clone()]));
        let global = GlobalPositionLimit::new(states);
        global.membership_ready.store(true, Ordering::SeqCst);
        for state in [&first.state, &second.state] {
            state
                .global_position_initialized
                .store(true, Ordering::SeqCst);
            state.global_position_ready.store(true, Ordering::SeqCst);
            let _ = state.global_position_limit.set(Arc::downgrade(&global));
        }
        assert!(global.check().await);
        assert_eq!(*first.state.status.read().await, BotStatus::Paused);
        assert_eq!(*second.state.status.read().await, BotStatus::Paused);
        assert!(first.state.resume_trading().await.is_err());
        second.state.position.write().await.size = Decimal::ZERO;
        assert!(!global.check().await);
        assert!(first.state.resume_trading().await.is_err());
        first
            .state
            .global_cancel_pending
            .store(false, Ordering::SeqCst);
        first.state.resume_trading().await.unwrap();
    }

    #[tokio::test]
    async fn global_position_limit_blocks_until_data_is_ready() {
        let fixture = notification_fixture(false, false).await;
        fixture
            .state
            .config
            .write()
            .await
            .global_max_position_notional = Some(Decimal::ONE);
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
        global.membership_ready.store(true, Ordering::SeqCst);
        let _ = fixture
            .state
            .global_position_limit
            .set(Arc::downgrade(&global));
        assert!(global.check().await);
        assert!(fixture.state.resume_trading().await.is_err());
        fixture
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        fixture.state.position.write().await.size = Decimal::ONE;
        assert!(global.check().await);
        fixture.state.position.write().await.mark_price = Decimal::ONE;
        fixture.state.position.write().await.update_time = Some(Utc::now());
        assert!(global.check().await);
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    }

    #[tokio::test]
    async fn initialized_member_failure_pauses_even_while_another_initializes() {
        let mut first = notification_fixture(false, false).await;
        let second = notification_fixture(false, false).await;
        first
            .state
            .config
            .write()
            .await
            .global_max_position_notional = Some(Decimal::ONE);
        first
            .state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        first
            .state
            .global_position_ready
            .store(false, Ordering::SeqCst);
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
            first.state.clone(),
            second.state.clone(),
        ])));
        global.membership_ready.store(true, Ordering::SeqCst);
        assert!(global.check().await);
        assert_eq!(*first.state.status.read().await, BotStatus::Paused);
        assert_eq!(*second.state.status.read().await, BotStatus::Paused);
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), first.actions.recv())
                .await
                .unwrap(),
            Some(BotControlAction::GlobalRiskCancel)
        ));
    }

    #[tokio::test]
    async fn risk_checks_use_committed_config_during_propagation() {
        let fixture = notification_fixture(false, false).await;
        let old = crate::config::GlobalConfig::from_app(&*fixture.state.config.read().await);
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
        global.initialize_config(old);
        global.membership_ready.store(true, Ordering::SeqCst);
        fixture
            .state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        fixture
            .state
            .config
            .write()
            .await
            .pause_on_zero_available_margin = true;
        fixture.state.account.write().await.available_balance = Decimal::ZERO;
        assert!(!global.check().await);
        assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
        let new = crate::config::GlobalConfig::from_app(&*fixture.state.config.read().await);
        global.commit_config(new).await;
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    }

    #[tokio::test]
    async fn zero_margin_guard_blocks_failed_initialization_until_full_reconciliation() {
        let fixture = notification_fixture(false, false).await;
        fixture
            .state
            .config
            .write()
            .await
            .pause_on_zero_available_margin = true;
        fixture
            .state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
        global.membership_ready.store(true, Ordering::SeqCst);
        assert!(global.check().await);
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
        fixture
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        assert!(!global.check().await);
    }

    #[tokio::test]
    async fn zero_margin_pauses_all_and_requires_confirmed_cancellation() {
        use rust_decimal_macros::dec;
        let first = notification_fixture(false, false).await;
        let mut second = notification_fixture(false, false).await;
        for state in [&first.state, &second.state] {
            state.config.write().await.pause_on_zero_available_margin = true;
            state
                .global_position_initialized
                .store(true, Ordering::SeqCst);
            state.global_position_ready.store(true, Ordering::SeqCst);
        }
        second.state.account.write().await.available_balance = Decimal::ZERO;
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
            first.state.clone(),
            second.state.clone(),
        ])));
        global.membership_ready.store(true, Ordering::SeqCst);
        for state in [&first.state, &second.state] {
            let _ = state.global_position_limit.set(Arc::downgrade(&global));
        }
        assert!(global.check().await);
        assert_eq!(*first.state.status.read().await, BotStatus::Paused);
        assert_eq!(*second.state.status.read().await, BotStatus::Paused);
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), second.actions.recv())
                .await
                .unwrap(),
            Some(BotControlAction::GlobalRiskCancel)
        ));
        second.state.account.write().await.available_balance = dec!(1);
        assert!(second.state.resume_trading().await.is_err());
        second
            .state
            .global_cancel_pending
            .store(false, Ordering::SeqCst);
        second.state.resume_trading().await.unwrap();
    }

    #[tokio::test]
    async fn zero_margin_guard_ignores_uninitialized_default_balance() {
        let fixture = notification_fixture(false, false).await;
        fixture
            .state
            .config
            .write()
            .await
            .pause_on_zero_available_margin = true;
        fixture.state.config.write().await.exchange.dry_run = false;
        *fixture.state.account.write().await = AccountInfo::default();
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
        global.membership_ready.store(true, Ordering::SeqCst);
        assert!(global.check().await);
        assert!(!fixture.state.global_cancel_pending.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn zero_margin_guard_requires_enabled_switch_and_detects_negative_balance() {
        let fixture = notification_fixture(false, false).await;
        fixture
            .state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        fixture.state.account.write().await.available_balance = -Decimal::ONE;
        fixture
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
        global.membership_ready.store(true, Ordering::SeqCst);
        assert!(!global.check().await);
        assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
        fixture
            .state
            .config
            .write()
            .await
            .pause_on_zero_available_margin = true;
        assert!(global.check().await);
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    }

    async fn notification_fixture(enabled: bool, reject: bool) -> NotificationFixture {
        use axum::{routing::post, Json, Router};
        let (request_tx, requests) = mpsc::unbounded_channel();
        let app = Router::new().route(
            "/botTEST/sendMessage",
            post(move |Json(payload): Json<serde_json::Value>| {
                let tx = request_tx.clone();
                async move {
                    tx.send(payload).unwrap();
                    Json(serde_json::json!({"ok": !reject, "description": "test rejection"}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut config = AppConfig::default();
        config.telegram = crate::config::TelegramConfig {
            enabled,
            bot_token: "TEST".into(),
            chat_id: "12345".into(),
        };
        let (action_tx, actions) = mpsc::channel(8);
        let mut state = AppState::new(
            config,
            Arc::new(Database::open(":memory:").unwrap()),
            action_tx,
        );
        Arc::get_mut(&mut state).unwrap().telegram_notifier =
            TelegramNotifier::with_base_url(reqwest::Client::new(), url.clone());
        NotificationFixture {
            state,
            requests,
            actions,
            url,
            server,
        }
    }

    async fn next_notification(fixture: &mut NotificationFixture) -> serde_json::Value {
        let request = tokio::time::timeout(Duration::from_secs(1), fixture.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request["chat_id"], "12345");
        assert_eq!(request["parse_mode"], "HTML");
        request
    }

    async fn no_more_notifications(fixture: &mut NotificationFixture) {
        assert!(
            tokio::time::timeout(Duration::from_millis(100), fixture.requests.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn strategy_notifications_cover_start_pause_resume_without_duplicates() {
        let mut fixture = notification_fixture(true, false).await;
        fixture.state.notify_strategy_started().await;
        assert!(next_notification(&mut fixture).await["text"]
            .as_str()
            .unwrap()
            .contains("策略已启动"));
        fixture
            .state
            .pause_trading_with_reason("手动暂停策略")
            .await
            .unwrap();
        fixture
            .state
            .pause_trading_with_reason("手动暂停策略")
            .await
            .unwrap();
        let message = next_notification(&mut fixture).await;
        assert!(message["text"]
            .as_str()
            .unwrap()
            .contains("原因：手动暂停策略"));
        assert_eq!(
            fixture.state.db.load_bot_status().unwrap(),
            Some(BotStatus::Paused)
        );
        fixture.state.resume_trading().await.unwrap();
        fixture.state.resume_trading().await.unwrap();
        assert!(next_notification(&mut fixture).await["text"]
            .as_str()
            .unwrap()
            .contains("策略已恢复"));
        no_more_notifications(&mut fixture).await;
        fixture.server.abort();
    }

    #[tokio::test]
    async fn maintenance_is_silent_but_an_actual_pause_during_maintenance_notifies() {
        let mut fixture = notification_fixture(true, false).await;
        fixture.state.pause_trading_silently().await.unwrap();
        fixture.state.resume_trading_silently().await.unwrap();
        no_more_notifications(&mut fixture).await;
        fixture.state.pause_trading_silently().await.unwrap();
        fixture
            .state
            .pause_trading_with_reason("撤单失败")
            .await
            .unwrap();
        let message = next_notification(&mut fixture).await;
        assert!(message["text"].as_str().unwrap().contains("原因：撤单失败"));
        fixture
            .state
            .pause_trading_with_reason("重复暂停")
            .await
            .unwrap();
        no_more_notifications(&mut fixture).await;
        fixture.server.abort();
    }

    #[tokio::test]
    async fn disabled_telegram_does_not_send_strategy_notifications() {
        let mut fixture = notification_fixture(false, false).await;
        fixture.state.notify_strategy_started().await;
        fixture
            .state
            .pause_trading_with_reason("手动暂停策略")
            .await
            .unwrap();
        fixture.state.resume_trading().await.unwrap();
        assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
        no_more_notifications(&mut fixture).await;
        fixture.server.abort();
    }

    #[tokio::test]
    async fn rejected_telegram_notification_logs_warning_without_changing_pause() {
        let mut fixture = notification_fixture(true, true).await;
        let mut logs = fixture.state.ws_broadcast_tx.subscribe();
        fixture
            .state
            .pause_trading_with_reason("手动暂停策略")
            .await
            .unwrap();
        next_notification(&mut fixture).await;
        let log = tokio::time::timeout(Duration::from_secs(1), logs.recv())
            .await
            .unwrap()
            .unwrap();
        let log: serde_json::Value = serde_json::from_str(&log).unwrap();
        assert_eq!(log["data"]["level"], "WARN");
        assert!(log["data"]["message"]
            .as_str()
            .unwrap()
            .contains("Telegram 策略启停通知发送失败"));
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
        assert_eq!(
            fixture.state.db.load_bot_status().unwrap(),
            Some(BotStatus::Paused)
        );
        fixture.server.abort();
    }

    #[tokio::test]
    async fn engine_start_notifies_or_reports_position_limit_pause() {
        for at_limit in [false, true] {
            let mut fixture = notification_fixture(true, false).await;
            {
                let mut config = fixture.state.config.write().await;
                config.grid.pause_on_max_position = at_limit;
                config.grid.max_position_usdc = Some(dec!(1000));
            }
            fixture.state.position.write().await.size = dec!(10);
            {
                let mut ticker = fixture.state.ticker.write().await;
                ticker.last_price = dec!(100);
                ticker.mark_price = dec!(100);
                ticker.mark_update_time = Utc::now();
                ticker.update_time = Utc::now();
            }
            let config = fixture.state.config.read().await.clone();
            let client = Arc::new(
                crate::exchange::client::BinanceFuturesClient::with_base_url(
                    &config.exchange,
                    fixture.url.clone(),
                ),
            );
            let (_, ticker_rx) = broadcast::channel(1);
            // Keep the fixture usable while moving the real action receiver into the engine.
            let (_, unused_actions) = mpsc::channel(1);
            let action_rx = std::mem::replace(&mut fixture.actions, unused_actions);
            let mut engine = crate::strategy::grid::GridTradingEngine::new(
                fixture.state.clone(),
                client,
                action_rx,
                ticker_rx,
            );
            let task = tokio::spawn(async move { engine.run().await });
            let message = next_notification(&mut fixture).await;
            let text = message["text"].as_str().unwrap();
            if at_limit {
                assert!(text.contains("策略已暂停"));
                assert!(text.contains("持仓名义价值 1000 已达到最大持仓名义价值 1000"));
                assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
            } else {
                assert!(text.contains("策略已启动"));
                assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
            }
            task.abort();
            no_more_notifications(&mut fixture).await;
            fixture.server.abort();
        }
    }

    #[tokio::test]
    async fn trade_notifications_capture_position_amount_after_buy_and_sell_fills() {
        let mut fixture = notification_fixture(true, false).await;
        fixture.state.position.write().await.size = dec!(9.5);
        {
            let mut ticker = fixture.state.ticker.write().await;
            ticker.last_price = dec!(100);
            ticker.mark_price = dec!(100);
            ticker.mark_update_time = Utc::now();
            ticker.update_time = Utc::now();
        }
        let config = fixture.state.config.read().await.clone();
        let client = Arc::new(
            crate::exchange::client::BinanceFuturesClient::with_base_url(
                &config.exchange,
                fixture.url.clone(),
            ),
        );
        let (ticker_tx, ticker_rx) = broadcast::channel(4);
        let (_, unused_actions) = mpsc::channel(1);
        let action_rx = std::mem::replace(&mut fixture.actions, unused_actions);
        let mut engine = crate::strategy::GridTradingEngine::new(
            fixture.state.clone(),
            client,
            action_rx,
            ticker_rx,
        );
        let task = tokio::spawn(async move { engine.run().await });
        let start = next_notification(&mut fixture).await;
        assert!(start["text"].as_str().unwrap().contains("策略已启动"));
        for (price, side, size, amount) in [
            (dec!(99.9), "BUY", dec!(10.5), "1050.00"),
            (dec!(100.1), "SELL", dec!(9.51), "951.00"),
        ] {
            ticker_tx
                .send(TickerInfo {
                    symbol: config.exchange.symbol.clone(),
                    last_price: price,
                    mark_price: dec!(100),
                    mark_update_time: Utc::now(),
                    update_time: Utc::now(),
                    ..Default::default()
                })
                .unwrap();
            let message = next_notification(&mut fixture).await;
            let text = message["text"].as_str().unwrap();
            assert!(text.contains(side));
            assert!(text.contains(&format!("持仓金额：{amount} USDC")));
            assert_eq!(fixture.state.position.read().await.size, size);
        }
        task.abort();
        fixture.server.abort();
    }

    fn cache_test_trade(id: &str, seconds: i64, mode: TradingMode) -> TradeRecord {
        TradeRecord {
            trade_id: id.into(),
            client_order_id: format!("gb_b_{id}"),
            symbol: "SOLUSDC".into(),
            mode,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            pnl_verified: false,
            is_maker: true,
            timestamp: chrono::DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap(),
            note: String::new(),
        }
    }

    #[tokio::test]
    async fn recent_trade_cache_keeps_latest_after_out_of_order_backfill_and_reload() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, _) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
        // A permutation of 0..260 forces old and new fills to arrive in mixed order.
        for i in 0..260 {
            let timestamp = (i * 73) % 260;
            let trade =
                cache_test_trade(&format!("paper-{timestamp}"), timestamp, TradingMode::Paper);
            assert!(state.record_trade(trade, None, None, None).await);
        }
        // Equal timestamps use insertion order, including at the retention boundary.
        for id in ["tie-first", "tie-second"] {
            assert!(
                state
                    .record_trade(
                        cache_test_trade(id, 200, TradingMode::Paper),
                        None,
                        None,
                        None
                    )
                    .await
            );
        }
        let cached = state.recent_trades.read().await.clone();
        assert_eq!(cached.len(), 200);
        assert_eq!(
            cached.front().unwrap().timestamp,
            cache_test_trade("x", 62, TradingMode::Paper).timestamp
        );
        assert!(cached
            .iter()
            .zip(cached.iter().skip(1))
            .all(|(a, b)| a.timestamp <= b.timestamp));
        let ids: Vec<_> = cached.iter().map(|t| t.trade_id.as_str()).collect();
        assert!(
            ids.iter().position(|id| *id == "tie-first").unwrap()
                < ids.iter().position(|id| *id == "tie-second").unwrap()
        );
        let snapshot = state.snapshot().await;
        let expected: Vec<_> = cached
            .iter()
            .rev()
            .take(50)
            .map(|t| t.trade_id.clone())
            .collect();
        assert_eq!(
            snapshot
                .recent_trades
                .iter()
                .map(|t| t.trade_id.clone())
                .collect::<Vec<_>>(),
            expected
        );

        let (tx, _) = mpsc::channel(1);
        let restarted = AppState::new(AppConfig::default(), db.clone(), tx);
        assert_eq!(
            restarted
                .recent_trades
                .read()
                .await
                .iter()
                .map(|t| &t.trade_id)
                .collect::<Vec<_>>(),
            cached.iter().map(|t| &t.trade_id).collect::<Vec<_>>()
        );
        for i in 0..205 {
            db.insert_trade(&cache_test_trade(
                &format!("live-{i}"),
                i,
                TradingMode::Live,
            ))
            .unwrap();
        }
        state.config.write().await.exchange.dry_run = false;
        state.refresh_current_scope().await;
        let live = state.recent_trades.read().await;
        assert_eq!(live.len(), 200);
        assert_eq!(live.front().unwrap().trade_id, "live-5");
        assert_eq!(live.back().unwrap().trade_id, "live-204");
        drop(live);
        state.config.write().await.exchange.dry_run = true;
        state.refresh_current_scope().await;
        assert_eq!(
            state
                .recent_trades
                .read()
                .await
                .iter()
                .map(|t| t.trade_id.clone())
                .collect::<Vec<_>>(),
            cached
                .iter()
                .map(|t| t.trade_id.clone())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn recent_trade_verification_preserves_cached_timestamp() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        let original = cache_test_trade("verify", 1, TradingMode::Paper);
        assert!(state.record_trade(original.clone(), None, None, None).await);
        let mut verified = original.clone();
        verified.timestamp += chrono::Duration::seconds(100);
        verified.realized_pnl = dec!(3);
        state.verify_trade_pnl(verified).await.unwrap();
        let cached = state.recent_trades.read().await;
        assert_eq!(cached[0].timestamp, original.timestamp);
        assert_eq!(cached[0].realized_pnl, dec!(3));
        assert!(cached[0].pnl_verified);
    }

    #[tokio::test]
    async fn switching_mode_reloads_only_that_modes_trades() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        for (id, mode, pnl) in [
            ("paper", TradingMode::Paper, dec!(10)),
            ("live", TradingMode::Live, dec!(-2)),
        ] {
            db.insert_trade(&TradeRecord {
                trade_id: id.into(),
                client_order_id: format!("gb_b_{id}"),
                symbol: "SOLUSDC".into(),
                mode,
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(1),
                amount_usdc: dec!(100),
                realized_pnl: pnl,
                commission: Decimal::ZERO,
                pnl_verified: true,
                is_maker: true,
                timestamp: Utc::now(),
                note: String::new(),
            })
            .unwrap();
        }
        let (action_tx, _) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, action_tx);
        assert_eq!(state.stats.read().await.total_realized_pnl, dec!(10));
        assert_eq!(state.recent_trades.read().await.len(), 1);
        let snapshot = state.snapshot().await;
        let hourly = snapshot.hourly_trade_stats.unwrap();
        assert_eq!(hourly.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 1);

        state.config.write().await.exchange.dry_run = false;
        state.refresh_current_scope().await;
        assert_eq!(state.stats.read().await.total_realized_pnl, dec!(-2));
        let trades = state.recent_trades.read().await;
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].mode, TradingMode::Live);
        drop(trades);
        let snapshot = state.snapshot().await;
        let hourly = snapshot.hourly_trade_stats.unwrap();
        assert_eq!(hourly.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 1);
    }
}
