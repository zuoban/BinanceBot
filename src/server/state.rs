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
                asset: if symbol.ends_with("USDT") {
                    "USDT"
                } else {
                    "USDC"
                }
                .to_string(),
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
            .get_recent_trades_for(&symbol, mode, 100)
            .unwrap_or_default();
        let mut recent_trades = VecDeque::with_capacity(200);
        for trade in saved_trades.into_iter().rev() {
            recent_trades.push_back(trade);
        }

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
            if trades.len() >= 200 {
                trades.pop_front();
            }
            trades.push_back(trade.clone());
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
            *existing = trade;
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
                    db.get_recent_trades_for(&query_symbol, mode, 100)?,
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
        *self.recent_trades.write().await = trades.into_iter().rev().collect();
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

    pub(crate) fn try_order_submission(&self) -> Option<OwnedRwLockReadGuard<()>> {
        // Skip replenishment instead of waiting: the engine must stay available for
        // configuration actions and cancellation while the coordinator holds the writer.
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
