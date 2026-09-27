use crate::config::AppConfig;
use crate::db::Database;
use crate::fx::CnyRateCache;
use crate::strategy::precision::SymbolRules;
use crate::telegram::send_trade_notification;
use crate::types::*;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tracing::{error, warn};

pub struct AppState {
    pub db: Arc<Database>,
    pub config: RwLock<AppConfig>,
    pub rules: RwLock<SymbolRules>,
    pub status: RwLock<BotStatus>,
    pub ticker: RwLock<TickerInfo>,
    pub stats: RwLock<GridStats>,
    pub position: RwLock<PositionInfo>,
    pub account: RwLock<AccountInfo>,
    pub cny_rates: CnyRateCache,
    pub active_orders: RwLock<HashMap<String, GridOrder>>,
    pub recent_trades: RwLock<VecDeque<TradeRecord>>,
    pub recent_logs: RwLock<VecDeque<LogEntry>>,
    pause_epoch: AtomicU64,
    status_update_lock: Mutex<()>,
    trade_commit_lock: Mutex<()>,
    login_failures: Mutex<VecDeque<Instant>>,
    setup_code: Option<String>,

    pub action_tx: mpsc::Sender<BotControlAction>,
    pub ws_broadcast_tx: broadcast::Sender<String>,
    telegram_client: reqwest::Client,
}

impl AppState {
    pub fn new(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
    ) -> Arc<Self> {
        let (ws_broadcast_tx, _) = broadcast::channel(100);
        let symbol = config.exchange.symbol.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let initial_status = match db.load_bot_status() {
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
            cny_rates: CnyRateCache::new(),
            active_orders: RwLock::new(HashMap::new()),
            recent_trades: RwLock::new(recent_trades),
            recent_logs: RwLock::new(VecDeque::with_capacity(300)),
            pause_epoch: AtomicU64::new(0),
            status_update_lock: Mutex::new(()),
            trade_commit_lock: Mutex::new(()),
            login_failures: Mutex::new(VecDeque::new()),
            setup_code,
            action_tx,
            ws_broadcast_tx,
            telegram_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("Failed to build Telegram HTTP client"),
        })
    }

    pub async fn record_trade(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
    ) -> bool {
        // Persist the trade and its cumulative statistics together.
        let current_config = self.config.read().await;
        let current_symbol = current_config.exchange.symbol.clone();
        let current_mode = TradingMode::from_exchange(
            current_config.exchange.dry_run,
            current_config.exchange.is_testnet,
        );
        drop(current_config);
        if trade.symbol != current_symbol || trade.mode != current_mode {
            error!(
                "Trade scope differs from the active strategy: {}",
                trade.trade_id
            );
            return false;
        }
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
                .run_blocking(move |db| db.insert_trade_with_stats(&stored_trade, &stored_stats))
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
        }

        // Add to in-memory recent trades
        {
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

        let config = self.config.read().await;
        if config.telegram.enabled {
            let telegram = config.telegram.clone();
            let is_dry_run = config.exchange.dry_run;
            let is_testnet = config.exchange.is_testnet;
            let client = self.telegram_client.clone();
            let state = Arc::clone(self);
            tokio::spawn(async move {
                if let Err(err) =
                    send_trade_notification(&client, &telegram, &trade, is_dry_run, is_testnet)
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

    pub async fn snapshot(&self) -> BotSnapshot {
        let config = self.config.read().await;
        let summary = config.summary();
        let dry_run = config.exchange.dry_run;
        let symbol = config.exchange.symbol.clone();
        drop(config);

        let status = *self.status.read().await;
        let ticker = self.ticker.read().await.clone();
        let mut stats = self.stats.read().await.clone();
        let position = self.position.read().await.clone();
        let account = self.account.read().await.clone();
        let account_cny_rate = self.cny_rates.get(&account.asset).await;

        let orders_map = self.active_orders.read().await;
        let mut active_orders: Vec<GridOrder> = orders_map.values().cloned().collect();
        // Sort orders by price descending
        active_orders.sort_by_key(|order| std::cmp::Reverse(order.price));

        stats.active_buy_orders = active_orders
            .iter()
            .filter(|o| o.side == OrderSide::Buy)
            .count();
        stats.active_sell_orders = active_orders
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .count();
        stats.uptime_secs = (Utc::now() - stats.start_time).num_seconds().max(0) as u64;

        let recent_trades: Vec<TradeRecord> = self
            .recent_trades
            .read()
            .await
            .iter()
            .cloned()
            .rev()
            .take(50)
            .collect();
        let recent_logs: Vec<LogEntry> = self
            .recent_logs
            .read()
            .await
            .iter()
            .cloned()
            .rev()
            .take(50)
            .collect();

        BotSnapshot {
            status,
            dry_run,
            symbol,
            ticker,
            stats,
            position,
            account,
            account_cny_rate,
            grid_config: summary,
            active_orders,
            recent_trades,
            recent_logs,
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

    pub async fn pause_trading(&self) -> anyhow::Result<()> {
        self.pause_epoch.fetch_add(1, Ordering::SeqCst);
        let _update = self.status_update_lock.lock().await;
        *self.status.write().await = BotStatus::Paused;
        self.db
            .run_blocking(|db| db.save_bot_status(BotStatus::Paused))
            .await
    }

    pub async fn resume_trading(&self) -> anyhow::Result<()> {
        let _update = self.status_update_lock.lock().await;
        self.db
            .run_blocking(|db| db.save_bot_status(BotStatus::Running))
            .await?;
        *self.status.write().await = BotStatus::Running;
        Ok(())
    }

    pub fn pause_epoch(&self) -> u64 {
        self.pause_epoch.load(Ordering::SeqCst)
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

        state.config.write().await.exchange.dry_run = false;
        state.refresh_current_scope().await;
        assert_eq!(state.stats.read().await.total_realized_pnl, dec!(-2));
        let trades = state.recent_trades.read().await;
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].mode, TradingMode::Live);
    }
}
