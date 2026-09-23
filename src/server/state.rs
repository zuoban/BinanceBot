use crate::config::AppConfig;
use crate::db::Database;
use crate::strategy::precision::SymbolRules;
use crate::types::*;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::error;

pub struct AppState {
    pub db: Arc<Database>,
    pub config: RwLock<AppConfig>,
    pub rules: RwLock<SymbolRules>,
    pub status: RwLock<BotStatus>,
    pub ticker: RwLock<TickerInfo>,
    pub stats: RwLock<GridStats>,
    pub position: RwLock<PositionInfo>,
    pub account: RwLock<AccountInfo>,
    pub active_orders: RwLock<HashMap<String, GridOrder>>,
    pub recent_trades: RwLock<VecDeque<TradeRecord>>,
    pub recent_logs: RwLock<VecDeque<LogEntry>>,

    pub action_tx: mpsc::Sender<BotControlAction>,
    pub ws_broadcast_tx: broadcast::Sender<String>,
}

impl AppState {
    pub fn new(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
    ) -> Arc<Self> {
        let (ws_broadcast_tx, _) = broadcast::channel(100);
        let symbol = config.exchange.symbol.clone();

        let initial_account = if config.exchange.dry_run {
            AccountInfo {
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
        let saved_trades = db.get_recent_trades(100).unwrap_or_default();
        let mut recent_trades = VecDeque::with_capacity(200);
        for trade in saved_trades.into_iter().rev() {
            recent_trades.push_back(trade);
        }

        // Restore cumulative grid stats from SQLite database if available
        let mut initial_stats = GridStats::default();
        if let Ok(Some((total_trades, cycles, profit, volume))) = db.load_stats() {
            initial_stats.total_trades = total_trades;
            initial_stats.completed_cycles = cycles;
            initial_stats.total_realized_profit = profit;
            initial_stats.total_volume_usdc = volume;
        }

        Arc::new(Self {
            db,
            config: RwLock::new(config),
            rules: RwLock::new(SymbolRules::default()),
            status: RwLock::new(BotStatus::Running),
            ticker: RwLock::new(TickerInfo {
                symbol,
                ..Default::default()
            }),
            stats: RwLock::new(initial_stats),
            position: RwLock::new(PositionInfo::default()),
            account: RwLock::new(initial_account),
            active_orders: RwLock::new(HashMap::new()),
            recent_trades: RwLock::new(recent_trades),
            recent_logs: RwLock::new(VecDeque::with_capacity(300)),
            action_tx,
            ws_broadcast_tx,
        })
    }

    pub async fn record_trade(&self, trade: TradeRecord, is_completed_cycle: bool) {
        // Persist trade into SQLite
        if let Err(e) = self.db.insert_trade(&trade) {
            error!("Failed to persist trade to SQLite database: {}", e);
        }

        // Update grid performance statistics
        {
            let mut stats = self.stats.write().await;
            stats.total_trades += 1;
            stats.total_volume_usdc += trade.amount_usdc;
            if is_completed_cycle {
                stats.completed_cycles += 1;
                stats.total_realized_profit += trade.realized_pnl;
            }
            if let Err(e) = self.db.save_stats(&stats) {
                error!("Failed to persist grid stats to SQLite database: {}", e);
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

        let orders_map = self.active_orders.read().await;
        let mut active_orders: Vec<GridOrder> = orders_map.values().cloned().collect();
        // Sort orders by price descending
        active_orders.sort_by(|a, b| b.price.cmp(&a.price));

        stats.active_buy_orders = active_orders.iter().filter(|o| o.side == OrderSide::Buy).count();
        stats.active_sell_orders = active_orders.iter().filter(|o| o.side == OrderSide::Sell).count();
        stats.uptime_secs = (Utc::now() - stats.start_time).num_seconds().max(0) as u64;

        let recent_trades: Vec<TradeRecord> = self.recent_trades.read().await.iter().cloned().rev().take(50).collect();
        let recent_logs: Vec<LogEntry> = self.recent_logs.read().await.iter().cloned().rev().take(50).collect();

        BotSnapshot {
            status,
            dry_run,
            symbol,
            ticker,
            stats,
            position,
            account,
            grid_config: summary,
            active_orders,
            recent_trades,
            recent_logs,
        }
    }
}

impl AppState {
    pub async fn is_token_valid(&self, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        self.db.is_session_valid(token).unwrap_or(false)
    }

    pub async fn create_session(&self, token: &str) -> anyhow::Result<()> {
        self.db.create_session(token)
    }

    pub async fn delete_session(&self, token: &str) -> anyhow::Result<()> {
        self.db.delete_session(token)
    }

    pub async fn clear_all_sessions(&self) -> anyhow::Result<()> {
        self.db.clear_all_sessions()
    }
}
