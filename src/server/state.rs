use crate::config::AppConfig;
use crate::strategy::precision::SymbolRules;
use crate::types::*;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

pub struct AppState {
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
    pub fn new(config: AppConfig, action_tx: mpsc::Sender<BotControlAction>) -> Arc<Self> {
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

        Arc::new(Self {
            config: RwLock::new(config),
            rules: RwLock::new(SymbolRules::default()),
            status: RwLock::new(BotStatus::Running),
            ticker: RwLock::new(TickerInfo {
                symbol,
                ..Default::default()
            }),
            stats: RwLock::new(GridStats::default()),
            position: RwLock::new(PositionInfo::default()),
            account: RwLock::new(initial_account),
            active_orders: RwLock::new(HashMap::new()),
            recent_trades: RwLock::new(VecDeque::with_capacity(200)),
            recent_logs: RwLock::new(VecDeque::with_capacity(300)),
            action_tx,
            ws_broadcast_tx,
        })
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
