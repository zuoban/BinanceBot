use crate::config::AppConfig;
use crate::db::Database;
use crate::fx::CnyRateCache;
use crate::strategy::precision::SymbolRules;
use crate::telegram::TelegramNotifier;
use crate::types::{
    AccountInfo, BotControlAction, BotStatus, GridOrder, GridStats, LogEntry, PositionInfo,
    TickerInfo, TradeRecord,
};
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch, Mutex, RwLock};
use tracing::{error, warn};

#[path = "auth_context.rs"]
mod auth_context;
#[path = "global_risk.rs"]
mod global_risk;
#[path = "snapshot.rs"]
pub(crate) mod snapshot;
#[path = "state_lifecycle.rs"]
mod state_lifecycle;
#[path = "state_trades.rs"]
mod state_trades;

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;

pub(crate) use auth_context::AuthContext;
pub(crate) use global_risk::{
    ExchangeSubmissionGuard, GlobalPositionLimit, PendingFillReconciliation,
};

use state_trades::{build_grid_stats, recent_trade_cache, RECENT_TRADE_LIMIT};

const MAX_RECENT_LOGS: usize = 250;

pub struct AppState {
    pub db: Arc<Database>,
    pub config: RwLock<AppConfig>,
    pub(crate) config_update_lock: Mutex<()>,
    pub(crate) config_instance: u128,
    pub rules: RwLock<SymbolRules>,
    pub status: RwLock<BotStatus>,
    pub ticker: RwLock<TickerInfo>,
    pub stats: RwLock<GridStats>,
    pub position: RwLock<PositionInfo>,
    // Paper mode stores a symbol's durable PnL subledger here; consumers use
    // `current_account` or `account_view` for the shared asset balance.
    pub account: RwLock<AccountInfo>,
    pub cny_rates: Arc<CnyRateCache>,
    pub active_orders: RwLock<HashMap<String, GridOrder>>,
    pub recent_trades: RwLock<VecDeque<TradeRecord>>,
    pub recent_logs: RwLock<VecDeque<LogEntry>>,
    pub(crate) global_position_limit: std::sync::OnceLock<std::sync::Weak<GlobalPositionLimit>>,
    pub(crate) global_position_ready: AtomicBool,
    pub(crate) global_position_initialized: AtomicBool,
    pub(crate) paper_restore_failed: AtomicBool,
    pub(crate) global_cancel_pending: AtomicBool,
    pub(crate) global_cancel_queued: AtomicBool,
    pause_epoch: AtomicU64,
    safety_pause_epoch: AtomicU64,
    safety_pauses_pending: AtomicU64,
    safety_pause_admission: Mutex<()>,
    shutdown_guard_cleared: AtomicBool,
    shutdown_requested: AtomicBool,
    retired: watch::Sender<bool>,
    pub(crate) socket_control_lock: Arc<RwLock<()>>,
    pub(crate) pending_control_pauses: watch::Sender<usize>,
    // Serializes status updates and protects the shutdown recovery eligibility.
    status_update_lock: Mutex<Option<u64>>,
    trade_commit_lock: Arc<Mutex<()>>,
    /// A global configuration transition holds the writer until commit or rollback completes.
    pub(crate) order_submission_lock: Arc<RwLock<()>>,
    pair_scope: Option<String>,
    pub(crate) snapshot_scope_lock: RwLock<()>,
    snapshot_publish_lock: Mutex<()>,
    snapshot_cache: snapshot::SnapshotCache,
    pub(crate) auth: Arc<AuthContext>,

    pub action_tx: mpsc::Sender<BotControlAction>,
    pub ws_broadcast_tx: broadcast::Sender<String>,
    pub(crate) managed_router: std::sync::OnceLock<axum::Router>,
    telegram_notifier: TelegramNotifier,
}

impl AppState {
    pub fn new(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
    ) -> Arc<Self> {
        Self::new_with_admin_auth(config, db, action_tx, None)
    }

    pub fn new_with_admin_auth(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> Arc<Self> {
        Self::new_with_rates(
            config,
            db,
            action_tx,
            Arc::new(CnyRateCache::new()),
            admin_auth,
        )
    }

    pub(crate) fn new_with_rates(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        cny_rates: Arc<CnyRateCache>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> Arc<Self> {
        let auth = AuthContext::new(db.clone(), admin_auth);
        Self::build_with_auth(
            config,
            db,
            action_tx,
            None,
            Arc::new(Mutex::new(())),
            cny_rates,
            auth,
        )
    }

    pub fn new_for_pair(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        trade_commit_lock: Arc<Mutex<()>>,
    ) -> Arc<Self> {
        Self::new_for_pair_with_admin_auth(config, db, action_tx, trade_commit_lock, None)
    }

    pub fn new_for_pair_with_admin_auth(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        trade_commit_lock: Arc<Mutex<()>>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> Arc<Self> {
        Self::new_for_pair_with_rates(
            config,
            db,
            action_tx,
            trade_commit_lock,
            Arc::new(CnyRateCache::new()),
            admin_auth,
        )
    }

    pub(crate) fn new_for_pair_with_rates(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        trade_commit_lock: Arc<Mutex<()>>,
        cny_rates: Arc<CnyRateCache>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> Arc<Self> {
        let symbol = config.exchange.symbol.clone();
        let auth = AuthContext::new(db.clone(), admin_auth);
        Self::build_with_auth(
            config,
            db,
            action_tx,
            Some(symbol),
            trade_commit_lock,
            cny_rates,
            auth,
        )
    }

    pub(crate) fn build_with_auth(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        pair_scope: Option<String>,
        trade_commit_lock: Arc<Mutex<()>>,
        cny_rates: Arc<CnyRateCache>,
        auth: Arc<AuthContext>,
    ) -> Arc<Self> {
        let (ws_broadcast_tx, _) = broadcast::channel(100);
        let symbol = config.exchange.symbol.clone();
        let mode = config.exchange.trading_mode();
        let saved_status = db.load_pair_status(&symbol, mode).and_then(|status| {
            if status.is_some() || pair_scope.is_some() {
                Ok(status)
            } else {
                db.load_bot_status()
            }
        });
        let mut initial_status = match saved_status {
            Ok(Some(status)) => status,
            Ok(None) if config.exchange.is_paper() => BotStatus::Running,
            Ok(None) => BotStatus::Paused,
            Err(error) => {
                warn!(
                    "Could not load saved bot status; starting paused: {}",
                    error
                );
                BotStatus::Paused
            }
        };

        let initial_account = if config.exchange.is_paper() {
            AccountInfo::initial_paper(&symbol, Some(Utc::now()))
        } else {
            AccountInfo::default()
        };
        let mut paper_restore_failed = false;
        let (initial_position, initial_account) = if config.exchange.is_paper() {
            match db.load_paper_state(&symbol) {
                Ok(Some(saved)) => saved,
                Ok(None) => (PositionInfo::default(), initial_account),
                Err(error) => {
                    error!(
                        "Could not restore paper account; starting paused: {}",
                        error
                    );
                    initial_status = BotStatus::Paused;
                    paper_restore_failed = true;
                    (PositionInfo::default(), initial_account)
                }
            }
        } else {
            (PositionInfo::default(), initial_account)
        };
        if config.exchange.is_paper() && !paper_restore_failed {
            if let Err(error) =
                db.shared_paper_account(&symbol, &initial_position, &initial_account)
            {
                error!(%symbol, %error, "Could not restore shared paper account; starting paused");
                initial_status = BotStatus::Paused;
                paper_restore_failed = true;
            }
        } else if !config.exchange.is_paper() {
            // Warm the shared paper ledger while already on a blocking thread so a
            // subsequent Live -> Paper switch never queries SQLite on Tokio workers.
            db.warm_paper_ledger();
        }

        // Preload recent trades from SQLite database
        let saved_trades = db
            .get_recent_trades_for(&symbol, mode, RECENT_TRADE_LIMIT)
            .unwrap_or_default();
        let recent_trades = recent_trade_cache(saved_trades);

        // Restore cumulative grid stats from SQLite database if available
        let scoped_stats = db.load_scoped_stats(&symbol, mode).ok().flatten();
        let pnl_totals = match db.get_pnl_totals(&symbol, mode) {
            Ok(totals) => Some(totals),
            Err(e) => {
                warn!("Failed to load realized PnL totals: {}", e);
                None
            }
        };
        let initial_stats = build_grid_stats(scoped_stats, pnl_totals);

        Arc::new(Self {
            db,
            config: RwLock::new(config),
            config_update_lock: Mutex::new(()),
            config_instance: rand::random(),
            rules: RwLock::new(SymbolRules::default()),
            status: RwLock::new(initial_status),
            ticker: RwLock::new(TickerInfo {
                symbol,
                ..Default::default()
            }),
            stats: RwLock::new(initial_stats),
            position: RwLock::new(initial_position),
            account: RwLock::new(initial_account),
            cny_rates,
            active_orders: RwLock::new(HashMap::new()),
            recent_trades: RwLock::new(recent_trades),
            recent_logs: RwLock::new(VecDeque::with_capacity(MAX_RECENT_LOGS)),
            global_position_limit: std::sync::OnceLock::new(),
            global_position_ready: AtomicBool::new(false),
            global_position_initialized: AtomicBool::new(false),
            paper_restore_failed: AtomicBool::new(paper_restore_failed),
            global_cancel_pending: AtomicBool::new(false),
            global_cancel_queued: AtomicBool::new(false),
            pause_epoch: AtomicU64::new(0),
            safety_pause_epoch: AtomicU64::new(0),
            safety_pauses_pending: AtomicU64::new(0),
            safety_pause_admission: Mutex::new(()),
            shutdown_requested: AtomicBool::new(false),
            shutdown_guard_cleared: AtomicBool::new(false),
            retired: watch::channel(false).0,
            socket_control_lock: Arc::new(RwLock::new(())),
            pending_control_pauses: watch::channel(0).0,
            status_update_lock: Mutex::new(None),
            trade_commit_lock,
            order_submission_lock: Arc::new(RwLock::new(())),
            pair_scope,
            snapshot_scope_lock: RwLock::new(()),
            snapshot_publish_lock: Mutex::new(()),
            snapshot_cache: snapshot::SnapshotCache::default(),
            auth,
            action_tx,
            ws_broadcast_tx,
            managed_router: std::sync::OnceLock::new(),
            telegram_notifier: TelegramNotifier::new(TelegramNotifier::shared_client()),
        })
    }

    pub fn pair_scope(&self) -> Option<&str> {
        self.pair_scope.as_deref()
    }

    pub async fn add_log(&self, level: &str, message: impl Into<String>) {
        let entry = LogEntry {
            timestamp: Utc::now(),
            level: level.to_string(),
            message: message.into(),
        };

        let mut logs = self.recent_logs.write().await;
        if logs.len() >= MAX_RECENT_LOGS {
            logs.pop_front();
        }
        logs.push_back(entry.clone());

        // Also broadcast log event to WebSocket clients
        if self.ws_broadcast_tx.receiver_count() > 0 {
            if let Some(json) = crate::types::ws_frame_json("log", &entry) {
                let _ = self.ws_broadcast_tx.send(json);
            }
        }
    }

    pub async fn run_cny_rate_refresh(self: Arc<Self>) {
        let mut timer = tokio::time::interval(Duration::from_secs(10));
        loop {
            timer.tick().await;
            let mut asset = self.account.read().await.asset.clone();
            if asset.is_empty() {
                asset = crate::types::quote_asset(&self.config.read().await.exchange.symbol)
                    .unwrap_or_default()
                    .to_string();
            }
            if let Err(err) = self.cny_rates.refresh_if_due(&asset).await {
                warn!("Could not refresh {}/CNY exchange rate: {}", asset, err);
            }
        }
    }

    pub(crate) fn telegram_notifier(&self) -> &TelegramNotifier {
        &self.telegram_notifier
    }
}
