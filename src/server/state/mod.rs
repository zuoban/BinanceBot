use crate::config::AppConfig;
use crate::db::Database;
use crate::fx::CnyRateCache;
use crate::strategy::SymbolRules;
use crate::telegram::TelegramNotifier;
use crate::types::{
    AccountInfo, BotControlAction, BotStatus, GridOrder, GridStats, LogEntry, PositionInfo,
    TickerInfo, TradeRecord,
};
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch, Mutex, RwLock};
use tracing::{error, warn};

mod auth_context;
mod global_risk;
mod notifications;
mod risk_guards;
pub(crate) mod snapshot;
mod state_cleanup;
mod state_lifecycle;
mod state_trades;

#[cfg(test)]
mod tests;

pub(crate) use auth_context::{sanitize_session_token, AuthContext, SessionCheckError};
pub(crate) use global_risk::GlobalPositionLimit;
pub(crate) use risk_guards::{ExchangeSubmissionGuard, PendingFillReconciliation};
use state_trades::{build_grid_stats, recent_trade_cache, RECENT_TRADE_LIMIT};

const MAX_RECENT_LOGS: usize = 250;

pub struct AppState {
    pub db: Arc<Database>,
    pub config: RwLock<AppConfig>,
    pub(crate) config_update_lock: Mutex<()>,
    pub(crate) config_instance: u128,
    pub(crate) rules: RwLock<SymbolRules>,
    pub status: RwLock<BotStatus>,
    pub(crate) ticker: RwLock<TickerInfo>,
    pub stats: RwLock<GridStats>,
    pub position: RwLock<PositionInfo>,
    // Paper mode stores a symbol's durable PnL subledger here; consumers use
    // `current_account` or `account_view` for the shared asset balance.
    pub account: RwLock<AccountInfo>,
    pub(crate) cny_rates: Arc<CnyRateCache>,
    pub active_orders: RwLock<HashMap<String, GridOrder>>,
    pub recent_trades: RwLock<VecDeque<TradeRecord>>,
    pub(crate) recent_logs: RwLock<VecDeque<LogEntry>>,
    pub(crate) global_position_limit: std::sync::OnceLock<std::sync::Weak<GlobalPositionLimit>>,
    pub(crate) global_position_ready: AtomicBool,
    pub(crate) global_position_initialized: AtomicBool,
    pub(crate) paper_restore_failed: AtomicBool,
    pub(super) stats_dirty: AtomicBool,
    pub(super) recent_trades_dirty: AtomicBool,
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
    snapshot_publish_lock: Mutex<Option<snapshot::LastSnapshotBroadcast>>,
    snapshot_cache: snapshot::SnapshotCache,
    pub(crate) auth: Arc<AuthContext>,

    pub(crate) action_tx: mpsc::Sender<BotControlAction>,
    pub ws_broadcast_tx: broadcast::Sender<Arc<str>>,
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
        let auth = AuthContext::new(db.clone(), admin_auth);
        Self::build_with_auth(
            config,
            db,
            action_tx,
            None,
            Arc::new(Mutex::new(())),
            Arc::new(CnyRateCache::new()),
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
        let symbol = config.exchange.symbol.clone();
        let auth = AuthContext::new(db.clone(), admin_auth);
        Self::build_with_auth(
            config,
            db,
            action_tx,
            Some(symbol),
            trade_commit_lock,
            Arc::new(CnyRateCache::new()),
            auth,
        )
    }

    fn restore_initial_status_and_paper(
        config: &AppConfig,
        db: &Database,
        pair_scope: Option<&str>,
    ) -> (BotStatus, PositionInfo, AccountInfo, bool) {
        let symbol = &config.exchange.symbol;
        let mode = config.exchange.trading_mode();
        let saved_status = db.load_pair_status(symbol, mode).and_then(|status| {
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
        if !config.exchange.is_paper() {
            // Warm the shared paper ledger while already on a blocking thread so a
            // subsequent Live -> Paper switch never queries SQLite on Tokio workers.
            db.warm_paper_ledger();
            return (
                initial_status,
                PositionInfo::default(),
                AccountInfo::default(),
                false,
            );
        }
        let initial_account = AccountInfo::initial_paper(symbol, Some(Utc::now()));
        let mut paper_restore_failed = false;
        let (initial_position, initial_account) = match db.load_paper_state(symbol) {
            Ok(Some(saved)) => saved,
            Ok(None) => (PositionInfo::default(), initial_account),
            Err(error) => {
                error!("Could not restore paper account; starting paused: {error}");
                initial_status = BotStatus::Paused;
                paper_restore_failed = true;
                (PositionInfo::default(), initial_account)
            }
        };
        if !paper_restore_failed {
            if let Err(error) = db.shared_paper_account(symbol, &initial_position, &initial_account)
            {
                error!(%symbol, %error, "Could not restore shared paper account; starting paused");
                initial_status = BotStatus::Paused;
                paper_restore_failed = true;
            }
        }
        (
            initial_status,
            initial_position,
            initial_account,
            paper_restore_failed,
        )
    }

    fn restore_initial_trades_and_stats(
        db: &Database,
        symbol: &str,
        mode: crate::types::TradingMode,
    ) -> (VecDeque<TradeRecord>, GridStats, bool, bool) {
        let mut stats_dirty = false;
        let mut recent_trades_dirty = false;
        let saved_trades = match db.get_recent_trades_for(symbol, mode, RECENT_TRADE_LIMIT) {
            Ok(trades) => trades,
            Err(e) => {
                warn!("Failed to load recent trades: {}", e);
                recent_trades_dirty = true;
                Vec::new()
            }
        };
        let scoped_stats = match db.load_scoped_stats(symbol, mode) {
            Ok(stats) => stats,
            Err(e) => {
                warn!("Failed to load scoped grid stats: {}", e);
                stats_dirty = true;
                None
            }
        };
        let pnl_totals = match db.get_pnl_totals(symbol, mode) {
            Ok(totals) => Some(totals),
            Err(e) => {
                warn!("Failed to load realized PnL totals: {}", e);
                stats_dirty = true;
                None
            }
        };
        (
            recent_trade_cache(saved_trades),
            build_grid_stats(scoped_stats, pnl_totals),
            stats_dirty,
            recent_trades_dirty,
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
        let (initial_status, initial_position, initial_account, paper_restore_failed) =
            Self::restore_initial_status_and_paper(&config, &db, pair_scope.as_deref());
        let (recent_trades, initial_stats, stats_dirty, recent_trades_dirty) =
            Self::restore_initial_trades_and_stats(&db, &symbol, mode);
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
            stats_dirty: AtomicBool::new(stats_dirty),
            recent_trades_dirty: AtomicBool::new(recent_trades_dirty),
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
            snapshot_publish_lock: Mutex::new(None),
            snapshot_cache: snapshot::SnapshotCache::default(),
            auth,
            action_tx,
            ws_broadcast_tx,
            managed_router: std::sync::OnceLock::new(),
            telegram_notifier: TelegramNotifier::new(),
        })
    }

    pub(crate) fn is_stats_dirty(&self) -> bool {
        self.stats_dirty.load(std::sync::atomic::Ordering::SeqCst)
            || self
                .recent_trades_dirty
                .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn pair_scope(&self) -> Option<&str> {
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

    pub(crate) fn telegram_notifier(&self) -> &TelegramNotifier {
        &self.telegram_notifier
    }

    pub(crate) fn account_view(
        &self,
        symbol: &str,
        mode: crate::types::TradingMode,
        position: &PositionInfo,
        local: &AccountInfo,
    ) -> AccountInfo {
        if mode != crate::types::TradingMode::Paper {
            return local.clone();
        }
        if self
            .paper_restore_failed
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return AccountInfo::default();
        }
        match self.db.shared_paper_account(symbol, position, local) {
            Ok(account) => account,
            Err(error) => {
                error!(%symbol, %error, "Could not restore shared paper account");
                self.paper_restore_failed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
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
}
