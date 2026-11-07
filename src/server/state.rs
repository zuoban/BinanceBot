use crate::config::AppConfig;
use crate::db::Database;
use crate::fx::CnyRateCache;
use crate::strategy::precision::SymbolRules;
use crate::telegram::{TelegramNotifier, TradeNotificationContext};
use crate::types::*;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{
    broadcast, mpsc, watch, Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard,
    OwnedSemaphorePermit, RwLock, Semaphore,
};
use tracing::{error, warn};

#[path = "snapshot.rs"]
pub(crate) mod snapshot;

/// Coordinates the filled-position limit across all currently configured strategies.
pub(crate) struct GlobalPositionLimit {
    pub(crate) states: Arc<RwLock<Vec<Arc<AppState>>>>,
    lock: Mutex<()>,
    submission_barrier: Arc<RwLock<()>>,
    committed_config: std::sync::RwLock<Option<crate::config::GlobalConfig>>,
    triggered: AtomicBool,
    blocked: AtomicBool,
    pending_fill_reconciliations: AtomicU64,
    fill_failure_generation: AtomicU64,
    fill_recovered_generation: AtomicU64,
    pub(crate) enabled: AtomicBool,
    pub(crate) membership_ready: AtomicBool,
    pub(crate) shutdown_requested: AtomicBool,
}

pub(crate) struct ExchangeSubmissionGuard {
    state: Arc<AppState>,
    global: Option<Arc<GlobalPositionLimit>>,
    _barrier: Option<OwnedRwLockReadGuard<()>>,
    completed: bool,
}

impl ExchangeSubmissionGuard {
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for ExchangeSubmissionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.state
                .global_position_ready
                .store(false, Ordering::SeqCst);
            if let Some(global) = &self.global {
                global
                    .fill_failure_generation
                    .fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

pub(crate) struct PendingFillReconciliation {
    state: Arc<AppState>,
    global: Option<Arc<GlobalPositionLimit>>,
    barrier: Option<OwnedRwLockWriteGuard<()>>,
    resolved: bool,
}

impl PendingFillReconciliation {
    pub(crate) async fn acquire(&mut self) {
        if let Some(global) = &self.global {
            if global.enabled.load(Ordering::SeqCst) {
                self.barrier = Some(global.submission_barrier.clone().write_owned().await);
            }
        }
    }

    pub(crate) fn resolve(&mut self) {
        self.resolved = true;
    }
}

impl Drop for PendingFillReconciliation {
    fn drop(&mut self) {
        if !self.resolved {
            self.state
                .global_position_ready
                .store(false, Ordering::SeqCst);
        }
        if let Some(global) = &self.global {
            if !self.resolved {
                global
                    .fill_failure_generation
                    .fetch_add(1, Ordering::SeqCst);
            }
            global
                .pending_fill_reconciliations
                .fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl GlobalPositionLimit {
    pub(crate) fn new(states: Arc<RwLock<Vec<Arc<AppState>>>>) -> Arc<Self> {
        Arc::new(Self {
            states,
            lock: Mutex::new(()),
            submission_barrier: Arc::new(RwLock::new(())),
            committed_config: std::sync::RwLock::new(None),
            triggered: AtomicBool::new(false),
            blocked: AtomicBool::new(false),
            pending_fill_reconciliations: AtomicU64::new(0),
            fill_failure_generation: AtomicU64::new(0),
            fill_recovered_generation: AtomicU64::new(0),
            enabled: AtomicBool::new(false),
            membership_ready: AtomicBool::new(false),
            shutdown_requested: AtomicBool::new(false),
        })
    }

    async fn check_locked(&self) -> bool {
        let fill_generation = self.fill_failure_generation.load(Ordering::SeqCst);
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
        let margin_pct = config
            .min_available_margin_pct
            .filter(|pct| *pct > Decimal::ZERO);
        let margin_guard = config.pause_on_zero_available_margin || margin_pct.is_some();
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
            let mut member_incomplete = !initialized
                || !state.global_position_ready.load(Ordering::SeqCst)
                || state.paper_restore_failed.load(Ordering::SeqCst);
            let mut config = state.config.read().await.clone();
            if let Some(ref committed) = committed {
                committed.apply_to(&mut config);
            }
            let max_age = config
                .exchange
                .sync_interval_secs
                .saturating_mul(3)
                .max(30)
                .min(i64::MAX as u64) as i64;
            let now = Utc::now();
            let fresh = |time| {
                let age = now.signed_duration_since(time);
                age >= chrono::Duration::zero() && age <= chrono::Duration::seconds(max_age)
            };
            if margin_guard {
                let account = state.current_account().await;
                if account.asset.is_empty()
                    || (!config.exchange.is_paper() && !fresh(account.update_time))
                {
                    member_incomplete = true;
                } else if account.available_balance <= Decimal::ZERO
                    || margin_pct.is_some_and(|pct| {
                        account.margin_balance > Decimal::ZERO
                            && account.available_balance * Decimal::ONE_HUNDRED
                                <= pct * account.margin_balance
                    })
                {
                    zero_margin = true;
                }
            }
            if limit.is_none() {
                incomplete |= member_incomplete;
                initialized_incomplete |= initialized && member_incomplete;
                continue;
            }
            let position = state.position.read().await.clone();
            if !config.exchange.is_paper() && !position.update_time.is_some_and(fresh) {
                member_incomplete = true;
            }
            let ticker = state.ticker.read().await.clone();
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
        if !incomplete {
            self.fill_recovered_generation
                .store(fill_generation, Ordering::SeqCst);
        }
        let first_trigger = unsafe_now && !self.triggered.swap(true, Ordering::SeqCst);
        if !unsafe_now {
            self.triggered.store(false, Ordering::SeqCst);
        }
        if first_trigger {
            for state in &states {
                state.global_cancel_pending.store(true, Ordering::SeqCst);
            }
        }
        let mut reasons = Vec::new();
        if zero_margin {
            reasons.push("可用保证金已达到零或低于设定比例".to_string());
        }
        if incomplete {
            reasons.push("全局风控数据不可用或已过期".to_string());
        }
        if position_limit_reached {
            if let Some(limit_val) = limit {
                reasons.push(format!(
                    "全局持仓名义价值 {} 已达到上限 {}",
                    total, limit_val
                ));
            }
        }
        let reason = if reasons.is_empty() {
            String::new()
        } else {
            format!("{}，需手动恢复", reasons.join("；"))
        };
        for state in &states {
            if unsafe_now && (first_trigger || *state.status.read().await == BotStatus::Running) {
                state.global_cancel_pending.store(true, Ordering::SeqCst);
                if let Err(error) = Box::pin(state.pause_trading_with_reason(&reason)).await {
                    state
                        .add_log("ERROR", format!("全局风控暂停状态保存失败: {}", error))
                        .await;
                }
                state.add_log("WARN", &reason).await;
            }
            state.queue_global_cancel();
        }
        if first_trigger && config.telegram.enabled {
            let telegram = config.telegram.clone();
            let notifier = first.telegram_notifier.clone();
            let state = Arc::clone(first);
            tokio::spawn(async move {
                if let Err(error) = notifier
                    .send_global_risk_notification(&telegram, &reason)
                    .await
                {
                    warn!(
                        "Failed to send Telegram global risk notification: {}",
                        error
                    );
                    state
                        .add_log("WARN", format!("Telegram 全局风控通知发送失败: {}", error))
                        .await;
                }
            });
        }
        incomplete || position_limit_reached || zero_margin
    }

    pub(crate) fn initialize_config(&self, config: crate::config::GlobalConfig) {
        self.enabled.store(
            config
                .global_max_position_notional
                .is_some_and(|v| v > Decimal::ZERO)
                || config.pause_on_zero_available_margin
                || config
                    .min_available_margin_pct
                    .is_some_and(|pct| pct > Decimal::ZERO),
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

struct PendingSafetyPause<'a>(&'a AtomicU64);

impl Drop for PendingSafetyPause<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct LoginAttempts {
    by_source: HashMap<IpAddr, VecDeque<Instant>>,
    global: VecDeque<Instant>,
}

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
    login_failures: Mutex<LoginAttempts>,
    auth_work: Arc<Semaphore>,
    admin_auth: Option<Arc<crate::auth::AdminAuth>>,

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
        Self::build(
            config,
            db,
            action_tx,
            None,
            Arc::new(Mutex::new(())),
            cny_rates,
            admin_auth,
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
        Self::build(
            config,
            db,
            action_tx,
            Some(symbol),
            trade_commit_lock,
            cny_rates,
            admin_auth,
        )
    }

    fn build(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: mpsc::Sender<BotControlAction>,
        pair_scope: Option<String>,
        trade_commit_lock: Arc<Mutex<()>>,
        cny_rates: Arc<CnyRateCache>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
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
        }

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
            recent_logs: RwLock::new(VecDeque::with_capacity(300)),
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
            login_failures: Mutex::new(LoginAttempts::default()),
            auth_work: Arc::new(Semaphore::new(4)),
            admin_auth,
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

    async fn record_trade_inner(
        self: &Arc<Self>,
        trade: TradeRecord,
        cycle_profit: Option<Decimal>,
        pair_intent: Option<GridOrder>,
        consumed_pair_source: Option<String>,
        paper_state: Option<(PositionInfo, AccountInfo)>,
    ) -> bool {
        // Persist the trade and its cumulative statistics together.
        let current_config = self.config.read().await;
        let current_symbol = current_config.exchange.symbol.clone();
        let current_mode = current_config.exchange.trading_mode();
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
            let stored_paper_state = paper_state.clone();
            match self
                .db
                .run_blocking(move |db| {
                    db.insert_trade_with_paper_recovery(
                        &stored_trade,
                        &stored_stats,
                        pair_intent.as_ref(),
                        consumed_pair_source.as_deref(),
                        stored_paper_state.as_ref(),
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
            if let Some((position, account)) = paper_state {
                let mut current_position = self.position.write().await;
                let mut current_account = self.account.write().await;
                *current_position = position;
                *current_account = account;
                self.account_view(
                    &current_symbol,
                    current_mode,
                    &current_position,
                    &current_account,
                );
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
        true
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
            let _commit = commit;
            state
                .verify_trade_pnl_inner(trade, expected_clear_revision)
                .await
        })
        .await?
    }

    async fn verify_trade_pnl_inner(
        &self,
        trade: TradeRecord,
        expected_clear_revision: u64,
    ) -> anyhow::Result<()> {
        let stored_trade = trade.clone();
        if !self
            .db
            .run_blocking(move |db| db.verify_trade_pnl(&stored_trade, expected_clear_revision))
            .await?
        {
            return Ok(());
        }
        let config = self.config.read().await;
        let current_mode = config.exchange.trading_mode();
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

    pub async fn clear_trade_history(self: &Arc<Self>) -> anyhow::Result<usize> {
        let state = Arc::clone(self);
        tokio::spawn(async move {
            let _commit = state.trade_commit_lock.lock().await;
            let deleted = state.db.run_blocking(Database::clear_trade_history).await?;
            state.reset_trade_history_cache().await;
            Ok(deleted)
        })
        .await?
    }

    pub async fn clear_symbol_trade_history(
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

    pub async fn clear_trade_history_for(states: &[Arc<Self>]) -> anyhow::Result<usize> {
        Ok(Self::clear_selected_database_for(states, true, false, None)
            .await?
            .0)
    }

    pub async fn clear_selected_database_for(
        states: &[Arc<Self>],
        trades: bool,
        snapshots: bool,
        membership_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
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
            if trades {
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

    pub fn pair_scope(&self) -> Option<&str> {
        self.pair_scope.as_deref()
    }

    pub async fn save_runtime_config(self: &Arc<Self>, config: AppConfig) -> anyhow::Result<()> {
        let state = self.clone();
        tokio::spawn(async move { state.save_runtime_config_inner(config).await }).await?
    }

    async fn save_runtime_config_inner(&self, config: AppConfig) -> anyhow::Result<()> {
        // An immediate HTTP pause cannot slip between saving a new mode and installing it.
        let mut update = self.status_update_lock.lock().await;
        anyhow::ensure!(!self.is_retired(), "交易对已移除，无法更新配置");
        anyhow::ensure!(
            !self.shutdown_guard_cleared.load(Ordering::SeqCst),
            "策略已完成停机，无法更新配置"
        );
        let status = *self.status.read().await;
        let previous = self.config.read().await;
        let previous_symbol = previous.exchange.symbol.clone();
        let previous_mode = previous.exchange.trading_mode();
        drop(previous);
        let scope = self.pair_scope.clone();
        let saved = config.clone();
        let (saved_status, guarded) = self
            .db
            .run_blocking(move |db| {
                db.save_runtime_config(
                    &saved,
                    scope.as_deref(),
                    status,
                    (&previous_symbol, previous_mode),
                )
            })
            .await?;
        if guarded {
            self.pause_epoch.fetch_add(1, Ordering::SeqCst);
            self.safety_pause_epoch.fetch_add(1, Ordering::SeqCst);
            *update = None;
            *self.status.write().await = saved_status;
        }
        *self.config.write().await = config;
        Ok(())
    }

    async fn persist_status(&self, status: BotStatus) -> anyhow::Result<()> {
        let config = self.config.read().await;
        let symbol = config.exchange.symbol.clone();
        let mode = config.exchange.trading_mode();
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
    pub async fn check_token_valid(&self, token: &str) -> anyhow::Result<bool> {
        if !self.is_auth_configured() {
            return Ok(true);
        }
        if token.is_empty() {
            return Ok(false);
        }
        if self.db.session_recently_validated(token) {
            return Ok(true);
        }
        let token = token.to_string();
        self.db
            .run_blocking(move |db| db.is_session_valid(&token))
            .await
    }

    pub async fn is_token_valid(&self, token: &str) -> bool {
        self.check_token_valid(token).await.unwrap_or(false)
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

    pub async fn begin_login_attempt(&self, source: IpAddr) -> bool {
        let mut failures = self.login_failures.lock().await;
        failures.by_source.retain(|_, attempts| {
            attempts.retain(|time| time.elapsed() < Duration::from_secs(60));
            !attempts.is_empty()
        });
        failures
            .global
            .retain(|time| time.elapsed() < Duration::from_secs(60));
        if failures.global.len() >= 100 {
            return false;
        }
        // Active windows must not be evicted by churn from other sources.
        if !failures.by_source.contains_key(&source) && failures.by_source.len() >= 10_000 {
            return false;
        }
        let attempts = failures.by_source.entry(source).or_default();
        if attempts.len() >= 10 {
            return false;
        }
        let now = Instant::now();
        attempts.push_back(now);
        failures.global.push_back(now);
        true
    }

    pub async fn clear_login_attempts(&self, source: IpAddr) {
        self.login_failures.lock().await.by_source.remove(&source);
    }

    pub fn try_begin_auth_work(&self) -> Option<OwnedSemaphorePermit> {
        self.auth_work.clone().try_acquire_owned().ok()
    }

    pub async fn pause_trading(self: &Arc<Self>) -> anyhow::Result<()> {
        self.pause_trading_with_reason("策略运行异常，已暂停，请查看系统运行日志")
            .await
    }

    pub async fn pause_trading_with_reason(self: &Arc<Self>, _reason: &str) -> anyhow::Result<()> {
        let admission = self.safety_pause_admission.lock().await;
        anyhow::ensure!(
            !self.shutdown_guard_cleared.load(Ordering::SeqCst),
            "策略已完成停机，无法修改暂停状态"
        );
        self.safety_pauses_pending.fetch_add(1, Ordering::SeqCst);
        self.safety_pause_epoch.fetch_add(1, Ordering::SeqCst);
        self.pause_epoch.fetch_add(1, Ordering::SeqCst);
        drop(admission);
        let state = self.clone();
        tokio::spawn(async move {
            let _pending = PendingSafetyPause(&state.safety_pauses_pending);
            state.persist_safety_pause().await
        })
        .await?
    }

    async fn persist_safety_pause(self: &Arc<Self>) -> anyhow::Result<()> {
        let mut update = self.status_update_lock.lock().await;
        *update = None;
        *self.status.write().await = BotStatus::Paused;
        self.persist_status(BotStatus::Paused).await
    }

    pub async fn resume_trading(self: &Arc<Self>) -> anyhow::Result<()> {
        self.set_trading_status(BotStatus::Running, Some("手动恢复策略"))
            .await
    }

    pub(crate) async fn resume_trading_in_mode(
        self: &Arc<Self>,
        expected_mode: TradingMode,
        expected_epoch: u64,
    ) -> anyhow::Result<()> {
        let state = self.clone();
        tokio::spawn(async move {
            state
                .set_trading_status_inner(
                    BotStatus::Running,
                    Some(expected_epoch),
                    Some(expected_mode),
                )
                .await
        })
        .await?
    }

    pub(crate) async fn pause_for_shutdown(self: &Arc<Self>) -> anyhow::Result<()> {
        let state = self.clone();
        tokio::spawn(async move { state.pause_for_shutdown_inner().await }).await?
    }

    async fn pause_for_shutdown_inner(self: &Arc<Self>) -> anyhow::Result<()> {
        let global = self
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade);
        let _global_guard = if let Some(coordinator) = &global {
            let guard = coordinator.lock.lock().await;
            coordinator.check_locked().await;
            Some(guard)
        } else {
            None
        };
        self.pause_epoch.fetch_add(1, Ordering::SeqCst);
        let mut eligibility = self.status_update_lock.lock().await;
        if *self.status.read().await == BotStatus::Running {
            let blocked = global.as_ref().is_some_and(|coordinator| {
                coordinator.blocked.load(Ordering::SeqCst)
                    || (coordinator.enabled.load(Ordering::SeqCst)
                        && !coordinator.membership_ready.load(Ordering::SeqCst))
            });
            *eligibility = if self.global_position_initialized.load(Ordering::SeqCst)
                && self.global_position_ready.load(Ordering::SeqCst)
                && self.safety_pauses_pending.load(Ordering::SeqCst) == 0
                && !blocked
                && !self.global_cancel_pending.load(Ordering::SeqCst)
            {
                Some(self.safety_pause_epoch.load(Ordering::SeqCst))
            } else {
                None
            };
        }
        *self.status.write().await = BotStatus::Paused;
        if let Err(error) = self.persist_status(BotStatus::Paused).await {
            *eligibility = None;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn commit_shutdown_recovery(self: &Arc<Self>) -> anyhow::Result<()> {
        let state = self.clone();
        tokio::spawn(async move { state.commit_shutdown_recovery_inner().await }).await?
    }

    async fn commit_shutdown_recovery_inner(self: &Arc<Self>) -> anyhow::Result<()> {
        let global = self
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade);
        let _global_guard = if let Some(coordinator) = &global {
            let guard = coordinator.lock.lock().await;
            coordinator.check_locked().await;
            Some(guard)
        } else {
            None
        };
        let _admission = self.safety_pause_admission.lock().await;
        let mut eligibility = self.status_update_lock.lock().await;
        let blocked = global.as_ref().is_some_and(|coordinator| {
            coordinator.blocked.load(Ordering::SeqCst)
                || (coordinator.enabled.load(Ordering::SeqCst)
                    && !coordinator.membership_ready.load(Ordering::SeqCst))
        });
        // A pending pause must finish durably before a clean restart can be allowed.
        anyhow::ensure!(
            self.safety_pauses_pending.load(Ordering::SeqCst) == 0,
            "安全暂停仍在处理中，无法确认停机恢复状态"
        );
        if let Some(epoch) = eligibility.take().filter(|epoch| {
            *epoch == self.safety_pause_epoch.load(Ordering::SeqCst)
                && self.safety_pauses_pending.load(Ordering::SeqCst) == 0
                && !blocked
                && !self.global_cancel_pending.load(Ordering::SeqCst)
                && self.global_position_initialized.load(Ordering::SeqCst)
                && self.global_position_ready.load(Ordering::SeqCst)
        }) {
            self.persist_status(BotStatus::Running).await?;
            // A pause may have started and been cancelled while the database
            // worker was writing Running. Its epoch still requires a durable pause.
            if epoch != self.safety_pause_epoch.load(Ordering::SeqCst) {
                self.persist_status(BotStatus::Paused).await?;
            }
        }
        let config = self.config.read().await;
        let symbol = config.exchange.symbol.clone();
        let mode = config.exchange.trading_mode();
        drop(config);
        self.db
            .run_blocking(move |db| db.clear_execution_guard(&symbol, mode))
            .await?;
        self.shutdown_guard_cleared.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) async fn pause_trading_silently(self: &Arc<Self>) -> anyhow::Result<()> {
        self.set_trading_status(BotStatus::Paused, None).await
    }

    pub(crate) async fn resume_trading_silently(
        self: &Arc<Self>,
        expected_epoch: u64,
    ) -> anyhow::Result<()> {
        let state = self.clone();
        tokio::spawn(async move {
            state
                .set_trading_status_inner(BotStatus::Running, Some(expected_epoch), None)
                .await
        })
        .await?
    }

    async fn set_trading_status(
        self: &Arc<Self>,
        status: BotStatus,
        _reason: Option<&str>,
    ) -> anyhow::Result<()> {
        let state = self.clone();
        tokio::spawn(async move { state.set_trading_status_inner(status, None, None).await })
            .await?
    }

    async fn set_trading_status_inner(
        self: &Arc<Self>,
        status: BotStatus,
        expected_epoch: Option<u64>,
        expected_mode: Option<TradingMode>,
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
        let _update = self.status_update_lock.lock().await;
        if let Some(expected) = expected_mode {
            let config = self.config.read().await;
            let current = config.exchange.trading_mode();
            anyhow::ensure!(
                expected != TradingMode::Unknown && current == expected,
                "策略运行模式已改变，无法恢复策略"
            );
        }
        anyhow::ensure!(
            status != BotStatus::Running || !self.paper_restore_failed.load(Ordering::SeqCst),
            "模拟盘仓位和余额未能恢复，请核对数据并重启"
        );
        anyhow::ensure!(
            !self.shutdown_guard_cleared.load(Ordering::SeqCst),
            "策略已完成停机，无法修改运行状态"
        );
        anyhow::ensure!(
            expected_epoch.is_none_or(|epoch| self.pause_epoch.load(Ordering::SeqCst) == epoch),
            "策略暂停状态已改变，无法恢复策略"
        );
        anyhow::ensure!(
            status != BotStatus::Running || self.safety_pauses_pending.load(Ordering::SeqCst) == 0,
            "策略暂停正在处理中，无法恢复策略"
        );
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
        saved
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

    pub(crate) fn publish_fill_reconciliation(self: &Arc<Self>) -> PendingFillReconciliation {
        let global = self
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade);
        if let Some(global) = &global {
            global
                .pending_fill_reconciliations
                .fetch_add(1, Ordering::SeqCst);
        }
        PendingFillReconciliation {
            state: self.clone(),
            global,
            barrier: None,
            resolved: false,
        }
    }

    pub(crate) async fn begin_fill_reconciliation(self: &Arc<Self>) -> PendingFillReconciliation {
        let mut guard = self.publish_fill_reconciliation();
        guard.acquire().await;
        guard
    }

    pub(crate) fn try_exchange_submission(self: &Arc<Self>) -> Option<ExchangeSubmissionGuard> {
        let global = self
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade);
        let barrier = if let Some(global) = &global {
            if global.enabled.load(Ordering::SeqCst) {
                Some(global.submission_barrier.clone().try_read_owned().ok()?)
            } else {
                None
            }
        } else {
            None
        };
        if self.is_shutting_down()
            || self.global_cancel_pending.load(Ordering::SeqCst)
            || global.as_ref().is_some_and(|global| {
                global.enabled.load(Ordering::SeqCst)
                    && (global.blocked.load(Ordering::SeqCst)
                        || global.pending_fill_reconciliations.load(Ordering::SeqCst) != 0
                        || global.fill_failure_generation.load(Ordering::SeqCst)
                            != global.fill_recovered_generation.load(Ordering::SeqCst)
                        || !global.membership_ready.load(Ordering::SeqCst))
            })
        {
            return None;
        }
        Some(ExchangeSubmissionGuard {
            state: self.clone(),
            global,
            _barrier: barrier,
            completed: false,
        })
    }

    pub(crate) fn try_order_submission(&self) -> Option<OwnedRwLockReadGuard<()>> {
        // Skip replenishment instead of waiting: the engine must stay available for
        // configuration actions and cancellation while the coordinator holds the writer.
        if self.is_shutting_down() || self.global_cancel_pending.load(Ordering::SeqCst) {
            return None;
        }
        if self
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|global| {
                global.enabled.load(Ordering::SeqCst)
                    && (global.blocked.load(Ordering::SeqCst)
                        || global.pending_fill_reconciliations.load(Ordering::SeqCst) != 0
                        || global.fill_failure_generation.load(Ordering::SeqCst)
                            != global.fill_recovered_generation.load(Ordering::SeqCst)
                        || !global.membership_ready.load(Ordering::SeqCst))
            })
        {
            return None;
        }
        self.order_submission_lock.clone().try_read_owned().ok()
    }

    pub(crate) fn subscribe_retirement(&self) -> watch::Receiver<bool> {
        self.retired.subscribe()
    }

    pub(crate) fn retire(&self) {
        self.retired.send_replace(true);
    }

    pub(crate) fn is_retired(&self) -> bool {
        *self.retired.borrow()
    }

    pub fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.pause_epoch.fetch_add(1, Ordering::SeqCst);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
            || self
                .global_position_limit
                .get()
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|global| global.shutdown_requested.load(Ordering::SeqCst))
    }

    pub fn admin_auth(&self) -> Option<Arc<crate::auth::AdminAuth>> {
        self.admin_auth.clone()
    }

    pub fn is_auth_configured(&self) -> bool {
        self.admin_auth.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn login_global_limit_is_shared_and_expires_without_reset_on_success() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        let first = IpAddr::V4(std::net::Ipv4Addr::from(1));
        for _ in 0..10 {
            assert!(state.begin_login_attempt(first).await);
        }
        for _ in 0..10 {
            assert!(!state.begin_login_attempt(first).await);
        }
        state.clear_login_attempts(first).await;
        for i in 2..=91 {
            assert!(
                state
                    .begin_login_attempt(IpAddr::V4(std::net::Ipv4Addr::from(i)))
                    .await
            );
        }
        let other = IpAddr::V4(std::net::Ipv4Addr::from(100));
        assert!(!state.begin_login_attempt(other).await);
        state.clear_login_attempts(first).await;
        assert!(!state.begin_login_attempt(other).await);
        state.login_failures.lock().await.global[0] = Instant::now() - Duration::from_secs(61);
        assert!(state.begin_login_attempt(other).await);
    }

    #[tokio::test]
    async fn saturated_login_table_keeps_active_windows_until_expiry() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        let blocked = IpAddr::V4(std::net::Ipv4Addr::from(1));
        let new_source = IpAddr::V4(std::net::Ipv4Addr::from(10_001));
        {
            let mut attempts = state.login_failures.lock().await;
            for i in 1..=10_000 {
                let count = if i == 1 { 10 } else { 1 };
                attempts.by_source.insert(
                    IpAddr::V4(std::net::Ipv4Addr::from(i)),
                    std::iter::repeat_n(Instant::now(), count).collect(),
                );
            }
        }
        assert!(!state.begin_login_attempt(new_source).await);
        assert!(!state.begin_login_attempt(blocked).await);
        state.login_failures.lock().await.by_source.insert(
            IpAddr::V4(std::net::Ipv4Addr::from(2)),
            VecDeque::from([Instant::now() - Duration::from_secs(61)]),
        );
        assert!(state.begin_login_attempt(new_source).await);
        assert!(!state.begin_login_attempt(blocked).await);
    }

    #[tokio::test]
    async fn cancelled_paper_fill_finishes_publishing_before_retry_checks_history() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        let trade = cache_test_trade("cancel-paper-fill", 1, TradingMode::Paper);
        let position = PositionInfo {
            size: dec!(1),
            entry_price: dec!(100),
            ..Default::default()
        };
        let mut account = state.account.read().await.clone();
        account.available_balance = dec!(9900);
        let position_guard = state.position.write().await;
        let revision = db.trade_data_revision();
        let committing_state = state.clone();
        let committing_trade = trade.clone();
        let commit = tokio::spawn(async move {
            committing_state
                .record_trade_with_paper_state(
                    committing_trade,
                    None,
                    None,
                    None,
                    Some((position, account)),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while db.trade_data_revision() == revision {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        commit.abort();
        assert!(commit.await.unwrap_err().is_cancelled());
        assert!(state.trade_commit_lock.try_lock().is_err());
        let retry_state = state.clone();
        let retry_id = trade.client_order_id.clone();
        let retry = tokio::spawn(async move {
            retry_state
                .has_recorded_paper_order(retry_id, TradingMode::Paper)
                .await
                .unwrap()
        });
        drop(position_guard);
        assert!(tokio::time::timeout(Duration::from_secs(5), retry)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(state.position.read().await.size, dec!(1));
        assert_eq!(state.position.read().await.entry_price, dec!(100));
        assert_eq!(state.account.read().await.available_balance, dec!(9900));
        assert_eq!(state.stats.read().await.total_trades, 1);
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancelled_cleanup_finishes_all_cache_resets_before_releasing_commit_lock() {
        for cleanup_kind in 0..3 {
            let db = Arc::new(Database::open(":memory:").unwrap());
            let lock = Arc::new(Mutex::new(()));
            let mut config = AppConfig::default();
            config.exchange.symbol = "SOLUSDC".into();
            let (tx, _rx) = mpsc::channel(1);
            let first =
                AppState::new_for_pair(config.clone(), db.clone(), tx.clone(), lock.clone());
            config.exchange.symbol = "BTCUSDC".into();
            let second = AppState::new_for_pair(config, db.clone(), tx, lock);
            let trade = cache_test_trade("cancel-cleanup", 1, TradingMode::Paper);
            assert!(db.insert_trade(&trade).unwrap());
            first.stats.write().await.total_trades = 1;
            first.recent_trades.write().await.push_back(trade);
            second.stats.write().await.total_trades = 2;
            let stats_guard = first.stats.write().await;
            let revision = db.trade_data_revision();
            let states = vec![first.clone(), second.clone()];
            let cleanup = tokio::spawn(async move {
                match cleanup_kind {
                    0 => {
                        states[0]
                            .clear_symbol_trade_history("SOLUSDC".into())
                            .await
                            .unwrap();
                    }
                    1 => {
                        states[0].clear_trade_history().await.unwrap();
                    }
                    _ => {
                        AppState::clear_selected_database_for(&states, true, false, None)
                            .await
                            .unwrap();
                    }
                }
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while db.trade_data_revision() == revision {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            cleanup.abort();
            assert!(cleanup.await.unwrap_err().is_cancelled());
            assert!(first.trade_commit_lock.try_lock().is_err());
            drop(stats_guard);
            let _commit =
                tokio::time::timeout(Duration::from_secs(5), first.trade_commit_lock.lock())
                    .await
                    .unwrap();
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            assert_eq!(first.stats.read().await.total_trades, 0);
            assert!(first.recent_trades.read().await.is_empty());
            assert_eq!(
                second.stats.read().await.total_trades,
                if cleanup_kind == 2 { 0 } else { 2 }
            );
        }
    }

    #[tokio::test]
    async fn clearing_previous_symbol_preserves_current_symbol_cache() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let mut config = AppConfig::default();
        config.exchange.symbol = "BTCUSDC".into();
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(config, db.clone(), tx);
        let old = cache_test_trade("old-symbol", 1, TradingMode::Paper);
        let mut current = cache_test_trade("current-symbol", 2, TradingMode::Paper);
        current.symbol = "BTCUSDC".into();
        assert!(db.insert_trade(&old).unwrap());
        assert!(db.insert_trade(&current).unwrap());
        state.stats.write().await.total_trades = 1;
        state.recent_trades.write().await.push_back(current);
        assert_eq!(
            state
                .clear_symbol_trade_history("SOLUSDC".into())
                .await
                .unwrap()
                .0,
            1
        );
        assert_eq!(state.stats.read().await.total_trades, 1);
        assert_eq!(
            state.recent_trades.read().await[0].trade_id,
            "current-symbol"
        );
        assert_eq!(db.get_recent_trades(10).unwrap()[0].symbol, "BTCUSDC");
    }

    #[tokio::test]
    async fn shutdown_global_lock_timeout_persists_pause_after_database_write_finishes() {
        let directory =
            std::env::temp_dir().join(format!("shutdown-lock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        {
            let db = Arc::new(Database::open(&path).unwrap());
            let (tx, _rx) = mpsc::channel(1);
            let state = AppState::new(AppConfig::default(), db.clone(), tx);
            state.resume_trading().await.unwrap();
            let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![state.clone()])));
            state
                .global_position_limit
                .set(Arc::downgrade(&global))
                .ok()
                .unwrap();
            let guard = global.lock.lock().await;
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            let shutdown_state = state.clone();
            let mut shutdown = tokio::spawn(async move {
                let mut engine = tokio::spawn(std::future::pending::<()>());
                crate::lifecycle::stop_engine(
                    &shutdown_state,
                    &mut engine,
                    Duration::from_millis(30),
                )
                .await
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(150), &mut shutdown)
                    .await
                    .is_err()
            );
            connection.execute_batch("COMMIT").unwrap();
            assert!(shutdown.await.unwrap().is_err());
            drop(guard);
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn shutdown_timeout_waits_for_inflight_pause_before_failure_returns() {
        let directory =
            std::env::temp_dir().join(format!("shutdown-write-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        {
            let db = Arc::new(Database::open(&path).unwrap());
            let (tx, _rx) = mpsc::channel(1);
            let state = AppState::new(AppConfig::default(), db, tx);
            state.resume_trading().await.unwrap();
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            let shutdown_state = state.clone();
            let mut shutdown = tokio::spawn(async move {
                let mut engine = tokio::spawn(std::future::pending::<()>());
                crate::lifecycle::stop_engine(
                    &shutdown_state,
                    &mut engine,
                    Duration::from_millis(30),
                )
                .await
            });
            while state.status_update_lock.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(150), &mut shutdown)
                    .await
                    .is_err()
            );
            connection.execute_batch("COMMIT").unwrap();
            assert!(shutdown.await.unwrap().is_err());
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn cancelled_recovery_commit_finishes_before_safety_pause() {
        let directory =
            std::env::temp_dir().join(format!("cancelled-commit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        {
            let db = Arc::new(Database::open(&path).unwrap());
            let (tx, _rx) = mpsc::channel(1);
            let state = AppState::new(AppConfig::default(), db, tx);
            state
                .global_position_initialized
                .store(true, Ordering::SeqCst);
            state.global_position_ready.store(true, Ordering::SeqCst);
            state.resume_trading().await.unwrap();
            state.pause_for_shutdown().await.unwrap();
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            let commit_state = state.clone();
            let commit = tokio::spawn(async move { commit_state.commit_shutdown_recovery().await });
            while state.status_update_lock.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
            commit.abort();
            assert!(commit.await.unwrap_err().is_cancelled());
            let pause_state = state.clone();
            let pause = tokio::spawn(async move { pause_state.pause_trading().await });
            tokio::task::yield_now().await;
            connection.execute_batch("COMMIT").unwrap();
            assert!(pause.await.unwrap().is_err());
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn cancelled_pause_during_recovery_write_remains_paused_after_reopen() {
        let directory =
            std::env::temp_dir().join(format!("shutdown-cancelled-pause-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("bot.db");
        {
            let db = Arc::new(Database::open(&path).unwrap());
            let (tx, _rx) = mpsc::channel(1);
            let state = AppState::new(AppConfig::default(), db.clone(), tx);
            state
                .global_position_initialized
                .store(true, Ordering::SeqCst);
            state.global_position_ready.store(true, Ordering::SeqCst);
            state.resume_trading().await.unwrap();
            state.pause_for_shutdown().await.unwrap();
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            let commit_state = state.clone();
            let commit = tokio::spawn(async move { commit_state.commit_shutdown_recovery().await });
            // Acquiring the update lock precedes spawning the blocked write without yielding.
            while state.status_update_lock.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
            let pause_state = state.clone();
            let pause = tokio::spawn(async move { pause_state.pause_trading().await });
            tokio::task::yield_now().await;
            pause.abort();
            assert!(pause.await.unwrap_err().is_cancelled());
            connection.execute_batch("COMMIT").unwrap();
            commit.await.unwrap().unwrap();
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn shutdown_recovery_obeys_safety_pause_ordering() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = mpsc::channel(8);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);

        state.request_shutdown();
        state.pause_for_shutdown().await.unwrap();
        state.commit_shutdown_recovery().await.unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
        assert!(state.pause_trading().await.is_err());

        let (tx, _) = mpsc::channel(8);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
        state.resume_trading().await.unwrap();
        state.request_shutdown();
        state.pause_for_shutdown().await.unwrap();
        state.pause_trading().await.unwrap();
        state.commit_shutdown_recovery().await.unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }

    #[tokio::test]
    async fn interrupted_initialization_cannot_restore_running() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = mpsc::channel(8);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.request_shutdown();
        state.pause_for_shutdown().await.unwrap();
        state.commit_shutdown_recovery().await.unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }

    #[tokio::test]
    async fn legacy_and_multi_pair_formats_preserve_pause_and_manual_resume() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let mut config = AppConfig::default();
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
    async fn pending_safety_pause_prevents_silent_resume() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        state.resume_trading().await.unwrap();
        state.pause_trading_silently().await.unwrap();
        let epoch = state.pause_epoch();
        let guard = state.status_update_lock.lock().await;
        let pause_state = state.clone();
        let pause = tokio::spawn(async move { pause_state.pause_trading().await });
        while state.safety_pauses_pending.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_ne!(state.pause_epoch(), epoch);
        let resume_state = state.clone();
        let resume = tokio::spawn(async move { resume_state.resume_trading_silently(epoch).await });
        drop(guard);
        pause.await.unwrap().unwrap();
        assert!(resume.await.unwrap().is_err());
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }

    #[tokio::test]
    async fn manual_resume_waiting_for_status_lock_rejects_newer_pause() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        state.pause_trading().await.unwrap();
        let config = state.config.read().await;
        let mode = config.exchange.trading_mode();
        drop(config);
        let epoch = state.pause_epoch();
        let guard = state.status_update_lock.lock().await;
        let resume_state = state.clone();
        let resume =
            tokio::spawn(async move { resume_state.resume_trading_in_mode(mode, epoch).await });
        let pause_state = state.clone();
        let pause = tokio::spawn(async move { pause_state.pause_trading().await });
        while state.pause_epoch() == epoch {
            tokio::task::yield_now().await;
        }
        drop(guard);
        assert!(resume.await.unwrap().is_err());
        pause.await.unwrap().unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        state
            .resume_trading_in_mode(mode, state.pause_epoch())
            .await
            .unwrap();
        assert_eq!(*state.status.read().await, BotStatus::Running);
    }

    #[tokio::test]
    async fn cancelled_running_writes_finish_before_shutdown_pause() {
        for operation in 0..3 {
            let directory =
                std::env::temp_dir().join(format!("cancelled-running-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&directory).unwrap();
            let path = directory.join("bot.db");
            {
                let db = Arc::new(Database::open(&path).unwrap());
                let (tx, _rx) = mpsc::channel(1);
                let state = AppState::new(AppConfig::default(), db, tx);
                if operation == 2 {
                    state.resume_trading().await.unwrap();
                }
                let connection = rusqlite::Connection::open(&path).unwrap();
                connection.execute_batch("BEGIN IMMEDIATE").unwrap();
                let operation_state = state.clone();
                let task = tokio::spawn(async move {
                    match operation {
                        0 => operation_state.resume_trading().await,
                        1 => {
                            operation_state
                                .resume_trading_silently(operation_state.pause_epoch())
                                .await
                        }
                        _ => {
                            let config = operation_state.config.read().await.clone();
                            operation_state.save_runtime_config(config).await
                        }
                    }
                });
                while state.status_update_lock.try_lock().is_ok() {
                    tokio::task::yield_now().await;
                }
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                state.request_shutdown();
                let pause_state = state.clone();
                let pause = tokio::spawn(async move { pause_state.pause_trading().await });
                while state.safety_pauses_pending.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
                connection.execute_batch("COMMIT").unwrap();
                pause.await.unwrap().unwrap();
            }
            let db = Database::open(&path).unwrap();
            assert_eq!(
                db.load_bot_status().unwrap(),
                Some(BotStatus::Paused),
                "operation {operation}"
            );
            drop(db);
            std::fs::remove_dir_all(directory).unwrap();
        }
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
        live.exchange.api_key = "live-key".into();
        live.exchange.api_secret = "live-secret".into();
        let (committed, paused) =
            tokio::join!(state.save_runtime_config(live), state.pause_trading());
        committed.unwrap();
        paused.unwrap();
        assert!(!state.config.read().await.exchange.is_paper());
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Paused)
        );
    }

    #[tokio::test]
    async fn dirty_target_mode_pauses_until_manual_resume_without_self_guarding() {
        for legacy in [false, true] {
            let db = Arc::new(Database::open(":memory:").unwrap());
            let mut config = AppConfig::default();
            if !legacy {
                config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
            }
            db.save_config(&config).unwrap();
            let config = if legacy {
                config
            } else {
                config.strategy_configs().unwrap().remove(0)
            };
            assert!(!db
                .arm_execution_guard("SOLUSDC", TradingMode::Paper)
                .unwrap());
            assert!(!db
                .arm_execution_guard("SOLUSDC", TradingMode::Live)
                .unwrap());
            let (tx, _) = mpsc::channel(8);
            let state = if legacy {
                AppState::new(config.clone(), db.clone(), tx)
            } else {
                AppState::new_for_pair(config.clone(), db.clone(), tx, Arc::new(Mutex::new(())))
            };
            state.resume_trading().await.unwrap();
            let epoch = state.pause_epoch();
            state.save_runtime_config(config.clone()).await.unwrap();
            assert_eq!(*state.status.read().await, BotStatus::Running);
            assert_eq!(state.pause_epoch(), epoch);

            let mut live = config;
            live.exchange.api_key = "live-key".into();
            live.exchange.api_secret = "live-secret".into();
            state.save_runtime_config(live.clone()).await.unwrap();
            assert_eq!(*state.status.read().await, BotStatus::Paused);
            assert_eq!(
                db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
                Some(BotStatus::Paused)
            );
            if legacy {
                assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
            }
            assert!(state.resume_trading_silently(epoch).await.is_err());
            state.save_runtime_config(live).await.unwrap();
            assert_eq!(*state.status.read().await, BotStatus::Paused);
            state.resume_trading().await.unwrap();
            assert_eq!(*state.status.read().await, BotStatus::Running);
            assert_eq!(
                db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
                Some(BotStatus::Running)
            );
        }
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
        fixture.state.account.write().await.total_wallet_balance = Decimal::ZERO;
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
    async fn margin_ratio_guard_pauses_only_at_or_below_threshold() {
        let fixture = notification_fixture(false, false).await;
        {
            let mut config = fixture.state.config.write().await;
            config.min_available_margin_pct = Some(dec!(10));
            // Live accounts are read as stored; paper accounts come from the shared ledger.
            config.exchange.api_key = "key".into();
            config.exchange.api_secret = "secret".into();
        }
        fixture
            .state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        fixture
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
        global.membership_ready.store(true, Ordering::SeqCst);
        {
            let mut account = fixture.state.account.write().await;
            account.asset = "USDC".into();
            account.margin_balance = dec!(1000);
            account.available_balance = dec!(101);
            account.update_time = Utc::now();
        }
        assert!(!global.check().await);
        assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
        fixture.state.account.write().await.available_balance = dec!(100);
        assert!(global.check().await);
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
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
        second.state.account.write().await.total_wallet_balance = Decimal::ZERO;
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
        second.state.account.write().await.total_wallet_balance = dec!(1);
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
        {
            let mut config = fixture.state.config.write().await;
            config.exchange.api_key = "live-key".into();
            config.exchange.api_secret = "live-secret".into();
        }
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
        fixture.state.account.write().await.total_wallet_balance = -Decimal::ONE;
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

    #[tokio::test]
    async fn zero_margin_guard_uses_combined_paper_positions() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let commit = Arc::new(Mutex::new(()));
        let mut states = Vec::new();
        let mut receivers = Vec::new();
        for (symbol, size, price) in [
            ("SOLUSDC", dec!(60), dec!(100)),
            ("BNBUSDC", dec!(-20), dec!(200)),
        ] {
            let mut config = AppConfig::default();
            config.exchange.symbol = symbol.into();
            config.pause_on_zero_available_margin = true;
            let (tx, rx) = mpsc::channel(8);
            let state = AppState::new_for_pair(config, db.clone(), tx, commit.clone());
            *state.position.write().await = PositionInfo {
                symbol: symbol.into(),
                size,
                entry_price: price,
                mark_price: price,
                ..Default::default()
            };
            state.account.write().await.available_balance = dec!(10000) - size.abs() * price;
            state
                .global_position_initialized
                .store(true, Ordering::SeqCst);
            state.global_position_ready.store(true, Ordering::SeqCst);
            states.push(state);
            receivers.push(rx);
        }
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(states.clone())));
        global.membership_ready.store(true, Ordering::SeqCst);
        assert!(global.check().await);
        for state in states {
            assert_eq!(*state.status.read().await, BotStatus::Paused);
            assert_eq!(
                state.current_account().await.available_balance,
                Decimal::ZERO
            );
        }
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
    async fn global_risk_sends_one_notification_per_trigger_across_pairs() {
        let mut first = notification_fixture(true, false).await;
        let mut second = notification_fixture(true, false).await;
        first
            .state
            .config
            .write()
            .await
            .global_max_position_notional = Some(dec!(100));
        first.state.position.write().await.size = dec!(2);
        {
            let mut ticker = first.state.ticker.write().await;
            ticker.mark_price = dec!(50);
            ticker.mark_update_time = Utc::now();
        }
        let states = Arc::new(RwLock::new(vec![first.state.clone(), second.state.clone()]));
        let global = GlobalPositionLimit::new(states);
        global.membership_ready.store(true, Ordering::SeqCst);
        for state in [&first.state, &second.state] {
            state
                .global_position_initialized
                .store(true, Ordering::SeqCst);
            state.global_position_ready.store(true, Ordering::SeqCst);
        }
        assert!(global.check().await);
        let message = next_notification(&mut first).await;
        assert!(message["text"]
            .as_str()
            .unwrap()
            .contains("全局持仓名义价值 100 已达到上限 100"));
        assert_eq!(*first.state.status.read().await, BotStatus::Paused);
        assert_eq!(*second.state.status.read().await, BotStatus::Paused);
        assert!(global.check().await);
        no_more_notifications(&mut first).await;
        no_more_notifications(&mut second).await;
        first.state.position.write().await.size = Decimal::ZERO;
        assert!(!global.check().await);
        first.state.position.write().await.size = dec!(2);
        assert!(global.check().await);
        next_notification(&mut first).await;
        no_more_notifications(&mut second).await;
        first.server.abort();
        second.server.abort();
    }

    #[tokio::test]
    async fn global_risk_notification_reports_limit_when_another_pair_is_initializing() {
        let mut first = notification_fixture(true, false).await;
        let second = notification_fixture(true, false).await;
        first
            .state
            .config
            .write()
            .await
            .global_max_position_notional = Some(dec!(100));
        first.state.position.write().await.size = dec!(2);
        {
            let mut ticker = first.state.ticker.write().await;
            ticker.mark_price = dec!(50);
            ticker.mark_update_time = Utc::now();
        }
        first
            .state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        first
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
            first.state.clone(),
            second.state.clone(),
        ])));
        global.membership_ready.store(true, Ordering::SeqCst);
        assert!(global.check().await);
        let message = next_notification(&mut first).await;
        let text = message["text"].as_str().unwrap();
        assert!(text.contains("全局持仓名义价值 100 已达到上限 100"));
        assert!(text.contains("全局风控数据不可用或已过期"));
        first.server.abort();
        second.server.abort();
    }

    #[tokio::test]
    async fn global_risk_notification_failure_does_not_prevent_pause() {
        let mut fixture = notification_fixture(true, true).await;
        fixture
            .state
            .config
            .write()
            .await
            .pause_on_zero_available_margin = true;
        fixture.state.account.write().await.asset = "USDC".into();
        fixture.state.account.write().await.total_wallet_balance = Decimal::ZERO;
        fixture
            .state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        fixture
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
        global.membership_ready.store(true, Ordering::SeqCst);
        let mut logs = fixture.state.ws_broadcast_tx.subscribe();
        assert!(global.check().await);
        let message = next_notification(&mut fixture).await;
        assert!(message["text"]
            .as_str()
            .unwrap()
            .contains("可用保证金已达到零"));
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
        assert!(fixture.state.global_cancel_pending.load(Ordering::SeqCst));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let log = logs.recv().await.unwrap();
                if log.contains("Telegram 全局风控通知发送失败") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        fixture.server.abort();
    }

    #[tokio::test]
    async fn strategy_status_changes_do_not_send_notifications() {
        let mut fixture = notification_fixture(true, false).await;
        fixture
            .state
            .pause_trading_with_reason("手动暂停策略")
            .await
            .unwrap();
        assert_eq!(
            fixture.state.db.load_bot_status().unwrap(),
            Some(BotStatus::Paused)
        );
        fixture.state.resume_trading().await.unwrap();
        fixture.state.pause_trading_silently().await.unwrap();
        fixture
            .state
            .pause_trading_with_reason("撤单失败")
            .await
            .unwrap();
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
        no_more_notifications(&mut fixture).await;
        fixture.server.abort();
    }

    #[tokio::test]
    async fn engine_start_and_position_limit_pause_do_not_notify() {
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
            if at_limit {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while *fixture.state.status.read().await != BotStatus::Paused {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            } else {
                assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
            }
            no_more_notifications(&mut fixture).await;
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
        // The engine constructs its initial window asynchronously. Start the
        // notification deadline only after both fillable orders exist.
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let orders = fixture.state.active_orders.read().await;
                let buy = orders
                    .values()
                    .any(|order| order.side == OrderSide::Buy && order.price == dec!(99.9));
                let sell = orders
                    .values()
                    .any(|order| order.side == OrderSide::Sell && order.price == dec!(100.1));
                if buy && sell {
                    break;
                }
                drop(orders);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            ready.is_ok(),
            "initial orders missing; engine_finished={}; status={:?}; orders={:?}; logs={:?}",
            task.is_finished(),
            fixture.state.status.try_read(),
            fixture.state.active_orders.try_read(),
            fixture.state.recent_logs.try_read(),
        );
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
            let message = match tokio::time::timeout(Duration::from_secs(1), fixture.requests.recv())
                .await
            {
                Ok(Some(message)) => message,
                result => panic!(
                    "{side} notification missing: {result:?}; engine_finished={}; status={:?}; orders={:?}; trades={:?}; logs={:?}",
                    task.is_finished(),
                    fixture.state.status.try_read(),
                    fixture.state.active_orders.try_read(),
                    fixture.state.recent_trades.try_read(),
                    fixture.state.recent_logs.try_read(),
                ),
            };
            assert_eq!(message["chat_id"], "12345");
            assert_eq!(message["parse_mode"], "HTML");
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
        state.config.write().await.exchange.api_key = "live-key".into();
        state.config.write().await.exchange.api_secret = "live-secret".into();
        state.refresh_current_scope().await;
        let live = state.recent_trades.read().await;
        assert_eq!(live.len(), 200);
        assert_eq!(live.front().unwrap().trade_id, "live-5");
        assert_eq!(live.back().unwrap().trade_id, "live-204");
        drop(live);
        state.config.write().await.exchange.api_key.clear();
        state.config.write().await.exchange.api_secret.clear();
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
    async fn canceled_verification_request_still_publishes_its_committed_result() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.save_config(&AppConfig::default()).unwrap();
        let (tx, _) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        let mut trade = cache_test_trade("cancel-verification", 1, TradingMode::Paper);
        trade.pnl_verified = false;
        trade.realized_pnl = Decimal::ZERO;
        trade.commission = Decimal::ZERO;
        assert!(state.record_trade(trade.clone(), None, None, None).await);
        let clear_revision = db.symbol_trade_clear_revision(&trade.symbol);
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        db.set_membership_commit_hook("add", reached_tx, resume_rx);
        let writer = tokio::task::spawn_blocking({
            let db = db.clone();
            move || {
                let mut config = AppConfig::default();
                config.exchange.symbol = "ETHUSDT".into();
                db.add_pair_config(&config)
            }
        });
        reached_rx.await.unwrap();
        trade.realized_pnl = dec!(3.25);
        trade.commission = dec!(0.02);
        trade.pnl_verified = true;
        let request = tokio::spawn({
            let state = state.clone();
            async move { state.verify_trade_pnl(trade, clear_revision).await }
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while state.trade_commit_lock.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        resume_tx.send(()).unwrap();
        writer.await.unwrap().unwrap();
        let _commit = tokio::time::timeout(Duration::from_secs(3), state.trade_commit_lock.lock())
            .await
            .unwrap();
        let stats = state.stats.read().await;
        assert_eq!(stats.total_realized_pnl, dec!(3.25));
        assert_eq!(stats.total_commission, dec!(0.02));
        assert_eq!(stats.pending_pnl_trades, 0);
        let recent = state.recent_trades.read().await;
        assert!(recent[0].pnl_verified);
        assert_eq!(recent[0].realized_pnl, dec!(3.25));
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
            (dec!(3.25), dec!(0.02), 0)
        );
    }

    #[tokio::test]
    async fn canceled_verification_waiter_never_commits_after_the_lock_is_released() {
        use std::future::Future;
        use std::task::Poll;

        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        let mut trade = cache_test_trade("cancel-waiting-verification", 1, TradingMode::Paper);
        assert!(state.record_trade(trade.clone(), None, None, None).await);
        let clear_revision = db.symbol_trade_clear_revision(&trade.symbol);
        trade.realized_pnl = dec!(3.25);
        trade.commission = dec!(0.02);
        trade.pnl_verified = true;

        let commit = state.trade_commit_lock.lock().await;
        let (waiting_tx, waiting_rx) = tokio::sync::oneshot::channel();
        let request = tokio::spawn({
            let state = state.clone();
            async move {
                let verification = state.verify_trade_pnl(trade, clear_revision);
                tokio::pin!(verification);
                std::future::poll_fn(|cx| {
                    assert!(verification.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                waiting_tx.send(()).unwrap();
                verification.await
            }
        });
        waiting_rx.await.unwrap();
        tokio::task::yield_now().await;
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        drop(commit);

        let _commit = tokio::time::timeout(Duration::from_secs(3), state.trade_commit_lock.lock())
            .await
            .unwrap();
        let stats = state.stats.read().await;
        assert_eq!(stats.total_realized_pnl, Decimal::ZERO);
        assert_eq!(stats.total_commission, Decimal::ZERO);
        assert_eq!(stats.pending_pnl_trades, 1);
        let recent = state.recent_trades.read().await;
        assert!(!recent[0].pnl_verified);
        assert_eq!(recent[0].realized_pnl, Decimal::ZERO);
        assert_eq!(recent[0].commission, Decimal::ZERO);
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
            (Decimal::ZERO, Decimal::ZERO, 1)
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
        let clear_revision = state.db.symbol_trade_clear_revision(&verified.symbol);
        state
            .verify_trade_pnl(verified, clear_revision)
            .await
            .unwrap();
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

        state.config.write().await.exchange.api_key = "live-key".into();
        state.config.write().await.exchange.api_secret = "live-secret".into();
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

    #[tokio::test]
    async fn retired_verification_is_left_for_the_replacement_state() {
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.save_config(&config).unwrap();
        let commit = Arc::new(Mutex::new(()));
        let (tx, _) = mpsc::channel(1);
        let retired = AppState::new_for_pair(config.clone(), db.clone(), tx, commit.clone());
        let mut trade = cache_test_trade("retired-verification", 1, TradingMode::Paper);
        trade.pnl_verified = false;
        trade.realized_pnl = Decimal::ZERO;
        trade.commission = Decimal::ZERO;
        assert!(retired.record_trade(trade.clone(), None, None, None).await);
        let revision = db.symbol_trade_clear_revision(&trade.symbol);
        trade.realized_pnl = dec!(3.25);
        trade.commission = dec!(0.02);
        trade.pnl_verified = true;

        retired.retire();
        retired.drain_trade_commits().await;
        retired
            .verify_trade_pnl(trade.clone(), revision)
            .await
            .unwrap();
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
            (Decimal::ZERO, Decimal::ZERO, 1)
        );
        assert!(!retired.recent_trades.read().await[0].pnl_verified);

        let (tx, _) = mpsc::channel(1);
        let replacement = AppState::new_for_pair(config, db.clone(), tx, commit);
        replacement.verify_trade_pnl(trade, revision).await.unwrap();
        let stats = replacement.stats.read().await;
        assert_eq!(stats.total_realized_pnl, dec!(3.25));
        assert_eq!(stats.total_commission, dec!(0.02));
        assert_eq!(stats.pending_pnl_trades, 0);
        assert!(replacement.recent_trades.read().await[0].pnl_verified);
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
            (dec!(3.25), dec!(0.02), 0)
        );
    }
}
