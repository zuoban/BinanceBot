use super::AppState;
use crate::types::{BotControlAction, BotStatus};
use chrono::Utc;
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use tracing::warn;

/// Coordinates the filled-position limit across all currently configured strategies.
pub(crate) struct GlobalPositionLimit {
    pub(crate) states: Arc<RwLock<Vec<Arc<AppState>>>>,
    pub(super) lock: Mutex<()>,
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

    async fn resolve_risk_thresholds(
        first: &AppState,
        committed_ref: Option<&crate::config::GlobalConfig>,
    ) -> (Option<Decimal>, Option<Decimal>, bool) {
        let (
            global_max_position_notional,
            min_available_margin_pct,
            pause_on_zero_available_margin,
        ) = match committed_ref {
            Some(committed) => (
                committed.global_max_position_notional,
                committed.min_available_margin_pct,
                committed.pause_on_zero_available_margin,
            ),
            None => {
                let config = first.config.read().await;
                (
                    config.global_max_position_notional,
                    config.min_available_margin_pct,
                    config.pause_on_zero_available_margin,
                )
            }
        };
        let limit = global_max_position_notional.filter(|v| *v > Decimal::ZERO);
        let margin_pct = min_available_margin_pct.filter(|pct| *pct > Decimal::ZERO);
        let margin_guard = pause_on_zero_available_margin || margin_pct.is_some();
        (limit, margin_pct, margin_guard)
    }

    pub(super) async fn check_locked(&self) -> bool {
        let fill_generation = self.fill_failure_generation.load(Ordering::SeqCst);
        let states = self.states.read().await.clone();
        let Some(first) = states.first() else {
            return self.enabled.load(Ordering::SeqCst)
                && !self.membership_ready.load(Ordering::SeqCst);
        };
        let committed = self
            .committed_config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let committed_ref = committed.as_ref();
        let (limit, margin_pct, margin_guard) =
            Self::resolve_risk_thresholds(first, committed_ref).await;
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
            let (member_incomplete, member_init_incomplete, member_zero_margin) =
                Self::evaluate_member_risk(
                    state,
                    committed_ref,
                    margin_guard,
                    margin_pct,
                    limit,
                    &mut total,
                )
                .await;
            incomplete |= member_incomplete;
            initialized_incomplete |= member_init_incomplete;
            zero_margin |= member_zero_margin;
        }
        let position_limit_reached = limit.is_some_and(|limit| total >= limit);
        let unsafe_now = initialized_incomplete || position_limit_reached || zero_margin;
        let blocked = incomplete || position_limit_reached || zero_margin;
        self.blocked.store(blocked, Ordering::SeqCst);
        if !incomplete {
            self.fill_recovered_generation
                .store(fill_generation, Ordering::SeqCst);
        }
        let reason = if unsafe_now {
            Self::format_global_risk_reason(
                zero_margin,
                incomplete,
                position_limit_reached,
                total,
                limit,
            )
        } else {
            String::new()
        };
        self.apply_risk_outcome(&states, first, committed_ref, unsafe_now, reason)
            .await;
        blocked
    }

    async fn apply_risk_outcome(
        &self,
        states: &[Arc<AppState>],
        first: &Arc<AppState>,
        committed_ref: Option<&crate::config::GlobalConfig>,
        unsafe_now: bool,
        reason: String,
    ) {
        let first_trigger = unsafe_now && !self.triggered.swap(true, Ordering::SeqCst);
        if !unsafe_now {
            self.triggered.store(false, Ordering::SeqCst);
        }
        if first_trigger {
            for state in states {
                state.global_cancel_pending.store(true, Ordering::SeqCst);
            }
        }
        for state in states {
            if unsafe_now && (first_trigger || *state.status.read().await == BotStatus::Running) {
                state.global_cancel_pending.store(true, Ordering::SeqCst);
                if let Err(error) = Box::pin(state.pause_trading()).await {
                    state
                        .add_log("ERROR", format!("全局风控暂停状态保存失败: {}", error))
                        .await;
                }
                state.add_log("WARN", &reason).await;
            }
            state.queue_global_cancel();
        }
        if first_trigger {
            Self::spawn_global_risk_notification(first, committed_ref, reason).await;
        }
    }

    async fn evaluate_member_margin(
        state: &AppState,
        is_paper: bool,
        fresh: impl Fn(chrono::DateTime<Utc>) -> bool,
        margin_pct: Option<Decimal>,
    ) -> (bool, bool) {
        let account = state.current_account().await;
        if account.asset.is_empty() || (!is_paper && !fresh(account.update_time)) {
            return (true, false);
        }
        if account.available_balance <= Decimal::ZERO {
            return (false, true);
        }
        if let Some(pct) = margin_pct {
            if account.margin_balance > Decimal::ZERO {
                return match (
                    account.available_balance.checked_mul(Decimal::ONE_HUNDRED),
                    pct.checked_mul(account.margin_balance),
                ) {
                    (Some(lhs), Some(rhs)) => (false, lhs <= rhs),
                    _ => (true, false),
                };
            }
        }
        (false, false)
    }

    async fn evaluate_member_risk(
        state: &Arc<AppState>,
        committed_ref: Option<&crate::config::GlobalConfig>,
        margin_guard: bool,
        margin_pct: Option<Decimal>,
        limit: Option<Decimal>,
        total: &mut Decimal,
    ) -> (bool, bool, bool) {
        let initialized = state.global_position_initialized.load(Ordering::SeqCst);
        let mut member_incomplete = !initialized
            || !state.global_position_ready.load(Ordering::SeqCst)
            || state.paper_restore_failed.load(Ordering::SeqCst);
        let (sync_interval_secs, is_paper) = match committed_ref {
            Some(committed) => (
                committed.sync_interval_secs,
                committed.api_key.trim().is_empty() && committed.api_secret.trim().is_empty(),
            ),
            None => {
                let config = state.config.read().await;
                (
                    config.exchange.sync_interval_secs,
                    config.exchange.is_paper(),
                )
            }
        };
        let max_age = crate::types::max_reconciliation_age_secs(sync_interval_secs);
        let now = Utc::now();
        let fresh = |time| crate::types::is_timestamp_fresh(time, now, max_age);
        let mut zero_margin = false;
        if margin_guard {
            let (margin_incomplete, margin_zero) =
                Self::evaluate_member_margin(state, is_paper, fresh, margin_pct).await;
            member_incomplete |= margin_incomplete;
            zero_margin |= margin_zero;
        }
        if limit.is_none() {
            return (
                member_incomplete,
                initialized && member_incomplete,
                zero_margin,
            );
        }
        let (position_size, price) = {
            let position = state.position.read().await;
            if !is_paper && !position.update_time.is_some_and(fresh) {
                member_incomplete = true;
            }
            let ticker = state.ticker.read().await;
            (
                position.size,
                position.fresh_market_valuation_price(&ticker, fresh),
            )
        };
        if !position_size.is_zero() && price <= Decimal::ZERO {
            member_incomplete = true;
        }
        if price > Decimal::ZERO {
            match position_size
                .abs()
                .checked_mul(price)
                .and_then(|v| total.checked_add(v))
            {
                Some(value) => *total = value,
                None => member_incomplete = true,
            }
        }
        (
            member_incomplete,
            initialized && member_incomplete,
            zero_margin,
        )
    }

    fn format_global_risk_reason(
        zero_margin: bool,
        incomplete: bool,
        position_limit_reached: bool,
        total: Decimal,
        limit: Option<Decimal>,
    ) -> String {
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
        if reasons.is_empty() {
            String::new()
        } else {
            format!("{}，需手动恢复", reasons.join("；"))
        }
    }

    async fn spawn_global_risk_notification(
        first: &Arc<AppState>,
        committed_ref: Option<&crate::config::GlobalConfig>,
        reason: String,
    ) {
        let telegram = match committed_ref {
            Some(committed) => committed.telegram.clone(),
            None => first.config.read().await.telegram.clone(),
        };
        if telegram.enabled {
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
        *self
            .committed_config
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(config);
    }

    pub(crate) async fn commit_config(&self, config: crate::config::GlobalConfig) {
        let _guard = self.lock.lock().await;
        self.initialize_config(config);
        self.check_locked().await;
    }

    pub(crate) async fn check(&self) -> bool {
        if !self.enabled.load(Ordering::SeqCst)
            && !self.blocked.load(Ordering::SeqCst)
            && !self.triggered.load(Ordering::SeqCst)
            && self
                .committed_config
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .is_some()
        {
            return false;
        }
        let _guard = self.lock.lock().await;
        self.check_locked().await
    }

    fn blocks_submission(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
            && (self.blocked.load(Ordering::SeqCst)
                || self.pending_fill_reconciliations.load(Ordering::SeqCst) != 0
                || self.fill_failure_generation.load(Ordering::SeqCst)
                    != self.fill_recovered_generation.load(Ordering::SeqCst)
                || !self.membership_ready.load(Ordering::SeqCst))
    }

    pub(super) fn is_blocked_or_unready(&self) -> bool {
        self.blocked.load(Ordering::SeqCst)
            || (self.enabled.load(Ordering::SeqCst)
                && !self.membership_ready.load(Ordering::SeqCst))
    }
}

impl AppState {
    pub(crate) fn global_limit(&self) -> Option<Arc<GlobalPositionLimit>> {
        self.global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade)
    }

    pub(crate) async fn check_global_position_limit(&self) {
        if let Some(global) = self.global_limit() {
            global.check().await;
        }
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
        let global = self.global_limit();
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
        let global = self.global_limit();
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
            || global
                .as_ref()
                .is_some_and(|global| global.blocks_submission())
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
            .global_limit()
            .is_some_and(|global| global.blocks_submission())
        {
            return None;
        }
        self.order_submission_lock.clone().try_read_owned().ok()
    }
}
