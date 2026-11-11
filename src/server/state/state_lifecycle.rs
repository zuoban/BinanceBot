use super::{AppState, GlobalPositionLimit};
use crate::config::AppConfig;
use crate::types::{BotStatus, TradingMode};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

struct PendingSafetyPause<'a>(&'a AtomicU64);

impl Drop for PendingSafetyPause<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl AppState {
    async fn run_detached<T, F, Fut>(self: &Arc<Self>, f: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        let state = Arc::clone(self);
        tokio::spawn(async move { f(state).await }).await?
    }

    async fn lock_and_check_global(
        global: &Option<Arc<GlobalPositionLimit>>,
    ) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        let coordinator = global.as_deref()?;
        let guard = coordinator.lock.lock().await;
        coordinator.check_locked().await;
        Some(guard)
    }

    pub(crate) async fn save_runtime_config(
        self: &Arc<Self>,
        config: AppConfig,
    ) -> anyhow::Result<()> {
        self.run_detached(|state| async move { state.save_runtime_config_inner(config).await })
            .await
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
        let credentials_changed = previous.exchange.api_key != config.exchange.api_key
            || previous.exchange.api_secret != config.exchange.api_secret;
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
                    credentials_changed,
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

    pub async fn pause_trading(self: &Arc<Self>) -> anyhow::Result<()> {
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
        self.set_trading_status(BotStatus::Running).await
    }

    pub(crate) async fn resume_trading_in_mode(
        self: &Arc<Self>,
        expected_mode: TradingMode,
        expected_epoch: u64,
    ) -> anyhow::Result<()> {
        self.run_detached(move |state| async move {
            state
                .set_trading_status_inner(
                    BotStatus::Running,
                    Some(expected_epoch),
                    Some(expected_mode),
                )
                .await
        })
        .await
    }

    pub(crate) async fn pause_for_shutdown(self: &Arc<Self>) -> anyhow::Result<()> {
        self.run_detached(|state| async move { state.pause_for_shutdown_inner().await })
            .await
    }

    fn shutdown_recovery_eligible(&self, global: Option<&GlobalPositionLimit>) -> bool {
        let blocked = global.is_some_and(GlobalPositionLimit::is_blocked_or_unready);
        self.global_position_initialized.load(Ordering::SeqCst)
            && self.global_position_ready.load(Ordering::SeqCst)
            && self.safety_pauses_pending.load(Ordering::SeqCst) == 0
            && !blocked
            && !self.global_cancel_pending.load(Ordering::SeqCst)
    }

    async fn pause_for_shutdown_inner(self: &Arc<Self>) -> anyhow::Result<()> {
        let global = self.global_limit();
        let _global_guard = Self::lock_and_check_global(&global).await;
        self.pause_epoch.fetch_add(1, Ordering::SeqCst);
        let mut eligibility = self.status_update_lock.lock().await;
        if *self.status.read().await == BotStatus::Running {
            *eligibility = self
                .shutdown_recovery_eligible(global.as_deref())
                .then(|| self.safety_pause_epoch.load(Ordering::SeqCst));
        }
        *self.status.write().await = BotStatus::Paused;
        if let Err(error) = self.persist_status(BotStatus::Paused).await {
            *eligibility = None;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn commit_shutdown_recovery(self: &Arc<Self>) -> anyhow::Result<()> {
        self.run_detached(|state| async move { state.commit_shutdown_recovery_inner().await })
            .await
    }

    async fn commit_shutdown_recovery_inner(self: &Arc<Self>) -> anyhow::Result<()> {
        let global = self.global_limit();
        let _global_guard = Self::lock_and_check_global(&global).await;
        let _admission = self.safety_pause_admission.lock().await;
        let mut eligibility = self.status_update_lock.lock().await;
        // A pending pause must finish durably before a clean restart can be allowed.
        anyhow::ensure!(
            self.safety_pauses_pending.load(Ordering::SeqCst) == 0,
            "安全暂停仍在处理中，无法确认停机恢复状态"
        );
        if let Some(epoch) = eligibility.take().filter(|epoch| {
            *epoch == self.safety_pause_epoch.load(Ordering::SeqCst)
                && self.shutdown_recovery_eligible(global.as_deref())
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
        self.set_trading_status(BotStatus::Paused).await
    }

    pub(crate) async fn resume_trading_silently(
        self: &Arc<Self>,
        expected_epoch: u64,
    ) -> anyhow::Result<()> {
        self.run_detached(move |state| async move {
            state
                .set_trading_status_inner(BotStatus::Running, Some(expected_epoch), None)
                .await
        })
        .await
    }

    async fn set_trading_status(self: &Arc<Self>, status: BotStatus) -> anyhow::Result<()> {
        self.run_detached(move |state| async move {
            state.set_trading_status_inner(status, None, None).await
        })
        .await
    }

    async fn set_trading_status_inner(
        self: &Arc<Self>,
        status: BotStatus,
        expected_epoch: Option<u64>,
        expected_mode: Option<TradingMode>,
    ) -> anyhow::Result<()> {
        let global = (status == BotStatus::Running)
            .then(|| self.global_limit())
            .flatten();
        let _global_guard = if let Some(coordinator) = &global {
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
        self.validate_status_transition(status, expected_epoch, expected_mode)
            .await?;
        if status == BotStatus::Paused {
            // An unsafe strategy must stop even if persistence fails.
            *self.status.write().await = status;
            self.persist_status(status).await
        } else {
            self.persist_status(status).await?;
            *self.status.write().await = status;
            Ok(())
        }
    }

    async fn validate_status_transition(
        &self,
        status: BotStatus,
        expected_epoch: Option<u64>,
        expected_mode: Option<TradingMode>,
    ) -> anyhow::Result<()> {
        if let Some(expected) = expected_mode {
            let current = self.config.read().await.exchange.trading_mode();
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
        Ok(())
    }

    pub(crate) fn pause_epoch(&self) -> u64 {
        self.pause_epoch.load(Ordering::SeqCst)
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

    pub(crate) fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.pause_epoch.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
            || self
                .global_limit()
                .is_some_and(|global| global.shutdown_requested.load(Ordering::SeqCst))
    }
}
