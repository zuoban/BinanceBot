use super::{EarlyStopEntry, EngineAbortEntry, PairTask, RuntimeCommand};
use crate::config::{AppConfig, GlobalConfig};
use crate::db::Database;
use crate::exchange::rate_limit::RateLimiter;
use crate::server::AppState;
use crate::sync_ext::MutexUnpoisonedExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};

#[derive(Clone)]
pub struct RuntimeHandle {
    pub(crate) states: Arc<RwLock<Vec<Arc<AppState>>>>,
    pub(crate) membership_lock: Arc<Mutex<()>>,
    pub(super) global_position_limit: Arc<crate::server::state::GlobalPositionLimit>,
    pub(crate) db: Arc<Database>,
    pub(crate) auth: Arc<crate::server::state::AuthContext>,
    pub(crate) active_server: crate::config::ServerConfig,
    pub(crate) rate_limiter: Arc<RateLimiter>,
    pub(crate) probes: Arc<crate::server::routes::ProbeGates>,
    pub(super) commands: mpsc::Sender<RuntimeCommand>,
    pub(crate) stop: watch::Sender<bool>,
    pub(super) active_removal: Arc<std::sync::Mutex<Option<String>>>,
    pub(super) engine_aborts: Arc<std::sync::Mutex<Vec<EngineAbortEntry>>>,
    pub(super) early_stops: Arc<Mutex<Vec<EarlyStopEntry>>>,
}

impl RuntimeHandle {
    pub fn set_metrics_auth(&self, metrics_auth: Arc<crate::auth::MetricsAuth>) {
        self.auth.set_metrics_auth(metrics_auth);
    }

    pub fn set_trusted_proxies(&self, proxies: crate::auth::TrustedProxies) {
        self.auth.set_trusted_proxies(proxies);
    }

    /// Stop order admission immediately; the running membership command is allowed to finish.
    pub fn request_shutdown(&self) {
        let _admission = self.active_removal.lock_unpoisoned();
        self.global_position_limit
            .shutdown_requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.stop.send_replace(true);
    }

    pub(crate) async fn active_states(&self) -> Vec<Arc<AppState>> {
        self.states
            .read()
            .await
            .iter()
            .filter(|state| !state.is_retired())
            .cloned()
            .collect()
    }

    pub(super) fn register_engine(&self, pair: &PairTask) {
        let mut engines = self.engine_aborts.lock_unpoisoned();
        engines.retain(|(state, _)| {
            state
                .upgrade()
                .is_some_and(|state| !Arc::ptr_eq(&state, &pair.state))
        });
        engines.push((Arc::downgrade(&pair.state), pair.engine.abort_handle()));
    }

    /// Begin stopping existing engines while an admitted membership change finishes.
    pub async fn start_early_shutdown(&self, limit: Duration) {
        let removing = self.active_removal.lock_unpoisoned().clone();
        let states = self.states.read().await.clone();
        let mut stops = self.early_stops.lock().await;
        for state in states {
            if state.is_retired()
                || removing
                    .as_deref()
                    .is_some_and(|symbol| state.pair_scope() == Some(symbol))
                || stops
                    .iter()
                    .any(|(existing, _)| Arc::ptr_eq(existing, &state))
            {
                continue;
            }
            let abort =
                self.engine_aborts
                    .lock_unpoisoned()
                    .iter()
                    .find_map(|(registered, abort)| {
                        registered
                            .upgrade()
                            .filter(|registered| Arc::ptr_eq(registered, &state))
                            .map(|_| abort.clone())
                    });
            let Some(abort) = abort else {
                continue;
            };
            let worker_state = state.clone();
            stops.push((
                state,
                tokio::spawn(async move {
                    let result = crate::lifecycle::begin_stop_engine(worker_state, limit).await;
                    if result.1.is_err() {
                        abort.abort();
                    }
                    result
                }),
            ));
        }
    }

    pub(crate) async fn update_global_config(
        &self,
        payload: crate::types::UpdateGlobalConfigPayload,
    ) -> anyhow::Result<GlobalConfig> {
        // Finish propagation/rollback even if the HTTP client disconnects.
        let handle = self.clone();
        tokio::spawn(async move { handle.apply_global_config(payload).await }).await?
    }

    async fn send_global_config(
        state: &Arc<AppState>,
        config: &GlobalConfig,
    ) -> anyhow::Result<()> {
        let (reply, response) = oneshot::channel();
        state
            .action_tx
            .send(crate::types::BotControlAction::UpdateGlobalConfig {
                config: Box::new(config.clone()),
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("策略引擎已退出"))?;
        response
            .await
            .map_err(|_| anyhow::anyhow!("策略未确认全局配置"))?
            .map_err(anyhow::Error::msg)
    }

    async fn apply_global_config(
        &self,
        payload: crate::types::UpdateGlobalConfigPayload,
    ) -> anyhow::Result<GlobalConfig> {
        let _membership = self.membership_lock.lock().await;
        let saved = self.db.load_saved_config().await?;
        let mut config = GlobalConfig::from_app(&saved);
        if payload
            .baseline_version
            .as_deref()
            .is_some_and(|version| version != crate::db::config_version(&config))
        {
            return Err(crate::config::ConfigVersionConflict(
                "全局设置已被其他请求修改，请刷新后重试",
            )
            .into());
        }
        payload.validate_credential_updates()?;
        payload.apply_to(&mut config);
        // Retired members remain published for risk accounting, but their engines
        // are no longer available to validate or apply configuration changes.
        let states = self.active_states().await;
        Self::validate_global_config_for_states(&states, &config).await?;
        // Drain in-flight submissions in every old scope before installing any new
        // credentials. Keep all writers through persistence and any rollback so no
        // engine can replenish orders using settings that are not yet durable.
        let mut submission_guards = Vec::with_capacity(states.len());
        for state in &states {
            submission_guards.push(state.order_submission_lock.clone().write_owned().await);
        }
        self.propagate_and_persist_global_config(&states, &config)
            .await?;
        self.global_position_limit
            .commit_config(config.clone())
            .await;
        Ok(config)
    }

    async fn validate_global_config_for_states(
        states: &[Arc<AppState>],
        config: &GlobalConfig,
    ) -> anyhow::Result<()> {
        // Validate every active mode before changing any engine (including credential clearing).
        for state in states {
            let mut strategy = state.config.read().await.clone();
            config.apply_to(&mut strategy);
            strategy.validate()?;
        }
        Ok(())
    }

    async fn propagate_and_persist_global_config(
        &self,
        states: &[Arc<AppState>],
        config: &GlobalConfig,
    ) -> anyhow::Result<()> {
        let mut attempted = Vec::new();
        let mut failure = None;
        for state in states {
            let old = GlobalConfig::from_app(&*state.config.read().await);
            attempted.push((state.clone(), old));
            if let Err(error) = Self::send_global_config(state, config).await {
                failure = Some(error);
                break;
            }
        }
        if failure.is_none() {
            let saved = config.clone();
            if let Err(error) = self
                .db
                .run_blocking(move |db| db.save_global_config(&saved))
                .await
            {
                failure = Some(error);
            }
        }
        if let Some(error) = failure {
            return Err(Self::rollback_global_config_attempt(states, attempted, error).await);
        }
        Ok(())
    }

    async fn rollback_global_config_attempt(
        states: &[Arc<AppState>],
        attempted: Vec<(Arc<AppState>, GlobalConfig)>,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let mut rollback_errors = Vec::new();
        for (state, old) in attempted.into_iter().rev() {
            if GlobalConfig::from_app(&*state.config.read().await) == old {
                continue;
            }
            if let Err(rollback) = Self::send_global_config(&state, &old).await {
                let symbol = state.config.read().await.exchange.symbol.clone();
                rollback_errors.push(format!("{}: {}", symbol, rollback));
            }
        }
        if !rollback_errors.is_empty() {
            for state in states {
                if let Err(pause_err) = state.pause_trading().await {
                    tracing::error!(
                        %pause_err,
                        "Failed to persist emergency pause after global config rollback failure"
                    );
                }
                let _ = state
                    .action_tx
                    .send(crate::types::BotControlAction::Pause)
                    .await;
            }
            return anyhow::anyhow!(
                "全局配置更新失败: {}；恢复失败，策略已暂停: {}",
                error,
                rollback_errors.join("; ")
            );
        }
        anyhow::anyhow!("全局配置更新失败，已恢复原配置: {}", error)
    }

    async fn send_command(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<(), String>>) -> RuntimeCommand,
        shutting_down_msg: &'static str,
        incomplete_msg: &'static str,
    ) -> anyhow::Result<()> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(build(reply))
            .await
            .map_err(|_| anyhow::anyhow!(shutting_down_msg))?;
        response
            .await
            .map_err(|_| anyhow::anyhow!(incomplete_msg))?
            .map_err(anyhow::Error::msg)
    }

    pub(crate) async fn add_pair(&self, config: AppConfig) -> anyhow::Result<()> {
        self.send_command(
            |reply| RuntimeCommand::Add {
                config: Box::new(config),
                reply,
            },
            "程序正在退出，无法添加交易对",
            "添加交易对未完成",
        )
        .await
    }

    pub(crate) async fn remove_pair(&self, symbol: String) -> anyhow::Result<()> {
        self.send_command(
            |reply| RuntimeCommand::Remove { symbol, reply },
            "程序正在退出，无法移除交易对",
            "移除交易对未完成",
        )
        .await
    }
}

pub(super) fn spawn_cny_rate_refresh_task(
    states: Arc<RwLock<Vec<Arc<AppState>>>>,
    cny_rates: Arc<crate::fx::CnyRateCache>,
    mut stop: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(10));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = stop.changed() => {
                    if *stop.borrow_and_update() {
                        break;
                    }
                }
                _ = timer.tick() => {
                    if *stop.borrow_and_update() {
                        break;
                    }
                    let snapshot_states = states.read().await.clone();
                    let mut assets = std::collections::BTreeSet::new();
                    for state in snapshot_states {
                        if state.is_retired() {
                            continue;
                        }
                        let mut asset = state.account.read().await.asset.clone();
                        if asset.is_empty() {
                            asset = crate::types::quote_asset(
                                &state.config.read().await.exchange.symbol,
                            )
                            .unwrap_or_default()
                            .to_string();
                        }
                        if !asset.is_empty() {
                            assets.insert(asset);
                        }
                    }
                    for asset in assets {
                        if *stop.borrow() {
                            return;
                        }
                        if let Err(err) = cny_rates.refresh_if_due(&asset).await {
                            tracing::warn!("Could not refresh {}/CNY exchange rate: {}", asset, err);
                        }
                    }
                }
            }
        }
    })
}
