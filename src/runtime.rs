use crate::config::{AppConfig, GlobalConfig};
use crate::db::Database;
use crate::exchange::account_cache::AccountCache;
use crate::exchange::market_cache::MarketCache;
use crate::exchange::position_cache::PositionCache;
use crate::exchange::rate_limit::RateLimiter;
use crate::exchange::{BinanceFuturesClient, BinanceWsStream};
use crate::fx::CnyRateCache;
use crate::lifecycle::{remove_engine, stop_engine};
use crate::server::AppState;
use crate::strategy::{GridTradingEngine, SymbolRules};
use crate::types::TickerInfo;
use futures_util::future::{join_all, select_all};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;

struct PairTask {
    state: Arc<AppState>,
    engine: JoinHandle<()>,
    engine_joined: bool,
    recovery_failed: bool,
    shutdown_tx: watch::Sender<bool>,
    background: Vec<JoinHandle<()>>,
}

enum RuntimeCommand {
    Add {
        config: Box<AppConfig>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Remove {
        symbol: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

impl PairTask {
    fn signal_shutdown(&self) {
        self.state.request_shutdown();
        let _ = self.shutdown_tx.send(true);
    }
}

impl RuntimeCommand {
    fn reject_on_shutdown(self) {
        let (Self::Add { reply, .. } | Self::Remove { reply, .. }) = self;
        let _ = reply.send(Err("程序正在退出，无法修改交易对列表".into()));
    }
}

struct ActiveRemoval(Arc<std::sync::Mutex<Option<String>>>);

impl Drop for ActiveRemoval {
    fn drop(&mut self) {
        *self.0.lock().unwrap() = None;
    }
}

type EngineAbortEntry = (std::sync::Weak<AppState>, tokio::task::AbortHandle);
type EarlyStopEntry = (
    Arc<AppState>,
    JoinHandle<(tokio::time::Instant, anyhow::Result<()>)>,
);

#[derive(Default)]
struct PreparePairOptions {
    multi: bool,
    previous_state: Option<(crate::types::PositionInfo, crate::types::AccountInfo)>,
    force_paused: bool,
    prearmed: Option<bool>,
    start: Option<oneshot::Receiver<()>>,
    startup_shutdown: Option<watch::Receiver<bool>>,
    #[cfg(test)]
    prepare_hook: Option<mpsc::Sender<(String, oneshot::Sender<()>)>>,
}

#[derive(Clone)]
pub struct RuntimeHandle {
    pub(crate) states: Arc<RwLock<Vec<Arc<AppState>>>>,
    pub(crate) membership_lock: Arc<Mutex<()>>,
    global_position_limit: Arc<crate::server::state::GlobalPositionLimit>,
    pub(crate) db: Arc<Database>,
    pub(crate) active_server: crate::config::ServerConfig,
    pub(crate) rate_limiter: Arc<RateLimiter>,
    pub(crate) probes: Arc<crate::server::routes::ProbeGates>,
    commands: mpsc::Sender<RuntimeCommand>,
    stop: watch::Sender<bool>,
    active_removal: Arc<std::sync::Mutex<Option<String>>>,
    engine_aborts: Arc<std::sync::Mutex<Vec<EngineAbortEntry>>>,
    early_stops: Arc<Mutex<Vec<EarlyStopEntry>>>,
}

impl RuntimeHandle {
    /// Stop order admission immediately; the running membership command is allowed to finish.
    pub fn request_shutdown(&self) {
        let _admission = self.active_removal.lock().unwrap();
        self.global_position_limit
            .shutdown_requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.stop.send_replace(true);
    }

    fn register_engine(&self, pair: &PairTask) {
        let mut engines = self.engine_aborts.lock().unwrap();
        engines.retain(|(state, _)| {
            state
                .upgrade()
                .is_some_and(|state| !Arc::ptr_eq(&state, &pair.state))
        });
        engines.push((Arc::downgrade(&pair.state), pair.engine.abort_handle()));
    }

    /// Begin stopping existing engines while an admitted membership change finishes.
    pub async fn start_early_shutdown(&self, limit: Duration) {
        let removing = self.active_removal.lock().unwrap().clone();
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
                    .lock()
                    .unwrap()
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

    pub async fn update_global_config(
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
            anyhow::bail!("全局设置已被其他请求修改，请刷新后重试");
        }
        payload.apply_to(&mut config);
        // Retired members remain published for risk accounting, but their engines
        // are no longer available to validate or apply configuration changes.
        let states: Vec<_> = self
            .states
            .read()
            .await
            .iter()
            .filter(|state| !state.is_retired())
            .cloned()
            .collect();
        // Validate every active mode before changing any engine (including credential clearing).
        for state in &states {
            let mut strategy = state.config.read().await.clone();
            config.apply_to(&mut strategy);
            strategy.validate()?;
        }
        // Drain in-flight submissions in every old scope before installing any new
        // credentials. Keep all writers through persistence and any rollback so no
        // engine can replenish orders using settings that are not yet durable.
        let mut submission_guards = Vec::with_capacity(states.len());
        for state in &states {
            submission_guards.push(state.order_submission_lock.clone().write_owned().await);
        }
        let mut attempted = Vec::new();
        let mut failure = None;
        for state in &states {
            let old = GlobalConfig::from_app(&*state.config.read().await);
            attempted.push((state.clone(), old));
            if let Err(error) = Self::send_global_config(state, &config).await {
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
                for state in &states {
                    let _ = state.pause_trading().await;
                    let _ = state
                        .action_tx
                        .send(crate::types::BotControlAction::Pause)
                        .await;
                }
                anyhow::bail!(
                    "全局配置更新失败: {}；恢复失败，策略已暂停: {}",
                    error,
                    rollback_errors.join("; ")
                );
            }
            anyhow::bail!("全局配置更新失败，已恢复原配置: {}", error);
        }
        self.global_position_limit
            .commit_config(config.clone())
            .await;
        Ok(config)
    }

    pub async fn add_pair(&self, config: AppConfig) -> anyhow::Result<()> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Add {
                config: Box::new(config),
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("程序正在退出，无法添加交易对"))?;
        response
            .await
            .map_err(|_| anyhow::anyhow!("添加交易对未完成"))?
            .map_err(anyhow::Error::msg)
    }

    pub async fn remove_pair(&self, symbol: String) -> anyhow::Result<()> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Remove { symbol, reply })
            .await
            .map_err(|_| anyhow::anyhow!("程序正在退出，无法移除交易对"))?;
        response
            .await
            .map_err(|_| anyhow::anyhow!("移除交易对未完成"))?
            .map_err(anyhow::Error::msg)
    }
}

/// Owns every engine, including pairs added through the dashboard.
pub struct TradingRuntime {
    pairs: Vec<PairTask>,
    handle: RuntimeHandle,
    command_rx: mpsc::Receiver<RuntimeCommand>,
    trade_lock: Arc<Mutex<()>>,
    cny_rates: Arc<CnyRateCache>,
    account_cache: Arc<AccountCache>,
    position_cache: Arc<PositionCache>,
    market_cache: Arc<MarketCache>,
    rate_limiter: Arc<RateLimiter>,
    global_position_limit: Arc<crate::server::state::GlobalPositionLimit>,
    admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    #[cfg(test)]
    test_exchange_url: Option<String>,
}

impl TradingRuntime {
    fn from_pairs(
        pairs: Vec<PairTask>,
        db: Arc<Database>,
        trade_lock: Arc<Mutex<()>>,
        active_server: crate::config::ServerConfig,
        global_config: GlobalConfig,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> Self {
        let (commands, command_rx) = mpsc::channel(16);
        let (stop, _) = watch::channel(false);
        let states = Arc::new(RwLock::new(
            pairs.iter().map(|pair| pair.state.clone()).collect(),
        ));
        let global_position_limit = crate::server::state::GlobalPositionLimit::new(states.clone());
        global_position_limit.initialize_config(global_config);
        for state in pairs.iter().map(|pair| &pair.state) {
            let _ = state
                .global_position_limit
                .set(Arc::downgrade(&global_position_limit));
        }
        let rate_limiter = Arc::new(RateLimiter::default());
        let engine_aborts = pairs
            .iter()
            .map(|pair| (Arc::downgrade(&pair.state), pair.engine.abort_handle()))
            .collect();
        Self {
            pairs,
            global_position_limit: global_position_limit.clone(),
            handle: RuntimeHandle {
                states,
                membership_lock: Arc::new(Mutex::new(())),
                global_position_limit,
                db,
                active_server,
                rate_limiter: rate_limiter.clone(),
                probes: Arc::default(),
                commands,
                stop,
                active_removal: Arc::new(std::sync::Mutex::new(None)),
                engine_aborts: Arc::new(std::sync::Mutex::new(engine_aborts)),
                early_stops: Arc::new(Mutex::new(Vec::new())),
            },
            command_rx,
            trade_lock,
            cny_rates: Arc::new(CnyRateCache::new()),
            account_cache: Arc::new(AccountCache::default()),
            position_cache: Arc::new(PositionCache::default()),
            market_cache: Arc::new(MarketCache::default()),
            rate_limiter,
            admin_auth,
            #[cfg(test)]
            test_exchange_url: None,
        }
    }

    pub async fn start(
        config: &AppConfig,
        db: Arc<Database>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> anyhow::Result<Self> {
        Self::start_impl(
            config,
            db,
            None,
            admin_auth,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    pub async fn start_interruptible(
        config: &AppConfig,
        db: Arc<Database>,
        shutdown: watch::Receiver<bool>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> anyhow::Result<Self> {
        Self::start_impl(
            config,
            db,
            Some(shutdown),
            admin_auth,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    #[cfg(test)]
    async fn start_with_test_exchange(
        config: &AppConfig,
        db: Arc<Database>,
        url: String,
    ) -> anyhow::Result<Self> {
        Self::start_impl(config, db, None, None, Some(url), None, None).await
    }

    #[cfg(test)]
    async fn start_with_test_exchange_and_hook(
        config: &AppConfig,
        db: Arc<Database>,
        url: String,
        shutdown: watch::Receiver<bool>,
        hook: mpsc::Sender<(String, oneshot::Sender<()>)>,
    ) -> anyhow::Result<Self> {
        Self::start_impl(
            config,
            db,
            Some(shutdown),
            None,
            Some(url),
            Some(hook),
            None,
        )
        .await
    }

    #[cfg(test)]
    async fn start_with_test_exchange_and_prepare_hook(
        config: &AppConfig,
        db: Arc<Database>,
        url: String,
        shutdown: watch::Receiver<bool>,
        hook: mpsc::Sender<(String, oneshot::Sender<()>)>,
    ) -> anyhow::Result<Self> {
        Self::start_impl(
            config,
            db,
            Some(shutdown),
            None,
            Some(url),
            None,
            Some(hook),
        )
        .await
    }

    async fn start_impl(
        config: &AppConfig,
        db: Arc<Database>,
        mut shutdown: Option<watch::Receiver<bool>>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
        #[cfg(test)] test_exchange_url: Option<String>,
        #[cfg(test)] hook: Option<mpsc::Sender<(String, oneshot::Sender<()>)>>,
        #[cfg(test)] prepare_hook: Option<mpsc::Sender<(String, oneshot::Sender<()>)>>,
    ) -> anyhow::Result<Self> {
        let trade_lock = Arc::new(Mutex::new(()));
        let mut runtime = Self::from_pairs(
            Vec::new(),
            db.clone(),
            trade_lock.clone(),
            config.server.clone(),
            GlobalConfig::from_app(config),
            admin_auth,
        );
        #[cfg(test)]
        {
            runtime.test_exchange_url = test_exchange_url;
        }
        let mut startup_error = None;
        for strategy in config.strategy_configs()? {
            if shutdown.as_ref().is_some_and(|rx| *rx.borrow()) {
                startup_error = Some(anyhow::anyhow!("启动过程中收到退出信号"));
                break;
            }
            // Preparation may have an in-flight database write. Let it finish, but
            // block every existing engine as soon as an interruption arrives.
            let (start_tx, start_rx) = oneshot::channel();
            let engine_shutdown = shutdown.clone();
            let prepared = {
                let preparing = runtime.prepare_pair(
                    strategy,
                    !config.pairs.is_empty(),
                    start_rx,
                    engine_shutdown,
                    #[cfg(test)]
                    prepare_hook.clone(),
                );
                tokio::pin!(preparing);
                tokio::select! {
                    biased;
                    _ = Self::wait_for_startup_shutdown(&mut shutdown) => {
                        for pair in &runtime.pairs {
                            pair.signal_shutdown();
                        }
                        preparing.await
                    }
                    result = &mut preparing => result,
                }
            };
            match prepared {
                Ok(pair) => {
                    #[cfg(test)]
                    let symbol = pair.state.config.read().await.exchange.symbol.clone();
                    let stopped = shutdown.as_ref().is_some_and(|rx| *rx.borrow());
                    if stopped {
                        pair.signal_shutdown();
                    }
                    runtime.handle.register_engine(&pair);
                    runtime.pairs.push(pair);
                    if !stopped {
                        let _ = start_tx.send(());
                    }
                    #[cfg(test)]
                    if let Some(hook) = &hook {
                        let (ack, received) = oneshot::channel();
                        if hook.send((symbol, ack)).await.is_ok() {
                            let _ = received.await;
                        }
                    }
                }
                Err(error) => {
                    startup_error = Some(error);
                    break;
                }
            }
        }
        if startup_error.is_none() && shutdown.as_ref().is_some_and(|rx| *rx.borrow()) {
            startup_error = Some(anyhow::anyhow!("启动过程中收到退出信号"));
        }
        if let Some(error) = startup_error {
            return match runtime.shutdown(Duration::from_secs(30)).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!("启动失败后的清理也失败: {cleanup:#}"))),
            };
        }
        *runtime.handle.states.write().await = runtime.states();
        runtime
            .global_position_limit
            .membership_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        {
            let check = runtime.global_position_limit.check();
            tokio::pin!(check);
            tokio::select! {
                biased;
                _ = Self::wait_for_startup_shutdown(&mut shutdown) => {
                    for pair in &runtime.pairs {
                        pair.signal_shutdown();
                    }
                    check.await;
                }
                _ = &mut check => {}
            }
        }
        if shutdown.as_ref().is_some_and(|rx| *rx.borrow()) {
            let error = anyhow::anyhow!("启动过程中收到退出信号");
            return match runtime.shutdown(Duration::from_secs(30)).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!("启动中断后的清理失败: {cleanup:#}"))),
            };
        }
        let weak = Arc::downgrade(&runtime.global_position_limit);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_millis(250));
            loop {
                timer.tick().await;
                let Some(global) = weak.upgrade() else {
                    break;
                };
                global.check().await;
                drop(global);
            }
        });
        Ok(runtime)
    }

    async fn wait_for_startup_shutdown(shutdown: &mut Option<watch::Receiver<bool>>) {
        if let Some(receiver) = shutdown {
            loop {
                if *receiver.borrow_and_update() {
                    return;
                }
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        }
        std::future::pending::<()>().await;
    }

    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }

    fn exchange_client(&self, config: &crate::config::ExchangeConfig) -> BinanceFuturesClient {
        #[cfg(test)]
        if let Some(url) = &self.test_exchange_url {
            return BinanceFuturesClient::with_base_url_and_limiter(
                config,
                url.clone(),
                self.rate_limiter.clone(),
            );
        }
        BinanceFuturesClient::new_with_limiter(config, self.rate_limiter.clone())
    }

    async fn prepare_pair(
        &self,
        strategy: AppConfig,
        multi: bool,
        start: oneshot::Receiver<()>,
        startup_shutdown: Option<watch::Receiver<bool>>,
        #[cfg(test)] prepare_hook: Option<mpsc::Sender<(String, oneshot::Sender<()>)>>,
    ) -> anyhow::Result<PairTask> {
        self.prepare_pair_with_state(
            strategy,
            PreparePairOptions {
                multi,
                start: Some(start),
                startup_shutdown,
                #[cfg(test)]
                prepare_hook,
                ..Default::default()
            },
        )
        .await
    }

    async fn prepare_pair_with_state(
        &self,
        strategy: AppConfig,
        options: PreparePairOptions,
    ) -> anyhow::Result<PairTask> {
        let db = self.handle.db.clone();
        let multi = options.multi;
        let previous_state = options.previous_state;
        let force_paused = options.force_paused;
        let prearmed = options.prearmed;
        let start = options.start;
        let mut startup_shutdown = options.startup_shutdown;
        #[cfg(test)]
        let prepare_hook = options.prepare_hook;
        let previous_unclean = if let Some(prearmed) = prearmed {
            prearmed
        } else {
            let symbol = strategy.exchange.symbol.clone();
            let mode = strategy.exchange.trading_mode();
            db.run_blocking(move |db| db.arm_execution_guard(&symbol, mode))
                .await?
        };
        let (action_tx, action_rx) = mpsc::channel(32);
        let (ticker_tx, ticker_rx) = broadcast::channel::<TickerInfo>(64);
        let (market_tx, market_rx) = watch::channel(strategy.exchange.symbol.clone());
        let client = Arc::new(self.exchange_client(&strategy.exchange));
        // Restoring durable state can wait for SQLite and scan trade history.
        // Keep that work off the Tokio thread that handles membership and shutdown.
        let state_config = strategy.clone();
        let state_db = db.clone();
        let trade_lock = self.trade_lock.clone();
        let cny_rates = self.cny_rates.clone();
        let admin_auth = self.admin_auth.clone();
        let state = db
            .run_blocking(move |_| {
                Ok(if multi {
                    AppState::new_for_pair_with_rates(
                        state_config,
                        state_db,
                        action_tx,
                        trade_lock,
                        cny_rates,
                        admin_auth,
                    )
                } else {
                    AppState::new_with_rates(
                        state_config,
                        state_db,
                        action_tx,
                        cny_rates,
                        admin_auth,
                    )
                })
            })
            .await?;
        if force_paused || previous_unclean {
            *state.status.write().await = crate::types::BotStatus::Paused;
        }
        if let Some((position, account)) = previous_state {
            *state.position.write().await = position;
            *state.account.write().await = account;
        }
        let _ = state
            .global_position_limit
            .set(Arc::downgrade(&self.global_position_limit));
        if let Err(error) = strategy.validate() {
            state.pause_trading().await?;
            state
                .add_log(
                    "ERROR",
                    format!("Saved configuration is invalid; trading paused: {}", error),
                )
                .await;
        }
        if self.admin_auth.is_none() && !strategy.exchange.is_paper() {
            if prearmed.is_none() {
                state.pause_trading().await?;
            }
            state
                .add_log(
                    "WARN",
                    "Live trading paused because no administrator password is configured",
                )
                .await;
        }
        #[cfg(test)]
        if let Some(hook) = prepare_hook {
            let (ack, received) = oneshot::channel();
            if hook
                .send((strategy.exchange.symbol.clone(), ack))
                .await
                .is_ok()
            {
                let _ = received.await;
            }
        }
        let mut engine = GridTradingEngine::new_with_caches(
            state.clone(),
            client,
            action_rx,
            ticker_rx,
            self.account_cache.clone(),
            self.position_cache.clone(),
            self.market_cache.clone(),
        );
        engine.set_market_stream_tx(market_tx);
        let stream = BinanceWsStream::new(market_rx, ticker_tx);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let engine_state = state.clone();
        let mut runtime_shutdown = Some(self.handle.stop.subscribe());
        let engine_task = tokio::spawn(async move {
            // Do not initialize until startup has registered the new engine.
            let permitted = if let Some(start) = start {
                tokio::select! {
                    biased;
                    _ = Self::wait_for_startup_shutdown(&mut startup_shutdown) => false,
                    _ = Self::wait_for_startup_shutdown(&mut runtime_shutdown) => false,
                    result = start => result.is_ok(),
                }
            } else {
                true
            };
            let initialized = if permitted {
                tokio::select! {
                    biased;
                    _ = Self::wait_for_startup_shutdown(&mut startup_shutdown) => {
                        engine_state.request_shutdown();
                        None
                    }
                    _ = Self::wait_for_startup_shutdown(&mut runtime_shutdown) => {
                        engine_state.request_shutdown();
                        None
                    }
                    _ = shutdown_rx.changed() => None,
                    result = engine.initialize() => Some(result),
                }
            } else {
                engine_state.request_shutdown();
                None
            };
            if let Some(Err(error)) = &initialized {
                let _ = engine_state.pause_trading().await;
                engine_state
                    .add_log(
                        "ERROR",
                        format!("Startup reconciliation failed; trading paused: {}", error),
                    )
                    .await;
            }
            engine_state
                .global_position_initialized
                .store(true, std::sync::atomic::Ordering::SeqCst);
            engine_state.global_position_ready.store(
                matches!(initialized, Some(Ok(()))),
                std::sync::atomic::Ordering::SeqCst,
            );
            engine_state.check_global_position_limit().await;
            engine.reconcile_initial_pause().await;
            engine.run().await;
        });
        #[cfg(test)]
        let background = if self.test_exchange_url.is_some() {
            Vec::new()
        } else {
            vec![
                tokio::spawn(stream.run()),
                tokio::spawn(state.clone().run_cny_rate_refresh()),
                tokio::spawn(state.clone().run_snapshot_broadcast()),
            ]
        };
        #[cfg(not(test))]
        let background = vec![
            tokio::spawn(stream.run()),
            tokio::spawn(state.clone().run_cny_rate_refresh()),
            tokio::spawn(state.clone().run_snapshot_broadcast()),
        ];
        Ok(PairTask {
            state,
            engine: engine_task,
            engine_joined: false,
            recovery_failed: false,
            shutdown_tx,
            background,
        })
    }

    async fn add_pair(&mut self, mut config: AppConfig) -> anyhow::Result<()> {
        let _membership = self.handle.membership_lock.lock().await;
        let saved = self.handle.db.load_saved_config().await?;
        GlobalConfig::from_app(&saved).apply_to(&mut config);
        config.validate()?;
        anyhow::ensure!(
            !self
                .pairs
                .iter()
                .any(|pair| pair.state.pair_scope() == Some(config.exchange.symbol.as_str())),
            "交易对已存在"
        );
        let symbol = &config.exchange.symbol;
        let info = self
            .exchange_client(&config.exchange)
            .get_exchange_info(Some(symbol))
            .await
            .map_err(|error| {
                if matches!(
                    error.downcast_ref::<crate::exchange::client::ExchangeError>(),
                    Some(crate::exchange::client::ExchangeError::ApiError { code: -1121, .. })
                ) {
                    anyhow::anyhow!("币安交易对 {} 不存在", symbol)
                } else {
                    anyhow::anyhow!("暂时无法验证交易对 {}: {}", symbol, error)
                }
            })?;
        let rules = SymbolRules::from_trading_exchange_info(&info, symbol)
            .map_err(|error| error.into_chinese_error(symbol))?;
        rules.validate_grid_interval(config.grid.grid_interval)?;
        let saved = config.clone();
        let previous_unclean = self
            .handle
            .db
            .run_blocking(move |db| db.add_pair_config(&saved))
            .await?;
        self.global_position_limit
            .membership_ready
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let result = self
            .prepare_pair_with_state(
                config,
                PreparePairOptions {
                    multi: true,
                    prearmed: Some(previous_unclean),
                    ..Default::default()
                },
            )
            .await;
        if let Ok(pair) = result {
            *pair.state.rules.write().await = rules;
            self.handle.register_engine(&pair);
            self.pairs.push(pair);
            *self.handle.states.write().await = self.states();
        } else {
            self.global_position_limit
                .membership_ready
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return result.map(|_| ());
        }
        self.global_position_limit
            .membership_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.global_position_limit.check().await;
        Ok(())
    }

    async fn remove_pair(&mut self, symbol: &str) -> anyhow::Result<()> {
        let _membership = self.handle.membership_lock.lock().await;
        anyhow::ensure!(self.pairs.len() > 1, "至少保留一个交易对");
        let index = self
            .pairs
            .iter()
            .position(|pair| pair.state.pair_scope() == Some(symbol))
            .ok_or_else(|| anyhow::anyhow!("交易对不存在"))?;
        anyhow::ensure!(
            !self.pairs[index].recovery_failed,
            "策略重建失败，请重启程序并核实交易所状态"
        );
        anyhow::ensure!(
            !self.pairs[index].engine_joined,
            "策略引擎已退出，请重启程序并核实交易所状态"
        );
        let pair = &mut self.pairs[index];
        let control_lock = pair.state.socket_control_lock.clone();
        let control = control_lock.write().await;
        anyhow::ensure!(
            *pair.state.status.read().await == crate::types::BotStatus::Paused,
            "请先暂停该策略并等待挂单撤销完成"
        );
        anyhow::ensure!(
            pair.state.active_orders.read().await.is_empty(),
            "仍有挂单，请先撤单并等待交易所确认"
        );
        // Claim the engine only once all potentially blocking admission checks
        // are complete. Shutdown can stop it while removal waits for those locks.
        let _removal = {
            let mut active = self.handle.active_removal.lock().unwrap();
            anyhow::ensure!(
                !*self.handle.stop.borrow(),
                "程序正在退出，无法修改交易对列表"
            );
            *active = Some(symbol.to_string());
            ActiveRemoval(self.handle.active_removal.clone())
        };
        pair.state.retire();
        drop(control);
        pair.signal_shutdown();
        let mut result =
            remove_engine(&pair.state, &mut pair.engine, Duration::from_secs(30)).await;
        let state = pair.state.clone();
        for task in pair.background.drain(..) {
            task.abort();
            let _ = task.await;
        }
        // Retired states reject new verification admission. Finish old commits
        // before either removing the member or restoring a replacement.
        // Use the old state's lock, and release it before replacement startup.
        state.drain_trade_commits().await;
        if result.is_ok() {
            let config = state.config.read().await.clone();
            result = if config.exchange.is_paper() {
                let paper_symbol = symbol.to_string();
                let saved = state
                    .db
                    .run_blocking(move |db| db.load_paper_state(&paper_symbol))
                    .await;
                if let Err(error) = saved {
                    Err(error.context("无法确认模拟盘持仓，无法移除交易对"))
                } else if state.position.read().await.size.is_zero() {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("仍有持仓，请先平仓再移除交易对"))
                }
            } else {
                let client = self.exchange_client(&config.exchange);
                let position = match client.sync_server_time().await {
                    Ok(_) => client.get_position(symbol).await,
                    Err(error) => Err(error.context("同步交易所时间失败")),
                };
                match position {
                    Ok(Some(position)) => {
                        let has_position = !position.position_amt.is_zero();
                        *state.position.write().await =
                            position.into_position_info(Some(chrono::Utc::now()));
                        if has_position {
                            self.global_position_limit.check().await;
                            Err(anyhow::anyhow!("仍有持仓，请先平仓再移除交易对"))
                        } else {
                            Ok(())
                        }
                    }
                    Ok(None) => {
                        state
                            .global_position_ready
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        self.global_position_limit.check().await;
                        Err(anyhow::anyhow!("无法确认交易所持仓，请稍后重试"))
                    }
                    Err(error) => {
                        state
                            .global_position_ready
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        self.global_position_limit.check().await;
                        Err(error.context("查询交易所持仓失败，无法移除交易对"))
                    }
                }
            };
        }
        if result.is_ok() {
            let symbol = symbol.to_string();
            result = self
                .handle
                .db
                .run_blocking(move |db| db.remove_pair_config(&symbol))
                .await;
        }
        if let Err(error) = result {
            // The engine may have committed a configuration update while stopping.
            let config = self.pairs[index].state.config.read().await.clone();
            // Keep the last known exposure visible while a replacement reconciles.
            let previous_state = Some((
                self.pairs[index].state.position.read().await.clone(),
                self.pairs[index].state.account.read().await.clone(),
            ));
            match self
                .prepare_pair_with_state(
                    config,
                    PreparePairOptions {
                        multi: true,
                        previous_state,
                        force_paused: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(replacement) => {
                    self.handle.register_engine(&replacement);
                    self.pairs[index] = replacement;
                    *self.handle.states.write().await = self.states();
                    self.global_position_limit.check().await;
                    return Err(error);
                }
                Err(restart_error) => {
                    self.pairs[index].recovery_failed = true;
                    // Keep the retired member in the risk set. Its position can
                    // still exist, but its stopped engine can no longer refresh it.
                    self.pairs[index]
                        .state
                        .global_position_initialized
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    self.pairs[index]
                        .state
                        .global_position_ready
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                    self.global_position_limit.check().await;
                    return Err(error.context(format!("删除失败后无法重启策略: {restart_error:#}")));
                }
            }
        }
        self.pairs.remove(index);
        *self.handle.states.write().await = self.states();
        Ok(())
    }

    pub async fn run(&mut self) -> String {
        let mut stop = self.handle.stop.subscribe();
        loop {
            if *stop.borrow_and_update() {
                return "Runtime shutdown requested".into();
            }
            // A failed recovery keeps the retired state for risk accounting, but
            // its already-joined engine must not terminate the remaining runtime.
            if let Some(pair) = self
                .pairs
                .iter()
                .find(|pair| !pair.recovery_failed && pair.engine.is_finished())
            {
                return format!("Strategy engine {:?} stopped", pair.state.pair_scope());
            }
            tokio::select! {
                biased;
                _ = stop.changed() => return "Runtime shutdown requested".into(),
                reason = Self::wait_for_active_exit(&mut self.pairs) => {
                    return reason;
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else { return "Runtime command channel closed".into(); };
                    {
                        let _admission = self.handle.active_removal.lock().unwrap();
                        if *self.handle.stop.borrow() {
                            command.reject_on_shutdown();
                            return "Runtime shutdown requested".into();
                        }
                    }
                    match command {
                        RuntimeCommand::Add { config, reply } => {
                            let result = self.add_pair(*config).await.map_err(|error| error.to_string());
                            let _ = reply.send(result);
                        }
                        RuntimeCommand::Remove { symbol, reply } => {
                            let result = self.remove_pair(&symbol).await.map_err(|error| error.to_string());
                            let _ = reply.send(result);
                        }
                    }
                }
            }
        }
    }

    pub fn states(&self) -> Vec<Arc<AppState>> {
        self.pairs.iter().map(|pair| pair.state.clone()).collect()
    }

    async fn wait_for_active_exit(pairs: &mut [PairTask]) -> String {
        if let Some(pair) = pairs.iter().find(|pair| pair.engine_joined) {
            return format!("Strategy engine {:?} stopped", pair.state.pair_scope());
        }
        let active: Vec<_> = pairs
            .iter_mut()
            .filter(|pair| !pair.recovery_failed && !pair.engine_joined)
            .collect();
        if active.is_empty() {
            return std::future::pending().await;
        }
        let futures = active.into_iter().map(|pair| async move {
            let symbol = pair.state.config.read().await.exchange.symbol.clone();
            let result = (&mut pair.engine).await;
            pair.engine_joined = true;
            (symbol, result)
        });
        let ((symbol, result), _, _) = select_all(futures.map(Box::pin)).await;
        format!("Strategy engine {} stopped: {:?}", symbol, result)
    }

    pub async fn wait_for_exit(&mut self) -> String {
        Self::wait_for_active_exit(&mut self.pairs).await
    }

    pub async fn shutdown(&mut self, limit: Duration) -> anyhow::Result<()> {
        self.handle.request_shutdown();
        self.command_rx.close();
        while let Ok(command) = self.command_rx.try_recv() {
            command.reject_on_shutdown();
        }
        // Stop submissions for ALL pairs before awaiting any cancellation.
        for pair in &self.pairs {
            pair.signal_shutdown();
        }
        let mut early_stops = std::mem::take(&mut *self.handle.early_stops.lock().await);
        let mut pair_stops = Vec::with_capacity(self.pairs.len());
        for pair in &self.pairs {
            let stop = early_stops
                .iter()
                .position(|(state, _)| Arc::ptr_eq(state, &pair.state))
                .map(|index| early_stops.swap_remove(index).1);
            pair_stops.push(stop);
        }
        let results = join_all(self.pairs.iter_mut().zip(pair_stops).map(
            |(pair, early_stop)| async move {
                let symbol = pair.state.config.read().await.exchange.symbol.clone();
                let result = if pair.engine_joined {
                    if let Some(early_stop) = early_stop {
                        let _ = early_stop.await;
                    }
                    let paused = pair.state.pause_trading().await;
                    paused.and(Err(anyhow::anyhow!("策略引擎已退出，请核实交易所挂单")))
                } else if let Some(early_stop) = early_stop {
                    match early_stop.await {
                        Ok(early) => {
                            crate::lifecycle::finish_stop_engine(
                                &pair.state,
                                &mut pair.engine,
                                early,
                            )
                            .await
                        }
                        Err(error) => {
                            pair.engine.abort();
                            let _ = (&mut pair.engine).await;
                            let _ = pair.state.pause_trading().await;
                            Err(anyhow::anyhow!("停机任务失败: {error}"))
                        }
                    }
                } else if pair.engine.is_finished() {
                    let paused = pair.state.pause_trading().await;
                    paused.and(Err(anyhow::anyhow!("策略引擎已退出，请核实交易所挂单")))
                } else {
                    stop_engine(&pair.state, &mut pair.engine, limit).await
                };
                for task in pair.background.drain(..) {
                    task.abort();
                    let _ = task.await;
                }
                result.map_err(|error| format!("{}: {}", symbol, error))
            },
        ))
        .await;
        let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();
        anyhow::ensure!(errors.is_empty(), "{}", errors.join("; "));
        Ok(())
    }
}

impl Drop for TradingRuntime {
    fn drop(&mut self) {
        // Normal exits use shutdown(); this prevents detached tasks after a panic or canceled owner.
        for pair in &self.pairs {
            pair.state.request_shutdown();
            pair.engine.abort();
            for task in &pair.background {
                task.abort();
            }
        }
    }
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod management_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TradingPairConfig;
    use crate::types::{BotControlAction, BotStatus, TradingMode};

    #[tokio::test]
    async fn shutdown_stops_all_pairs_before_waiting_and_cancels_them_concurrently() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let config = AppConfig {
            pairs: ["SOLUSDC", "BTCUSDT"]
                .map(|s| TradingPairConfig::new(s.into()))
                .to_vec(),
            ..Default::default()
        };
        db.save_config(&config).unwrap();
        let commit = Arc::new(Mutex::new(()));
        let mut receivers = Vec::new();
        let states: Vec<_> = config
            .strategy_configs()
            .unwrap()
            .into_iter()
            .map(|config| {
                let (tx, rx) = mpsc::channel(8);
                receivers.push(rx);
                AppState::new_for_pair(config, db.clone(), tx, commit.clone())
            })
            .collect();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let pairs = states
            .iter()
            .zip(receivers)
            .map(|(state, mut rx)| {
                let (shutdown_tx, _) = watch::channel(false);
                let all_states = states.clone();
                let barrier = barrier.clone();
                let engine = tokio::spawn(async move {
                    let BotControlAction::Shutdown { reply, .. } = rx.recv().await.unwrap() else {
                        panic!("Expected shutdown");
                    };
                    assert!(all_states.iter().all(|state| state.is_shutting_down()));
                    // Serial cleanup would time out here because neither engine can finish alone.
                    barrier.wait().await;
                    reply.send(Ok(())).unwrap();
                });
                PairTask {
                    state: state.clone(),
                    engine,
                    engine_joined: false,
                    recovery_failed: false,
                    shutdown_tx,
                    background: vec![],
                }
            })
            .collect();
        let mut runtime = TradingRuntime::from_pairs(
            pairs,
            db.clone(),
            commit,
            config.server.clone(),
            GlobalConfig::from_app(&config),
            None,
        );
        runtime.shutdown(Duration::from_secs(2)).await.unwrap();
        for state in states {
            let symbol = state.config.read().await.exchange.symbol.clone();
            assert_eq!(
                db.load_pair_status(&symbol, TradingMode::Paper).unwrap(),
                Some(BotStatus::Paused)
            );
            assert!(state.resume_trading().await.is_err());
        }
    }
}
