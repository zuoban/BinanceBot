use crate::config::{AppConfig, GlobalConfig};
use crate::db::Database;
use crate::exchange::account_cache::AccountCache;
use crate::exchange::market_cache::MarketCache;
use crate::exchange::position_cache::PositionCache;
use crate::exchange::rate_limit::RateLimiter;
use crate::exchange::BinanceFuturesClient;
use crate::fx::CnyRateCache;
use crate::lifecycle::stop_engine;
use crate::server::AppState;
use futures_util::future::{join_all, select_all};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;

#[path = "runtime/handle.rs"]
mod handle;
#[path = "runtime/membership.rs"]
mod membership;

pub use handle::RuntimeHandle;

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

    async fn stop_background(&mut self) {
        for task in self.background.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }
}

impl RuntimeCommand {
    fn reject_on_shutdown(self) {
        let (Self::Add { reply, .. } | Self::Remove { reply, .. }) = self;
        let _ = reply.send(Err("程序正在退出，无法修改交易对列表".into()));
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

/// Owns every engine, including pairs added through the dashboard.
pub struct TradingRuntime {
    pairs: Vec<PairTask>,
    handle: RuntimeHandle,
    command_rx: mpsc::Receiver<RuntimeCommand>,
    trade_lock: Arc<Mutex<()>>,
    cny_rates: Arc<CnyRateCache>,
    cny_refresh_task: Option<JoinHandle<()>>,
    account_cache: Arc<AccountCache>,
    position_cache: Arc<PositionCache>,
    market_cache: Arc<MarketCache>,
    rate_limiter: Arc<RateLimiter>,
    global_position_limit: Arc<crate::server::state::GlobalPositionLimit>,
    auth: Arc<crate::server::state::AuthContext>,
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
        let auth = pairs
            .first()
            .map(|pair| pair.state.auth.clone())
            .unwrap_or_else(|| crate::server::state::AuthContext::new(db.clone(), admin_auth));
        Self {
            pairs,
            global_position_limit: global_position_limit.clone(),
            handle: RuntimeHandle {
                states,
                membership_lock: Arc::new(Mutex::new(())),
                global_position_limit,
                db,
                auth: auth.clone(),
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
            cny_refresh_task: None,
            account_cache: Arc::new(AccountCache::default()),
            position_cache: Arc::new(PositionCache::default()),
            market_cache: Arc::new(MarketCache::default()),
            rate_limiter,
            auth,
            #[cfg(test)]
            test_exchange_url: None,
        }
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
    pub(crate) async fn start_with_test_exchange(
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
            db,
            trade_lock,
            config.server.clone(),
            GlobalConfig::from_app(config),
            admin_auth,
        );
        #[cfg(test)]
        {
            runtime.test_exchange_url = test_exchange_url;
        }
        let strategies = config.strategy_configs()?;
        let mut startup_error = runtime
            .prepare_startup_pairs(
                strategies,
                !config.pairs.is_empty(),
                &mut shutdown,
                #[cfg(test)]
                hook,
                #[cfg(test)]
                prepare_hook,
            )
            .await;
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
        runtime.run_initial_global_risk_check(&mut shutdown).await;
        if shutdown.as_ref().is_some_and(|rx| *rx.borrow()) {
            let error = anyhow::anyhow!("启动过程中收到退出信号");
            return match runtime.shutdown(Duration::from_secs(30)).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!("启动中断后的清理失败: {cleanup:#}"))),
            };
        }
        runtime.spawn_runtime_background_tasks();
        Ok(runtime)
    }

    async fn prepare_startup_pairs(
        &mut self,
        strategies: Vec<AppConfig>,
        multi: bool,
        shutdown: &mut Option<watch::Receiver<bool>>,
        #[cfg(test)] hook: Option<mpsc::Sender<(String, oneshot::Sender<()>)>>,
        #[cfg(test)] prepare_hook: Option<mpsc::Sender<(String, oneshot::Sender<()>)>>,
    ) -> Option<anyhow::Error> {
        for strategy in strategies {
            if shutdown.as_ref().is_some_and(|rx| *rx.borrow()) {
                return Some(anyhow::anyhow!("启动过程中收到退出信号"));
            }
            // Preparation may have an in-flight database write. Let it finish, but
            // block every existing engine as soon as an interruption arrives.
            let (start_tx, start_rx) = oneshot::channel();
            let engine_shutdown = shutdown.clone();
            let prepared = {
                let preparing = self.prepare_pair(
                    strategy,
                    multi,
                    start_rx,
                    engine_shutdown,
                    #[cfg(test)]
                    prepare_hook.clone(),
                );
                tokio::pin!(preparing);
                tokio::select! {
                    biased;
                    _ = Self::wait_for_startup_shutdown(shutdown) => {
                        for pair in &self.pairs {
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
                    self.handle.register_engine(&pair);
                    self.pairs.push(pair);
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
                Err(error) => return Some(error),
            }
        }
        None
    }

    async fn run_initial_global_risk_check(&self, shutdown: &mut Option<watch::Receiver<bool>>) {
        let check = self.global_position_limit.check();
        tokio::pin!(check);
        tokio::select! {
            biased;
            _ = Self::wait_for_startup_shutdown(shutdown) => {
                for pair in &self.pairs {
                    pair.signal_shutdown();
                }
                check.await;
            }
            _ = &mut check => {}
        }
    }

    fn spawn_runtime_background_tasks(&mut self) {
        let weak = Arc::downgrade(&self.global_position_limit);
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
        #[cfg(test)]
        let spawn_cny = self.test_exchange_url.is_none();
        #[cfg(not(test))]
        let spawn_cny = true;
        if spawn_cny {
            self.cny_refresh_task = Some(handle::spawn_cny_rate_refresh_task(
                self.handle.states.clone(),
                self.cny_rates.clone(),
                self.handle.stop.subscribe(),
            ));
        }
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

    async fn wait_for_any_shutdown(
        first: &mut Option<watch::Receiver<bool>>,
        second: &mut Option<watch::Receiver<bool>>,
    ) {
        tokio::select! {
            biased;
            _ = Self::wait_for_startup_shutdown(first) => {}
            _ = Self::wait_for_startup_shutdown(second) => {}
        }
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
                        let _admission = self.handle.active_removal.lock().unwrap_or_else(|e| e.into_inner());
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

    pub(crate) fn states(&self) -> Vec<Arc<AppState>> {
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

    #[cfg(test)]
    pub(crate) async fn wait_for_exit(&mut self) -> String {
        Self::wait_for_active_exit(&mut self.pairs).await
    }

    pub async fn shutdown(&mut self, limit: Duration) -> anyhow::Result<()> {
        self.handle.request_shutdown();
        if let Some(task) = self.cny_refresh_task.take() {
            task.abort();
            let _ = task.await;
        }
        self.command_rx.close();
        while let Ok(command) = self.command_rx.try_recv() {
            command.reject_on_shutdown();
        }
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
                let result =
                    if pair.engine_joined || (early_stop.is_none() && pair.engine.is_finished()) {
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
                                if let Err(pause_err) = pair.state.pause_trading().await {
                                    tracing::error!(
                                        %pause_err,
                                        "Failed to persist pause after early shutdown task failure"
                                    );
                                }
                                Err(anyhow::anyhow!("停机任务失败: {error}"))
                            }
                        }
                    } else {
                        stop_engine(&pair.state, &mut pair.engine, limit).await
                    };
                pair.stop_background().await;
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
        if let Some(task) = &self.cny_refresh_task {
            task.abort();
        }
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
mod tests;
