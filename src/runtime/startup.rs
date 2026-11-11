use super::{handle, PairTask, TradingRuntime};
use crate::config::{AppConfig, GlobalConfig};
use crate::db::Database;
use crate::exchange::account_cache::AccountCache;
use crate::exchange::market_cache::MarketCache;
use crate::exchange::position_cache::PositionCache;
use crate::exchange::rate_limit::RateLimiter;
use crate::fx::CnyRateCache;
use crate::runtime::RuntimeHandle;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};

impl TradingRuntime {
    pub(super) fn from_pairs(
        pairs: Vec<PairTask>,
        db: Arc<Database>,
        trade_lock: Arc<Mutex<()>>,
        active_server: crate::config::ServerConfig,
        global_config: GlobalConfig,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> Self {
        let (commands, command_rx) = mpsc::channel(16);
        let rate_limiter = Arc::new(RateLimiter::default());
        let auth = pairs
            .first()
            .map(|pair| pair.state.auth.clone())
            .unwrap_or_else(|| crate::server::state::AuthContext::new(db.clone(), admin_auth));
        let (global_position_limit, handle) = Self::build_runtime_handle(
            &pairs,
            db,
            auth.clone(),
            active_server,
            global_config,
            rate_limiter.clone(),
            commands,
        );
        Self {
            pairs,
            global_position_limit,
            handle,
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
            #[cfg(test)]
            fail_next_prepare_pair: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn build_runtime_handle(
        pairs: &[PairTask],
        db: Arc<Database>,
        auth: Arc<crate::server::state::AuthContext>,
        active_server: crate::config::ServerConfig,
        global_config: GlobalConfig,
        rate_limiter: Arc<RateLimiter>,
        commands: mpsc::Sender<super::RuntimeCommand>,
    ) -> (
        Arc<crate::server::state::GlobalPositionLimit>,
        RuntimeHandle,
    ) {
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
        let engine_aborts = pairs
            .iter()
            .map(|pair| (Arc::downgrade(&pair.state), pair.engine.abort_handle()))
            .collect();
        let handle = RuntimeHandle {
            states,
            membership_lock: Arc::new(Mutex::new(())),
            global_position_limit: global_position_limit.clone(),
            db,
            auth,
            active_server,
            rate_limiter,
            probes: Arc::default(),
            commands,
            stop: watch::channel(false).0,
            active_removal: Arc::new(std::sync::Mutex::new(None)),
            engine_aborts: Arc::new(std::sync::Mutex::new(engine_aborts)),
            early_stops: Arc::new(Mutex::new(Vec::new())),
        };
        (global_position_limit, handle)
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
    pub(super) async fn start_with_test_exchange_and_hook(
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
    pub(super) async fn start_with_test_exchange_and_prepare_hook(
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
            if let Err(error) = self
                .prepare_single_startup_pair(
                    strategy,
                    multi,
                    shutdown,
                    #[cfg(test)]
                    hook.as_ref(),
                    #[cfg(test)]
                    prepare_hook.clone(),
                )
                .await
            {
                return Some(error);
            }
        }
        None
    }

    async fn prepare_single_startup_pair(
        &mut self,
        strategy: AppConfig,
        multi: bool,
        shutdown: &mut Option<watch::Receiver<bool>>,
        #[cfg(test)] hook: Option<&mpsc::Sender<(String, oneshot::Sender<()>)>>,
        #[cfg(test)] prepare_hook: Option<mpsc::Sender<(String, oneshot::Sender<()>)>>,
    ) -> anyhow::Result<()> {
        // Preparation may have an in-flight database write. Let it finish, but
        // block every existing engine as soon as an interruption arrives.
        let (start_tx, start_rx) = oneshot::channel();
        let engine_shutdown = shutdown.clone();
        let pair = {
            let preparing = self.prepare_pair(
                strategy,
                multi,
                start_rx,
                engine_shutdown,
                #[cfg(test)]
                prepare_hook,
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
        }?;
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
        if let Some(hook) = hook {
            let (ack, received) = oneshot::channel();
            if hook.send((symbol, ack)).await.is_ok() {
                let _ = received.await;
            }
        }
        Ok(())
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

    pub(super) async fn wait_for_startup_shutdown(shutdown: &mut Option<watch::Receiver<bool>>) {
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

    pub(super) async fn wait_for_any_shutdown(
        first: &mut Option<watch::Receiver<bool>>,
        second: &mut Option<watch::Receiver<bool>>,
    ) {
        tokio::select! {
            biased;
            _ = Self::wait_for_startup_shutdown(first) => {}
            _ = Self::wait_for_startup_shutdown(second) => {}
        }
    }
}
