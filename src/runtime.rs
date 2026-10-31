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
use crate::strategy::GridTradingEngine;
use crate::types::TickerInfo;
use futures_util::future::{join_all, select_all};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;

struct PairTask {
    state: Arc<AppState>,
    engine: JoinHandle<()>,
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

#[derive(Clone)]
pub struct RuntimeHandle {
    pub(crate) states: Arc<RwLock<Vec<Arc<AppState>>>>,
    pub(crate) membership_lock: Arc<Mutex<()>>,
    global_position_limit: Arc<crate::server::state::GlobalPositionLimit>,
    pub(crate) db: Arc<Database>,
    pub(crate) active_server: crate::config::ServerConfig,
    commands: mpsc::Sender<RuntimeCommand>,
}

impl RuntimeHandle {
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
        let saved = self
            .db
            .run_blocking(|db| db.load_config())
            .await?
            .ok_or_else(|| anyhow::anyhow!("无法读取全局配置"))?;
        let mut config = GlobalConfig::from_app(&saved);
        if payload
            .baseline_version
            .as_deref()
            .is_some_and(|version| version != crate::db::config_version(&config))
        {
            anyhow::bail!("全局设置已被其他请求修改，请刷新后重试");
        }
        payload.apply_to(&mut config);
        let states = self.states.read().await.clone();
        // Validate every mode before changing any engine (including credential clearing).
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
                    let _ = state
                        .pause_trading_with_reason("全局配置恢复失败，请检查系统运行日志")
                        .await;
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
    ) -> Self {
        let (commands, command_rx) = mpsc::channel(16);
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
        Self {
            pairs,
            global_position_limit: global_position_limit.clone(),
            handle: RuntimeHandle {
                states,
                membership_lock: Arc::new(Mutex::new(())),
                global_position_limit,
                db,
                active_server,
                commands,
            },
            command_rx,
            trade_lock,
            cny_rates: Arc::new(CnyRateCache::new()),
            account_cache: Arc::new(AccountCache::default()),
            position_cache: Arc::new(PositionCache::default()),
            market_cache: Arc::new(MarketCache::default()),
            rate_limiter: Arc::new(RateLimiter::default()),
            #[cfg(test)]
            test_exchange_url: None,
        }
    }

    pub async fn start(config: &AppConfig, db: Arc<Database>) -> anyhow::Result<Self> {
        Self::start_impl(
            config,
            db,
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
        Self::start_impl(config, db, Some(url)).await
    }

    async fn start_impl(
        config: &AppConfig,
        db: Arc<Database>,
        #[cfg(test)] test_exchange_url: Option<String>,
    ) -> anyhow::Result<Self> {
        let trade_lock = Arc::new(Mutex::new(()));
        let mut runtime = Self::from_pairs(
            Vec::new(),
            db.clone(),
            trade_lock.clone(),
            config.server.clone(),
            GlobalConfig::from_app(config),
        );
        #[cfg(test)]
        {
            runtime.test_exchange_url = test_exchange_url;
        }
        for strategy in config.strategy_configs()? {
            let pair = runtime
                .prepare_pair(strategy, !config.pairs.is_empty())
                .await?;
            runtime.pairs.push(pair);
        }
        *runtime.handle.states.write().await = runtime.states();
        runtime
            .global_position_limit
            .membership_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        runtime.global_position_limit.check().await;
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

    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }

    async fn prepare_pair(&self, strategy: AppConfig, multi: bool) -> anyhow::Result<PairTask> {
        let db = self.handle.db.clone();
        let has_admin = db.is_admin_password_set()?;
        let (action_tx, action_rx) = mpsc::channel(32);
        let (ticker_tx, ticker_rx) = broadcast::channel::<TickerInfo>(64);
        let (market_tx, market_rx) = watch::channel((
            strategy.exchange.symbol.clone(),
            strategy.exchange.is_testnet,
        ));
        #[cfg(test)]
        let client = if let Some(url) = &self.test_exchange_url {
            Arc::new(BinanceFuturesClient::with_base_url(
                &strategy.exchange,
                url.clone(),
            ))
        } else {
            Arc::new(BinanceFuturesClient::new_with_limiter(
                &strategy.exchange,
                self.rate_limiter.clone(),
            ))
        };
        #[cfg(not(test))]
        let client = Arc::new(BinanceFuturesClient::new_with_limiter(
            &strategy.exchange,
            self.rate_limiter.clone(),
        ));
        let state = if multi {
            AppState::new_for_pair_with_rates(
                strategy.clone(),
                db.clone(),
                action_tx,
                self.trade_lock.clone(),
                self.cny_rates.clone(),
            )
        } else {
            AppState::new_with_rates(
                strategy.clone(),
                db.clone(),
                action_tx,
                self.cny_rates.clone(),
            )
        };
        let _ = state
            .global_position_limit
            .set(Arc::downgrade(&self.global_position_limit));
        if let Err(error) = strategy.validate() {
            state
                .pause_trading_with_reason("启动时发现策略配置无效，请检查系统运行日志")
                .await?;
            state
                .add_log(
                    "ERROR",
                    format!("Saved configuration is invalid; trading paused: {}", error),
                )
                .await;
        }
        if !has_admin && !strategy.exchange.dry_run {
            state
                .pause_trading_with_reason("实盘策略等待管理员完成初始化")
                .await?;
            state
                .add_log(
                    "WARN",
                    "Live trading paused until administrator setup is complete",
                )
                .await;
        }
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        engine.set_account_cache(self.account_cache.clone());
        engine.set_position_cache(self.position_cache.clone());
        engine.set_market_cache(self.market_cache.clone());
        engine.set_market_stream_tx(market_tx);
        let stream = BinanceWsStream::new(market_rx, ticker_tx);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let engine_state = state.clone();
        let engine_task = tokio::spawn(async move {
            // Shutdown may arrive while an exchange request is still initializing.
            let initialized = tokio::select! {
                result = engine.initialize() => Some(result),
                _ = shutdown_rx.changed() => None,
            };
            if let Some(Err(error)) = &initialized {
                let _ = engine_state
                    .pause_trading_with_reason("策略启动时账户或挂单对账失败，请检查系统运行日志")
                    .await;
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
            if let Some(global) = engine_state
                .global_position_limit
                .get()
                .and_then(std::sync::Weak::upgrade)
            {
                global.check().await;
            }
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
            shutdown_tx,
            background,
        })
    }

    async fn add_pair(&mut self, mut config: AppConfig) -> anyhow::Result<()> {
        let _membership = self.handle.membership_lock.lock().await;
        let saved = self
            .handle
            .db
            .run_blocking(|db| db.load_config())
            .await?
            .ok_or_else(|| anyhow::anyhow!("无法读取全局配置"))?;
        GlobalConfig::from_app(&saved).apply_to(&mut config);
        config.validate()?;
        anyhow::ensure!(
            !self
                .pairs
                .iter()
                .any(|pair| pair.state.pair_scope() == Some(config.exchange.symbol.as_str())),
            "交易对已存在"
        );
        let saved = config.clone();
        self.handle
            .db
            .run_blocking(move |db| db.add_pair_config(&saved))
            .await?;
        self.global_position_limit
            .membership_ready
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let result = self.prepare_pair(config, true).await;
        if let Ok(pair) = result {
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
        let pair = &mut self.pairs[index];
        anyhow::ensure!(
            *pair.state.status.read().await == crate::types::BotStatus::Paused,
            "请先暂停该策略并等待挂单撤销完成"
        );
        anyhow::ensure!(
            pair.state.active_orders.read().await.is_empty(),
            "仍有挂单，请先撤单并等待交易所确认"
        );
        pair.state.request_shutdown();
        let _ = pair.shutdown_tx.send(true);
        remove_engine(&pair.state, &mut pair.engine, Duration::from_secs(30)).await?;
        let symbol = symbol.to_string();
        self.handle
            .db
            .run_blocking(move |db| db.remove_pair_config(&symbol))
            .await?;
        let pair = self.pairs.remove(index);
        for task in pair.background {
            task.abort();
            let _ = task.await;
        }
        *self.handle.states.write().await = self.states();
        Ok(())
    }

    pub async fn run(&mut self) -> String {
        loop {
            // Cleanup may already have consumed a finished JoinHandle on an unsuccessful removal.
            if let Some(pair) = self.pairs.iter().find(|pair| pair.engine.is_finished()) {
                return format!("Strategy engine {:?} stopped", pair.state.pair_scope());
            }
            tokio::select! {
                (result, index, _) = select_all(self.pairs.iter_mut().map(|pair| &mut pair.engine)) => {
                    let symbol = self.pairs[index].state.config.read().await.exchange.symbol.clone();
                    return format!("Strategy engine {} stopped: {:?}", symbol, result);
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else { return "Runtime command channel closed".into(); };
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

    pub async fn wait_for_exit(&mut self) -> String {
        let (result, index, _) =
            select_all(self.pairs.iter_mut().map(|pair| &mut pair.engine)).await;
        let symbol = self.pairs[index]
            .state
            .config
            .read()
            .await
            .exchange
            .symbol
            .clone();
        format!("Strategy engine {} stopped: {:?}", symbol, result)
    }

    pub async fn shutdown(&mut self, limit: Duration) -> anyhow::Result<()> {
        self.command_rx.close();
        while let Ok(command) = self.command_rx.try_recv() {
            let reply = match command {
                RuntimeCommand::Add { reply, .. } | RuntimeCommand::Remove { reply, .. } => reply,
            };
            let _ = reply.send(Err("程序正在退出，无法修改交易对列表".into()));
        }
        // Stop submissions for ALL pairs before awaiting any cancellation.
        for pair in &self.pairs {
            pair.state.request_shutdown();
            let _ = pair.shutdown_tx.send(true);
        }
        let results = join_all(self.pairs.iter_mut().map(|pair| async move {
            let symbol = pair.state.config.read().await.exchange.symbol.clone();
            let result = if pair.engine.is_finished() {
                let paused = pair
                    .state
                    .pause_trading_with_reason("策略引擎退出，请核实交易所挂单")
                    .await;
                paused.and(Err(anyhow::anyhow!("策略引擎已退出，请核实交易所挂单")))
            } else {
                stop_engine(&pair.state, &mut pair.engine, limit).await
            };
            for task in pair.background.drain(..) {
                task.abort();
                let _ = task.await;
            }
            result.map_err(|error| format!("{}: {}", symbol, error))
        }))
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
                    let BotControlAction::Shutdown { reply } = rx.recv().await.unwrap() else {
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
