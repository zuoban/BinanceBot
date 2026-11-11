use super::{PairTask, PreparePairOptions, TradingRuntime};
use crate::config::{AppConfig, GlobalConfig};
use crate::exchange::BinanceWsStream;
use crate::lifecycle::remove_engine;
use crate::server::AppState;
use crate::strategy::{GridTradingEngine, SymbolRules};
use crate::types::TickerInfo;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
struct ActiveRemoval(Arc<std::sync::Mutex<Option<String>>>);

impl Drop for ActiveRemoval {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

impl TradingRuntime {
    pub(super) async fn prepare_pair(
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

    async fn build_and_configure_pair_state(
        &self,
        strategy: &AppConfig,
        options: &mut PreparePairOptions,
        action_tx: mpsc::Sender<crate::types::BotControlAction>,
    ) -> anyhow::Result<Arc<AppState>> {
        let db = self.handle.db.clone();
        let previous_unclean = if let Some(prearmed) = options.prearmed {
            prearmed
        } else {
            let symbol = strategy.exchange.symbol.clone();
            let mode = strategy.exchange.trading_mode();
            db.run_blocking(move |db| db.arm_execution_guard(&symbol, mode))
                .await?
        };
        // Restoring durable state can wait for SQLite and scan trade history.
        // Keep that work off the Tokio thread that handles membership and shutdown.
        let state_config = strategy.clone();
        let state_db = db.clone();
        let trade_lock = self.trade_lock.clone();
        let cny_rates = self.cny_rates.clone();
        let auth = self.auth.clone();
        let pair_scope = options.multi.then(|| state_config.exchange.symbol.clone());
        let state = db
            .run_blocking(move |_| {
                Ok(AppState::build_with_auth(
                    state_config,
                    state_db,
                    action_tx,
                    pair_scope,
                    trade_lock,
                    cny_rates,
                    auth,
                ))
            })
            .await?;
        if options.force_paused || previous_unclean {
            *state.status.write().await = crate::types::BotStatus::Paused;
        }
        if let Some((position, account)) = options.previous_state.take() {
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
        if !self.auth.is_auth_configured() && !strategy.exchange.is_paper() {
            if options.prearmed.is_none() {
                state.pause_trading().await?;
            }
            state
                .add_log(
                    "WARN",
                    "Live trading paused because no administrator password is configured",
                )
                .await;
        }
        Ok(state)
    }

    pub(super) async fn prepare_pair_with_state(
        &self,
        strategy: AppConfig,
        mut options: PreparePairOptions,
    ) -> anyhow::Result<PairTask> {
        #[cfg(test)]
        if self
            .fail_next_prepare_pair
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("injected prepare_pair_with_state failure");
        }
        let (action_tx, action_rx) = mpsc::channel(32);
        let (ticker_tx, ticker_rx) = broadcast::channel::<TickerInfo>(64);
        let (market_tx, market_rx) = watch::channel(strategy.exchange.symbol.clone());
        let client = Arc::new(self.exchange_client(&strategy.exchange));
        let state = self
            .build_and_configure_pair_state(&strategy, &mut options, action_tx)
            .await?;
        #[cfg(test)]
        if let Some(hook) = options.prepare_hook.take() {
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
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let engine_task = Self::spawn_pair_engine_task(
            engine,
            state.clone(),
            options.start,
            options.startup_shutdown,
            Some(self.handle.stop.subscribe()),
            shutdown_rx,
        );
        #[cfg(test)]
        let spawn_background = self.test_exchange_url.is_none();
        #[cfg(not(test))]
        let spawn_background = true;
        let background = if spawn_background {
            vec![
                tokio::spawn(stream.run()),
                tokio::spawn(state.clone().run_snapshot_broadcast()),
            ]
        } else {
            Vec::new()
        };
        Ok(PairTask {
            state,
            engine: engine_task,
            engine_joined: false,
            recovery_failed: false,
            shutdown_tx,
            background,
        })
    }

    fn spawn_pair_engine_task(
        mut engine: GridTradingEngine,
        engine_state: Arc<AppState>,
        start: Option<oneshot::Receiver<()>>,
        mut startup_shutdown: Option<watch::Receiver<bool>>,
        mut runtime_shutdown: Option<watch::Receiver<bool>>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            // Do not initialize until startup has registered the new engine.
            let permitted = if let Some(start) = start {
                tokio::select! {
                    biased;
                    _ = Self::wait_for_any_shutdown(&mut startup_shutdown, &mut runtime_shutdown) => false,
                    result = start => result.is_ok(),
                }
            } else {
                true
            };
            let initialized = if permitted {
                tokio::select! {
                    biased;
                    _ = Self::wait_for_any_shutdown(&mut startup_shutdown, &mut runtime_shutdown) => {
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
                if let Err(pause_err) = engine_state.pause_trading().await {
                    tracing::error!(
                        %pause_err,
                        "Failed to persist pause after startup reconciliation failure"
                    );
                }
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
        })
    }

    pub(super) async fn add_pair(&mut self, mut config: AppConfig) -> anyhow::Result<()> {
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
                    error.downcast_ref::<crate::exchange::ExchangeError>(),
                    Some(crate::exchange::ExchangeError::ApiError { code: -1121, .. })
                ) {
                    anyhow::anyhow!("币安交易对 {} 不存在", symbol)
                } else {
                    anyhow::anyhow!("暂时无法验证交易对 {}: {}", symbol, error)
                }
            })?;
        let rules = SymbolRules::from_trading_exchange_info(&info, symbol)
            .map_err(|error| error.into_chinese_error(symbol))?;
        rules.validate_grid_interval(config.grid.grid_interval)?;
        let added_symbol = config.exchange.symbol.clone();
        let added_mode = config.exchange.trading_mode();
        let saved = config.clone();
        let previous_unclean = self
            .handle
            .db
            .run_blocking(move |db| db.add_pair_config(&saved))
            .await?;
        self.global_position_limit
            .membership_ready
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let pair = match self
            .prepare_pair_with_state(
                config,
                PreparePairOptions {
                    multi: true,
                    prearmed: Some(previous_unclean),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(pair) => pair,
            Err(err) => {
                self.rollback_failed_add_pair(added_symbol, added_mode, previous_unclean)
                    .await;
                return Err(err);
            }
        };
        *pair.state.rules.write().await = rules;
        self.handle.register_engine(&pair);
        self.pairs.push(pair);
        *self.handle.states.write().await = self.states();
        self.global_position_limit
            .membership_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.global_position_limit.check().await;
        Ok(())
    }

    async fn rollback_failed_add_pair(
        &self,
        symbol: String,
        mode: crate::types::TradingMode,
        previous_unclean: bool,
    ) {
        let rollback = self
            .handle
            .db
            .run_blocking(move |db| {
                db.remove_pair_config(&symbol)?;
                if !previous_unclean {
                    db.clear_execution_guard(&symbol, mode)?;
                }
                Ok(())
            })
            .await;
        match rollback {
            Ok(()) => {
                self.global_position_limit
                    .membership_ready
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Err(rollback_err) => {
                tracing::error!(
                    %rollback_err,
                    "Failed to roll back persisted pair config after add_pair failure"
                );
                self.global_position_limit.check().await;
            }
        }
    }

    pub(super) async fn remove_pair(&mut self, symbol: &str) -> anyhow::Result<()> {
        let membership_lock = self.handle.membership_lock.clone();
        let _membership = membership_lock.lock().await;
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
            let mut active = self
                .handle
                .active_removal
                .lock()
                .unwrap_or_else(|e| e.into_inner());
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
        pair.stop_background().await;
        // Retired states reject new verification admission. Finish old commits
        // before either removing the member or restoring a replacement.
        // Use the old state's lock, and release it before replacement startup.
        state.drain_trade_commits().await;
        if result.is_ok() {
            result = self.verify_pair_flat_before_removal(&state, symbol).await;
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
            return self.recover_pair_after_failed_removal(index, error).await;
        }
        self.pairs.remove(index);
        *self.handle.states.write().await = self.states();
        Ok(())
    }

    async fn verify_pair_flat_before_removal(
        &self,
        state: &Arc<AppState>,
        symbol: &str,
    ) -> anyhow::Result<()> {
        let config = state.config.read().await.clone();
        if config.exchange.is_paper() {
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
        }
    }

    async fn recover_pair_after_failed_removal(
        &mut self,
        index: usize,
        error: anyhow::Error,
    ) -> anyhow::Result<()> {
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
                Err(error)
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
                Err(error.context(format!("删除失败后无法重启策略: {restart_error:#}")))
            }
        }
    }
}
