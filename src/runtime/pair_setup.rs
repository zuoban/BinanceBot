use super::{PairTask, PreparePairOptions, TradingRuntime};
use crate::config::AppConfig;
use crate::exchange::UserDataScope;
use crate::server::AppState;
use crate::strategy::GridTradingEngine;
use crate::types::TickerInfo;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

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
        self.enforce_pair_safety_pauses(&state, strategy, options.prearmed)
            .await?;
        Ok(state)
    }

    async fn enforce_pair_safety_pauses(
        &self,
        state: &Arc<AppState>,
        strategy: &AppConfig,
        prearmed: Option<bool>,
    ) -> anyhow::Result<()> {
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
        Ok(())
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
        Self::notify_test_prepare_hook(&mut options, &strategy.exchange.symbol).await;
        let mut engine = GridTradingEngine::new_with_caches(
            state.clone(),
            client.clone(),
            action_rx,
            ticker_rx,
            self.account_cache.clone(),
            self.position_cache.clone(),
            self.market_cache.clone(),
        );
        engine.set_market_stream_tx(market_tx);
        let (user_scope_tx, user_scope_rx) = watch::channel(UserDataScope {
            client,
            symbol: strategy.exchange.symbol.clone(),
            is_paper: strategy.exchange.is_paper(),
        });
        let background = self.spawn_pair_background_tasks(
            &mut engine,
            market_rx,
            ticker_tx,
            user_scope_tx,
            user_scope_rx,
            &state,
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let engine_task = Self::spawn_pair_engine_task(
            engine,
            state.clone(),
            options.start,
            options.startup_shutdown,
            Some(self.handle.stop.subscribe()),
            shutdown_rx,
        );
        Ok(PairTask {
            state,
            engine: engine_task,
            engine_joined: false,
            recovery_failed: false,
            shutdown_tx,
            background,
        })
    }

    #[cfg(test)]
    async fn notify_test_prepare_hook(options: &mut PreparePairOptions, symbol: &str) {
        if let Some(hook) = options.prepare_hook.take() {
            let (ack, received) = oneshot::channel();
            if hook.send((symbol.to_string(), ack)).await.is_ok() {
                let _ = received.await;
            }
        }
    }

    fn spawn_pair_background_tasks(
        &self,
        engine: &mut GridTradingEngine,
        market_rx: watch::Receiver<String>,
        ticker_tx: broadcast::Sender<TickerInfo>,
        user_scope_tx: watch::Sender<UserDataScope>,
        user_scope_rx: watch::Receiver<UserDataScope>,
        state: &Arc<AppState>,
    ) -> Vec<JoinHandle<()>> {
        #[cfg(test)]
        if self.test_exchange_url.is_some() {
            return Vec::new();
        }
        let market_task = self.market_hub.register(market_rx, ticker_tx);
        let (user_data_rx, user_data_task) = self.user_data_hub.register(user_scope_rx);
        engine.set_user_data_channels(user_scope_tx, user_data_rx);
        vec![
            market_task,
            user_data_task,
            tokio::spawn(state.clone().run_snapshot_broadcast()),
        ]
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
                    result = Box::pin(engine.initialize()) => Some(result),
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
            Box::pin(engine.run()).await;
        })
    }
}
