use crate::config::AppConfig;
use crate::db::Database;
use crate::exchange::{BinanceFuturesClient, BinanceWsStream};
use crate::lifecycle::stop_engine;
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
    pub(crate) db: Arc<Database>,
    pub(crate) active_server: crate::config::ServerConfig,
    commands: mpsc::Sender<RuntimeCommand>,
}

impl RuntimeHandle {
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
}

impl TradingRuntime {
    fn from_pairs(
        pairs: Vec<PairTask>,
        db: Arc<Database>,
        trade_lock: Arc<Mutex<()>>,
        active_server: crate::config::ServerConfig,
    ) -> Self {
        let (commands, command_rx) = mpsc::channel(16);
        let states = Arc::new(RwLock::new(
            pairs.iter().map(|pair| pair.state.clone()).collect(),
        ));
        Self {
            pairs,
            handle: RuntimeHandle {
                states,
                membership_lock: Arc::new(Mutex::new(())),
                db,
                active_server,
                commands,
            },
            command_rx,
            trade_lock,
        }
    }

    pub async fn start(config: &AppConfig, db: Arc<Database>) -> anyhow::Result<Self> {
        let trade_lock = Arc::new(Mutex::new(()));
        let mut runtime = Self::from_pairs(
            Vec::new(),
            db.clone(),
            trade_lock.clone(),
            config.server.clone(),
        );
        for strategy in config.strategy_configs()? {
            let pair = Self::prepare_pair(
                strategy,
                db.clone(),
                trade_lock.clone(),
                !config.pairs.is_empty(),
            )
            .await?;
            runtime.pairs.push(pair);
        }
        *runtime.handle.states.write().await = runtime.states();
        Ok(runtime)
    }

    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }

    async fn prepare_pair(
        strategy: AppConfig,
        db: Arc<Database>,
        trade_lock: Arc<Mutex<()>>,
        multi: bool,
    ) -> anyhow::Result<PairTask> {
        let has_admin = db.is_admin_password_set()?;
        let (action_tx, action_rx) = mpsc::channel(32);
        let (ticker_tx, ticker_rx) = broadcast::channel::<TickerInfo>(64);
        let (market_tx, market_rx) = watch::channel((
            strategy.exchange.symbol.clone(),
            strategy.exchange.is_testnet,
        ));
        let client = Arc::new(BinanceFuturesClient::new(&strategy.exchange));
        let state = if multi {
            AppState::new_for_pair(strategy.clone(), db.clone(), action_tx, trade_lock.clone())
        } else {
            AppState::new(strategy.clone(), db.clone(), action_tx)
        };
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
        engine.set_market_stream_tx(market_tx);
        let stream = BinanceWsStream::new(market_rx, ticker_tx);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let engine_state = state.clone();
        let engine_task = tokio::spawn(async move {
            // Shutdown may arrive while an exchange request is still initializing.
            let initialized = tokio::select! {
                result = engine.initialize() => result,
                _ = shutdown_rx.changed() => Ok(()),
            };
            if let Err(error) = initialized {
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
            engine.reconcile_initial_pause().await;
            engine.run().await;
        });
        let background = vec![
            tokio::spawn(stream.run()),
            tokio::spawn(state.clone().run_cny_rate_refresh()),
            tokio::spawn(state.clone().run_price_history_refresh()),
            tokio::spawn(state.clone().run_snapshot_broadcast()),
        ];
        Ok(PairTask {
            state,
            engine: engine_task,
            shutdown_tx,
            background,
        })
    }

    async fn add_pair(&mut self, config: AppConfig) -> anyhow::Result<()> {
        let _membership = self.handle.membership_lock.lock().await;
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
        let pair = Self::prepare_pair(
            config,
            self.handle.db.clone(),
            self.trade_lock.clone(),
            true,
        )
        .await?;
        self.pairs.push(pair);
        *self.handle.states.write().await = self.states();
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
        stop_engine(&pair.state, &mut pair.engine, Duration::from_secs(30)).await?;
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
        let mut runtime =
            TradingRuntime::from_pairs(pairs, db.clone(), commit, config.server.clone());
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
