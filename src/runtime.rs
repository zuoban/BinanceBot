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
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tokio::task::JoinHandle;

struct PairTask {
    state: Arc<AppState>,
    engine: JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
    background: Vec<JoinHandle<()>>,
}

/// One engine and market stream per pair, sharing only durable storage and authentication.
pub struct TradingRuntime {
    pairs: Vec<PairTask>,
}

impl TradingRuntime {
    pub async fn start(config: &AppConfig, db: Arc<Database>) -> anyhow::Result<Self> {
        let multi = !config.pairs.is_empty();
        let has_admin = db.is_admin_password_set()?;
        let trade_lock = Arc::new(Mutex::new(()));
        let mut prepared = Vec::new();
        for strategy in config.strategy_configs()? {
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
            prepared.push((state, engine, BinanceWsStream::new(market_rx, ticker_tx)));
        }
        let pairs = prepared
            .into_iter()
            .map(|(state, mut engine, stream)| {
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
                            .pause_trading_with_reason(
                                "策略启动时账户或挂单对账失败，请检查系统运行日志",
                            )
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
                PairTask {
                    state,
                    engine: engine_task,
                    shutdown_tx,
                    background,
                }
            })
            .collect();
        Ok(Self { pairs })
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
        let mut runtime = TradingRuntime { pairs };
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
