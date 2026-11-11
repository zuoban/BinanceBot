use crate::config::AppConfig;
use crate::exchange::account_cache::AccountCache;
use crate::exchange::market_cache::MarketCache;
use crate::exchange::position_cache::PositionCache;
use crate::exchange::rate_limit::RateLimiter;
use crate::exchange::BinanceFuturesClient;
use crate::fx::CnyRateCache;
use crate::lifecycle::stop_engine;
use crate::server::AppState;
use crate::sync_ext::MutexUnpoisonedExt;
use futures_util::future::{join_all, select_all};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::JoinHandle;

mod handle;
mod membership;
mod pair_setup;
mod startup;
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
    JoinHandle<(Option<tokio::time::Instant>, anyhow::Result<()>)>,
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
    #[cfg(test)]
    fail_next_prepare_pair: std::sync::atomic::AtomicBool,
}

impl TradingRuntime {
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
                        let _admission = self.handle.active_removal.lock_unpoisoned();
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
        let results = join_all(
            self.pairs
                .iter_mut()
                .zip(pair_stops)
                .map(|(pair, early_stop)| Self::stop_pair_for_shutdown(pair, early_stop, limit)),
        )
        .await;
        let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();
        anyhow::ensure!(errors.is_empty(), "{}", errors.join("; "));
        Ok(())
    }

    async fn stop_pair_for_shutdown(
        pair: &mut PairTask,
        early_stop: Option<JoinHandle<(Option<tokio::time::Instant>, anyhow::Result<()>)>>,
        limit: Duration,
    ) -> Result<(), String> {
        let symbol = pair.state.config.read().await.exchange.symbol.clone();
        let result = if pair.engine_joined || (early_stop.is_none() && pair.engine.is_finished()) {
            if let Some(early_stop) = early_stop {
                let _ = early_stop.await;
            }
            pair.state
                .pause_trading()
                .await
                .and(Err(anyhow::anyhow!("策略引擎已退出，请核实交易所挂单")))
        } else if let Some(early_stop) = early_stop {
            match early_stop.await {
                Ok(early) => {
                    crate::lifecycle::finish_stop_engine(&pair.state, &mut pair.engine, early).await
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
mod tests;
