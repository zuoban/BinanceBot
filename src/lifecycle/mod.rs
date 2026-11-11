use crate::server::AppState;
use crate::types::BotControlAction;
use anyhow::{anyhow, Context, Result};
#[cfg(not(unix))]
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{timeout_at, Instant};

/// Registered before startup so signals cannot bypass runtime cleanup.
pub struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(not(unix))]
    interrupt: std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>,
    #[cfg(not(unix))]
    received: bool,
}

impl ShutdownSignals {
    pub async fn register() -> Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                interrupt: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::interrupt(),
                )?,
                terminate: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?,
            })
        }
        #[cfg(not(unix))]
        {
            let mut interrupt: std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> =
                Box::pin(tokio::signal::ctrl_c());
            // ctrl_c installs its handler on first poll. Preserve a signal that
            // arrives during registration for the subsequent wait.
            let received = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(match interrupt.as_mut().poll(cx) {
                    std::task::Poll::Ready(result) => result.map(|()| true),
                    std::task::Poll::Pending => Ok(false),
                })
            })
            .await?;
            Ok(Self {
                interrupt,
                received,
            })
        }
    }

    pub async fn wait(&mut self) -> Result<&'static str> {
        #[cfg(unix)]
        {
            tokio::select! {
                signal = self.interrupt.recv() => signal.map(|_| "Ctrl+C").ok_or_else(|| anyhow!("SIGINT handler closed")),
                signal = self.terminate.recv() => signal.map(|_| "SIGTERM").ok_or_else(|| anyhow!("SIGTERM handler closed")),
            }
        }
        #[cfg(not(unix))]
        {
            if !self.received {
                self.interrupt.as_mut().await?;
            }
            Ok("Ctrl+C")
        }
    }
}

/// Stop submissions immediately, then let the single engine finish any in-flight
/// request and reconcile cancellations. Keep durable intents on failure/timeout.
pub(crate) async fn stop_engine(
    state: &Arc<AppState>,
    engine: &mut JoinHandle<()>,
    limit: Duration,
) -> Result<()> {
    let early = begin_stop_engine(Arc::clone(state), limit).await;
    finish_stop_engine(state, engine, early).await
}

pub(crate) async fn remove_engine(
    state: &Arc<AppState>,
    engine: &mut JoinHandle<()>,
    limit: Duration,
) -> Result<()> {
    let early = begin_stop_engine_inner(state, limit, false).await;
    finish_stop_engine_inner(state, engine, early, false).await
}

/// Begin a normal shutdown without requiring ownership of the engine handle.
/// The returned deadline bounds both this phase and the subsequent engine join.
pub(crate) async fn begin_stop_engine(
    state: Arc<AppState>,
    limit: Duration,
) -> (Instant, Result<()>) {
    begin_stop_engine_inner(&state, limit, true).await
}

pub(crate) async fn finish_stop_engine(
    state: &Arc<AppState>,
    engine: &mut JoinHandle<()>,
    early: (Instant, Result<()>),
) -> Result<()> {
    finish_stop_engine_inner(state, engine, early, true).await
}

async fn begin_stop_engine_inner(
    state: &Arc<AppState>,
    limit: Duration,
    recover: bool,
) -> (Instant, Result<()>) {
    state.request_shutdown();
    let deadline = Instant::now() + limit;
    let requested = timeout_at(deadline, async {
        if recover {
            state.pause_for_shutdown().await?;
        } else {
            state.pause_trading().await?;
        }
        let (reply, response) = oneshot::channel();
        state
            .action_tx
            .send(BotControlAction::Shutdown {
                removing: !recover,
                reply,
            })
            .await
            .context("Strategy engine stopped before shutdown was requested")?;
        response
            .await
            .context("Strategy engine did not acknowledge shutdown")?
            .map_err(|error| anyhow!(error))
    })
    .await;
    let outcome = match requested {
        Ok(result) => result,
        Err(_) => Err(anyhow!(
            "停机超过 {} 秒；恢复元数据已保留，请核实交易所挂单",
            limit.as_secs()
        )),
    };
    (deadline, outcome)
}

async fn finish_stop_engine_inner(
    state: &Arc<AppState>,
    engine: &mut JoinHandle<()>,
    early: (Instant, Result<()>),
    recover: bool,
) -> Result<()> {
    let (deadline, outcome) = early;
    let outcome = match outcome {
        Err(error) => {
            engine.abort();
            let _ = engine.await;
            Err(error)
        }
        Ok(()) => match timeout_at(deadline, &mut *engine).await {
            Ok(result) => result.context("Strategy engine failed during shutdown"),
            Err(_) => {
                engine.abort();
                let _ = engine.await;
                Err(anyhow!(
                    "Strategy engine did not exit before the shutdown deadline"
                ))
            }
        },
    };
    if let Err(error) = outcome {
        // The deadline may have cancelled the shutdown pause before it obtained
        // the global lock or before its database worker finished. Persist the
        // failure pause outside that deadline and await the actual write.
        return match state.pause_trading().await {
            Ok(()) => Err(error),
            Err(persist_error) => Err(error.context(format!(
                "Failed to persist paused status after shutdown failure: {persist_error:#}"
            ))),
        };
    }
    if recover {
        state.commit_shutdown_recovery().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
