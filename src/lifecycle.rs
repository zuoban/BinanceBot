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

/// Both interactive and container shutdowns use the same order-cleanup path.
#[cfg(test)]
pub async fn shutdown_signal() -> Result<&'static str> {
    ShutdownSignals::register().await?.wait().await
}

/// Stop submissions immediately, then let the single engine finish any in-flight
/// request and reconcile cancellations. Keep durable intents on failure/timeout.
pub async fn stop_engine(
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
mod tests {
    use super::*;
    use crate::{config::AppConfig, db::Database, types::BotStatus};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn shutdown_deadline_aborts_an_unresponsive_engine_and_keeps_pause() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, mut rx) = mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), tx);
        let mut engine = tokio::spawn(async move {
            let action = rx.recv().await.unwrap();
            assert!(matches!(action, BotControlAction::Shutdown { .. }));
            std::future::pending::<()>().await;
            drop(action);
        });
        let result = stop_engine(&state, &mut engine, Duration::from_millis(50)).await;
        assert!(result.unwrap_err().to_string().contains("停机超过"));
        assert!(engine.is_finished());
        assert!(state.is_shutting_down());
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        assert!(state.resume_trading().await.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn signal_probe_child() {
        let Ok(expected) = std::env::var("BINANCEBOT_TEST_SHUTDOWN_SIGNAL") else {
            return;
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mut signals = ShutdownSignals::register().await.unwrap();
            // The parent sends the signal before wait() is first polled.
            println!("SIGNAL_READY");
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
            std::io::Read::read_exact(&mut std::io::stdin(), &mut [0u8; 1]).unwrap();
            assert_eq!(signals.wait().await.unwrap(), expected);
        });
    }

    #[cfg(unix)]
    #[test]
    fn sigterm_and_sigint_are_handled_in_isolated_processes() {
        use std::io::BufRead;
        use std::process::{Command, Stdio};
        for (signal, expected) in [("-TERM", "SIGTERM"), ("-INT", "Ctrl+C")] {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "lifecycle::tests::signal_probe_child",
                    "--nocapture",
                ])
                .env("BINANCEBOT_TEST_SHUTDOWN_SIGNAL", expected)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let reader = std::thread::spawn(move || {
                for line in std::io::BufReader::new(stdout)
                    .lines()
                    .map_while(Result::ok)
                {
                    if line.contains("SIGNAL_READY") {
                        let _ = ready_tx.send(());
                    }
                }
            });
            if ready_rx.recv_timeout(Duration::from_secs(5)).is_err() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("Signal probe did not become ready");
            }
            assert!(Command::new("kill")
                .args([signal, &child.id().to_string()])
                .status()
                .unwrap()
                .success());
            std::io::Write::write_all(child.stdin.as_mut().unwrap(), &[1]).unwrap();
            let until = std::time::Instant::now() + Duration::from_secs(5);
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                if std::time::Instant::now() >= until {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("Signal probe did not exit after {signal}");
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            reader.join().unwrap();
            assert!(status.success(), "Signal probe failed after {signal}");
        }
    }
}
