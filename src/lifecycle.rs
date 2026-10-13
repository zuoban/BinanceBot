use crate::server::AppState;
use crate::types::BotControlAction;
use anyhow::{anyhow, Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{timeout_at, Instant};

/// Both interactive and container shutdowns use the same order-cleanup path.
pub async fn shutdown_signal() -> Result<&'static str> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok("Ctrl+C")
            }
            _ = terminate.recv() => Ok("SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok("Ctrl+C")
    }
}

/// Stop submissions immediately, then let the single engine finish any in-flight
/// request and reconcile cancellations. Keep durable intents on failure/timeout.
pub async fn stop_engine(
    state: &Arc<AppState>,
    engine: &mut JoinHandle<()>,
    limit: Duration,
) -> Result<()> {
    state.request_shutdown();
    let deadline = Instant::now() + limit;
    let requested = timeout_at(deadline, async {
        state
            .pause_trading_with_reason("程序退出，暂停策略并撤销机器人挂单")
            .await?;
        let (reply, response) = oneshot::channel();
        state
            .action_tx
            .send(BotControlAction::Shutdown { reply })
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
    if outcome.is_err() {
        engine.abort();
        let _ = engine.await;
        return outcome;
    }
    match timeout_at(deadline, &mut *engine).await {
        Ok(result) => result.context("Strategy engine failed during shutdown"),
        Err(_) => {
            engine.abort();
            let _ = engine.await;
            Err(anyhow!(
                "Strategy engine did not exit before the shutdown deadline"
            ))
        }
    }
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
            let signal = shutdown_signal();
            tokio::pin!(signal);
            // Poll once to install handlers before the parent sends a signal.
            assert!(tokio::time::timeout(Duration::from_millis(1), &mut signal)
                .await
                .is_err());
            println!("SIGNAL_READY");
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
            assert_eq!(signal.await.unwrap(), expected);
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
