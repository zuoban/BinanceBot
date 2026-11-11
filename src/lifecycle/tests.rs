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
