use serde_json::Value;
use std::path::PathBuf;
use std::process::{Command, Output};

struct Fixture {
    directory: PathBuf,
    csv: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("backtest-cli-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let csv = directory.join("prices.csv");
        std::fs::write(&csv, "time_ms,price\n0,101\n60000,100\n120000,101\n").unwrap();
        Self { directory, csv }
    }

    fn replay(&self, extra: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_binance-grid-bot"))
            .args(["backtest", "--csv"])
            .arg(&self.csv)
            .args([
                "--grid-interval",
                "1",
                "--amount",
                "100",
                "--buy-window",
                "1",
                "--sell-window",
                "1",
                "--step-size",
                "1",
                "--min-qty",
                "1",
                "--maker-fee",
                "0",
                "--json",
            ])
            .args(extra)
            .env("RUST_LOG", "binance_grid_bot=warn")
            .output()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn replay_json_remains_machine_readable_when_exit_quantity_is_unplaceable() {
    let fixture = Fixture::new();
    let output = fixture.replay(&[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["fills"], 1);
    assert_eq!(report["sells"], 0);
    assert_eq!(report["final_position"], "1");
    assert_eq!(report["realized_pnl"], "0");
    assert_eq!(report["unrealized_pnl"], "1");
}

#[test]
fn replay_rejects_invalid_caps_and_price_bounds_instead_of_silently_ignoring_them() {
    let fixture = Fixture::new();
    for extra in [
        vec!["--max-position", "0"],
        vec!["--max-position=-1"],
        vec!["--min-price", "110", "--max-price", "90"],
        vec!["--min-price", "0"],
    ] {
        let output = fixture.replay(&extra);
        assert!(
            !output.status.success(),
            "accepted invalid arguments {extra:?}"
        );
        assert!(serde_json::from_slice::<Value>(&output.stdout).is_err());
    }
}
