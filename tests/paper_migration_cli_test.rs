#![cfg(unix)]

mod common;
use binance_grid_bot::db::Database;
use rusqlite::Connection;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Fixture {
    directory: common::TempTestDir,
    database: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = common::TempTestDir::new("paper-migration-cli");
        let database = directory.path().join("bot.db");
        drop(Database::open(&database).unwrap());
        let conn = Connection::open(&database).unwrap();
        conn.execute_batch(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity, amount_usdc, realized_pnl, commission, is_maker, timestamp, note)
             VALUES ('legacy', 'legacy-order', 'SOLUSDC', 'PAPER', 'BUY', '100', '1', '100', '0', '0', 1, '2026-10-10T00:00:00Z', 'legacy');"
        ).unwrap();
        drop(conn);
        Self {
            directory,
            database,
        }
    }
}

fn migrate(path: &Path, symbol: &str, confirmation: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_binance-grid-bot"));
    command
        .arg("migrate-legacy-paper")
        .arg("--db")
        .arg(path)
        .arg("--symbol")
        .arg(symbol);
    if let Some(confirmation) = confirmation {
        command.arg("--confirm").arg(confirmation);
    }
    command.output().unwrap()
}

fn paper_account_count(path: &Path) -> i64 {
    Connection::open(path)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM paper_accounts", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn migration_requires_confirmation_and_exclusive_lock() {
    let fixture = Fixture::new();
    let phrase = "INITIALIZE-EMPTY-PAPER-ACCOUNT";
    assert!(!migrate(&fixture.database, "SOLUSDC", None).status.success());
    assert!(!migrate(&fixture.database, "SOLUSDC", Some("wrong"))
        .status
        .success());
    assert_eq!(paper_account_count(&fixture.database), 0);

    let lock_path = fixture.directory.path().join("bot.db.process-lock");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(lock_path)
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let blocked = migrate(&fixture.database, "SOLUSDC", Some(phrase));
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("process lock"));
    assert_eq!(paper_account_count(&fixture.database), 0);
    drop(lock);

    assert!(!migrate(&fixture.database, "BTCUSDT", Some(phrase))
        .status
        .success());
    let result = migrate(&fixture.database, "SOLUSDC", Some(phrase));
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(paper_account_count(&fixture.database), 1);
    assert!(!migrate(&fixture.database, "SOLUSDC", Some(phrase))
        .status
        .success());
    assert_eq!(paper_account_count(&fixture.database), 1);
}

#[test]
fn migration_rejects_database_with_hard_link_alias() {
    let fixture = Fixture::new();
    std::fs::hard_link(&fixture.database, fixture.directory.path().join("alias.db")).unwrap();
    let result = migrate(
        &fixture.database,
        "SOLUSDC",
        Some("INITIALIZE-EMPTY-PAPER-ACCOUNT"),
    );
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("hard links"));
    assert_eq!(paper_account_count(&fixture.database), 0);
}
