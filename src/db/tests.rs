use super::*;
use crate::config::AppConfig;
use crate::types::{
    AccountInfo, BotStatus, GridOrder, GridStats, OrderSide, PositionInfo, RemainderPlan,
    TradeRecord, TradingMode,
};
use chrono::Duration as ChronoDuration;
use rusqlite::params;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[test]
fn daily_trade_summaries_respects_non_hour_aligned_half_open_windows() {
    let db = Database::open(":memory:").unwrap();
    let ts = |raw: &str| {
        DateTime::parse_from_rfc3339(raw)
            .unwrap()
            .with_timezone(&Utc)
    };
    let window_start = ts("2026-10-10T10:15:00.500000000Z");
    let window_end = ts("2026-10-10T12:45:00.250000000Z");

    let make = |id: &str,
                symbol: &str,
                mode: TradingMode,
                side: OrderSide,
                pnl: Decimal,
                verified: bool,
                timestamp: DateTime<Utc>| TradeRecord {
        trade_id: id.into(),
        client_order_id: format!("cid-{id}"),
        symbol: symbol.into(),
        mode,
        side,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: pnl,
        commission: Decimal::ZERO,
        pnl_verified: verified,
        is_maker: true,
        timestamp,
        note: String::new(),
    };

    for record in [
        make(
            "before-start",
            "SOLUSDC",
            TradingMode::Paper,
            OrderSide::Buy,
            dec!(100),
            true,
            window_start - ChronoDuration::nanoseconds(1),
        ),
        make(
            "at-start",
            "SOLUSDC",
            TradingMode::Paper,
            OrderSide::Buy,
            dec!(1.5),
            true,
            window_start,
        ),
        make(
            "mid-hour",
            "SOLUSDC",
            TradingMode::Paper,
            OrderSide::Sell,
            dec!(2.25),
            true,
            ts("2026-10-10T11:30:00Z"),
        ),
        make(
            "before-end-pending",
            "SOLUSDC",
            TradingMode::Paper,
            OrderSide::Sell,
            dec!(99),
            false,
            window_end - ChronoDuration::nanoseconds(1),
        ),
        make(
            "at-end",
            "SOLUSDC",
            TradingMode::Paper,
            OrderSide::Buy,
            dec!(50),
            true,
            window_end,
        ),
        make(
            "after-end",
            "SOLUSDC",
            TradingMode::Paper,
            OrderSide::Sell,
            dec!(60),
            true,
            ts("2026-10-10T12:59:00Z"),
        ),
        make(
            "outside-only-scope",
            "BTCUSDC",
            TradingMode::Live,
            OrderSide::Buy,
            dec!(10),
            true,
            ts("2026-10-10T10:05:00Z"),
        ),
    ] {
        assert!(db.insert_trade(&record).unwrap());
    }

    assert!(db
        .daily_trade_summaries(window_end, window_end)
        .unwrap()
        .is_empty());
    assert!(db
        .daily_trade_summaries(window_end, window_start)
        .unwrap()
        .is_empty());

    let summaries = db.daily_trade_summaries(window_start, window_end).unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].symbol, "SOLUSDC");
    assert_eq!(summaries[0].mode, TradingMode::Paper);
    assert_eq!(summaries[0].buy_count, 1);
    assert_eq!(summaries[0].sell_count, 2);
    assert_eq!(summaries[0].realized_pnl, dec!(3.75));
    assert_eq!(summaries[0].pending_pnl_count, 1);

    let sub_hour = db
        .daily_trade_summaries(window_start, ts("2026-10-10T10:20:00Z"))
        .unwrap();
    assert_eq!(sub_hour.len(), 1);
    assert_eq!(sub_hour[0].buy_count, 1);
    assert_eq!(sub_hour[0].sell_count, 0);
    assert_eq!(sub_hour[0].realized_pnl, dec!(1.5));
    assert_eq!(sub_hour[0].pending_pnl_count, 0);

    db.conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE trade_hourly_rollups SET latest_timestamp = '2026-10-10T13:00:00Z', realized_pnl = '999999' \
             WHERE symbol = 'SOLUSDC' AND mode = 'PAPER' AND hour = ?1",
            params![ts("2026-10-10T11:00:00Z").timestamp()],
        )
        .unwrap();
    let aligned = db
        .daily_trade_summaries(ts("2026-10-10T11:00:00Z"), ts("2026-10-10T12:00:00Z"))
        .unwrap();
    assert_eq!(aligned.len(), 1);
    assert_eq!(aligned[0].buy_count, 0);
    assert_eq!(aligned[0].sell_count, 1);
    assert_eq!(aligned[0].realized_pnl, dec!(2.25));
}

#[test]
fn save_remainder_plan_prunes_inactive_rows_beyond_64_per_scope() {
    let db = Database::open(":memory:").unwrap();
    let order = |id: &str, symbol: &str| GridOrder {
        client_order_id: id.into(),
        order_id: Some(1),
        symbol: symbol.into(),
        side: OrderSide::Sell,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: crate::types::OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: true,
        purpose: crate::types::OrderPurpose::Remainder,
        merge_sources: Vec::new(),
    };

    db.save_remainder_plan(&RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        sources: Vec::new(),
        target: order("paper-inactive", "SOLUSDC"),
        phase: crate::types::RemainderPhase::Complete,
    })
    .unwrap();

    for index in 0..70 {
        let id = format!("live-inactive-{index}");
        db.save_remainder_plan(&RemainderPlan {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            sources: Vec::new(),
            target: order(&id, "SOLUSDC"),
            phase: crate::types::RemainderPhase::Complete,
        })
        .unwrap();
    }

    db.save_remainder_plan(&RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: Vec::new(),
        target: order("live-active", "SOLUSDC"),
        phase: crate::types::RemainderPhase::Submitting,
    })
    .unwrap();

    let conn = db.conn.lock().unwrap();
    let live_inactive: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM remainder_plans WHERE symbol = 'SOLUSDC' AND mode = 'LIVE' AND active = 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(live_inactive, 64);
    let oldest_retained: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM remainder_plans WHERE target_client_order_id = 'live-inactive-6')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let pruned: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM remainder_plans WHERE target_client_order_id = 'live-inactive-5')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let paper_kept: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM remainder_plans WHERE target_client_order_id = 'paper-inactive')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(oldest_retained);
    assert!(!pruned);
    assert!(paper_kept);
    drop(conn);
    assert_eq!(
        db.load_remainder_plan("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .target
            .client_order_id,
        "live-active"
    );
}

#[test]
fn init_tables_includes_stats_hour_without_alter_table() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE trade_hourly_rollup_state (id INTEGER PRIMARY KEY CHECK (id = 1));
         INSERT INTO trade_hourly_rollup_state (id) VALUES (1);",
    )
    .unwrap();
    let db = Database {
        conn: Mutex::new(conn),
        path: ":memory:".into(),
        #[cfg(unix)]
        file_identity: None,
        history_account_ids: Mutex::new(HashMap::new()),
        trade_revisions: RevisionTracker::default(),
        trade_clear_revisions: RevisionTracker::default(),
        revision_publish_hook: Mutex::new(None),
        membership_commit_hook: Mutex::new(None),
        today_pnl_cache: tokio::sync::Mutex::new(None),
        session_cache: Default::default(),
        paper_ledger: Mutex::new(None),
        analysis_export: Mutex::new(()),
        hourly_stats_queries: AtomicU64::new(0),
        benchmark_wait_samples: std::sync::OnceLock::new(),
    };
    db.init_tables().unwrap();
    assert!(db.insert_trade(&revision_test_trade()).unwrap());
}

fn revision_test_trade() -> TradeRecord {
    TradeRecord {
        trade_id: "revision-trade".into(),
        client_order_id: "revision-order".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn revision_read_does_not_block_runtime_during_sql_write() {
    use std::sync::{mpsc, Arc, LazyLock};
    use std::time::Duration;

    type BusyHandshake = (tokio::sync::oneshot::Sender<()>, mpsc::Receiver<()>);
    static BUSY: LazyLock<Mutex<Option<BusyHandshake>>> = LazyLock::new(|| Mutex::new(None));
    fn busy_handler(_: i32) -> bool {
        let (reached, resume) = BUSY.lock().unwrap().take().unwrap();
        reached.send(()).unwrap();
        resume.recv().unwrap();
        true
    }

    let path = std::env::temp_dir().join(format!("revision-lock-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let external = Connection::open(&path).unwrap();
    external.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (busy_tx, busy_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    *BUSY.lock().unwrap() = Some((busy_tx, resume_rx));
    db.conn
        .lock()
        .unwrap()
        .busy_handler(Some(busy_handler))
        .unwrap();
    let writer_db = Arc::clone(&db);
    let writer =
        tokio::task::spawn_blocking(move || writer_db.insert_trade(&revision_test_trade()));
    busy_rx.await.unwrap();

    // Release SQLite even if a regression blocks the runtime thread.
    let (read_tx, read_rx) = mpsc::channel();
    let watchdog = std::thread::spawn(move || {
        let completed = read_rx.recv_timeout(Duration::from_secs(5)).is_ok();
        external.execute_batch("ROLLBACK").unwrap();
        resume_tx.send(()).unwrap();
        completed
    });
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 0);
    read_tx.send(()).unwrap();
    assert!(
        watchdog.join().unwrap(),
        "revision read blocked the Tokio thread"
    );
    assert!(writer.await.unwrap().unwrap());
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 1);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn committed_write_publishes_revision_before_releasing_connection() {
    use std::sync::{mpsc, Arc};

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    *db.revision_publish_hook.lock().unwrap() = Some((reached_tx, resume_rx));
    let writer_db = Arc::clone(&db);
    let writer = std::thread::spawn(move || writer_db.insert_trade(&revision_test_trade()));
    reached_rx.recv().unwrap();
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 0);
    assert_eq!(db.trade_data_revision(), 0);
    assert!(db.conn.try_lock().is_err());
    resume_tx.send(()).unwrap();
    assert!(writer.join().unwrap().unwrap());
    let conn = db.conn.lock().unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM trades", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 1);
    assert_eq!(db.trade_data_revision(), 1);
}

#[test]
fn pnl_commit_failure_rolls_back_without_publishing_revision() {
    let db = Database::open(":memory:").unwrap();
    let trade = revision_test_trade();
    assert!(db.insert_trade(&trade).unwrap());
    db.conn.lock().unwrap().execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE revision_parent (id INTEGER PRIMARY KEY);
             CREATE TABLE revision_child (parent_id INTEGER REFERENCES revision_parent(id) DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER reject_pnl_commit AFTER UPDATE ON trades BEGIN
                 INSERT INTO revision_child VALUES (1);
             END;"
        ).unwrap();
    assert!(db.verify_trade_pnl(&trade, 0).is_err());
    assert_eq!(db.trade_data_revision(), 1);
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 1);
    assert!(!db.get_recent_trades(1).unwrap()[0].pnl_verified);
    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_pnl_commit")
        .unwrap();
    assert!(db.verify_trade_pnl(&trade, 0).unwrap());
    assert_eq!(db.trade_data_revision(), 2);
}

#[test]
fn revisions_follow_successful_trade_changes_and_rollback_failures() {
    let db = Database::open(":memory:").unwrap();
    let trade = revision_test_trade();
    let stats = GridStats::default();
    db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_insert BEFORE INSERT ON trades BEGIN SELECT RAISE(ABORT, 'reject'); END;"
        ).unwrap();
    assert!(db.insert_trade(&trade).is_err());
    assert!(db.insert_trade_with_stats(&trade, &stats).is_err());
    assert_eq!(db.trade_data_revision(), 0);
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 0);
    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_insert")
        .unwrap();
    assert!(db.insert_trade_with_stats(&trade, &stats).unwrap());
    assert!(!db.insert_trade(&trade).unwrap());
    assert!(!db.insert_trade_with_stats(&trade, &stats).unwrap());
    assert_eq!(db.trade_data_revision(), 1);

    db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_update BEFORE UPDATE ON trades BEGIN SELECT RAISE(ABORT, 'reject'); END;
             CREATE TRIGGER reject_delete BEFORE DELETE ON trades BEGIN SELECT RAISE(ABORT, 'reject'); END;"
        ).unwrap();
    assert!(db.verify_trade_pnl(&trade, 0).is_err());
    assert!(db.clear_trade_history_for_symbol("SOLUSDC").is_err());
    assert!(db.clear_selected_database(true, false).is_err());
    assert_eq!(db.trade_data_revision(), 1);
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 1);
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_update; DROP TRIGGER reject_delete;")
        .unwrap();

    let mut other_symbol = trade.clone();
    other_symbol.symbol = "BTCUSDC".into();
    assert!(db.verify_trade_pnl(&other_symbol, 0).unwrap());
    assert!(!db.verify_trade_pnl(&other_symbol, 0).unwrap());
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 2);
    assert_eq!(db.symbol_trade_revision("BTCUSDC"), 0);
    db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 3);
    assert_eq!(db.symbol_trade_revision("BTCUSDC"), 0);
    db.clear_selected_database(false, true).unwrap();
    assert_eq!(db.trade_data_revision(), 3);
    db.clear_selected_database(true, false).unwrap();
    assert_eq!(db.trade_data_revision(), 4);
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), 4);
    assert_eq!(db.symbol_trade_revision("BTCUSDC"), 4);
}

#[test]
fn clear_revisions_track_successful_clears_by_symbol() {
    let db = Database::open(":memory:").unwrap();
    let sol = revision_test_trade();
    let mut btc = sol.clone();
    btc.trade_id = "btc-trade".into();
    btc.client_order_id = "btc-order".into();
    btc.symbol = "BTCUSDC".into();
    assert!(db.insert_trade(&sol).unwrap());
    assert!(db.insert_trade(&btc).unwrap());

    let (pending, revision) = db
        .get_unverified_trades_with_clear_revision("SOLUSDC", TradingMode::Paper, 10, 0)
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(revision, 0);
    assert_eq!(db.symbol_trade_clear_revision("BTCUSDC"), 0);

    db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_clear BEFORE DELETE ON trades BEGIN SELECT RAISE(ABORT, 'reject'); END;"
        ).unwrap();
    assert!(db.clear_trade_history_for_symbol("SOLUSDC").is_err());
    assert!(db.clear_trade_history().is_err());
    assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 0);
    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_clear")
        .unwrap();

    db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
    let (pending, sol_revision) = db
        .get_unverified_trades_with_clear_revision("SOLUSDC", TradingMode::Paper, 10, 0)
        .unwrap();
    assert!(pending.is_empty());
    assert_eq!(sol_revision, 1);
    let (pending, btc_revision) = db
        .get_unverified_trades_with_clear_revision("BTCUSDC", TradingMode::Paper, 10, 0)
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(btc_revision, 0);

    db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
    assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 2);
    db.clear_selected_database(false, true).unwrap();
    assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 2);
    db.clear_trade_history().unwrap();
    assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 3);
    assert_eq!(db.symbol_trade_clear_revision("BTCUSDC"), 3);
    assert_eq!(db.symbol_trade_clear_revision("ETHUSDC"), 3);
    db.clear_trade_history_for_symbol("BTCUSDC").unwrap();
    assert_eq!(db.symbol_trade_clear_revision("BTCUSDC"), 4);
    assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 3);
}

#[test]
fn committed_clear_publishes_revision_before_releasing_connection() {
    use std::sync::{mpsc, Arc};

    let db = Arc::new(Database::open(":memory:").unwrap());
    assert!(db.insert_trade(&revision_test_trade()).unwrap());
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    *db.revision_publish_hook.lock().unwrap() = Some((reached_tx, resume_rx));
    let writer_db = Arc::clone(&db);
    let writer = std::thread::spawn(move || writer_db.clear_trade_history_for_symbol("SOLUSDC"));
    reached_rx.recv().unwrap();
    assert!(db.conn.try_lock().is_err());
    resume_tx.send(()).unwrap();
    assert_eq!(writer.join().unwrap().unwrap(), 1);
    let (pending, revision) = db
        .get_unverified_trades_with_clear_revision("SOLUSDC", TradingMode::Paper, 10, 0)
        .unwrap();
    assert!(pending.is_empty());
    assert_eq!(revision, 1);
    assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), revision);
}

#[cfg(unix)]
#[test]
fn database_files_are_private_without_changing_existing_parent() {
    use std::os::unix::fs::PermissionsExt;

    let root = std::env::temp_dir().join(format!("db-permissions-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    let directory = root.join("new").join("nested");
    let path = directory.join("bot.db");
    let db = Database::open(&path).unwrap();
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&root), 0o755);
    assert_eq!(mode(&root.join("new")) & 0o077, 0);
    assert_eq!(mode(&directory) & 0o077, 0);
    assert_eq!(mode(&path) & 0o077, 0);
    let wal = directory.join("bot.db-wal");
    let shm = directory.join("bot.db-shm");
    assert_eq!(mode(&wal) & 0o077, 0);
    assert_eq!(mode(&shm) & 0o077, 0);
    let uid = unsafe { libc::geteuid() } as u32;
    for file in [&path, &wal, &shm] {
        validate_database_file(file, uid).unwrap();
        let error = validate_database_file(file, uid.wrapping_add(1)).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(error
            .to_string()
            .contains("differs from service effective user"));
    }

    // Existing insecure files must be repaired by the operator, without
    // changing any of their permissions during a failed open.
    for file in [&path, &wal, &shm] {
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = match Database::open(&path) {
            Ok(_) => panic!("accepted insecure {}", file.display()),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("remove group and other permissions"));
        assert_eq!(mode(file), 0o644);
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert_eq!(mode(&root), 0o755);
    let reopened = Database::open(&path).unwrap();
    drop(reopened);
    drop(db);
    let journal = directory.join("bot.db-journal");
    std::fs::write(&journal, b"old journal").unwrap();
    std::fs::set_permissions(&journal, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(prepare_database_path(&path).is_err());
    assert_eq!(mode(&journal), 0o644);
    std::fs::set_permissions(&journal, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(validate_database_file(&journal, uid.wrapping_add(1)).is_err());
    prepare_database_path(&path).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn database_and_sidecar_symlinks_are_rejected_without_changing_targets() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let root = std::env::temp_dir().join(format!("db-symlinks-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("bot.db");
    let target = root.join("target");
    std::fs::write(&target, b"unrelated").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
    let target_mode = || std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;

    symlink(&target, &path).unwrap();
    assert!(prepare_database_path(&path).is_err());
    assert_eq!(target_mode(), 0o644);
    std::fs::remove_file(&path).unwrap();
    symlink(root.join("missing"), &path).unwrap();
    assert!(prepare_database_path(&path).is_err());
    assert!(!root.join("missing").exists());
    std::fs::remove_file(&path).unwrap();

    prepare_database_path(&path).unwrap();
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = root.join(format!("bot.db{suffix}"));
        symlink(&target, &sidecar).unwrap();
        assert!(prepare_database_path(&path).is_err());
        assert_eq!(target_mode(), 0o644);
        std::fs::remove_file(&sidecar).unwrap();
        symlink(root.join("missing"), &sidecar).unwrap();
        assert!(prepare_database_path(&path).is_err());
        std::fs::remove_file(&sidecar).unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn database_rejects_sqlite_uri_paths() {
    assert!(Database::open("file:other.db?mode=rwc").is_err());
}

#[test]
fn database_uses_durable_journal_settings() {
    let path = std::env::temp_dir().join(format!("durable-{}.db", uuid::Uuid::new_v4()));
    let db = Database::open(&path).unwrap();
    {
        let conn = db.conn.lock().unwrap();
        let journal: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        let synchronous: i64 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal, "wal");
        assert_eq!(synchronous, 2);
    }
    drop(db);
    std::fs::remove_file(path).unwrap();

    let memory = Database::open(":memory:").unwrap();
    let conn = memory.conn.lock().unwrap();
    let journal: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal, "memory");
}

#[test]
fn symbol_cleanup_preserves_unattributed_legacy_stats_but_selected_export_omits_them() {
    let db = Database::open(":memory:").unwrap();
    db.conn.lock().unwrap().execute_batch(
            "INSERT INTO grid_stats VALUES (1, 7, 3, '4', '500', '2026-10-10');
             INSERT INTO scoped_grid_stats VALUES ('SOLUSDC', 'LIVE', 1, 0, '0', '100', '2026-10-10');
             INSERT INTO scoped_grid_stats VALUES ('BTCUSDC', 'LIVE', 2, 1, '4', '400', '2026-10-10');"
        ).unwrap();
    assert_eq!(db.clear_trade_history_for_symbol("SOLUSDC").unwrap(), 0);
    assert!(db
        .load_scoped_stats("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_none());
    assert_eq!(
        db.load_scoped_stats("BTCUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .0,
        2
    );
    assert_eq!(db.load_stats().unwrap().unwrap().0, 7);

    let directory = std::env::temp_dir().join(format!("legacy-export-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    for (name, bytes, expected) in [
        (
            "selected.db",
            db.export_selected_analysis_snapshot(true, false).unwrap(),
            0,
        ),
        ("full.db", db.export_analysis_snapshot().unwrap(), 1),
    ] {
        let path = directory.join(name);
        std::fs::write(&path, bytes).unwrap();
        let snapshot =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let legacy: i64 = snapshot
            .query_row("SELECT COUNT(*) FROM grid_stats", [], |row| row.get(0))
            .unwrap();
        let other: i64 = snapshot
            .query_row(
                "SELECT COUNT(*) FROM scoped_grid_stats WHERE symbol = 'BTCUSDC'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(legacy, expected);
        assert_eq!(other, 1);
    }
    std::fs::remove_dir_all(directory).unwrap();
    db.clear_trade_history().unwrap();
    assert!(db.load_stats().unwrap().is_none());
}

#[test]
fn cleanup_preserves_frontiers_and_deduplication_across_restarts() {
    let path = std::env::temp_dir().join(format!("cleanup-{}.db", uuid::Uuid::new_v4()));
    let db = Database::open(&path).unwrap();
    let mut trade = TradeRecord {
        trade_id: "cleanup-buy".into(),
        client_order_id: "gb_b_cleanup".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side: OrderSide::Buy,
        price: dec!(119.2),
        quantity: dec!(1),
        amount_usdc: dec!(119.2),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    };
    db.insert_trade(&trade).unwrap();
    let mut latest = trade.clone();
    latest.trade_id = "cleanup-sell".into();
    latest.client_order_id = "gb_s_cleanup".into();
    latest.side = OrderSide::Sell;
    latest.price = dec!(119.3);
    // Equal timestamps must retain the later inserted fill.
    db.insert_trade(&latest).unwrap();
    assert_eq!(db.clear_trade_history().unwrap(), 2);
    drop(db);
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.get_pair_opening("SOLUSDC", "gb_b_cleanup", TradingMode::Live)
            .unwrap(),
        Some((OrderSide::Buy, dec!(119.2), dec!(1)))
    );
    assert_eq!(
        db.get_pair_opening("BTCUSDC", "gb_b_cleanup", TradingMode::Live)
            .unwrap(),
        None
    );
    assert_eq!(
        db.get_pair_opening("SOLUSDC", "gb_b_cleanup", TradingMode::Paper)
            .unwrap(),
        None
    );
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Sell, dec!(119.3)))
    );
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Paper).unwrap(),
        None
    );
    assert!(db
        .has_recorded_order(&trade.client_order_id, TradingMode::Live)
        .unwrap());
    assert!(!db
        .has_recorded_order(&trade.client_order_id, TradingMode::Paper)
        .unwrap());
    assert!(!db.insert_trade(&trade).unwrap());
    trade.trade_id = "different-replayed-id".into();
    assert!(!db.insert_trade(&trade).unwrap());
    trade.trade_id = "cleanup-buy".into();
    trade.pnl_verified = true;
    assert!(!db.verify_trade_pnl(&trade, 0).unwrap());
    assert_eq!(db.clear_trade_history().unwrap(), 0);
    assert_eq!(
        db.get_pair_opening("SOLUSDC", "gb_b_cleanup", TradingMode::Live)
            .unwrap(),
        Some((OrderSide::Buy, dec!(119.2), dec!(1)))
    );
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Sell, dec!(119.3)))
    );

    trade.trade_id = "cleanup-new-buy".into();
    trade.client_order_id = "gb_b_new_cleanup".into();
    assert!(db.insert_trade(&trade).unwrap());
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Buy, dec!(119.2)))
    );
    assert_eq!(db.clear_trade_history().unwrap(), 1);
    // Late historical data must not move the saved frontier backwards.
    trade.trade_id = "late-buy".into();
    trade.client_order_id = "gb_b_late_cleanup".into();
    trade.timestamp -= ChronoDuration::hours(1);
    trade.price = dec!(90);
    db.insert_trade(&trade).unwrap();
    db.clear_trade_history().unwrap();
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Buy, dec!(119.2)))
    );
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn failed_cleanup_rolls_back_history_stats_and_recovery_metadata() {
    let db = Database::open(":memory:").unwrap();
    db.conn.lock().unwrap().execute_batch(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity,
                 amount_usdc, realized_pnl, commission, is_maker, timestamp, note)
             VALUES ('t1', 'gb_b_1', 'SOLUSDC', 'LIVE', 'BUY', '100', '1', '100', '0', '0', 1, '2026-10-10T00:00:00Z', 'test');
             INSERT INTO scoped_grid_stats VALUES ('SOLUSDC', 'LIVE', 1, 0, '0', '100', '2026-10-10');
             CREATE TRIGGER reject_cleanup BEFORE DELETE ON trades
                 BEGIN SELECT RAISE(ABORT, 'cleanup test failure'); END;"
        ).unwrap();
    assert!(db.clear_trade_history().is_err());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .0,
        1
    );
    let conn = db.conn.lock().unwrap();
    for table in [
        "cleared_trade_ids",
        "execution_frontiers",
        "cleared_pair_openings",
    ] {
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[test]
fn selected_cleanup_rolls_back_trade_changes_when_snapshot_deletion_fails() {
    let db = Database::open(":memory:").unwrap();
    db.conn.lock().unwrap().execute_batch(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity,
                 amount_usdc, realized_pnl, commission, is_maker, timestamp, note)
             VALUES ('t1', 'gb_b_1', 'SOLUSDC', 'LIVE', 'BUY', '100', '1', '100', '0', '0', 1, '2026-10-10T00:00:00Z', 'test');
             INSERT INTO scoped_grid_stats VALUES ('SOLUSDC', 'LIVE', 1, 0, '0', '100', '2026-10-10');
             INSERT INTO account_hourly_snapshots (hour, payload) VALUES (1, '[]');
             CREATE TRIGGER reject_snapshot_cleanup BEFORE DELETE ON account_hourly_snapshots
                 BEGIN SELECT RAISE(ABORT, 'snapshot cleanup test failure'); END;"
        ).unwrap();

    let revision = db.trade_data_revision();
    assert!(db.clear_selected_database(true, true).is_err());
    assert_eq!(db.trade_data_revision(), revision);
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .0,
        1
    );
    let conn = db.conn.lock().unwrap();
    for (table, expected) in [
        ("account_hourly_snapshots", 1),
        ("cleared_trade_ids", 0),
        ("execution_frontiers", 0),
    ] {
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, expected, "{table}");
    }
}

#[test]
fn history_identity_survives_reopen_and_is_removed_from_analysis_export() {
    let directory =
        std::env::temp_dir().join(format!("history-identities-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("source.db");
    let identity = [42_u8; 32];
    let other = [43_u8; 32];
    let first = {
        let db = Database::open(&path).unwrap();
        let first = db.history_account_id(identity).unwrap();
        assert!(uuid::Uuid::parse_str(&first).is_ok());
        assert_ne!(first, db.history_account_id(other).unwrap());
        let recovery_identity = [44_u8; 32];
        db.conn.lock().unwrap().execute(
                "INSERT INTO terminal_order_disagreements VALUES (?1, 'SOLUSDC', 'test', '{}', '{}')",
                params![recovery_identity.as_slice()],
            ).unwrap();
        let exported = db.export_analysis_snapshot().unwrap();
        assert!(!exported
            .windows(identity.len())
            .any(|bytes| bytes == identity));
        let exported_path = directory.join("export.db");
        std::fs::write(&exported_path, &exported).unwrap();
        let copy = Connection::open(&exported_path).unwrap();
        let preserved: Vec<u8> = copy
            .query_row(
                "SELECT account_key FROM terminal_order_disagreements",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preserved.as_slice(), recovery_identity.as_slice());
        first
    };
    let reopened = Database::open(&path).unwrap();
    assert_eq!(first, reopened.history_account_id(identity).unwrap());
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn analysis_export_captures_wal_and_preserves_records_without_credentials() {
    let directory =
        std::env::temp_dir().join(format!("binancebot-export-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let source_path = directory.join("source.db");
    let export_path = directory.join("analysis.db");
    let db = Database::open(&source_path).unwrap();
    db.conn
        .lock()
        .unwrap()
        .execute_batch("PRAGMA wal_autocheckpoint = 0")
        .unwrap();
    let mut config = AppConfig::default();
    config.exchange.api_key = "export-test-api-key".into();
    config.exchange.api_secret = "historical-secret-marker".repeat(500);
    db.save_config(&config).unwrap();
    config.exchange.api_secret = "export-test-api-secret".into();
    config.telegram.bot_token = "export-test-telegram-token".into();
    config.telegram.chat_id = "export-test-chat-id".into();
    db.save_config(&config).unwrap();
    db.create_session("export-test-session-token").unwrap();
    db.save_bot_status(BotStatus::Running).unwrap();
    {
        let conn = db.conn.lock().unwrap();
        for index in 0..65 {
            conn.execute(
                    "INSERT INTO trades (rowid, trade_id, client_order_id, symbol, mode, side, price, quantity, amount_usdc, realized_pnl, commission, is_maker, timestamp, note) VALUES (?1, ?2, ?2, ?3, ?4, 'BUY', '118.3', '2.53', '299.299', '0', '0', 1, '2026-09-30T00:00:00Z', 'export test')",
                    params![10 + index * 2, format!("gb_b_export_{index}"), if index % 2 == 0 { "SOLUSDC" } else { "UNIUSDC" }, if index % 2 == 0 { "LIVE" } else { "PAPER" }],
                ).unwrap();
        }
        conn.execute_batch(
                r#"INSERT INTO managed_orders VALUES ('gb_s_exit', 'SOLUSDC', '{"paired_client_order_id":"gb_b_export_0"}', '2026-09-30');
                INSERT INTO pair_intents VALUES ('gb_b_export_0', 'SOLUSDC', 'LIVE', '{"price":"118.4"}', '2026-09-30');
                INSERT INTO remainder_plans VALUES ('gb_s_remainder', 'SOLUSDC', 'LIVE', 1, '{"sources":[]}', '2026-09-30');
                INSERT INTO scoped_grid_stats VALUES ('SOLUSDC', 'LIVE', 65, 0, '0', '19454.435', '2026-09-30');"#,
            ).unwrap();
    }
    assert!(
        std::fs::metadata(directory.join("source.db-wal"))
            .unwrap()
            .len()
            > 0
    );
    let bytes = db.export_analysis_snapshot().unwrap();
    assert!(bytes.starts_with(b"SQLite format 3\0"));
    let raw = String::from_utf8_lossy(&bytes);
    for secret in [
        "export-test-api-key",
        "export-test-api-secret",
        "historical-secret-marker",
        "export-test-telegram-token",
        "export-test-chat-id",
        "export-test-session-token",
    ] {
        assert!(!raw.contains(secret), "Credential bytes leaked: {secret}");
    }
    std::fs::write(&export_path, bytes).unwrap();
    let snapshot =
        Connection::open_with_flags(&export_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let integrity: String = snapshot
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    for (table, expected) in [
        ("trades", 65),
        ("managed_orders", 1),
        ("pair_intents", 1),
        ("remainder_plans", 1),
        ("scoped_grid_stats", 1),
        ("app_config", 1),
        ("bot_runtime", 1),
        ("auth_sessions", 0),
    ] {
        let count: i64 = snapshot
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, expected, "Incorrect exported count for {table}");
    }
    let rows: Vec<(i64, String)> = snapshot
        .prepare("SELECT rowid, client_order_id FROM trades ORDER BY rowid")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    for (index, (rowid, id)) in rows.iter().enumerate() {
        assert_eq!(
            *rowid,
            10 + index as i64 * 2,
            "Fill journal cursors must be preserved"
        );
        assert_eq!(id, &format!("gb_b_export_{index}"));
    }
    // Preserve all analysis fields, especially paired IDs and numeric strings.
    for table in [
        "trades",
        "managed_orders",
        "pair_intents",
        "remainder_plans",
        "grid_stats",
        "scoped_grid_stats",
        "bot_runtime",
    ] {
        let read_rows = |connection: &Connection| {
            let mut statement = connection
                .prepare(&format!("SELECT rowid, * FROM {table} ORDER BY rowid"))
                .unwrap();
            let columns = statement.column_count();
            statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get::<_, rusqlite::types::Value>(column))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            read_rows(&snapshot),
            read_rows(&db.conn.lock().unwrap()),
            "Changed analysis data in {table}"
        );
    }
    let json: String = snapshot
        .query_row("SELECT config_json FROM app_config", [], |r| r.get(0))
        .unwrap();
    let exported: AppConfig = serde_json::from_str(&json).unwrap();
    assert!(exported.exchange.api_key.is_empty());
    assert!(exported.exchange.api_secret.is_empty());
    assert!(exported.telegram.bot_token.is_empty());
    assert!(exported.telegram.chat_id.is_empty());
    assert_eq!(exported.grid, config.grid);
    assert_eq!(db.load_config().unwrap().unwrap().exchange, config.exchange);
    assert_eq!(
        db.load_config().unwrap().unwrap().telegram.bot_token,
        config.telegram.bot_token
    );
    assert!(db.is_session_valid("export-test-session-token").unwrap());
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
    drop(snapshot);
    drop(db);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn analysis_export_rejects_unreadable_config_instead_of_exporting_secrets() {
    let db = Database::open(":memory:").unwrap();
    db.conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO app_config VALUES (1, 'SOLUSDC', 'invalid-secret-config', '2026-09-30')",
            [],
        )
        .unwrap();
    assert!(db.export_analysis_snapshot().is_err());
}

#[test]
fn managed_order_metadata_survives_reload() {
    let db = Database::open(":memory:").unwrap();
    let order = GridOrder {
        client_order_id: "gb_s_paired".into(),
        order_id: Some(42),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(115.1),
        quantity: dec!(1),
        amount_usdc: dec!(115.1),
        status: crate::types::OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: Some("gb_b_purchase".into()),
        is_take_profit: true,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&order).unwrap();
    let loaded = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(
        loaded[0].paired_client_order_id,
        order.paired_client_order_id
    );
    assert!(loaded[0].is_take_profit);
    db.delete_managed_order(&order.client_order_id).unwrap();
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
}

#[test]
fn expired_session_is_rejected() {
    let db = Database::open(":memory:").unwrap();
    db.create_session("old-token").unwrap();
    db.conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE auth_sessions SET created_at = ?1 WHERE token = 'old-token';",
            params![(Utc::now() - ChronoDuration::hours(25)).to_rfc3339()],
        )
        .unwrap();
    assert!(!db.is_session_valid("old-token").unwrap());
    assert!(!db.is_session_valid("old-token").unwrap());
}

#[test]
fn bot_pause_state_is_persistent() {
    let db = Database::open(":memory:").unwrap();
    assert_eq!(db.load_bot_status().unwrap(), None);
    db.save_bot_status(BotStatus::Paused).unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    db.save_bot_status(BotStatus::Running).unwrap();
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
}

#[test]
fn test_db_config_lifecycle() {
    let db = Database::open(":memory:").unwrap();
    assert!(db.load_config().unwrap().is_none());

    let mut config = AppConfig::default();
    config.exchange.symbol = "BTCUSDC".to_string();
    config.grid.grid_interval = dec!(50.0);

    db.save_config(&config).unwrap();
    let loaded = db.load_config().unwrap().expect("config should exist");
    assert_eq!(loaded.exchange.symbol, "BTCUSDC");
    assert_eq!(loaded.grid.grid_interval, dec!(50.0));
}

#[test]
fn dashboard_membership_and_settings_survive_reopen_without_losing_history() {
    let path = std::env::temp_dir().join(format!("dashboard-settings-{}.db", uuid::Uuid::new_v4()));
    let db = Database::open(&path).unwrap();
    let mut initial = AppConfig::default();
    initial.exchange.recv_window = 9000;
    initial.ensure_pair_list().unwrap();
    db.save_config(&initial).unwrap();
    db.conn.lock().unwrap().execute_batch(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity, amount_usdc,
               realized_pnl, commission, is_maker, timestamp, note)
             VALUES ('history', 'gb_b_history', 'SOLUSDC', 'PAPER', 'BUY', '100', '1', '100', '0', '0', 1, '2026-10-10T00:00:00Z', 'test');"
        ).unwrap();
    let mut added = AppConfig::default();
    added.exchange.symbol = "BTCUSDT".into();
    added.grid.grid_interval = dec!(10);
    added.exchange.recv_window = 9000;
    db.add_pair_config(&added).unwrap();
    db.save_server_config_checked(
        &crate::config::ServerConfig {
            host: "127.0.0.1".into(),
            port: 9090,
        },
        None,
    )
    .unwrap();
    added.grid.buy_window = 3;
    db.save_pair_config("BTCUSDT", &added).unwrap();
    db.remove_pair_config("SOLUSDC").unwrap();
    assert!(db.remove_pair_config("BTCUSDT").is_err());
    drop(db);
    let db = Database::open(&path).unwrap();
    let config = db.load_config().unwrap().unwrap();
    assert_eq!(config.server.port, 9090);
    let strategies = config.strategy_configs().unwrap();
    assert_eq!(strategies.len(), 1);
    assert_eq!(strategies[0].exchange.symbol, "BTCUSDT");
    assert_eq!(strategies[0].exchange.recv_window, 9000);
    assert_eq!(strategies[0].grid.buy_window, 3);
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        db.get_recent_trades_for("SOLUSDC", TradingMode::Paper, 10)
            .unwrap()
            .len(),
        1
    );
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn adding_a_pair_rolls_back_if_initial_pause_cannot_be_saved() {
    let db = Database::open(":memory:").unwrap();
    let mut initial = AppConfig::default();
    initial.ensure_pair_list().unwrap();
    db.save_config(&initial).unwrap();
    db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_pair_status BEFORE INSERT ON pair_runtime BEGIN SELECT RAISE(ABORT, 'test status failure'); END;"
        ).unwrap();
    let mut added = AppConfig::default();
    added.exchange.symbol = "BTCUSDT".into();
    assert!(db.add_pair_config(&added).is_err());
    assert_eq!(db.load_config().unwrap().unwrap().pairs.len(), 1);
}

#[test]
fn hourly_trade_stats_cover_beijing_today_and_isolate_scope() {
    let db = Database::open(":memory:").unwrap();
    let now = DateTime::parse_from_rfc3339("2026-09-28T10:30:00.123456789Z")
        .unwrap()
        .with_timezone(&Utc);
    let start = DateTime::parse_from_rfc3339("2026-09-27T16:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let empty = db
        .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, now)
        .unwrap();
    assert_eq!(empty.realized_pnl, Decimal::ZERO);
    assert_eq!(empty.pending_pnl_count, 0);
    assert_eq!(empty.buckets.len(), 19);
    assert!(empty
        .buckets
        .iter()
        .all(|b| b.buy_count == 0 && b.sell_count == 0));

    let mut trade = TradeRecord {
        trade_id: String::new(),
        client_order_id: String::new(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: start,
        note: String::new(),
    };
    // More than both the snapshot (50) and in-memory (200) history limits.
    for index in 0..250 {
        trade.trade_id = format!("buy-{index}");
        trade.client_order_id = format!("order-{index}");
        assert!(db.insert_trade(&trade).unwrap());
    }
    assert!(!db.insert_trade(&trade).unwrap());
    for (id, timestamp, side, symbol, mode) in [
        ("last", now, OrderSide::Sell, "SOLUSDC", TradingMode::Paper),
        (
            "utc-midnight",
            DateTime::parse_from_rfc3339("2026-09-28T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            OrderSide::Sell,
            "SOLUSDC",
            TradingMode::Paper,
        ),
        (
            "old",
            start - ChronoDuration::nanoseconds(1),
            OrderSide::Buy,
            "SOLUSDC",
            TradingMode::Paper,
        ),
        (
            "future",
            now + ChronoDuration::nanoseconds(1),
            OrderSide::Sell,
            "SOLUSDC",
            TradingMode::Paper,
        ),
        ("symbol", now, OrderSide::Buy, "BTCUSDC", TradingMode::Paper),
        ("live", now, OrderSide::Buy, "SOLUSDC", TradingMode::Live),
    ] {
        trade.trade_id = id.into();
        trade.client_order_id = id.into();
        trade.timestamp = timestamp;
        trade.side = side;
        trade.symbol = symbol.into();
        trade.mode = mode;
        trade.pnl_verified = true;
        trade.realized_pnl = match id {
            "last" => dec!(1.23456789),
            "utc-midnight" => dec!(-0.23456788),
            _ => dec!(999),
        };
        assert!(db.insert_trade(&trade).unwrap());
    }
    let stats = db
        .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, now)
        .unwrap();
    assert_eq!(stats.window_start, start);
    assert_eq!(stats.window_end, now);
    assert_eq!(stats.realized_pnl, dec!(1.00000001));
    assert_eq!(stats.pending_pnl_count, 250);
    assert_eq!(stats.buckets[0].buy_count, 250);
    assert_eq!(stats.buckets[8].sell_count, 1);
    assert_eq!(stats.buckets[18].sell_count, 1);
    assert_eq!(
        stats
            .buckets
            .iter()
            .map(|b| b.buy_count + b.sell_count)
            .sum::<u64>(),
        252
    );
    assert!(stats
        .buckets
        .windows(2)
        .all(|b| b[1].hour_start - b[0].hour_start == ChronoDuration::hours(1)));
    let later = db
        .get_hourly_trade_stats(
            "SOLUSDC",
            TradingMode::Paper,
            now + ChronoDuration::hours(1),
        )
        .unwrap();
    assert_eq!(later.buckets.len(), 20);
    assert_eq!(later.window_start, start);
    assert_eq!(later.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 250);
    assert_eq!(later.buckets.iter().map(|b| b.sell_count).sum::<u64>(), 3);

    // Beijing's next day begins at 16:00 UTC, before the UTC date changes.
    let next_midnight = start + ChronoDuration::days(1);
    trade.trade_id = "next-day".into();
    trade.client_order_id = "next-day".into();
    trade.symbol = "SOLUSDC".into();
    trade.mode = TradingMode::Paper;
    trade.side = OrderSide::Buy;
    trade.timestamp = next_midnight;
    assert!(db.insert_trade(&trade).unwrap());
    let next_day = db
        .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, next_midnight)
        .unwrap();
    assert_eq!(next_day.window_start, next_midnight);
    assert_eq!(next_day.window_end, next_midnight);
    assert_eq!(next_day.realized_pnl, dec!(999));
    assert_eq!(next_day.pending_pnl_count, 0);
    assert_eq!(next_day.buckets.len(), 1);
    assert_eq!(next_day.buckets[0].hour_start, next_midnight);
    assert_eq!(next_day.buckets[0].buy_count, 1);
    assert_eq!(next_day.buckets[0].sell_count, 0);

    let before_midnight = db
        .get_hourly_trade_stats(
            "SOLUSDC",
            TradingMode::Paper,
            next_midnight - ChronoDuration::nanoseconds(1),
        )
        .unwrap();
    assert_eq!(before_midnight.window_start, start);
    assert_eq!(before_midnight.buckets.len(), 24);
    assert_eq!(
        before_midnight
            .buckets
            .iter()
            .map(|b| b.buy_count + b.sell_count)
            .sum::<u64>(),
        253
    );
}

#[test]
fn last_fill_uses_execution_time_scope_and_stable_ties() {
    let db = Database::open(":memory:").unwrap();
    let now = Utc::now();
    let mut trade = TradeRecord {
        trade_id: "latest-buy".into(),
        client_order_id: "gb_latest_buy".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(118.8),
        quantity: dec!(1),
        amount_usdc: dec!(118.8),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: now,
        note: String::new(),
    };
    db.insert_trade(&trade).unwrap();
    // An older execution discovered later must not overwrite the latest price.
    trade.trade_id = "late-old-buy".into();
    trade.price = dec!(118.6);
    trade.timestamp = now - chrono::Duration::seconds(10);
    db.insert_trade(&trade).unwrap();
    trade.side = OrderSide::Sell;
    trade.timestamp = now;
    for (id, price) in [("sell-a", dec!(118.9)), ("sell-b", dec!(119.1))] {
        trade.trade_id = id.into();
        trade.price = price;
        db.insert_trade(&trade).unwrap();
    }
    trade.trade_id = "other-mode".into();
    trade.mode = TradingMode::Live;
    trade.price = dec!(119.3);
    db.insert_trade(&trade).unwrap();
    trade.trade_id = "other-symbol".into();
    trade.symbol = "ETHUSDC".into();
    db.insert_trade(&trade).unwrap();
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Paper).unwrap(),
        Some((OrderSide::Sell, dec!(119.1)))
    );
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Sell, dec!(119.3)))
    );
    assert_eq!(
        db.get_recent_trades_for("SOLUSDC", TradingMode::Paper, 10)
            .unwrap()[0]
            .trade_id,
        "sell-b"
    );
    db.conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE trades SET price = 'invalid' WHERE trade_id = 'sell-b'",
            [],
        )
        .unwrap();
    assert!(db.last_fill_for("SOLUSDC", TradingMode::Paper).is_err());
}

#[test]
fn test_db_trade_lifecycle() {
    let db = Database::open(":memory:").unwrap();
    let trade = TradeRecord {
        trade_id: "trade_1".to_string(),
        client_order_id: "order_1".to_string(),
        symbol: "SOLUSDC".to_string(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(150.2),
        quantity: dec!(0.66),
        amount_usdc: dec!(99.132),
        realized_pnl: dec!(0.0),
        commission: dec!(0.0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "Test trade".to_string(),
    };

    db.insert_trade(&trade).unwrap();
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].trade_id, "trade_1");
    assert_eq!(trades[0].symbol, "SOLUSDC");
    assert_eq!(trades[0].price, dec!(150.2));
    assert!(trades[0].is_maker);
}

#[test]
fn trade_and_stats_commit_together_and_duplicates_do_not_recount() {
    let db = Database::open(":memory:").unwrap();
    let trade = TradeRecord {
        trade_id: "atomic_trade".into(),
        client_order_id: "gb_b_atomic".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "atomic test".into(),
    };
    let stats = GridStats {
        total_trades: 1,
        total_volume_usdc: dec!(100),
        ..GridStats::default()
    };
    db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_stats BEFORE INSERT ON scoped_grid_stats BEGIN SELECT RAISE(ABORT, 'reject stats'); END;",
        ).unwrap();
    assert!(db.insert_trade_with_stats(&trade, &stats).is_err());
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert!(db
        .load_scoped_stats("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_none());

    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_stats;")
        .unwrap();
    assert!(db.insert_trade_with_stats(&trade, &stats).unwrap());
    let double_stats = GridStats {
        total_trades: 2,
        total_volume_usdc: dec!(200),
        ..GridStats::default()
    };
    assert!(!db.insert_trade_with_stats(&trade, &double_stats).unwrap());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Paper)
            .unwrap()
            .unwrap()
            .0,
        1
    );
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Paper)
            .unwrap()
            .unwrap()
            .3,
        dec!(100)
    );
}

#[test]
fn pair_intent_and_fill_commit_atomically() {
    let db = Database::open(":memory:").unwrap();
    let parent = GridOrder {
        client_order_id: "gb_b_parent".into(),
        order_id: Some(41),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: crate::types::OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    let child = GridOrder {
        client_order_id: "gb_s_child".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(101),
        quantity: dec!(1),
        amount_usdc: dec!(101),
        status: crate::types::OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 0,
        paired_client_order_id: Some(parent.client_order_id.clone()),
        is_take_profit: true,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    let trade = TradeRecord {
        trade_id: "grid:gb_b_parent".into(),
        client_order_id: parent.client_order_id.clone(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side: OrderSide::Buy,
        price: parent.price,
        quantity: parent.quantity,
        amount_usdc: parent.amount_usdc,
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: Utc::now(),
        note: "fill".into(),
    };
    let stats = GridStats {
        total_trades: 1,
        ..GridStats::default()
    };
    db.save_managed_order(&parent).unwrap();
    db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_pair BEFORE INSERT ON pair_intents BEGIN SELECT RAISE(ABORT, 'reject pair'); END;",
        ).unwrap();
    assert!(db
        .insert_trade_with_recovery(&trade, &stats, Some(&child), None)
        .is_err());
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert!(db
        .load_scoped_stats("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_none());
    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_pair;")
        .unwrap();

    assert!(db
        .insert_trade_with_recovery(&trade, &stats, Some(&child), None)
        .unwrap());
    assert!(!db
        .insert_trade_with_recovery(&trade, &stats, Some(&child), None)
        .unwrap());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    let intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].client_order_id, child.client_order_id);
    assert_eq!(
        intents[0].paired_client_order_id,
        child.paired_client_order_id
    );
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .0,
        1
    );
}

#[test]
fn trading_modes_have_separate_history_and_totals() {
    let db = Database::open(":memory:").unwrap();
    for (id, mode, pnl) in [
        ("paper", TradingMode::Paper, dec!(12)),
        ("live", TradingMode::Live, dec!(-4)),
    ] {
        let trade = TradeRecord {
            trade_id: id.into(),
            client_order_id: format!("gb_b_{id}"),
            symbol: "SOLUSDC".into(),
            mode,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: pnl,
            commission: dec!(0.1),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        };
        let stats = GridStats {
            total_trades: 1,
            total_volume_usdc: dec!(100),
            ..GridStats::default()
        };
        assert!(db.insert_trade_with_stats(&trade, &stats).unwrap());
    }

    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Live).unwrap(),
        (dec!(-4), dec!(0.1), 0)
    );
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (dec!(12), dec!(0.1), 0)
    );
    assert_eq!(
        db.get_recent_trades_for("SOLUSDC", TradingMode::Live, 10)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn legacy_trades_can_be_backfilled_without_double_counting() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
            "CREATE TABLE trades (trade_id TEXT PRIMARY KEY, client_order_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL, price TEXT NOT NULL, quantity TEXT NOT NULL, amount_usdc TEXT NOT NULL, realized_pnl TEXT NOT NULL, commission TEXT NOT NULL, is_maker INTEGER NOT NULL, timestamp TEXT NOT NULL, note TEXT NOT NULL);",
        ).unwrap();
    conn.execute(
            "INSERT INTO trades VALUES ('t1', 'gb_s_1', 'SOLUSDC', 'SELL', '115.1', '17.37', '1999.29', '0', '0', 1, ?1, 'old trade');",
            params![Utc::now().to_rfc3339()],
        ).unwrap();
    let db = Database {
        conn: Mutex::new(conn),
        path: ":memory:".into(),
        #[cfg(unix)]
        file_identity: None,
        history_account_ids: Mutex::new(HashMap::new()),
        trade_revisions: RevisionTracker::default(),
        trade_clear_revisions: RevisionTracker::default(),
        revision_publish_hook: Mutex::new(None),
        membership_commit_hook: Mutex::new(None),
        today_pnl_cache: tokio::sync::Mutex::new(None),
        session_cache: Default::default(),
        paper_ledger: Mutex::new(None),
        analysis_export: Mutex::new(()),
        hourly_stats_queries: AtomicU64::new(0),
        benchmark_wait_samples: std::sync::OnceLock::new(),
    };
    db.init_tables().unwrap();

    let mut pending = db
        .get_unverified_trades("SOLUSDC", TradingMode::Unknown, 10, 0)
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Unknown).unwrap(),
        (dec!(0), dec!(0), 1)
    );
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Live).unwrap(),
        (dec!(0), dec!(0), 0)
    );

    let mut trade = pending.pop().unwrap();
    trade.realized_pnl = dec!(-3.25);
    trade.commission = dec!(0.80);
    trade.pnl_verified = true;
    assert!(db.verify_trade_pnl(&trade, 0).unwrap());
    assert!(!db.verify_trade_pnl(&trade, 0).unwrap());
    assert!(db
        .get_unverified_trades("SOLUSDC", TradingMode::Unknown, 10, 0)
        .unwrap()
        .is_empty());
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Unknown).unwrap(),
        (dec!(-3.25), dec!(0.80), 0)
    );
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
}

#[test]
fn test_db_stats_lifecycle() {
    let db = Database::open(":memory:").unwrap();
    assert!(db.load_stats().unwrap().is_none());

    let stats = GridStats {
        total_trades: 42,
        completed_cycles: 21,
        total_realized_profit: dec!(15.75),
        total_volume_usdc: dec!(4200.0),
        ..GridStats::default()
    };

    db.save_stats(&stats).unwrap();
    let (trades, cycles, profit, volume) = db.load_stats().unwrap().expect("stats should exist");
    assert_eq!(trades, 42);
    assert_eq!(cycles, 21);
    assert_eq!(profit, dec!(15.75));
    assert_eq!(volume, dec!(4200.0));
}

#[test]
fn opening_database_removes_legacy_admin_password_table() {
    let directory =
        std::env::temp_dir().join(format!("binancebot-legacy-auth-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("legacy.db");
    {
        let db = Database::open(&path).unwrap();
        db.conn
                .lock()
                .unwrap()
                .execute_batch(
                    "CREATE TABLE admin_auth (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        password_hash TEXT NOT NULL,
                        salt TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    INSERT INTO admin_auth VALUES (1, 'legacy-password-hash-marker', 'legacy-salt-marker', '2026-01-01');",
                )
                .unwrap();
    }

    let db = Database::open(&path).unwrap();
    let remaining: i64 = db
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'admin_auth'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);
    drop(db);
    let mut bytes = std::fs::read(&path).unwrap();
    if let Ok(wal) = std::fs::read(path.with_extension("db-wal")) {
        bytes.extend(wal);
    }
    let raw = String::from_utf8_lossy(&bytes);
    assert!(!raw.contains("legacy-password-hash-marker"));
    assert!(!raw.contains("legacy-salt-marker"));
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn test_db_sessions() {
    let db = Database::open(":memory:").unwrap();
    let token = "test_token_123456";
    assert!(!db.is_session_valid(token).unwrap());

    db.create_session(token).unwrap();
    assert!(db.is_session_valid(token).unwrap());

    db.delete_session(token).unwrap();
    assert!(!db.is_session_valid(token).unwrap());

    db.create_session("first-token").unwrap();
    db.create_session("second-token").unwrap();
    db.clear_all_sessions().unwrap();
    assert!(!db.is_session_valid("first-token").unwrap());
    assert!(!db.is_session_valid("second-token").unwrap());
}

#[test]
fn concurrent_pair_updates_preserve_the_other_strategy_and_shared_defaults() {
    use std::sync::Arc;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.pairs = ["SOLUSDC", "BTCUSDT"]
        .map(|s| crate::config::TradingPairConfig::new(s.into()))
        .to_vec();
    db.save_config(&config).unwrap();
    let mut strategies = config.strategy_configs().unwrap();
    strategies[0].grid.buy_window = 2;
    strategies[1].grid.grid_interval = dec!(10);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let workers: Vec<_> = strategies
        .into_iter()
        .map(|config| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                db.save_pair_config(&config.exchange.symbol, &config)
                    .unwrap();
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let saved = db.load_config().unwrap().unwrap();
    assert_eq!(saved.grid.buy_window, 5);
    assert_eq!(saved.grid.grid_interval, dec!(0.1));
    let restored = saved.strategy_configs().unwrap();
    assert_eq!(restored[0].grid.buy_window, 2);
    assert_eq!(restored[1].grid.grid_interval, dec!(10));
    assert!(db.save_pair_config("SOLUSDC", &restored[1]).is_err());
}

#[test]
fn a_failed_runtime_status_write_rolls_back_the_configuration_too() {
    let db = Database::open(":memory:").unwrap();
    let config = AppConfig {
        pairs: vec![crate::config::TradingPairConfig::new("SOLUSDC".into())],
        ..Default::default()
    };
    db.save_config(&config).unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Live, BotStatus::Running)
        .unwrap();
    let mut candidate = config.strategy_configs().unwrap().remove(0);
    candidate.exchange.api_key = "live-key".into();
    candidate.exchange.api_secret = "live-secret".into();
    candidate.grid.buy_window = 7;
    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_status BEFORE UPDATE ON pair_runtime
             BEGIN SELECT RAISE(ABORT, 'status write failed'); END;",
        )
        .unwrap();
    assert!(db
        .save_runtime_config(
            &candidate,
            Some("SOLUSDC"),
            BotStatus::Paused,
            ("SOLUSDC", TradingMode::Paper)
        )
        .is_err());
    assert_eq!(
        serde_json::to_value(db.load_config().unwrap().unwrap()).unwrap(),
        serde_json::to_value(config).unwrap()
    );
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Running)
    );
}

#[test]
fn switching_modes_clears_only_the_previous_guard_and_rolls_back_on_failure() {
    let db = Database::open(":memory:").unwrap();
    let paper = AppConfig::default();
    db.save_config(&paper).unwrap();
    let symbol = &paper.exchange.symbol;
    assert!(!db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
    assert!(!db.arm_execution_guard(symbol, TradingMode::Live).unwrap());
    let mut live = paper.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();

    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_old_guard BEFORE UPDATE ON pair_execution_guard
             WHEN OLD.mode = 'PAPER' AND NEW.dirty = 0
             BEGIN SELECT RAISE(ABORT, 'guard clear failed'); END;",
        )
        .unwrap();
    assert!(db
        .save_runtime_config(
            &live,
            None,
            BotStatus::Running,
            (symbol, TradingMode::Paper)
        )
        .is_err());
    assert!(db.load_config().unwrap().unwrap().exchange.is_paper());
    assert!(db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
    assert!(db.arm_execution_guard(symbol, TradingMode::Live).unwrap());
    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_old_guard")
        .unwrap();

    assert_eq!(
        db.save_runtime_config(
            &live,
            None,
            BotStatus::Running,
            (symbol, TradingMode::Paper)
        )
        .unwrap(),
        (BotStatus::Paused, true)
    );
    assert_eq!(
        db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert!(!db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
    assert!(db.arm_execution_guard(symbol, TradingMode::Live).unwrap());
    db.clear_execution_guard(symbol, TradingMode::Paper)
        .unwrap();
    assert_eq!(
        db.save_runtime_config(
            &paper,
            None,
            BotStatus::Running,
            (symbol, TradingMode::Live)
        )
        .unwrap(),
        (BotStatus::Paused, true)
    );
    assert_eq!(
        db.load_pair_status(symbol, TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
    assert!(!db.arm_execution_guard(symbol, TradingMode::Live).unwrap());
    assert!(db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
}

#[test]
fn adding_pair_rolls_back_config_and_status_when_guard_write_fails() {
    let db = Database::open(":memory:").unwrap();
    let original = AppConfig::default();
    db.save_config(&original).unwrap();
    let mut added = original.clone();
    added.exchange.symbol = "BTCUSDT".into();
    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_guard BEFORE INSERT ON pair_execution_guard
             BEGIN SELECT RAISE(ABORT, 'guard write failed'); END;",
        )
        .unwrap();
    assert!(db.add_pair_config(&added).is_err());
    assert_eq!(
        serde_json::to_value(db.load_config().unwrap().unwrap()).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        None
    );
}

fn save_legacy_config(db: &Database, config: &serde_json::Value) {
    db.conn
        .lock()
        .unwrap()
        .execute(
            "INSERT OR REPLACE INTO app_config (id, symbol, config_json, updated_at)
                 VALUES (1, 'SOLUSDC', ?1, 'legacy')",
            params![serde_json::to_string_pretty(config).unwrap()],
        )
        .unwrap();
}

fn legacy_mixed_testnet_config() -> serde_json::Value {
    let mut config = serde_json::to_value(AppConfig::default()).unwrap();
    config["exchange"]["api_key"] = serde_json::json!("root-key");
    config["exchange"]["api_secret"] = serde_json::json!("root-secret");
    config["exchange"]["is_testnet"] = serde_json::json!(false);
    config["exchange"]["dry_run"] = serde_json::json!(false);
    config["pairs"] = serde_json::json!([
        {"symbol": "SOLUSDC", "exchange": {
            "api_key": "live-key", "api_secret": "live-secret",
            "is_testnet": false, "dry_run": false
        }},
        {"symbol": "BTCUSDT", "exchange": {
            "api_key": "testnet-key", "api_secret": "testnet-secret",
            "is_testnet": true, "dry_run": false
        }}
    ]);
    config
}

fn assert_retired_testnet_paused(db: &Database, symbols: &[&str]) -> AppConfig {
    let mut config = db.load_config().unwrap().unwrap();
    assert!(config.exchange.api_key.is_empty());
    assert!(config.exchange.api_secret.is_empty());
    assert!(config
        .pairs
        .iter()
        .all(|pair| { pair.exchange.api_key.is_none() && pair.exchange.api_secret.is_none() }));
    // Exercise the same promotion and strategy resolution used at startup.
    config.ensure_pair_list().unwrap();
    config.validate().unwrap();
    let strategies = config.strategy_configs().unwrap();
    assert_eq!(strategies.len(), symbols.len());
    for (strategy, symbol) in strategies.iter().zip(symbols) {
        assert_eq!(strategy.exchange.symbol.as_str(), *symbol);
        assert_eq!(strategy.exchange.trading_mode(), TradingMode::Paper);
        assert!(strategy.exchange.api_key.is_empty());
        assert!(strategy.exchange.api_secret.is_empty());
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
    }
    config
}

fn non_testnet_recovery_rows(db: &Database) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    let conn = db.conn.lock().unwrap();
    [
        "paper_accounts",
        "pair_execution_guard",
        "trades",
        "scoped_grid_stats",
        "pair_intents",
        "remainder_plans",
        "execution_frontiers",
        "managed_orders",
        "account_hourly_snapshots",
    ]
    .iter()
    .map(|table| {
        let mut statement = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
            .unwrap();
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|index| row.get::<_, rusqlite::types::Value>(index))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();
        rows.collect::<rusqlite::Result<Vec<_>>>().unwrap()
    })
    .collect()
}

#[test]
fn legacy_testnet_configuration_is_retired_to_paused_paper_mode() {
    let path = std::env::temp_dir().join(format!("legacy-testnet-{}.db", uuid::Uuid::new_v4()));
    {
        let db = Database::open(&path).unwrap();
        db.save_config(&AppConfig::default()).unwrap();
        db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
            .unwrap();
        db.save_pair_status("BTCUSDT", TradingMode::Paper, BotStatus::Running)
            .unwrap();
        let mut legacy = serde_json::to_value(AppConfig::default()).unwrap();
        legacy["exchange"]["is_testnet"] = serde_json::json!(true);
        legacy["exchange"]["api_key"] = serde_json::json!("testnet-key");
        legacy["exchange"]["api_secret"] = serde_json::json!("testnet-secret");
        legacy["pairs"] = serde_json::json!([
            {"symbol": "SOLUSDC"},
            {"symbol": "BTCUSDT", "exchange": {"api_key": "pair-key", "api_secret": "pair-secret"}}
        ]);
        db.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE app_config SET config_json = ?1 WHERE id = 1",
                params![serde_json::to_string_pretty(&legacy).unwrap()],
            )
            .unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_retired_testnet_paused(&db, &["SOLUSDC", "BTCUSDT"]);
    let stored: String = db
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT config_json FROM app_config WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!stored.contains("is_testnet"));
    for symbol in ["SOLUSDC", "BTCUSDT"] {
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Live).unwrap(),
            None
        );
    }
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_root_testnet_without_pairs_pauses_existing_paper_runtime() {
    let path = std::env::temp_dir().join(format!("root-testnet-{}.db", uuid::Uuid::new_v4()));
    {
        let db = Database::open(&path).unwrap();
        let mut config = serde_json::to_value(AppConfig::default()).unwrap();
        config["exchange"]["is_testnet"] = serde_json::json!(true);
        config["exchange"]["dry_run"] = serde_json::json!(false);
        config["exchange"]["api_key"] = serde_json::json!("testnet-key");
        config["exchange"]["api_secret"] = serde_json::json!("testnet-secret");
        save_legacy_config(&db, &config);
        db.save_bot_status(BotStatus::Running).unwrap();
        db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
            .unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_retired_testnet_paused(&db, &["SOLUSDC"]);
    assert!(db.load_paper_state("SOLUSDC").unwrap().is_none());
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_first_pair_testnet_clears_credentials_for_all_pairs() {
    let path = std::env::temp_dir().join(format!("pair-testnet-{}.db", uuid::Uuid::new_v4()));
    {
        let db = Database::open(&path).unwrap();
        let mut config = legacy_mixed_testnet_config();
        config["pairs"][0]["exchange"]["is_testnet"] = serde_json::json!(true);
        config["pairs"][1]["exchange"]["is_testnet"] = serde_json::json!(false);
        save_legacy_config(&db, &config);
        for symbol in ["SOLUSDC", "BTCUSDT"] {
            db.save_pair_status(symbol, TradingMode::Paper, BotStatus::Running)
                .unwrap();
        }
    }
    let db = Database::open(&path).unwrap();
    assert_retired_testnet_paused(&db, &["SOLUSDC", "BTCUSDT"]);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_first_live_second_testnet_pauses_paper_and_preserves_recovery_across_reopen() {
    for paper_intent in [false, true] {
        let path = std::env::temp_dir().join(format!("mixed-testnet-{}.db", uuid::Uuid::new_v4()));
        let position = PositionInfo {
            symbol: "SOLUSDC".into(),
            size: dec!(2),
            entry_price: dec!(100),
            mark_price: dec!(100),
            leverage: 1,
            ..PositionInfo::default()
        };
        let account = AccountInfo {
            asset: "USDC".into(),
            total_wallet_balance: dec!(9876),
            available_balance: dec!(9676),
            margin_balance: dec!(9876),
            ..AccountInfo::default()
        };
        let before;
        {
            let db = Database::open(&path).unwrap();
            let mut config = legacy_mixed_testnet_config();
            config["pairs"][0]["exchange"]["dry_run"] = serde_json::json!(paper_intent);
            save_legacy_config(&db, &config);
            for symbol in ["SOLUSDC", "BTCUSDT"] {
                for mode in [TradingMode::Live, TradingMode::Paper] {
                    db.save_pair_status(symbol, mode, BotStatus::Running)
                        .unwrap();
                    db.arm_execution_guard(symbol, mode).unwrap();
                    // Clean LIVE/Running is eligible for automatic startup recovery.
                    if mode == TradingMode::Live {
                        db.clear_execution_guard(symbol, mode).unwrap();
                    }
                }
            }
            let child = GridOrder {
                client_order_id: "saved-live-child".into(),
                order_id: None,
                symbol: "SOLUSDC".into(),
                side: OrderSide::Sell,
                price: dec!(101),
                quantity: dec!(1),
                amount_usdc: dec!(101),
                status: crate::types::OrderStatus::New,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                grid_level: 0,
                paired_client_order_id: Some("revision-order".into()),
                is_take_profit: true,
                purpose: crate::types::OrderPurpose::TakeProfit,
                merge_sources: Vec::new(),
            };
            let mut live_trade = revision_test_trade();
            live_trade.mode = TradingMode::Live;
            db.insert_trade_with_recovery(&live_trade, &GridStats::default(), Some(&child), None)
                .unwrap();
            db.save_managed_order(&child).unwrap();
            let mut remainder = child.clone();
            remainder.client_order_id = "saved-live-remainder".into();
            remainder.purpose = crate::types::OrderPurpose::Remainder;
            db.save_remainder_plan(&RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: vec![child.clone()],
                target: remainder,
                phase: crate::types::RemainderPhase::Canceling,
            })
            .unwrap();
            let mut paper_trade = revision_test_trade();
            paper_trade.trade_id = "saved-paper-trade".into();
            paper_trade.client_order_id = "saved-paper-order".into();
            let mut paper_child = child.clone();
            paper_child.client_order_id = "saved-paper-child".into();
            paper_child.paired_client_order_id = Some(paper_trade.client_order_id.clone());
            db.insert_trade_with_recovery(
                &paper_trade,
                &GridStats::default(),
                Some(&paper_child),
                None,
            )
            .unwrap();
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO paper_accounts VALUES ('SOLUSDC', ?1, ?2)",
                params![
                    serde_json::to_string(&position).unwrap(),
                    serde_json::to_string(&account).unwrap()
                ],
            )
            .unwrap();
            conn.execute_batch(
                "INSERT INTO execution_frontiers VALUES (
                         'SOLUSDC', 'LIVE', 'BUY', '100', '2026-01-01T00:00:00Z'
                     );
                     INSERT INTO account_hourly_snapshots VALUES (1, '[]');",
            )
            .unwrap();
            drop(conn);
            before = non_testnet_recovery_rows(&db);
        }
        for _ in 0..2 {
            let db = Database::open(&path).unwrap();
            let normalized = assert_retired_testnet_paused(&db, &["SOLUSDC", "BTCUSDT"]);
            for symbol in ["SOLUSDC", "BTCUSDT"] {
                assert_eq!(
                    db.load_pair_status(symbol, TradingMode::Live).unwrap(),
                    Some(BotStatus::Running)
                );
            }
            assert_eq!(non_testnet_recovery_rows(&db), before);
            let (saved_position, saved_account) = db.load_paper_state("SOLUSDC").unwrap().unwrap();
            assert_eq!(saved_position.size, position.size);
            assert_eq!(saved_position.entry_price, position.entry_price);
            assert_eq!(
                saved_account.total_wallet_balance,
                account.total_wallet_balance
            );
            assert_eq!(saved_account.available_balance, account.available_balance);
            assert!(db.load_paper_state("BTCUSDT").unwrap().is_none());
            // Match main's save after ensure_pair_list, then restart again.
            db.save_config(&normalized).unwrap();
        }
        let db = Database::open(&path).unwrap();
        db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
            .unwrap();
        drop(db);
        let db = Database::open(&path).unwrap();
        let mut normalized = db.load_config().unwrap().unwrap();
        normalized.ensure_pair_list().unwrap();
        assert!(normalized.exchange.is_paper());
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
            Some(BotStatus::Running)
        );
        assert_eq!(non_testnet_recovery_rows(&db), before);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn legacy_live_only_config_keeps_credentials_and_running_state() {
    for legacy_flags in [false, true] {
        let path =
            std::env::temp_dir().join(format!("live-only-migration-{}.db", uuid::Uuid::new_v4()));
        {
            let db = Database::open(&path).unwrap();
            let mut config = legacy_mixed_testnet_config();
            config["pairs"][1]["exchange"]["is_testnet"] = serde_json::json!(false);
            if !legacy_flags {
                config["exchange"]
                    .as_object_mut()
                    .unwrap()
                    .remove("is_testnet");
                for pair in config["pairs"].as_array_mut().unwrap() {
                    pair["exchange"]
                        .as_object_mut()
                        .unwrap()
                        .remove("is_testnet");
                }
            }
            save_legacy_config(&db, &config);
            for symbol in ["SOLUSDC", "BTCUSDT"] {
                db.save_pair_status(symbol, TradingMode::Live, BotStatus::Running)
                    .unwrap();
                db.arm_execution_guard(symbol, TradingMode::Live).unwrap();
                db.clear_execution_guard(symbol, TradingMode::Live).unwrap();
            }
        }
        for _ in 0..2 {
            let db = Database::open(&path).unwrap();
            let mut config = db.load_config().unwrap().unwrap();
            assert_eq!(config.exchange.api_key, "root-key");
            assert_eq!(config.exchange.api_secret, "root-secret");
            assert_eq!(
                config.pairs[0].exchange.api_key.as_deref(),
                Some("live-key")
            );
            assert_eq!(
                config.pairs[0].exchange.api_secret.as_deref(),
                Some("live-secret")
            );
            assert_eq!(
                config.pairs[1].exchange.api_key.as_deref(),
                Some("testnet-key")
            );
            assert_eq!(
                config.pairs[1].exchange.api_secret.as_deref(),
                Some("testnet-secret")
            );
            config.ensure_pair_list().unwrap();
            assert_eq!(config.exchange.api_key, "live-key");
            assert_eq!(config.exchange.api_secret, "live-secret");
            for strategy in config.strategy_configs().unwrap() {
                assert_eq!(strategy.exchange.trading_mode(), TradingMode::Live);
                assert_eq!(
                    db.load_pair_status(&strategy.exchange.symbol, TradingMode::Live)
                        .unwrap(),
                    Some(BotStatus::Running)
                );
                assert_eq!(
                    db.load_pair_status(&strategy.exchange.symbol, TradingMode::Paper)
                        .unwrap(),
                    None
                );
                assert!(!db
                    .arm_execution_guard(&strategy.exchange.symbol, TradingMode::Live)
                    .unwrap());
                db.clear_execution_guard(&strategy.exchange.symbol, TradingMode::Live)
                    .unwrap();
            }
        }
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn legacy_testnet_migration_rolls_back_configuration_and_all_pauses_on_failure() {
    for trigger in [
        "CREATE TRIGGER reject_migration BEFORE UPDATE ON app_config
             BEGIN SELECT RAISE(ABORT, 'reject config migration'); END;",
        "CREATE TRIGGER reject_migration BEFORE INSERT ON pair_runtime
             WHEN NEW.symbol = 'SOLUSDC' AND NEW.mode = 'PAPER'
             BEGIN SELECT RAISE(ABORT, 'reject second pause'); END;",
    ] {
        let path = std::env::temp_dir().join(format!(
            "testnet-migration-rollback-{}.db",
            uuid::Uuid::new_v4()
        ));
        let legacy = legacy_mixed_testnet_config();
        {
            let db = Database::open(&path).unwrap();
            save_legacy_config(&db, &legacy);
            for symbol in ["SOLUSDC", "BTCUSDT"] {
                db.save_pair_status(symbol, TradingMode::Paper, BotStatus::Running)
                    .unwrap();
            }
            db.conn.lock().unwrap().execute_batch(trigger).unwrap();
        }
        assert!(Database::open(&path).is_err());
        let conn = Connection::open(&path).unwrap();
        let stored: String = conn
            .query_row(
                "SELECT config_json FROM app_config WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored).unwrap(),
            legacy
        );
        for symbol in ["SOLUSDC", "BTCUSDT"] {
            let status: String = conn
                .query_row(
                    "SELECT status FROM pair_runtime WHERE symbol = ?1 AND mode = 'PAPER'",
                    params![symbol],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                serde_json::from_str::<BotStatus>(&status).unwrap(),
                BotStatus::Running
            );
        }
        conn.execute_batch("DROP TRIGGER reject_migration;")
            .unwrap();
        drop(conn);
        let db = Database::open(&path).unwrap();
        assert_retired_testnet_paused(&db, &["SOLUSDC", "BTCUSDT"]);
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_retired_testnet_paused(&db, &["SOLUSDC", "BTCUSDT"]);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn legacy_paper_intent_with_credentials_pauses_derived_live_mode_once() {
    let path =
        std::env::temp_dir().join(format!("legacy-paper-intent-{}.db", uuid::Uuid::new_v4()));
    let mut config = AppConfig::default();
    config.exchange.api_key = "legacy-key".into();
    config.exchange.api_secret = "legacy-secret".into();
    config.pairs = ["SOLUSDC", "BTCUSDT"]
        .map(|symbol| crate::config::TradingPairConfig::new(symbol.into()))
        .to_vec();
    {
        let db = Database::open(&path).unwrap();
        db.save_config(&config).unwrap();
        for symbol in ["SOLUSDC", "BTCUSDT"] {
            db.save_pair_status(symbol, TradingMode::Live, BotStatus::Running)
                .unwrap();
        }
        let mut legacy = serde_json::to_value(&config).unwrap();
        legacy["exchange"]["api_key"] = serde_json::json!("");
        legacy["exchange"]["api_secret"] = serde_json::json!("");
        legacy["exchange"]["dry_run"] = serde_json::json!(true);
        legacy["pairs"][0]["exchange"] = serde_json::json!({
            "api_key": "pair-key",
            "api_secret": "pair-secret",
            "dry_run": true
        });
        legacy["pairs"][1]["exchange"] = serde_json::json!({"dry_run": false});
        db.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE app_config SET config_json = ?1 WHERE id = 1",
                params![serde_json::to_string_pretty(&legacy).unwrap()],
            )
            .unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
    // A pair that was explicitly live before the upgrade keeps its status.
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Live).unwrap(),
        Some(BotStatus::Running)
    );
    let stored: String = db
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT config_json FROM app_config WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!stored.contains("dry_run"));
    // A later manual resume must survive the next restart.
    db.save_pair_status("SOLUSDC", TradingMode::Live, BotStatus::Running)
        .unwrap();
    drop(db);
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Running)
    );
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn retired_testnet_rows_are_removed_and_trade_history_becomes_unknown() {
    let path =
        std::env::temp_dir().join(format!("legacy-testnet-rows-{}.db", uuid::Uuid::new_v4()));
    {
        let db = Database::open(&path).unwrap();
        db.save_config(&AppConfig::default()).unwrap();
        db.insert_trade(&revision_test_trade()).unwrap();
        db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
            .unwrap();
        let conn = db.conn.lock().unwrap();
        conn.execute(
            "UPDATE trades SET mode = 'TESTNET' WHERE trade_id = 'revision-trade'",
            [],
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO pair_runtime (symbol, mode, status, updated_at)
                    VALUES ('SOLUSDC', 'TESTNET', '\"Running\"', '2026-01-01T00:00:00Z');
                 INSERT INTO pair_execution_guard (symbol, mode, dirty)
                    VALUES ('SOLUSDC', 'TESTNET', 1);",
        )
        .unwrap();
    }
    let db = Database::open(&path).unwrap();
    let conn = db.conn.lock().unwrap();
    let mode: String = conn
        .query_row(
            "SELECT mode FROM trades WHERE trade_id = 'revision-trade'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(mode, "UNKNOWN");
    for table in ["pair_runtime", "pair_execution_guard"] {
        let leftover: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE mode = 'TESTNET'"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(leftover, 0, "{table}");
    }
    drop(conn);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn first_exchange_mode_switch_persists_pause_across_reopen() {
    let target = TradingMode::Live;
    for scoped in [false, true] {
        let path = std::env::temp_dir().join(format!("mode-switch-{}.db", uuid::Uuid::new_v4()));
        let mut config = AppConfig::default();
        if scoped {
            config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
        }
        {
            let db = Database::open(&path).unwrap();
            db.save_config(&config).unwrap();
            let mut candidate = if scoped {
                config.strategy_configs().unwrap().remove(0)
            } else {
                config.clone()
            };
            candidate.exchange.api_key = "live-key".into();
            candidate.exchange.api_secret = "live-secret".into();
            assert_eq!(
                db.save_runtime_config(
                    &candidate,
                    scoped.then_some("SOLUSDC"),
                    BotStatus::Running,
                    ("SOLUSDC", TradingMode::Paper)
                )
                .unwrap(),
                (BotStatus::Paused, true)
            );
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.load_pair_status("SOLUSDC", target).unwrap(),
            Some(BotStatus::Paused)
        );
        if !scoped {
            assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        }
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn pair_status_survives_restart_and_only_the_original_pair_inherits_legacy_pause() {
    let path = std::env::temp_dir().join(format!("multi-pair-status-{}.db", uuid::Uuid::new_v4()));
    {
        let db = Database::open(&path).unwrap();
        db.save_config(&AppConfig::default()).unwrap();
        db.save_bot_status(BotStatus::Paused).unwrap();
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            None
        );
        db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
            .unwrap();
        db.save_pair_status("BTCUSDT", TradingMode::Paper, BotStatus::Paused)
            .unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Live).unwrap(),
        None
    );
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_running_status_is_inherited_only_by_the_original_symbol_and_mode() {
    let db = Database::open(":memory:").unwrap();
    db.save_config(&AppConfig::default()).unwrap();
    db.save_bot_status(BotStatus::Running).unwrap();

    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        None
    );
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        None
    );

    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Paused)
        .unwrap();
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
}

#[test]
fn adding_and_readding_a_pair_persists_paused_status() {
    let db = Database::open(":memory:").unwrap();
    let original = AppConfig::default();
    db.save_config(&original).unwrap();
    db.save_runtime_status("SOLUSDC", TradingMode::Paper, BotStatus::Running, true)
        .unwrap();

    let mut added = original.clone();
    added.exchange.symbol = "BTCUSDT".into();
    assert!(!db.add_pair_config(&added).unwrap());
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );

    db.save_pair_status("BTCUSDT", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    db.remove_pair_config("BTCUSDT").unwrap();
    assert!(!db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .iter()
        .any(|pair| pair.symbol == "BTCUSDT"));
    assert!(db.add_pair_config(&added).unwrap());
    assert!(db
        .load_config()
        .unwrap()
        .unwrap()
        .pairs
        .iter()
        .any(|pair| pair.symbol == "BTCUSDT"));
    assert_eq!(
        db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
}

#[test]
fn analysis_export_removes_credentials_from_pair_overrides() {
    let db = Database::open(":memory:").unwrap();
    let mut config = AppConfig::default();
    let mut pair = crate::config::TradingPairConfig::new("BTCUSDT".into());
    pair.exchange.api_key = Some("pair-sensitive-key".into());
    pair.exchange.api_secret = Some("pair-sensitive-secret".into());
    pair.telegram = Some(crate::config::TelegramConfig {
        enabled: true,
        bot_token: "pair-sensitive-token".into(),
        chat_id: "pair-sensitive-chat".into(),
    });
    config.pairs.push(pair);
    db.save_config(&config).unwrap();
    let bytes = db.export_analysis_snapshot().unwrap();
    let exported = String::from_utf8_lossy(&bytes);
    for secret in [
        "pair-sensitive-key",
        "pair-sensitive-secret",
        "pair-sensitive-token",
        "pair-sensitive-chat",
    ] {
        assert!(!exported.contains(secret));
    }
    assert!(db.load_config().unwrap().unwrap().pairs[0]
        .exchange
        .api_secret
        .is_some());
}

#[test]
fn trade_from_row_surfaces_corrupted_decimal_and_side_values() {
    let db = Database::open(":memory:").unwrap();
    {
        let conn = db.lock_connection("corrupt_trade");
        conn.execute(
                "INSERT INTO trades (trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note, mode) \
                 VALUES ('bad-1', 'cid-1', 'SOLUSDC', 'BUY', 'not-a-decimal', '1', '100', '0', '0', 1, 1, '2026-01-01T00:00:00Z', '', 'PAPER')",
                [],
            )
            .unwrap();
    }
    assert!(db.get_recent_trades(10).is_err());

    let db2 = Database::open(":memory:").unwrap();
    {
        let conn = db2.lock_connection("corrupt_side");
        conn.execute(
                "INSERT INTO trades (trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note, mode) \
                 VALUES ('bad-2', 'cid-2', 'SOLUSDC', 'INVALID_SIDE', '100', '1', '100', '0', '0', 1, 1, '2026-01-01T00:00:00Z', '', 'PAPER')",
                [],
            )
            .unwrap();
    }
    assert!(db2.get_recent_trades(10).is_err());
}
