use super::super::*;
use super::revision_test_trade;
use crate::types::{GridOrder, GridStats, OrderSide, RemainderPlan, TradeRecord, TradingMode};
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
        commission_asset: None,
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
        read_conns: Vec::new(),
        next_reader: AtomicUsize::new(0),
        conn: Mutex::new(conn),
        path: ":memory:".into(),
        #[cfg(unix)]
        file_identity: None,
        history_account_ids: Mutex::new(HashMap::new()),
        trade_commit_seq: AtomicU64::new(0),
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

    let dir = crate::test_support::TempTestDir::new("revision-lock");
    let path = dir.join("bot.db");
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

#[test]
fn verify_trades_pnl_batch_commits_multiple_trades_and_updates_rollups_once() {
    let db = Database::open(":memory:").unwrap();
    let mut first = revision_test_trade();
    first.trade_id = "batch-1".into();
    first.client_order_id = "cid-batch-1".into();
    let mut second = revision_test_trade();
    second.trade_id = "batch-2".into();
    second.client_order_id = "cid-batch-2".into();
    assert!(db.insert_trade(&first).unwrap());
    assert!(db.insert_trade(&second).unwrap());
    let rev_before = db.symbol_trade_revision("SOLUSDC");

    first.realized_pnl = dec!(1.25);
    first.commission = dec!(0.02);
    first.pnl_verified = true;
    second.realized_pnl = dec!(2.75);
    second.commission = dec!(0.03);
    second.pnl_verified = true;
    let verified = db
        .verify_trades_pnl_batch(&[first.clone(), second.clone()], 0)
        .unwrap();
    assert_eq!(verified.len(), 2);
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), rev_before + 1);
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (dec!(4.00), dec!(0.05), 0)
    );
    assert!(db
        .verify_trades_pnl_batch(&[first, second], 0)
        .unwrap()
        .is_empty());
    assert_eq!(db.symbol_trade_revision("SOLUSDC"), rev_before + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_read_pool_serves_queries_without_blocking_on_held_write_transaction() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let dir = crate::test_support::TempTestDir::new("wal-read-pool");
    let path = dir.join("bot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    assert_eq!(db.read_conns.len(), read_pool_size());
    for slot in &db.read_conns {
        let conn = slot.lock().unwrap();
        assert!(conn.execute("DELETE FROM trades", []).is_err());
    }

    let now: DateTime<Utc> = "2026-10-14T03:20:00Z".parse().unwrap();
    let mut committed = revision_test_trade();
    committed.trade_id = "committed-1".into();
    committed.client_order_id = "cid-committed-1".into();
    committed.price = dec!(101.5);
    committed.realized_pnl = dec!(2.5);
    committed.pnl_verified = true;
    committed.timestamp = now;
    assert!(db.insert_trade(&committed).unwrap());
    db.save_hourly_account_snapshot(1_760_412_000, &[]).unwrap();

    let (locked_tx, locked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer_db = Arc::clone(&db);
    let writer = std::thread::spawn(move || {
        let mut conn = writer_db.conn.lock().unwrap();
        let tx = immediate_tx(&mut conn).unwrap();
        tx.execute(
            "INSERT INTO trades (
                trade_id, client_order_id, symbol, mode, side, price, quantity,
                amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note
            ) VALUES ('uncommitted-2', 'cid-uncommitted-2', 'SOLUSDC', 'PAPER', 'SELL', '105', '1',
                      '105', '99', '0', 1, 1, ?1, '')",
            params![format_sqlite_timestamp(now)],
        )
        .unwrap();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        tx.rollback().unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(db.conn.try_lock().is_err());

    let reader_db = Arc::clone(&db);
    let reads = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::task::spawn_blocking(move || {
            let today = reader_db.get_today_pnl(now).unwrap();
            assert_eq!(today.groups.len(), 1);
            assert_eq!(today.groups[0].realized_pnl, dec!(2.5));

            let hourly = reader_db
                .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, now)
                .unwrap();
            assert_eq!(hourly.realized_pnl, dec!(2.5));

            let recent = reader_db
                .get_recent_trades_for("SOLUSDC", TradingMode::Paper, 10)
                .unwrap();
            assert_eq!(recent.len(), 1);
            assert_eq!(recent[0].trade_id, "committed-1");

            assert_eq!(
                reader_db
                    .last_fill_for("SOLUSDC", TradingMode::Paper)
                    .unwrap(),
                Some((OrderSide::Buy, dec!(101.5)))
            );
            assert_eq!(reader_db.hourly_account_snapshots_raw(0).unwrap().len(), 1);
        }),
    )
    .await
    .expect("WAL read pool blocked on held write transaction");
    reads.unwrap();

    release_tx.send(()).unwrap();
    writer.join().unwrap();

    // Verify commit-epoch guard prevents caching torn WAL reads before revision publish.
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    *db.revision_publish_hook.lock().unwrap() = Some((reached_tx, resume_rx));
    let mut second = committed.clone();
    second.trade_id = "committed-2".into();
    second.client_order_id = "cid-committed-2".into();
    second.realized_pnl = dec!(3.5);
    let writer_db = Arc::clone(&db);
    let commit_writer = std::thread::spawn(move || writer_db.insert_trade(&second));
    reached_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(db.trade_commit_stable_token().is_none());
    assert_eq!(db.trade_data_revision(), 1);

    let inflight_pnl = db.cached_today_pnl(now).await.unwrap();
    assert_eq!(inflight_pnl.groups[0].realized_pnl, dec!(6.0));
    assert!(db.today_pnl_cache.lock().await.is_none());

    resume_tx.send(()).unwrap();
    assert!(commit_writer.join().unwrap().unwrap());
    assert!(db.trade_commit_stable_token().is_some());
    assert_eq!(db.trade_data_revision(), 2);
    let stable_pnl = db.cached_today_pnl(now).await.unwrap();
    assert_eq!(stable_pnl.groups[0].realized_pnl, dec!(6.0));
    assert!(db.today_pnl_cache.lock().await.is_some());
}
