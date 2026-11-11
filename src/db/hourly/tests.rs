use super::*;
use crate::db::Database;
use crate::types::{GridStats, OrderSide};
use chrono::Duration;
use rust_decimal_macros::dec;

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

fn trade(id: &str, at: DateTime<Utc>, verified: bool) -> TradeRecord {
    TradeRecord {
        trade_id: id.into(),
        client_order_id: format!("order-{id}"),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: dec!(0.0000000000000000000000000001),
        commission: Decimal::ZERO,
        pnl_verified: verified,
        is_maker: true,
        timestamp: at,
        note: String::new(),
    }
}

fn stats(db: &Database, now: DateTime<Utc>) -> HourlyTradeStats {
    db.get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, now)
        .unwrap()
}

#[test]
fn backdated_fills_and_verification_keep_exact_upper_bounds_and_scope() {
    let db = Database::open(":memory:").unwrap();
    let now = timestamp("2026-10-10T00:30:00.123456789Z");
    let future = trade("future", now + Duration::nanoseconds(1), true);
    let mut pending = trade("pending", now - Duration::hours(2), false);
    let at_bound = trade("bound", now, true);
    assert!(db.insert_trade(&future).unwrap());
    assert!(db
        .insert_trade_with_stats(&pending, &GridStats::default())
        .unwrap());
    assert!(db.insert_trade(&at_bound).unwrap());
    assert!(!db.insert_trade(&at_bound).unwrap());
    let mut other = trade("live", now, true);
    other.mode = TradingMode::Live;
    db.insert_trade(&other).unwrap();
    other.trade_id = "other-symbol".into();
    other.symbol = "BTCUSDT".into();
    db.insert_trade(&other).unwrap();

    let before = stats(&db, now);
    assert_eq!(before.realized_pnl, at_bound.realized_pnl);
    assert_eq!(before.pending_pnl_count, 1);
    assert_eq!(before.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 2);
    // The persisted timestamp and symbol determine the aggregate, even if
    // the verifier supplies a newer timestamp and a different symbol.
    pending.timestamp = now + Duration::days(1);
    pending.symbol = "BTCUSDT".into();
    pending.realized_pnl = dec!(-0.0000000000000000000000000002);
    assert!(db.verify_trade_pnl(&pending, 0).unwrap());
    assert!(!db.verify_trade_pnl(&pending, 0).unwrap());
    let verified = stats(&db, now);
    assert_eq!(verified.realized_pnl, -at_bound.realized_pnl);
    assert_eq!(verified.pending_pnl_count, 0);
    let inclusive = stats(&db, future.timestamp);
    assert_eq!(inclusive.realized_pnl, Decimal::ZERO);
    assert_eq!(
        inclusive.buckets.iter().map(|b| b.buy_count).sum::<u64>(),
        3
    );

    assert_eq!(db.clear_selected_database(false, true).unwrap(), (0, 0));
    assert_eq!(stats(&db, now).realized_pnl, verified.realized_pnl);
    assert_eq!(db.clear_trade_history_for_symbol("SOLUSDC").unwrap(), 4);
    assert_eq!(stats(&db, future.timestamp).realized_pnl, Decimal::ZERO);
    assert!(stats(&db, now).buckets.iter().all(|b| b.buy_count == 0));
    assert_eq!(
        db.get_recent_trades_for("BTCUSDT", TradingMode::Live, 10)
            .unwrap()
            .len(),
        1
    );
    assert!(!db.insert_trade(&at_bound).unwrap());
    assert!(!db.verify_trade_pnl(&pending, 0).unwrap());
    assert_eq!(db.clear_selected_database(true, false).unwrap(), (1, 0));
    assert_eq!(
        db.conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM trade_hourly_rollups", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        0
    );
}

#[test]
fn aggregate_write_failures_roll_back_trade_insert_verification_and_clear() {
    let db = Database::open(":memory:").unwrap();
    let now = timestamp("2026-10-10T00:30:00Z");
    let pending = trade("pending", now, false);
    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_hourly BEFORE INSERT ON trade_hourly_rollups
         BEGIN SELECT RAISE(ABORT, 'hourly failure'); END;",
        )
        .unwrap();
    assert!(db.insert_trade(&pending).is_err());
    assert!(db
        .insert_trade_with_stats(&pending, &GridStats::default())
        .is_err());
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert_eq!(db.trade_data_revision(), 0);
    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_hourly")
        .unwrap();
    assert!(db.insert_trade(&pending).unwrap());
    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_hourly BEFORE INSERT ON trade_hourly_rollups
         BEGIN SELECT RAISE(ABORT, 'hourly failure'); END;",
        )
        .unwrap();
    assert!(db.verify_trade_pnl(&pending, 0).is_err());
    assert!(!db.get_recent_trades(1).unwrap()[0].pnl_verified);
    assert_eq!(stats(&db, now).pending_pnl_count, 1);
    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "DROP TRIGGER reject_hourly;
         CREATE TRIGGER reject_hourly_clear BEFORE DELETE ON trade_hourly_rollups
         BEGIN SELECT RAISE(ABORT, 'hourly clear failure'); END;",
        )
        .unwrap();
    assert!(db.clear_trade_history_for_symbol("SOLUSDC").is_err());
    assert!(db.clear_selected_database(true, false).is_err());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert_eq!(stats(&db, now).pending_pnl_count, 1);
    assert_eq!(db.trade_data_revision(), 1);
    assert_eq!(
        db.conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM cleared_trade_ids", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn old_history_backfills_atomically_once_and_reopens_without_rebuilding() {
    let directory = crate::test_support::TempTestDir::new("hourly-migration");
    let path = directory.join("history.db");
    let db = Database::open(&path).unwrap();
    db.conn.lock().unwrap().execute_batch(
        "DROP TABLE trades;
         DROP TABLE trade_hourly_rollups;
         DROP TABLE trade_hourly_rollup_state;
         CREATE TABLE trades (
             trade_id TEXT PRIMARY KEY, client_order_id TEXT NOT NULL, symbol TEXT NOT NULL,
             side TEXT NOT NULL, price TEXT NOT NULL, quantity TEXT NOT NULL,
             amount_usdc TEXT NOT NULL, realized_pnl TEXT NOT NULL, commission TEXT NOT NULL,
             pnl_verified INTEGER NOT NULL, is_maker INTEGER NOT NULL,
             timestamp TEXT NOT NULL, note TEXT NOT NULL
         );
         INSERT INTO trades VALUES
             ('before', 'before', 'SOLUSDC', 'BUY', '1', '1', '1', '999', '0', 1, 1,
              '2026-10-09T15:59:59.999999999Z', ''),
             ('start', 'start', 'SOLUSDC', 'BUY', '1', '1', '1', '0.0000000000000000000000000001', '0', 1, 1,
              '2026-10-10T00:00:00+08:00', ''),
             ('bound', 'bound', 'SOLUSDC', 'SELL', '1', '1', '1', '0.0000000000000000000000000002', '0', 1, 1,
              '2026-10-10 00:30:00.123456789+00:00', ''),
             ('future', 'future', 'SOLUSDC', 'BUY', '1', '1', '1', '123', '0', 0, 1,
              '2026-10-10T00:30:00.123456790Z', '');"
    ).unwrap();
    db.conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE trades SET realized_pnl = 'invalid' WHERE trade_id = 'bound'",
            [],
        )
        .unwrap();
    drop(db);
    assert!(Database::open(&path).is_err());
    let recovery = Connection::open(&path).unwrap();
    assert_eq!(
        recovery
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name IN
         ('trade_hourly_rollups', 'trade_hourly_rollup_state', 'idx_trades_unverified_scope')",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        recovery
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('trades') WHERE name = 'stats_hour'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    recovery.execute(
        "UPDATE trades SET realized_pnl = '0.0000000000000000000000000002' WHERE trade_id = 'bound'", []
    ).unwrap();
    drop(recovery);
    let db = Database::open(&path).unwrap();
    let now = timestamp("2026-10-10T00:30:00.123456789Z");
    let result = db
        .get_hourly_trade_stats("SOLUSDC", TradingMode::Unknown, now)
        .unwrap();
    assert_eq!(result.realized_pnl, dec!(0.0000000000000000000000000003));
    assert_eq!(result.pending_pnl_count, 0);
    assert_eq!(result.buckets[0].buy_count, 1);
    assert_eq!(result.buckets[8].sell_count, 1);
    let later = db
        .get_hourly_trade_stats(
            "SOLUSDC",
            TradingMode::Unknown,
            now + Duration::nanoseconds(1),
        )
        .unwrap();
    assert_eq!(later.pending_pnl_count, 1);
    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_rebuild BEFORE UPDATE OF stats_hour ON trades
         BEGIN SELECT RAISE(ABORT, 'history rebuilt on reopen'); END;
         CREATE TRIGGER reject_rollup_rebuild BEFORE DELETE ON trade_hourly_rollups
         BEGIN SELECT RAISE(ABORT, 'rollups rebuilt on reopen'); END;",
        )
        .unwrap();
    drop(db);
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.get_hourly_trade_stats("SOLUSDC", TradingMode::Unknown, now)
            .unwrap()
            .realized_pnl,
        result.realized_pnl
    );
    let conn = db.conn.lock().unwrap();
    let index: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name = 'idx_trades_unverified_scope'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(index.contains("WHERE pnl_verified = 0"));
    let plan: String = conn
        .query_row(
            "EXPLAIN QUERY PLAN SELECT trade_id FROM trades WHERE symbol = 'SOLUSDC'
         AND mode = 'UNKNOWN' AND pnl_verified = 0 ORDER BY timestamp DESC LIMIT 10",
            [],
            |row| row.get(3),
        )
        .unwrap();
    assert!(plan.contains("idx_trades_unverified_scope"), "{plan}");
    drop(conn);
    drop(db);
}

#[test]
fn stale_verification_cannot_change_new_history_or_its_rollup_after_clear() {
    let db = Database::open(":memory:").unwrap();
    let now = timestamp("2026-10-10T00:30:00Z");
    let old_revision = db.symbol_trade_clear_revision("SOLUSDC");
    db.insert_trade(&trade("before-clear", now, false)).unwrap();
    db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
    let pending = trade("after-clear", now, false);
    db.insert_trade(&pending).unwrap();
    assert!(!db.verify_trade_pnl(&pending, old_revision).unwrap());
    assert_eq!(stats(&db, now).pending_pnl_count, 1);
    assert_eq!(stats(&db, now).realized_pnl, Decimal::ZERO);
    assert!(!db.get_recent_trades(1).unwrap()[0].pnl_verified);
    let current_revision = db.symbol_trade_clear_revision("SOLUSDC");
    assert!(db.verify_trade_pnl(&pending, current_revision).unwrap());
    assert_eq!(stats(&db, now).pending_pnl_count, 0);
    assert_eq!(stats(&db, now).realized_pnl, pending.realized_pnl);
}

#[test]
fn every_public_export_rejects_an_in_progress_export_and_releases_its_guard() {
    let db = Database::open(":memory:").unwrap();
    let guard = db.analysis_export.lock().unwrap();
    for result in [
        db.export_analysis_snapshot(),
        db.export_analysis_snapshot_for_symbol("SOLUSDC"),
        db.export_selected_analysis_snapshot(true, true),
    ] {
        assert!(result.unwrap_err().to_string().contains("export is busy"));
    }
    // A different Database has its own budget.
    assert!(Database::open(":memory:")
        .unwrap()
        .export_analysis_snapshot()
        .is_ok());
    drop(guard);
    assert!(db.export_analysis_snapshot().is_ok());
    assert!(db.export_analysis_snapshot_for_symbol("SOLUSDC").is_ok());
    assert!(db.export_selected_analysis_snapshot(true, true).is_ok());
    db.conn.lock().unwrap().execute(
        "INSERT INTO app_config (id, symbol, config_json, updated_at) VALUES (1, 'SOLUSDC', 'bad-json', '')", []
    ).unwrap();
    assert!(db.export_analysis_snapshot().is_err());
    assert!(db.export_selected_analysis_snapshot(false, false).is_ok());
}

#[test]
fn extreme_rollup_values_saturate_without_panicking() {
    let db = Database::open(":memory:").unwrap();
    let now = timestamp("2026-10-10T00:30:00Z");
    let mut first = trade("max-1", now, true);
    first.realized_pnl = Decimal::MAX;
    first.commission = Decimal::MAX;
    assert!(db.insert_trade(&first).unwrap());

    let mut second = trade("max-2", now, false);
    assert!(db.insert_trade(&second).unwrap());
    second.realized_pnl = Decimal::MAX;
    second.commission = Decimal::MAX;
    assert!(db.verify_trade_pnl(&second, 0).unwrap());

    assert_eq!(stats(&db, now).realized_pnl, Decimal::MAX);
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (Decimal::MAX, Decimal::MAX, 0)
    );
}
