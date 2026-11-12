use super::super::*;
use crate::types::{GridOrder, GridStats, OrderSide, TradeRecord, TradingMode};
use chrono::Duration as ChronoDuration;
use rusqlite::params;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

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
        commission_asset: None,
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
        commission_asset: None,
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
        commission_asset: None,
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
        commission_asset: None,
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
        commission_asset: None,
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
            commission_asset: None,
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
fn schema_migration_normalizes_legacy_timestamps_in_trades_and_frontiers() {
    let dir = crate::test_support::TempTestDir::new("legacy-timestamps");
    let path = dir.join("bot.db");
    {
        let db = Database::open(&path).unwrap();
        let conn = db.conn.lock().unwrap();
        conn.execute_batch(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity,
                 amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note)
             VALUES
                 ('t-whole-z', 'cid-1', 'SOLUSDC', 'PAPER', 'BUY', '101', '1', '101', '0', '0', 1, 1,
                  '2026-10-10T08:00:00Z', ''),
                 ('t-space-tz', 'cid-2', 'SOLUSDC', 'PAPER', 'SELL', '102', '1', '102', '0', '0', 1, 1,
                  '2026-10-10 08:00:00.125+00:00', ''),
                 ('t-space-naive', 'cid-3', 'SOLUSDC', 'PAPER', 'BUY', '103', '1', '103', '0', '0', 1, 1,
                  '2026-10-10 08:00:00.25', ''),
                 ('t-offset', 'cid-4', 'SOLUSDC', 'PAPER', 'SELL', '104', '1', '104', '0', '0', 1, 1,
                  '2026-10-10T16:00:00.5+08:00', '');
             INSERT INTO execution_frontiers (symbol, mode, side, price, timestamp)
             VALUES
                 ('SOLUSDC', 'LIVE', 'BUY', '110', '2026-10-10 08:00:00.05+00:00'),
                 ('BTCUSDC', 'PAPER', 'SELL', '60000', '2026-10-10T08:00:00Z');",
        )
        .unwrap();
    }

    let db = Database::open(&path).unwrap();
    {
        let conn = db.conn.lock().unwrap();
        let trade_timestamps: Vec<(String, String)> = conn
            .prepare("SELECT trade_id, timestamp FROM trades ORDER BY timestamp ASC")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            trade_timestamps,
            vec![
                ("t-whole-z".into(), "2026-10-10T08:00:00.000000000Z".into()),
                ("t-space-tz".into(), "2026-10-10T08:00:00.125000000Z".into()),
                (
                    "t-space-naive".into(),
                    "2026-10-10T08:00:00.250000000Z".into()
                ),
                ("t-offset".into(), "2026-10-10T08:00:00.500000000Z".into()),
            ]
        );
        let frontier_timestamps: Vec<(String, String, String)> = conn
            .prepare("SELECT symbol, mode, timestamp FROM execution_frontiers ORDER BY symbol")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            frontier_timestamps,
            vec![
                (
                    "BTCUSDC".into(),
                    "PAPER".into(),
                    "2026-10-10T08:00:00.000000000Z".into()
                ),
                (
                    "SOLUSDC".into(),
                    "LIVE".into(),
                    "2026-10-10T08:00:00.050000000Z".into()
                ),
            ]
        );
    }
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Paper).unwrap(),
        Some((OrderSide::Sell, dec!(104)))
    );
}

#[test]
fn subsecond_fills_sort_chronologically_for_last_fill_and_cleared_frontiers() {
    let db = Database::open(":memory:").unwrap();
    let parse_ts = |s: &str| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);
    let make_trade = |id: &str, side: OrderSide, price: Decimal, ts: &str| TradeRecord {
        trade_id: id.into(),
        client_order_id: format!("cid-{id}"),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side,
        price,
        quantity: dec!(1),
        amount_usdc: price,
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        commission_asset: None,
        pnl_verified: true,
        is_maker: true,
        timestamp: parse_ts(ts),
        note: String::new(),
    };

    // Insert latest sub-second fill FIRST (lower rowid), then whole-second fill SECOND
    // (higher rowid, where unpadded "...00Z" would falsely sort after "...00.900000000Z").
    assert!(db
        .insert_trade(&make_trade(
            "fill-900ms",
            OrderSide::Buy,
            dec!(105),
            "2026-10-10T12:00:00.900000000Z",
        ))
        .unwrap());
    assert!(db
        .insert_trade(&make_trade(
            "fill-0ms",
            OrderSide::Sell,
            dec!(101),
            "2026-10-10T12:00:00Z",
        ))
        .unwrap());
    assert!(db
        .insert_trade(&make_trade(
            "fill-50ms",
            OrderSide::Sell,
            dec!(102),
            "2026-10-10T12:00:00.050000000Z",
        ))
        .unwrap());

    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Buy, dec!(105)))
    );
    assert_eq!(db.clear_trade_history().unwrap(), 3);
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Buy, dec!(105)))
    );

    // An earlier sub-second fill arriving and clearing later must not regress the frontier.
    assert!(db
        .insert_trade(&make_trade(
            "fill-899ms",
            OrderSide::Sell,
            dec!(99),
            "2026-10-10T12:00:00.899999999Z",
        ))
        .unwrap());
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Buy, dec!(105)))
    );
    assert_eq!(db.clear_trade_history().unwrap(), 1);
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Buy, dec!(105)))
    );

    // A strictly newer nanosecond fill advances both live query and cleared frontier.
    assert!(db
        .insert_trade(&make_trade(
            "fill-900ms-plus-1ns",
            OrderSide::Sell,
            dec!(106),
            "2026-10-10T12:00:00.900000001Z",
        ))
        .unwrap());
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Sell, dec!(106)))
    );
    assert_eq!(db.clear_trade_history().unwrap(), 1);
    assert_eq!(
        db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
        Some((OrderSide::Sell, dec!(106)))
    );
}

#[test]
fn pnl_rollups_upgrade_existing_table_and_preserve_non_quote_counts() {
    let dir = crate::test_support::TempTestDir::new("pnl-rollup-non-quote");
    let path = dir.join("bot.db");
    {
        let db = Database::open(&path).unwrap();
        let conn = db.conn.lock().unwrap();
        conn.execute_batch(
            "DROP TABLE trade_pnl_rollups;
             CREATE TABLE trade_pnl_rollups (
                 symbol TEXT NOT NULL, mode TEXT NOT NULL, realized_pnl TEXT NOT NULL,
                 commission TEXT NOT NULL, pending_pnl_count INTEGER NOT NULL,
                 PRIMARY KEY (symbol, mode)
             );
             INSERT INTO trade_pnl_rollups (symbol, mode, realized_pnl, commission, pending_pnl_count)
             VALUES ('SOLUSDC', 'LIVE', '10.5', '0.12', 0);",
        )
        .unwrap();
    }

    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.get_pnl_rollup("SOLUSDC", TradingMode::Live).unwrap(),
        (dec!(10.5), dec!(0.12), 0, 0)
    );

    let mut bnb_trade = TradeRecord {
        trade_id: "bnb-1".into(),
        client_order_id: "cid-bnb-1".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side: OrderSide::Sell,
        price: dec!(105),
        quantity: dec!(1),
        amount_usdc: dec!(105),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        commission_asset: None,
        pnl_verified: false,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    };
    assert!(db.insert_trade(&bnb_trade).unwrap());
    bnb_trade.realized_pnl = dec!(1.5);
    bnb_trade.commission = dec!(0.0004);
    bnb_trade.commission_asset = Some("bnb".into());
    bnb_trade.pnl_verified = true;
    assert!(db.verify_trade_pnl(&bnb_trade, 0).unwrap());

    assert_eq!(
        db.get_pnl_rollup("SOLUSDC", TradingMode::Live).unwrap(),
        (dec!(12.0), dec!(0.12), 1, 0)
    );
    let loaded = db.get_recent_trades(1).unwrap();
    assert_eq!(loaded[0].commission_asset.as_deref(), Some("BNB"));
    assert_eq!(loaded[0].commission, dec!(0.0004));
}
