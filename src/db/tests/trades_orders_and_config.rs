use super::super::*;
use crate::config::AppConfig;
use crate::types::{BotStatus, GridOrder, GridStats, OrderSide, TradeRecord, TradingMode};
use chrono::Duration as ChronoDuration;
use rusqlite::params;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

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
            ("SOLUSDC", TradingMode::Paper),
            false,
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
            (symbol, TradingMode::Paper),
            false,
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
            (symbol, TradingMode::Paper),
            false,
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
            (symbol, TradingMode::Live),
            false,
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
    let mut live_rotated = live.clone();
    live_rotated.exchange.api_key = "live-key-b".into();
    assert_eq!(
        db.save_runtime_config(
            &live_rotated,
            None,
            BotStatus::Running,
            (symbol, TradingMode::Live),
            true,
        )
        .unwrap(),
        (BotStatus::Paused, true)
    );
    assert_eq!(
        db.load_pair_status(symbol, TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
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
