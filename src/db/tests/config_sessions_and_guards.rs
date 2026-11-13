use super::super::*;
use crate::config::AppConfig;
use crate::types::{BotStatus, GridOrder, OrderSide, TradingMode};
use chrono::Duration as ChronoDuration;
use rusqlite::params;
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
    let dir = crate::test_support::TempTestDir::new("dashboard-settings");
    let path = dir.join("bot.db");
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
    assert_eq!(config.exchange.symbol, "BTCUSDT");
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
fn opening_database_removes_legacy_admin_password_table() {
    let directory = crate::test_support::TempTestDir::new("binancebot-legacy-auth");
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

#[test]
fn analysis_export_clears_sqlite_stat_and_sequence_tables() {
    let db = Database::open(":memory:").unwrap();
    db.save_config(&AppConfig::default()).unwrap();
    db.create_session("stat1-secret-session-marker").unwrap();
    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TABLE seq_probe (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT);
             INSERT INTO seq_probe (label) VALUES ('probe');
             ANALYZE;",
        )
        .unwrap();

    let dir = crate::test_support::TempTestDir::new("stat-export");
    for (name, exported) in [
        ("full.db", db.export_analysis_snapshot().unwrap()),
        (
            "selected.db",
            db.export_selected_analysis_snapshot(true, false).unwrap(),
        ),
    ] {
        assert!(!String::from_utf8_lossy(&exported).contains("stat1-secret-session-marker"));
        let path = dir.join(name);
        std::fs::write(&path, exported).unwrap();
        let conn = Connection::open(&path).unwrap();
        for meta_table in ["sqlite_stat1", "sqlite_sequence"] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {meta_table}"), [], |row| {
                    row.get(0)
                })
                .unwrap_or(0);
            assert_eq!(count, 0, "{meta_table} not cleared in {name}");
        }
    }
}

#[cfg(unix)]
#[test]
fn listen_sidecar_shared_probe_ignores_concurrent_stale_readers() {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;

    let dir = crate::test_support::TempTestDir::new("listen-shared-probe");
    let dbf = dir.join("bot.db");
    let stale = dir.path().canonicalize().unwrap().join("bot.db-listen");
    std::fs::write(&stale, b"127.0.0.1:7777").unwrap();
    std::fs::set_permissions(&stale, std::fs::Permissions::from_mode(0o600)).unwrap();

    let concurrent_reader = std::fs::File::open(&stale).unwrap();
    assert_eq!(
        unsafe { libc::flock(concurrent_reader.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) },
        0
    );
    assert_eq!(read_active_listen_addr(&dbf), None);
    drop(concurrent_reader);

    let live_addr: std::net::SocketAddr = "127.0.0.1:8888".parse().unwrap();
    let guard = ActiveListenGuard::record(&dbf, live_addr).unwrap();
    assert_eq!(read_active_listen_addr(&dbf), Some(live_addr));
    drop(guard);
    assert_eq!(read_active_listen_addr(&dbf), None);
}

#[test]
fn paper_ledger_accepts_first_mark_valuation_with_earlier_mark_timestamp() {
    use crate::types::{AccountInfo, GridStats, PositionInfo, TradeRecord};
    use rust_decimal::Decimal;

    let db = Database::open(":memory:").unwrap();
    let initial_now = Utc::now();
    let initial_pos = PositionInfo {
        symbol: "SOLUSDC".into(),
        size: dec!(2),
        entry_price: dec!(100),
        mark_price: Decimal::ZERO,
        unrealized_pnl: Decimal::ZERO,
        leverage: 1,
        update_time: None,
        ..PositionInfo::default()
    };
    let initial_acc = AccountInfo::initial_paper("SOLUSDC", Some(initial_now));
    let fill = TradeRecord {
        trade_id: "paper-open".into(),
        client_order_id: "cid-paper-open".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        commission_asset: None,
        pnl_verified: true,
        is_maker: true,
        timestamp: initial_now,
        note: String::new(),
    };
    assert!(db
        .insert_trade_with_paper_recovery(
            &fill,
            &GridStats::default(),
            None,
            None,
            Some(&(initial_pos.clone(), initial_acc.clone())),
        )
        .unwrap());
    let before_mark = db
        .shared_paper_account("SOLUSDC", &initial_pos, &initial_acc)
        .unwrap();
    assert_eq!(before_mark.unrealized_profit, Decimal::ZERO);

    let mark_time = initial_now - ChronoDuration::seconds(1);
    let marked_pos = PositionInfo {
        mark_price: dec!(107.5),
        unrealized_pnl: dec!(15),
        update_time: Some(mark_time),
        ..initial_pos.clone()
    };
    let marked_acc = AccountInfo {
        unrealized_profit: dec!(15),
        margin_balance: dec!(10015),
        available_balance: dec!(9800),
        update_time: mark_time,
        ..initial_acc.clone()
    };
    let shared = db
        .shared_paper_account("SOLUSDC", &marked_pos, &marked_acc)
        .unwrap();
    assert_eq!(shared.unrealized_profit, dec!(15));
    assert_eq!(shared.margin_balance, dec!(10015));
    assert_eq!(shared.update_time, mark_time);

    // Once mark-backed, an even older mark update cannot roll back unrealized PnL.
    let older_time = mark_time - ChronoDuration::seconds(1);
    let stale_pos = PositionInfo {
        mark_price: dec!(95),
        unrealized_pnl: dec!(-10),
        update_time: Some(older_time),
        ..initial_pos
    };
    let stale_acc = AccountInfo {
        unrealized_profit: dec!(-10),
        margin_balance: dec!(9990),
        update_time: older_time,
        ..initial_acc
    };
    let after_stale = db
        .shared_paper_account("SOLUSDC", &stale_pos, &stale_acc)
        .unwrap();
    assert_eq!(after_stale.unrealized_profit, dec!(15));

    // Unresolved legacy symbols report deterministically in lexicographical order.
    for sym in ["ZECUSDC", "ADAUSDC", "MATICUSDC"] {
        let mut legacy = fill.clone();
        legacy.trade_id = format!("legacy-{sym}");
        legacy.client_order_id = format!("cid-{sym}");
        legacy.symbol = sym.into();
        assert!(db.insert_trade(&legacy).unwrap());
    }
    let err = db
        .shared_paper_account("SOLUSDC", &marked_pos, &marked_acc)
        .unwrap_err()
        .to_string();
    assert!(err.contains("ADAUSDC"), "unexpected error: {err}");
}

#[test]
fn fresh_database_keeps_indexes_created_only_by_migrations() {
    let db = Database::open(":memory:").unwrap();
    let conn = db.lock_connection("test_indexes");
    for name in [
        "idx_pair_intents_scope_created",
        "idx_remainder_plans_inactive_scope",
        "idx_auth_sessions_created_at",
        "idx_initial_order_responses_snapshot_scope",
        "idx_trades_unverified_scope",
    ] {
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [name],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "missing index {name}");
    }
}

#[test]
fn read_pool_size_stays_within_bounds() {
    let size = read_pool_size();
    assert!((MIN_READ_POOL_SIZE..=MAX_READ_POOL_SIZE).contains(&size));
}
