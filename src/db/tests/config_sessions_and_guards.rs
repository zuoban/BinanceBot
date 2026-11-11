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
