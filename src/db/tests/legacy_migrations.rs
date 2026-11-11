use super::super::*;
use super::revision_test_trade;
use crate::config::AppConfig;
use crate::types::{
    AccountInfo, BotStatus, GridOrder, GridStats, OrderSide, PositionInfo, RemainderPlan,
    TradingMode,
};
use rusqlite::params;
use rust_decimal_macros::dec;

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
                    ("SOLUSDC", TradingMode::Paper),
                    false,
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
