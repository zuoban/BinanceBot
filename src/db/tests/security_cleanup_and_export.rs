use super::super::*;
use crate::config::AppConfig;
use crate::types::{BotStatus, OrderSide, TradeRecord, TradingMode};
use chrono::Duration as ChronoDuration;
use rusqlite::params;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[cfg(unix)]
#[test]
fn database_files_are_private_without_changing_existing_parent() {
    use std::os::unix::fs::PermissionsExt;

    let root = crate::test_support::TempTestDir::new("db-permissions");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let directory = root.join("new").join("nested");
    let path = directory.join("bot.db");
    let db = Database::open(&path).unwrap();
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(root.path()), 0o755);
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
    assert_eq!(mode(root.path()), 0o755);
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
}

#[cfg(unix)]
#[test]
fn database_and_sidecar_symlinks_are_rejected_without_changing_targets() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let root = crate::test_support::TempTestDir::new("db-symlinks");
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
}

#[test]
fn database_rejects_sqlite_uri_paths() {
    assert!(Database::open("file:other.db?mode=rwc").is_err());
}

#[test]
fn database_uses_durable_journal_settings() {
    let dir = crate::test_support::TempTestDir::new("durable");
    let path = dir.join("bot.db");
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

    let directory = crate::test_support::TempTestDir::new("legacy-export");
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
    drop(directory);
    db.clear_trade_history().unwrap();
    assert!(db.load_stats().unwrap().is_none());
}

#[test]
fn cleanup_preserves_frontiers_and_deduplication_across_restarts() {
    let dir = crate::test_support::TempTestDir::new("cleanup");
    let path = dir.join("bot.db");
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
    let directory = crate::test_support::TempTestDir::new("history-identities");
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
}

#[test]
fn analysis_export_captures_wal_and_preserves_records_without_credentials() {
    let directory = crate::test_support::TempTestDir::new("binancebot-export");
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

#[cfg(unix)]
#[test]
fn database_rejects_hard_links_and_canonicalizes_stored_path() {
    let root = crate::test_support::TempTestDir::new("db-hardlink");
    std::fs::create_dir_all(root.join("sub")).unwrap();
    let nested = root.join("sub").join("..").join("bot.db");
    let db = Database::open(&nested).unwrap();
    let canonical = root.join("bot.db").canonicalize().unwrap();
    assert_eq!(db.path(), canonical.to_str().unwrap());

    let extra_link = root.join("bot-hardlink.db");
    std::fs::hard_link(&canonical, &extra_link).unwrap();
    assert!(db.export_analysis_snapshot().is_err());
    drop(db);
    assert!(Database::open(&canonical).is_err());
    std::fs::remove_file(&extra_link).unwrap();
    let reopened = Database::open(&canonical).unwrap();
    assert!(reopened.export_analysis_snapshot().is_ok());
    drop(reopened);
}
