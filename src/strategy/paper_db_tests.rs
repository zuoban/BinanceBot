use crate::db::Database;
use crate::types::{
    AccountInfo, BotStatus, GridOrder, GridStats, OrderPurpose, OrderSide, OrderStatus,
    PositionInfo, TradeRecord, TradingMode,
};
use chrono::Utc;
use rust_decimal_macros::dec;

fn trade(id: &str, symbol: &str, mode: TradingMode) -> TradeRecord {
    TradeRecord {
        trade_id: id.into(),
        client_order_id: format!("order-{id}"),
        symbol: symbol.into(),
        mode,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: dec!(0),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    }
}

fn paper_state(symbol: &str, size: i64, balance: i64) -> (PositionInfo, AccountInfo) {
    (
        PositionInfo {
            symbol: symbol.into(),
            size: size.into(),
            entry_price: dec!(100),
            ..Default::default()
        },
        AccountInfo {
            asset: "USDC".into(),
            total_wallet_balance: balance.into(),
            available_balance: balance.into(),
            ..Default::default()
        },
    )
}

fn pair_intent(symbol: &str) -> GridOrder {
    GridOrder {
        client_order_id: "paired-order".into(),
        order_id: None,
        symbol: symbol.into(),
        side: OrderSide::Sell,
        price: dec!(101),
        quantity: dec!(1),
        amount_usdc: dec!(101),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: Some("order-second".into()),
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: Vec::new(),
    }
}

#[test]
fn paper_trade_and_state_roll_back_on_database_failures() {
    let path = std::env::temp_dir().join(format!("paper-db-{}.sqlite", uuid::Uuid::new_v4()));
    let db = Database::open(&path).unwrap();
    let editor = rusqlite::Connection::open(&path).unwrap();
    let initial = paper_state("SOLUSDC", 1, 900);
    let other = paper_state("BTCUSDC", 3, 700);
    let mut stats = GridStats {
        total_trades: 1,
        ..Default::default()
    };
    for (id, symbol, state) in [("first", "SOLUSDC", &initial), ("other", "BTCUSDC", &other)] {
        assert!(db
            .insert_trade_with_paper_recovery(
                &trade(id, symbol, TradingMode::Paper),
                &stats,
                None,
                None,
                Some(state)
            )
            .unwrap());
    }
    let before = serde_json::to_value(db.load_paper_state("SOLUSDC").unwrap().unwrap()).unwrap();
    let other_before =
        serde_json::to_value(db.load_paper_state("BTCUSDC").unwrap().unwrap()).unwrap();
    let stats_before = db.load_scoped_stats("SOLUSDC", TradingMode::Paper).unwrap();
    stats.total_trades = 2;
    let changed = paper_state("SOLUSDC", 2, 800);

    // Reject the account update, then reject a later pair-intent write after the
    // account update has succeeded inside the transaction.
    for (trigger, sql) in [
        ("reject_account", "CREATE TRIGGER reject_account BEFORE UPDATE ON paper_accounts BEGIN SELECT RAISE(ABORT, 'account failure'); END;"),
        ("reject_intent", "CREATE TRIGGER reject_intent BEFORE INSERT ON pair_intents BEGIN SELECT RAISE(ABORT, 'intent failure'); END;"),
    ] {
        editor.execute_batch(sql).unwrap();
        assert!(db.insert_trade_with_paper_recovery(
            &trade("second", "SOLUSDC", TradingMode::Paper), &stats,
            Some(&pair_intent("SOLUSDC")), None, Some(&changed),
        ).is_err());
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 2);
        assert_eq!(db.load_scoped_stats("SOLUSDC", TradingMode::Paper).unwrap(), stats_before);
        assert_eq!(serde_json::to_value(db.load_paper_state("SOLUSDC").unwrap().unwrap()).unwrap(), before);
        assert_eq!(serde_json::to_value(db.load_paper_state("BTCUSDC").unwrap().unwrap()).unwrap(), other_before);
        assert!(db.load_pair_intents("SOLUSDC", TradingMode::Paper).unwrap().is_empty());
        editor.execute_batch(&format!("DROP TRIGGER {trigger};")).unwrap();
    }
    drop(editor);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_paper_history_is_rejected_even_after_cleanup_and_is_scoped_by_symbol() {
    let db = Database::open(":memory:").unwrap();
    assert!(db.load_paper_state("SOLUSDC").unwrap().is_none());
    assert!(db
        .insert_trade(&trade("legacy", "SOLUSDC", TradingMode::Paper))
        .unwrap());
    assert!(db.load_paper_state("SOLUSDC").is_err());
    assert!(db.load_paper_state("BTCUSDC").unwrap().is_none());
    assert!(db
        .insert_trade(&trade("live", "ETHUSDC", TradingMode::Live))
        .unwrap());
    assert!(db.load_paper_state("ETHUSDC").unwrap().is_none());

    assert_eq!(db.clear_trade_history_for_symbol("SOLUSDC").unwrap(), 1);
    assert!(db.load_paper_state("SOLUSDC").is_err());
    assert!(db.load_paper_state("BTCUSDC").unwrap().is_none());

    let other = paper_state("BTCUSDC", 2, 800);
    assert!(db
        .insert_trade_with_paper_recovery(
            &trade("other", "BTCUSDC", TradingMode::Paper),
            &GridStats::default(),
            None,
            None,
            Some(&other),
        )
        .unwrap());
    assert_eq!(
        serde_json::to_value(db.load_paper_state("BTCUSDC").unwrap().unwrap()).unwrap(),
        serde_json::to_value(other).unwrap()
    );
    assert!(db.load_paper_state("SOLUSDC").is_err());
}

#[test]
fn migrating_legacy_paper_history_initializes_empty_account_without_changing_trades() {
    let db = Database::open(":memory:").unwrap();
    for (id, symbol, mode) in [
        ("legacy", "SOLUSDC", TradingMode::Paper),
        ("live", "SOLUSDC", TradingMode::Live),
        ("other", "BTCUSDC", TradingMode::Paper),
    ] {
        assert!(db.insert_trade(&trade(id, symbol, mode)).unwrap());
    }
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Live, BotStatus::Running)
        .unwrap();
    db.save_pair_status("BTCUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    let history_before = serde_json::to_value(db.get_recent_trades(10).unwrap()).unwrap();

    db.initialize_legacy_paper_account("SOLUSDC").unwrap();

    let (position, account) = db.load_paper_state("SOLUSDC").unwrap().unwrap();
    assert_eq!(position.symbol, "SOLUSDC");
    assert_eq!(position.size, dec!(0));
    assert_eq!(position.entry_price, dec!(0));
    assert_eq!(account.asset, "USDC");
    assert_eq!(account.total_wallet_balance, dec!(10000));
    assert_eq!(account.available_balance, dec!(10000));
    assert_eq!(account.margin_balance, dec!(10000));
    assert_eq!(account.unrealized_profit, dec!(0));
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Running)
    );
    assert_eq!(
        db.load_pair_status("BTCUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    assert_eq!(
        serde_json::to_value(db.get_recent_trades(10).unwrap()).unwrap(),
        history_before
    );
    assert!(db.load_paper_state("BTCUSDC").is_err());
}

#[test]
fn migrating_legacy_paper_history_rejects_existing_account_without_overwriting_it() {
    let db = Database::open(":memory:").unwrap();
    let initial = paper_state("SOLUSDC", 3, 700);
    assert!(db
        .insert_trade_with_paper_recovery(
            &trade("existing", "SOLUSDC", TradingMode::Paper),
            &GridStats::default(),
            None,
            None,
            Some(&initial),
        )
        .unwrap());
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    let before = serde_json::to_value(db.load_paper_state("SOLUSDC").unwrap().unwrap()).unwrap();
    let history_before = serde_json::to_value(db.get_recent_trades(10).unwrap()).unwrap();

    assert!(db.initialize_legacy_paper_account("SOLUSDC").is_err());
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    assert_eq!(
        serde_json::to_value(db.load_paper_state("SOLUSDC").unwrap().unwrap()).unwrap(),
        before
    );
    assert_eq!(
        serde_json::to_value(db.get_recent_trades(10).unwrap()).unwrap(),
        history_before
    );
}

#[test]
fn migration_rejects_missing_or_live_only_legacy_scope() {
    let db = Database::open(":memory:").unwrap();
    assert!(db
        .insert_trade(&trade("live", "ETHUSDC", TradingMode::Live))
        .unwrap());
    assert!(db
        .insert_trade(&trade("paper", "SOLUSDC", TradingMode::Paper))
        .unwrap());
    let history_before = serde_json::to_value(db.get_recent_trades(10).unwrap()).unwrap();

    for symbol in ["ETHUSDC", "BTCUSDC"] {
        assert!(db.initialize_legacy_paper_account(symbol).is_err());
        assert!(db.load_paper_state(symbol).unwrap().is_none());
    }
    assert!(db.load_paper_state("SOLUSDC").is_err());
    assert_eq!(
        serde_json::to_value(db.get_recent_trades(10).unwrap()).unwrap(),
        history_before
    );
    assert_eq!(db.clear_trade_history_for_symbol("ETHUSDC").unwrap(), 1);
    assert!(db.initialize_legacy_paper_account("ETHUSDC").is_err());
    assert!(db.load_paper_state("ETHUSDC").unwrap().is_none());
}

#[test]
fn migration_rolls_back_account_if_pausing_fails() {
    let path =
        std::env::temp_dir().join(format!("paper-migration-{}.sqlite", uuid::Uuid::new_v4()));
    let db = Database::open(&path).unwrap();
    let editor = rusqlite::Connection::open(&path).unwrap();
    assert!(db
        .insert_trade(&trade("legacy", "SOLUSDC", TradingMode::Paper))
        .unwrap());
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    editor.execute_batch("CREATE TRIGGER reject_pause BEFORE UPDATE ON pair_runtime BEGIN SELECT RAISE(ABORT, 'pause failure'); END;").unwrap();

    assert!(db.initialize_legacy_paper_account("SOLUSDC").is_err());
    assert!(db.load_paper_state("SOLUSDC").is_err());
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    drop(editor);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn migration_accepts_unknown_legacy_history_and_preserves_cleanup_metadata() {
    for cleared in [false, true] {
        let path =
            std::env::temp_dir().join(format!("paper-migration-{}.sqlite", uuid::Uuid::new_v4()));
        let db = Database::open(&path).unwrap();
        let editor = rusqlite::Connection::open(&path).unwrap();
        assert!(db
            .insert_trade(&trade("unknown", "BTCUSDT", TradingMode::Paper))
            .unwrap());
        editor
            .execute(
                "UPDATE trades SET mode = 'UNKNOWN' WHERE trade_id = 'unknown'",
                [],
            )
            .unwrap();
        if cleared {
            assert_eq!(db.clear_trade_history_for_symbol("BTCUSDT").unwrap(), 1);
        }
        let snapshot = || -> (String, String, String, String) {
            let read = |sql: &str| {
                editor
                    .query_row(sql, [], |row| row.get::<_, String>(0))
                    .unwrap()
            };
            (
                read("SELECT COALESCE(group_concat(quote(trade_id) || ':' || quote(mode)), '') FROM trades"),
                read("SELECT COALESCE(group_concat(quote(symbol) || ':' || quote(mode) || ':' || quote(side) || ':' || quote(price) || ':' || quote(timestamp)), '') FROM execution_frontiers"),
                read("SELECT COALESCE(group_concat(quote(symbol) || ':' || quote(mode) || ':' || quote(client_order_id) || ':' || quote(side) || ':' || quote(price) || ':' || quote(quantity)), '') FROM cleared_pair_openings"),
                read("SELECT COALESCE(group_concat(quote(trade_id) || ':' || quote(client_order_id) || ':' || quote(mode)), '') FROM cleared_trade_ids"),
            )
        };
        let before = snapshot();
        assert!(db.load_paper_state("BTCUSDT").is_err());
        db.initialize_legacy_paper_account("BTCUSDT").unwrap();
        let (position, account) = db.load_paper_state("BTCUSDT").unwrap().unwrap();
        assert_eq!(position.size, dec!(0));
        assert_eq!(account.asset, "USDT");
        assert_eq!(account.available_balance, dec!(10000));
        assert_eq!(snapshot(), before);
        assert!(db.initialize_legacy_paper_account("BTCUSDT").is_err());
        drop(editor);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn legacy_paper_account_starts_paused_and_cannot_resume() {
    use crate::config::AppConfig;
    use crate::server::AppState;
    use crate::types::BotStatus;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    for cleared in [false, true] {
        let db = Arc::new(Database::open(":memory:").unwrap());
        assert!(db
            .insert_trade(&trade("legacy", "SOLUSDC", TradingMode::Paper))
            .unwrap());
        if cleared {
            assert_eq!(db.clear_trade_history_for_symbol("SOLUSDC").unwrap(), 1);
        }
        let mut config = AppConfig::default();
        config.exchange.symbol = "SOLUSDC".into();
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(config, db, tx);
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        let error = state.resume_trading().await.unwrap_err().to_string();
        assert!(error.contains("模拟盘仓位和余额未能恢复"), "{error}");
        assert_eq!(*state.status.read().await, BotStatus::Paused);
    }
}
