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
    let shared_before = db
        .shared_paper_account("SOLUSDC", &initial.0, &initial.1)
        .unwrap();
    assert_eq!(shared_before.total_wallet_balance, dec!(-8400));
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
        let shared_after = db
            .shared_paper_account("BTCUSDC", &other.0, &other.1)
            .unwrap();
        assert_eq!(
            serde_json::to_value(shared_after).unwrap(),
            serde_json::to_value(&shared_before).unwrap()
        );
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

#[test]
fn shared_paper_wallet_has_one_baseline_and_publishes_commits_immediately() {
    let db = Database::open(":memory:").unwrap();
    let sol = paper_state("SOLUSDC", 0, 10000);
    let btc = paper_state("BTCUSDC", 0, 10000);
    for (symbol, state) in [("SOLUSDC", &sol), ("BTCUSDC", &btc)] {
        let shared = db.shared_paper_account(symbol, &state.0, &state.1).unwrap();
        assert_eq!(shared.total_wallet_balance, dec!(10000));
        assert_eq!(shared.available_balance, dec!(10000));
    }
    let earned = paper_state("SOLUSDC", 0, 10005);
    let lost = paper_state("BTCUSDC", 0, 9997);
    for (id, symbol, state) in [("earned", "SOLUSDC", &earned), ("lost", "BTCUSDC", &lost)] {
        assert!(db
            .insert_trade_with_paper_recovery(
                &trade(id, symbol, TradingMode::Paper),
                &GridStats::default(),
                None,
                None,
                Some(state),
            )
            .unwrap());
    }
    // Both writes must be reflected even though only this old SOL snapshot is published.
    let shared = db.shared_paper_account("SOLUSDC", &sol.0, &sol.1).unwrap();
    assert_eq!(shared.total_wallet_balance, dec!(10002));
    assert_eq!(shared.available_balance, dec!(10002));
    assert_eq!(
        db.load_paper_state("SOLUSDC")
            .unwrap()
            .unwrap()
            .1
            .total_wallet_balance,
        dec!(10005)
    );
    assert_eq!(
        db.load_paper_state("BTCUSDC")
            .unwrap()
            .unwrap()
            .1
            .total_wallet_balance,
        dec!(9997)
    );
}

#[test]
fn shared_paper_margin_combines_long_and_short_positions_and_isolates_assets() {
    let db = Database::open(":memory:").unwrap();
    let mut long = paper_state("SOLUSDC", 2, 10000);
    long.0.mark_price = dec!(110);
    long.0.unrealized_pnl = dec!(20);
    long.0.leverage = 2;
    let mut short = paper_state("BTCUSDC", -3, 10000);
    short.0.entry_price = dec!(200);
    short.0.mark_price = dec!(190);
    short.0.unrealized_pnl = dec!(30);
    db.shared_paper_account("SOLUSDC", &long.0, &long.1)
        .unwrap();
    let shared = db
        .shared_paper_account("BTCUSDC", &short.0, &short.1)
        .unwrap();
    assert_eq!(shared.total_wallet_balance, dec!(10000));
    assert_eq!(shared.unrealized_profit, dec!(50));
    assert_eq!(shared.margin_balance, dec!(10050));
    assert_eq!(shared.available_balance, dec!(9370));

    let mut other_asset = paper_state("ETHUSDT", -2, 10007);
    other_asset.1.asset = "USDT".into();
    other_asset.0.entry_price = dec!(50);
    other_asset.0.unrealized_pnl = dec!(-4);
    let separate = db
        .shared_paper_account("ETHUSDT", &other_asset.0, &other_asset.1)
        .unwrap();
    assert_eq!(separate.total_wallet_balance, dec!(10007));
    assert_eq!(separate.unrealized_profit, dec!(-4));
    assert_eq!(separate.margin_balance, dec!(10003));
    assert_eq!(separate.available_balance, dec!(9903));
    let unchanged = db
        .shared_paper_account("SOLUSDC", &long.0, &long.1)
        .unwrap();
    assert_eq!(
        serde_json::to_value(unchanged).unwrap(),
        serde_json::to_value(shared).unwrap()
    );
}

#[test]
fn shared_paper_freshness_tracks_the_oldest_open_position() {
    let db = Database::open(":memory:").unwrap();
    let now = Utc::now();
    let old = now - chrono::Duration::minutes(2);
    let recent = now - chrono::Duration::seconds(2);
    let mut stale = paper_state("SOLUSDC", 2, 10000);
    stale.1.update_time = old;
    let mut fresh = paper_state("BTCUSDC", -1, 10000);
    fresh.1.update_time = now;
    fresh.0.update_time = Some(recent);
    let mut flat = paper_state("ETHUSDC", 0, 10000);
    flat.1.update_time = now;
    db.shared_paper_account("SOLUSDC", &stale.0, &stale.1)
        .unwrap();
    assert_eq!(
        db.shared_paper_account("BTCUSDC", &fresh.0, &fresh.1)
            .unwrap()
            .update_time,
        old
    );
    assert_eq!(
        db.shared_paper_account("ETHUSDC", &flat.0, &flat.1)
            .unwrap()
            .update_time,
        old
    );
    stale.1.update_time = now;
    assert_eq!(
        db.shared_paper_account("SOLUSDC", &stale.0, &stale.1)
            .unwrap()
            .update_time,
        recent
    );
    // A late publication cannot replace a fresher valuation.
    stale.1.update_time = old;
    assert_eq!(
        db.shared_paper_account("SOLUSDC", &stale.0, &stale.1)
            .unwrap()
            .update_time,
        recent
    );
}

#[test]
fn shared_paper_wallet_restores_inactive_subledgers_after_history_cleanup() {
    for cleanup in 0..3 {
        let path =
            std::env::temp_dir().join(format!("paper-shared-{}.sqlite", uuid::Uuid::new_v4()));
        let db = Database::open(&path).unwrap();
        let inactive = paper_state("SOLUSDC", 0, 10017);
        let mut open = paper_state("BTCUSDC", 2, 9995);
        open.0.mark_price = dec!(105);
        open.0.unrealized_pnl = dec!(7);
        for (id, symbol, state) in [
            ("inactive", "SOLUSDC", &inactive),
            ("open", "BTCUSDC", &open),
        ] {
            db.insert_trade_with_paper_recovery(
                &trade(id, symbol, TradingMode::Paper),
                &GridStats::default(),
                None,
                None,
                Some(state),
            )
            .unwrap();
        }
        db.insert_trade(&trade("live", "ETHUSDC", TradingMode::Live))
            .unwrap();
        match cleanup {
            1 => {
                db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
            }
            2 => {
                db.clear_trade_history().unwrap();
            }
            _ => {}
        }
        drop(db);
        let reopened = Database::open(&path).unwrap();
        // Neither persisted symbol is running or published in this process.
        let added = paper_state("ETHUSDC", 0, 10000);
        let shared = reopened
            .shared_paper_account("ETHUSDC", &added.0, &added.1)
            .unwrap();
        assert_eq!(shared.total_wallet_balance, dec!(10012));
        assert_eq!(shared.unrealized_profit, dec!(7));
        assert_eq!(shared.margin_balance, dec!(10019));
        assert_eq!(shared.available_balance, dec!(9809));
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn duplicate_paper_trades_cannot_change_shared_wallet_or_holdings() {
    let db = Database::open(":memory:").unwrap();
    let initial = paper_state("SOLUSDC", 2, 10005);
    let changed = paper_state("SOLUSDC", 4, 9990);
    let fill = trade("duplicate", "SOLUSDC", TradingMode::Paper);
    assert!(db
        .insert_trade_with_paper_recovery(&fill, &GridStats::default(), None, None, Some(&initial),)
        .unwrap());
    let before = db
        .shared_paper_account("SOLUSDC", &initial.0, &initial.1)
        .unwrap();
    for cleared in [false, true] {
        if cleared {
            db.clear_trade_history().unwrap();
        }
        assert!(!db
            .insert_trade_with_paper_recovery(
                &fill,
                &GridStats::default(),
                None,
                None,
                Some(&changed),
            )
            .unwrap());
        let after = db
            .shared_paper_account("SOLUSDC", &changed.0, &changed.1)
            .unwrap();
        assert_eq!(
            serde_json::to_value(after).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }
}

#[test]
fn shared_paper_legacy_failure_is_asset_scoped_and_migration_adds_no_capital() {
    let db = Database::open(":memory:").unwrap();
    let earned = paper_state("BTCUSDC", 0, 10005);
    db.insert_trade_with_paper_recovery(
        &trade("earned", "BTCUSDC", TradingMode::Paper),
        &GridStats::default(),
        None,
        None,
        Some(&earned),
    )
    .unwrap();
    db.insert_trade(&trade("legacy", "SOLUSDC", TradingMode::Unknown))
        .unwrap();
    db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
    let mut usdt = paper_state("ETHUSDT", 0, 10000);
    usdt.1.asset = "USDT".into();
    assert_eq!(
        db.shared_paper_account("ETHUSDT", &usdt.0, &usdt.1)
            .unwrap()
            .total_wallet_balance,
        dec!(10000)
    );
    assert!(db
        .shared_paper_account("BTCUSDC", &earned.0, &earned.1)
        .is_err());
    db.initialize_legacy_paper_account("SOLUSDC").unwrap();
    assert_eq!(
        db.shared_paper_account("BTCUSDC", &earned.0, &earned.1)
            .unwrap()
            .total_wallet_balance,
        dec!(10005)
    );
    // Legacy rows introduced after initialization must also fail closed.
    db.insert_trade(&trade("later-legacy", "XRPUSDC", TradingMode::Paper))
        .unwrap();
    assert!(db
        .shared_paper_account("BTCUSDC", &earned.0, &earned.1)
        .is_err());
    db.initialize_legacy_paper_account("XRPUSDC").unwrap();
    assert_eq!(
        db.shared_paper_account("BTCUSDC", &earned.0, &earned.1)
            .unwrap()
            .total_wallet_balance,
        dec!(10005)
    );
}

#[test]
fn concurrent_paper_commits_and_publications_keep_each_symbols_contribution() {
    let db = Database::open(":memory:").unwrap();
    let baseline = paper_state("ETHUSDC", 0, 10000);
    db.shared_paper_account("ETHUSDC", &baseline.0, &baseline.1)
        .unwrap();
    std::thread::scope(|scope| {
        for (symbol, delta) in [("SOLUSDC", 1), ("BTCUSDC", -2)] {
            let db = &db;
            scope.spawn(move || {
                for index in 1..=40 {
                    let state = paper_state(symbol, 0, 10000 + delta * index);
                    db.insert_trade_with_paper_recovery(
                        &trade(&format!("{symbol}-{index}"), symbol, TradingMode::Paper),
                        &GridStats::default(),
                        None,
                        None,
                        Some(&state),
                    )
                    .unwrap();
                    db.shared_paper_account(symbol, &state.0, &state.1).unwrap();
                }
            });
        }
    });
    let shared = db
        .shared_paper_account("ETHUSDC", &baseline.0, &baseline.1)
        .unwrap();
    assert_eq!(shared.total_wallet_balance, dec!(9960));
    assert_eq!(shared.available_balance, dec!(9960));
}

#[test]
fn registered_paper_baseline_survives_analysis_history_until_restart() {
    let path = std::env::temp_dir().join(format!("paper-known-{}.sqlite", uuid::Uuid::new_v4()));
    let db = Database::open(&path).unwrap();
    let initial = paper_state("SOLUSDC", 0, 10000);
    db.shared_paper_account("SOLUSDC", &initial.0, &initial.1)
        .unwrap();
    db.insert_trade(&trade("analysis", "SOLUSDC", TradingMode::Paper))
        .unwrap();
    assert_eq!(
        db.shared_paper_account("SOLUSDC", &initial.0, &initial.1)
            .unwrap()
            .total_wallet_balance,
        dec!(10000)
    );
    drop(db);
    let reopened = Database::open(&path).unwrap();
    assert!(reopened
        .shared_paper_account("SOLUSDC", &initial.0, &initial.1)
        .is_err());
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn failed_paper_migration_does_not_clear_cached_legacy_failure() {
    let path = std::env::temp_dir().join(format!(
        "paper-failed-migration-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let db = Database::open(&path).unwrap();
    let editor = rusqlite::Connection::open(&path).unwrap();
    db.insert_trade(&trade("legacy", "SOLUSDC", TradingMode::Paper))
        .unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    let mut separate = paper_state("ETHUSDT", 0, 10000);
    separate.1.asset = "USDT".into();
    db.shared_paper_account("ETHUSDT", &separate.0, &separate.1)
        .unwrap();
    let initial = paper_state("SOLUSDC", 0, 10000);
    assert!(db
        .shared_paper_account("SOLUSDC", &initial.0, &initial.1)
        .is_err());
    editor.execute_batch("CREATE TRIGGER reject_pause BEFORE UPDATE ON pair_runtime BEGIN SELECT RAISE(ABORT, 'pause failure'); END;").unwrap();
    assert!(db.initialize_legacy_paper_account("SOLUSDC").is_err());
    assert!(db
        .shared_paper_account("SOLUSDC", &initial.0, &initial.1)
        .is_err());
    assert_eq!(
        db.shared_paper_account("ETHUSDT", &separate.0, &separate.1)
            .unwrap()
            .total_wallet_balance,
        dec!(10000)
    );
    drop(editor);
    drop(db);
    std::fs::remove_file(path).unwrap();
}
