use super::*;
use crate::types::{BotStatus, GridStats, OrderSide, TradeRecord};
use rust_decimal_macros::dec;
use tokio::sync::{broadcast, mpsc};

fn test_time() -> DateTime<Utc> {
    "2026-10-14T03:20:00Z".parse().unwrap()
}

fn state_with_db(db: Arc<Database>) -> Arc<AppState> {
    let (tx, _) = mpsc::channel(1);
    AppState::new(AppConfig::default(), db, tx)
}

fn fixture() -> Arc<AppState> {
    state_with_db(Arc::new(Database::open(":memory:").unwrap()))
}

fn trade(id: &str, symbol: &str, mode: TradingMode, time: DateTime<Utc>) -> TradeRecord {
    TradeRecord {
        trade_id: id.into(),
        client_order_id: format!("gb_b_{id}"),
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
        timestamp: time,
        note: String::new(),
    }
}

#[tokio::test]
async fn overview_today_pnl_covers_history_modes_boundaries_and_cache_invalidation() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let now = test_time();
    let start = crate::db::beijing_midnight(now);
    assert!(db.cached_today_pnl(now).await.unwrap().groups.is_empty());
    let mut first = trade("aggregate-1", "DELETEDUSDT", TradingMode::Live, start);
    first.realized_pnl = dec!(0.123456789);
    db.insert_trade(&first).unwrap();
    let mut second = trade("aggregate-2", "DELETEDUSDT", TradingMode::Live, now);
    second.realized_pnl = dec!(0.000000001);
    db.insert_trade(&second).unwrap();
    let mut pending = trade("aggregate-3", "DELETEDUSDT", TradingMode::Live, now);
    pending.pnl_verified = false;
    pending.realized_pnl = dec!(999);
    db.insert_trade(&pending).unwrap();
    db.insert_trade(&trade("aggregate-4", "BTCUSDC", TradingMode::Paper, now))
        .unwrap();
    db.insert_trade(&trade("aggregate-6", "BTCUSDC", TradingMode::Unknown, now))
        .unwrap();
    db.insert_trade(&trade(
        "aggregate-old",
        "OLD",
        TradingMode::Live,
        start - chrono::Duration::nanoseconds(1),
    ))
    .unwrap();
    db.insert_trade(&trade(
        "aggregate-future",
        "FUTURE",
        TradingMode::Live,
        now + chrono::Duration::seconds(1),
    ))
    .unwrap();
    let result = db.cached_today_pnl(now).await.unwrap();
    assert_eq!(result.window_start, start);
    assert_eq!(result.window_end, now);
    assert_eq!(result.groups.len(), 3);
    let group = result
        .groups
        .iter()
        .find(|g| g.symbol == "DELETEDUSDT")
        .unwrap();
    assert_eq!(group.realized_pnl, dec!(0.123456790));
    assert_eq!(group.pending_pnl_count, 1);
    assert!(result.groups.iter().any(|g| g.mode == TradingMode::Unknown));
    assert!(db
        .cached_today_pnl(start - chrono::Duration::seconds(1))
        .await
        .unwrap()
        .groups
        .iter()
        .all(|g| g.symbol != "DELETEDUSDT"));
}

#[tokio::test]
async fn overview_today_pnl_failed_query_returns_none_and_recovers() {
    let directory = std::env::temp_dir().join(format!("overview-pnl-{}", uuid::Uuid::new_v4()));
    let path = directory.join("test.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let editor = rusqlite::Connection::open(&path).unwrap();
    editor
        .execute("ALTER TABLE trades RENAME TO unavailable_trades", [])
        .unwrap();
    let now = test_time();
    assert!(db.cached_today_pnl(now).await.is_none());
    editor
        .execute("ALTER TABLE unavailable_trades RENAME TO trades", [])
        .unwrap();
    assert!(db.cached_today_pnl(now).await.is_none());
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(db.cached_today_pnl(now).await.unwrap().groups.is_empty());
    drop(editor);
    drop(db);
    std::fs::remove_dir_all(directory).unwrap();
}

fn total(snapshot: &BotSnapshot) -> u64 {
    snapshot
        .hourly_trade_stats
        .as_ref()
        .unwrap()
        .buckets
        .iter()
        .map(|bucket| bucket.buy_count + bucket.sell_count)
        .sum()
}

#[tokio::test]
async fn repeated_snapshots_reuse_charts_but_keep_live_fields_current() {
    let state = fixture();
    let now = test_time();
    state.snapshot_at(now).await;
    assert_eq!(state.db.hourly_stats_query_count(), 1);
    *state.status.write().await = BotStatus::Paused;
    state.ticker.write().await.last_price = dec!(123);
    state.add_log("INFO", "live snapshot update").await;
    for offset in [800, 1600, 2400, 3200, 4000] {
        let snapshot = state
            .snapshot_at(now + chrono::Duration::milliseconds(offset))
            .await;
        assert_eq!(snapshot.status, BotStatus::Paused);
        assert_eq!(snapshot.ticker.last_price, dec!(123));
        assert_eq!(snapshot.recent_logs[0].message, "live snapshot update");
        assert_eq!(state.db.hourly_stats_query_count(), 1);
    }
    state.snapshot_at(now + chrono::Duration::seconds(5)).await;
    assert_eq!(state.db.hourly_stats_query_count(), 2);
    state.snapshot_at(now + chrono::Duration::seconds(60)).await;
    assert_eq!(state.db.hourly_stats_query_count(), 3);
}

#[tokio::test]
async fn concurrent_snapshot_requests_share_the_same_chart_queries() {
    let state = fixture();
    let now = test_time();
    let mut requests = Vec::new();
    for _ in 0..16 {
        let state = state.clone();
        requests.push(tokio::spawn(async move { state.snapshot_at(now).await }));
    }
    for request in requests {
        assert_eq!(total(&request.await.unwrap()), 0);
    }
    assert_eq!(state.db.hourly_stats_query_count(), 1);
}

#[tokio::test]
async fn moving_the_clock_back_does_not_reuse_counts_from_the_future() {
    let state = fixture();
    let now = test_time();
    state
        .db
        .insert_trade(&trade("future", "SOLUSDC", TradingMode::Paper, now))
        .unwrap();
    assert_eq!(total(&state.snapshot_at(now).await), 1);
    assert_eq!(
        total(&state.snapshot_at(now - chrono::Duration::seconds(1)).await),
        0
    );
    assert_eq!(state.db.hourly_stats_query_count(), 2);
}

#[tokio::test]
async fn failed_chart_reads_back_off_and_recover_without_caching_the_error_forever() {
    let directory = std::env::temp_dir().join(format!("snapshot-cache-{}", uuid::Uuid::new_v4()));
    let path = directory.join("test.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let state = state_with_db(db.clone());
    let now = test_time();
    let editor = rusqlite::Connection::open(&path).unwrap();
    editor
        .execute(
            "ALTER TABLE trade_hourly_rollups RENAME TO unavailable_trade_hourly_rollups",
            [],
        )
        .unwrap();
    assert!(state.snapshot_at(now).await.hourly_trade_stats.is_none());
    editor
        .execute(
            "ALTER TABLE unavailable_trade_hourly_rollups RENAME TO trade_hourly_rollups",
            [],
        )
        .unwrap();
    for offset in [800, 1600, 2400, 3200, 4000] {
        assert!(state
            .snapshot_at(now + chrono::Duration::milliseconds(offset))
            .await
            .hourly_trade_stats
            .is_none());
        assert_eq!(db.hourly_stats_query_count(), 1);
    }
    let recovered = state.snapshot_at(now + chrono::Duration::seconds(5)).await;
    assert_eq!(total(&recovered), 0);
    assert_eq!(db.hourly_stats_query_count(), 2);
    drop(editor);
    drop(state);
    drop(db);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn confirmed_trades_and_cleanup_invalidate_only_trade_counts() {
    let state = fixture();
    let now = test_time();
    assert_eq!(total(&state.snapshot_at(now).await), 0);
    let fill = trade("new", "SOLUSDC", TradingMode::Paper, now);
    assert!(state.record_trade(fill.clone(), None, None, None).await);
    let snapshot = state.snapshot_at(now).await;
    assert_eq!(total(&snapshot), 1);
    assert_eq!(snapshot.stats.total_trades, 1);
    assert_eq!(state.db.hourly_stats_query_count(), 2);
    assert!(state.record_trade(fill, None, None, None).await);
    state.snapshot_at(now).await;
    assert_eq!(state.db.hourly_stats_query_count(), 2);
    assert_eq!(state.clear_trade_history().await.unwrap(), 1);
    let cleared = state.snapshot_at(now).await;
    assert_eq!(total(&cleared), 0);
    assert_eq!(cleared.stats.total_trades, 0);
    assert!(cleared.recent_trades.is_empty());
    assert_eq!(state.db.hourly_stats_query_count(), 3);
}

#[tokio::test]
async fn other_symbols_do_not_invalidate_charts_but_global_cleanup_does() {
    let state = fixture();
    let now = test_time();
    assert_eq!(total(&state.snapshot_at(now).await), 0);
    let mut other = trade("other", "BTCUSDT", TradingMode::Paper, now);
    other.pnl_verified = false;
    assert!(state.db.insert_trade(&other).unwrap());
    assert_eq!(total(&state.snapshot_at(now).await), 0);
    assert_eq!(state.db.hourly_stats_query_count(), 1);

    // Verification must use the saved row's symbol, not the caller's symbol.
    other.symbol = "SOLUSDC".into();
    let clear_revision = state.db.symbol_trade_clear_revision(&other.symbol);
    assert!(state.db.verify_trade_pnl(&other, clear_revision).unwrap());
    assert_eq!(total(&state.snapshot_at(now).await), 0);
    assert_eq!(state.db.hourly_stats_query_count(), 1);
    assert_eq!(
        state.db.clear_trade_history_for_symbol("BTCUSDT").unwrap(),
        1
    );
    assert_eq!(total(&state.snapshot_at(now).await), 0);
    assert_eq!(state.db.hourly_stats_query_count(), 1);

    state.db.clear_trade_history().unwrap();
    assert_eq!(total(&state.snapshot_at(now).await), 0);
    assert_eq!(state.db.hourly_stats_query_count(), 2);
}

#[tokio::test]
async fn other_symbol_commit_does_not_reject_a_cached_chart() {
    let state = fixture();
    let now = test_time();
    let charts = state
        .snapshot_cache
        .get(
            &state.db,
            SnapshotScope::from_config(&AppConfig::default()),
            now,
        )
        .await;
    state
        .db
        .insert_trade(&trade("other-race", "BTCUSDT", TradingMode::Paper, now))
        .unwrap();
    assert!(state.collect_snapshot(Some(charts), now).await.is_some());
    assert_eq!(state.db.hourly_stats_query_count(), 1);
}

#[tokio::test]
async fn pnl_verification_invalidates_cached_today_stats() {
    let state = fixture();
    let now = test_time();
    let mut fill = trade("pending", "SOLUSDC", TradingMode::Paper, now);
    fill.pnl_verified = false;
    fill.realized_pnl = dec!(100);
    assert!(state.db.insert_trade(&fill).unwrap());
    let before = state.snapshot_at(now).await.hourly_trade_stats.unwrap();
    assert_eq!(before.realized_pnl, dec!(0));
    assert_eq!(before.pending_pnl_count, 1);
    assert_eq!(state.db.hourly_stats_query_count(), 1);

    fill.realized_pnl = dec!(-1.23456789);
    // The row's persisted symbol determines which chart is invalidated.
    fill.symbol = "BTCUSDT".into();
    let clear_revision = state.db.symbol_trade_clear_revision(&fill.symbol);
    assert!(state.db.verify_trade_pnl(&fill, clear_revision).unwrap());
    let after = state.snapshot_at(now).await.hourly_trade_stats.unwrap();
    assert_eq!(after.realized_pnl, dec!(-1.23456789));
    assert_eq!(after.pending_pnl_count, 0);
    assert_eq!(after.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 1);
    assert_eq!(state.db.hourly_stats_query_count(), 2);
    assert!(!state.db.verify_trade_pnl(&fill, clear_revision).unwrap());
    state.snapshot_at(now).await;
    assert_eq!(state.db.hourly_stats_query_count(), 2);
}

#[tokio::test]
async fn caches_follow_hour_and_beijing_midnight_boundaries() {
    for time in ["2026-10-14T03:59:59Z", "2026-10-14T15:59:59Z"] {
        let now: DateTime<Utc> = time.parse().unwrap();
        let next_hour = now + chrono::Duration::seconds(1);
        let state = fixture();
        state
            .db
            .insert_trade(&trade("before", "SOLUSDC", TradingMode::Paper, now))
            .unwrap();
        state
            .db
            .insert_trade(&trade("after", "SOLUSDC", TradingMode::Paper, next_hour))
            .unwrap();
        let before = state.snapshot_at(now).await;
        assert_eq!(total(&before), 1);
        let after = state.snapshot_at(next_hour).await;
        let rolled_day = crate::db::beijing_midnight(now) != crate::db::beijing_midnight(next_hour);
        assert_eq!(total(&after), if rolled_day { 1 } else { 2 });
        assert_eq!(
            after.hourly_trade_stats.as_ref().unwrap().buckets.len(),
            if rolled_day { 1 } else { 13 }
        );
        assert_eq!(state.db.hourly_stats_query_count(), 2);
    }
}

#[tokio::test]
async fn scope_changes_isolate_trade_modes_and_symbols() {
    let state = fixture();
    let now = test_time();
    for (id, symbol, mode) in [
        ("paper", "SOLUSDC", TradingMode::Paper),
        ("live1", "SOLUSDC", TradingMode::Live),
        ("live2", "SOLUSDC", TradingMode::Live),
        ("btc", "BTCUSDT", TradingMode::Paper),
    ] {
        state
            .db
            .insert_trade(&trade(id, symbol, mode, now))
            .unwrap();
    }
    assert_eq!(total(&state.snapshot_at(now).await), 1);
    {
        let mut config = state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
    }
    state.refresh_current_scope().await;
    let live = state.snapshot_at(now).await;
    assert_eq!(total(&live), 2);
    assert_eq!(state.db.hourly_stats_query_count(), 2);
    {
        let mut config = state.config.write().await;
        config.exchange.symbol = "BTCUSDT".into();
        config.exchange.api_key.clear();
        config.exchange.api_secret.clear();
    }
    state.refresh_current_scope().await;
    let btc = state.snapshot_at(now).await;
    assert_eq!(btc.symbol, "BTCUSDT");
    assert_eq!(total(&btc), 1);
    assert_eq!(state.db.hourly_stats_query_count(), 3);
}

#[tokio::test]
async fn stale_chart_read_cannot_be_combined_with_a_later_trade_commit() {
    let state = fixture();
    let now = test_time();
    let charts = state
        .snapshot_cache
        .get(
            &state.db,
            SnapshotScope::from_config(&AppConfig::default()),
            now,
        )
        .await;
    assert!(
        state
            .record_trade(
                trade("racing", "SOLUSDC", TradingMode::Paper, now),
                None,
                None,
                None
            )
            .await
    );
    assert!(state.collect_snapshot(Some(charts), now).await.is_none());
    let fresh = state.snapshot_at(now).await;
    assert_eq!(total(&fresh), 1);
    assert_eq!(fresh.stats.total_trades, 1);
}

#[tokio::test]
async fn snapshot_waits_for_scope_transition_and_retries_the_new_scope() {
    let state = fixture();
    let now = test_time();
    state
        .db
        .insert_trade_with_stats(
            &trade("btc", "BTCUSDT", TradingMode::Paper, now),
            &GridStats {
                total_trades: 1,
                ..Default::default()
            },
        )
        .unwrap();
    let transition = state.snapshot_scope_lock.write().await;
    let reading = state.clone();
    let task = tokio::spawn(async move { reading.snapshot_at(now).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while state.db.hourly_stats_query_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    state.config.write().await.exchange.symbol = "BTCUSDT".into();
    state.refresh_current_scope().await;
    drop(transition);
    let snapshot = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.symbol, "BTCUSDT");
    assert_eq!(total(&snapshot), 1);
    assert_eq!(snapshot.stats.total_trades, 1);
    assert_eq!(snapshot.recent_trades[0].symbol, "BTCUSDT");
}

#[tokio::test]
async fn publisher_skips_database_work_without_subscribers() {
    let state = fixture();
    let task = tokio::spawn(state.clone().run_snapshot_broadcast());
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(state.db.hourly_stats_query_count(), 0);
    let mut updates = state.ws_broadcast_tx.subscribe();
    let json = tokio::time::timeout(Duration::from_secs(2), updates.recv())
        .await
        .unwrap()
        .unwrap();
    let frame: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(frame["type"], "snapshot");
    assert_eq!(state.db.hourly_stats_query_count(), 1);
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn snapshot_push_continues_while_the_trading_engine_waits_for_rest() {
    use crate::exchange::client::BinanceFuturesClient;
    use crate::strategy::GridTradingEngine;
    use axum::{routing::get, Json, Router};
    use tokio::sync::Notify;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler_entered = entered.clone();
    let handler_release = release.clone();
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get(move || {
            let entered = handler_entered.clone();
            let release = handler_release.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                Json(serde_json::json!({"markPrice":"100"}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Paused).unwrap();
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let (tx, rx) = mpsc::channel(1);
    let (_ticks, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), tx);
    let mut engine = GridTradingEngine::new(
        state.clone(),
        Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url)),
        rx,
        ticker_rx,
    );
    let trading = tokio::spawn(async move { engine.run().await });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    let mut updates = state.ws_broadcast_tx.subscribe();
    let snapshots = tokio::spawn(state.clone().run_snapshot_broadcast());
    let first = tokio::time::timeout(Duration::from_secs(2), updates.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&first).unwrap()["type"],
        "snapshot"
    );
    state.ticker.write().await.last_price = dec!(123);
    let second = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame: serde_json::Value =
                serde_json::from_str(&updates.recv().await.unwrap()).unwrap();
            if frame["type"] == "snapshot"
                && frame["data"]["ticker"]["last_price"] == serde_json::json!(dec!(123))
            {
                break frame;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(second["data"]["status"], "PAUSED");
    assert_eq!(
        second["data"]["ticker"]["last_price"],
        serde_json::json!(dec!(123))
    );
    assert_eq!(db.hourly_stats_query_count(), 1);
    trading.abort();
    snapshots.abort();
    release.notify_waiters();
    server.abort();
    let _ = trading.await;
    let _ = snapshots.await;
    let _ = server.await;
}

#[tokio::test]
async fn snapshot_holds_position_until_account_valuation_is_published() {
    use crate::types::PositionInfo;
    use std::cell::Cell;
    use std::future::{poll_fn, Future};
    use std::task::Poll;

    for leverage in [1, 2] {
        let state = fixture();
        let symbol = state.config.read().await.exchange.symbol.clone();
        let old_time = state.account.read().await.update_time;
        let new_time = old_time + chrono::Duration::seconds(1);
        let initial_margin = dec!(100) / Decimal::from(leverage);
        {
            let mut position = state.position.write().await;
            *position = PositionInfo {
                symbol: symbol.clone(),
                size: dec!(1),
                entry_price: dec!(100),
                mark_price: dec!(100),
                leverage,
                // Legacy paper positions take their valuation time from the account.
                update_time: None,
                ..Default::default()
            };
            state.account.write().await.available_balance = dec!(10000) - initial_margin;
        }
        assert_eq!(state.current_account().await.margin_balance, dec!(10000));
        let observer_account = state.account.read().await.clone();
        let observe_shared = || {
            state
                .db
                .shared_paper_account("BTCUSDC", &PositionInfo::default(), &observer_account)
                .unwrap()
        };
        assert_eq!(observe_shared().margin_balance, dec!(10000));

        // Stop the snapshot after it reads the position, at the account read.
        // Poll each future explicitly so the boundary needs no scheduler timing.
        let account_blocker = state.account.write().await;
        let snapshot = state.collect_snapshot(None, test_time());
        tokio::pin!(snapshot);
        poll_fn(|cx| {
            assert!(snapshot.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;

        let valuation_has_position = Cell::new(false);
        let valuation = async {
            let config = state.config.read().await;
            let mut position = state.position.write().await;
            valuation_has_position.set(true);
            let mut account = state.account.write().await;
            position.mark_price = dec!(110);
            position.unrealized_pnl = dec!(10);
            account.unrealized_profit = dec!(10);
            account.margin_balance = dec!(10010);
            account.available_balance = dec!(10010) - dec!(110) / Decimal::from(leverage);
            account.update_time = new_time;
            state
                .db
                .shared_paper_account(&config.exchange.symbol, &position, &account)
                .unwrap()
        };
        tokio::pin!(valuation);
        poll_fn(|cx| {
            assert!(valuation.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            !valuation_has_position.get(),
            "a pending snapshot must retain its position read through account publication"
        );

        drop(account_blocker);
        let (snapshot, published) = tokio::join!(snapshot, valuation);
        let snapshot = snapshot.unwrap();
        assert_eq!(snapshot.position.unrealized_pnl, dec!(0));
        assert_eq!(snapshot.position.update_time, None);
        assert_eq!(snapshot.account.unrealized_profit, dec!(0));
        assert_eq!(snapshot.account.margin_balance, dec!(10000));
        assert_eq!(snapshot.account.update_time, old_time);
        assert_eq!(published.unrealized_profit, dec!(10));
        assert_eq!(published.margin_balance, dec!(10010));
        assert_eq!(published.update_time, new_time);
        assert_eq!(observe_shared().margin_balance, dec!(10010));

        let fresh = state.collect_snapshot(None, test_time()).await.unwrap();
        assert_eq!(fresh.position.unrealized_pnl, dec!(10));
        assert_eq!(fresh.account.margin_balance, dec!(10010));
        assert_eq!(fresh.account.update_time, new_time);
        assert_eq!(observe_shared().margin_balance, dec!(10010));
        // At 1x the increased position margin offsets the gain; at 2x it does not.
        assert_eq!(
            fresh.account.available_balance - snapshot.account.available_balance,
            dec!(10) - dec!(10) / Decimal::from(leverage)
        );
    }
}

#[tokio::test]
async fn overview_summary_rejects_old_chart_generations_and_mode_transitions() {
    let state = fixture();
    let now = test_time();
    let charts = state
        .snapshot_cache
        .get(
            &state.db,
            SnapshotScope::from_config(&AppConfig::default()),
            now,
        )
        .await;
    let fill = trade("overview-revision", "SOLUSDC", TradingMode::Paper, now);
    assert!(state.record_trade(fill, None, None, None).await);
    assert!(state.collect_overview(Some(charts)).await.is_none());
    let (summary, recent) = state.overview_snapshot(now).await;
    assert_eq!(summary.snapshot.today_trade_counts.unwrap().buy, 1);
    assert_eq!(recent[0].trade_id, "overview-revision");

    let charts = state
        .snapshot_cache
        .get(
            &state.db,
            SnapshotScope::from_config(&AppConfig::default()),
            now,
        )
        .await;
    {
        let mut config = state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
    }
    assert!(state.collect_overview(Some(charts)).await.is_none());
    let (summary, recent) = state.overview_snapshot(now).await;
    assert_eq!(summary.mode, TradingMode::Live);
    assert_eq!(summary.snapshot.today_trade_counts.unwrap().buy, 0);
    assert!(recent.is_empty());
}

#[tokio::test]
async fn account_snapshot_matches_full_snapshot_without_chart_queries() {
    let state = fixture();
    for paper in [true, false] {
        {
            let mut config = state.config.write().await;
            config.exchange.api_key = if paper {
                String::new()
            } else {
                "live-key".into()
            };
            config.exchange.api_secret = if paper {
                String::new()
            } else {
                "live-secret".into()
            };
        }
        {
            let mut ticker = state.ticker.write().await;
            ticker.mark_price = dec!(123.45);
            ticker.mark_update_time = test_time();
        }
        {
            let mut position = state.position.write().await;
            position.size = dec!(-2.5);
            position.unrealized_pnl = dec!(-12.34);
        }
        state.account.write().await.available_balance = dec!(567.89);
        let queries = state.db.hourly_stats_query_count();
        let account = serde_json::to_value(state.account_snapshot().await).unwrap();
        assert_eq!(state.db.hourly_stats_query_count(), queries);
        let full = state.snapshot().await;
        assert_eq!(
            account,
            serde_json::json!({
                "mode": full.mode,
                "snapshot": {
                    "symbol": full.symbol,
                    "status": full.status,
                    "ticker": {
                        "mark_price": full.ticker.mark_price,
                        "mark_update_time": full.ticker.mark_update_time,
                    },
                    "position": {
                        "size": full.position.size,
                        "unrealized_pnl": full.position.unrealized_pnl,
                    },
                    "account": full.account,
                    "account_cny_rate": full.account_cny_rate,
                }
            })
        );
    }
}

#[tokio::test]
async fn account_snapshot_waits_for_scope_transitions_and_trade_commits() {
    let state = fixture();
    let scope = state.snapshot_scope_lock.write().await;
    let reading = state.clone();
    let mut task = tokio::spawn(async move { reading.account_snapshot().await });
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut task)
        .await
        .is_err());
    {
        let mut config = state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
    }
    drop(scope);
    assert_eq!(
        serde_json::to_value(task.await.unwrap()).unwrap()["mode"],
        "LIVE"
    );

    let commit = state.trade_commit_lock.lock().await;
    let reading = state.clone();
    let mut task = tokio::spawn(async move { reading.account_snapshot().await });
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut task)
        .await
        .is_err());
    state.account.write().await.available_balance = dec!(42);
    drop(commit);
    assert_eq!(
        serde_json::to_value(task.await.unwrap()).unwrap()["snapshot"]["account"]
            ["available_balance"],
        "42"
    );
}
