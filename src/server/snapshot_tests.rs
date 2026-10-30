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
    db.insert_trade(&trade("aggregate-5", "BTCUSDC", TradingMode::Testnet, now))
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
    assert_eq!(result.groups.len(), 4);
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
        .execute("ALTER TABLE trades RENAME TO unavailable_trades", [])
        .unwrap();
    assert!(state.snapshot_at(now).await.hourly_trade_stats.is_none());
    editor
        .execute("ALTER TABLE unavailable_trades RENAME TO trades", [])
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
    assert!(state.db.verify_trade_pnl(&fill).unwrap());
    let after = state.snapshot_at(now).await.hourly_trade_stats.unwrap();
    assert_eq!(after.realized_pnl, dec!(-1.23456789));
    assert_eq!(after.pending_pnl_count, 0);
    assert_eq!(after.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 1);
    assert_eq!(state.db.hourly_stats_query_count(), 2);
    assert!(!state.db.verify_trade_pnl(&fill).unwrap());
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
    state.config.write().await.exchange.dry_run = false;
    state.refresh_current_scope().await;
    let live = state.snapshot_at(now).await;
    assert_eq!(total(&live), 2);
    assert_eq!(state.db.hourly_stats_query_count(), 2);
    {
        let mut config = state.config.write().await;
        config.exchange.symbol = "BTCUSDT".into();
        config.exchange.dry_run = true;
        config.exchange.is_testnet = true;
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
    config.exchange.dry_run = false;
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
    state.add_log("INFO", "updated while REST is blocked").await;
    let second = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame: serde_json::Value =
                serde_json::from_str(&updates.recv().await.unwrap()).unwrap();
            if frame["type"] == "snapshot" {
                break frame;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        second["data"]["recent_logs"][0]["message"],
        "updated while REST is blocked"
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
