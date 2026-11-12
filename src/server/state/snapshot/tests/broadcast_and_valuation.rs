use super::super::*;
use super::{fixture, test_time, trade};
use crate::types::BotStatus;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::{broadcast, mpsc};

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
    use crate::exchange::BinanceFuturesClient;
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
                Json(serde_json::json!({"symbol":"SOLUSDC","markPrice":"100"}))
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
    let first = tokio::time::timeout(Duration::from_secs(2), async {
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
    assert_eq!(first["data"]["status"], "PAUSED");
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
    let paper_scope = summary.snapshot.account_scope.clone();
    assert_eq!(paper_scope.len(), 16);
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
    assert_eq!(summary.snapshot.account_scope.len(), 16);
    assert_ne!(summary.snapshot.account_scope, paper_scope);
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
                    "account_scope": full.account_scope,
                    "sync_interval_secs": full.sync_interval_secs,
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

#[tokio::test]
async fn snapshot_frame_unchanged_ignores_generated_at_and_uptime_but_detects_account_scope() {
    let state = fixture();
    let t0 = test_time();
    let s1 = state.snapshot_at(t0).await;
    let s2 = state
        .snapshot_at(t0 + chrono::Duration::milliseconds(1850))
        .await;
    assert_eq!(s1.account_scope.len(), 16);
    assert_eq!(s1.account_scope, s2.account_scope);
    assert_ne!(s1.generated_at, s2.generated_at);
    let f1 = crate::types::ws_frame_json("snapshot", &s1).unwrap();
    let f2 = crate::types::ws_frame_json("snapshot", &s2).unwrap();
    assert!(snapshot_frame_unchanged(&f1, &f2));

    let mut scope_rotated = s2.clone();
    scope_rotated.account_scope = "ffffffffffffffff".into();
    let f_scope = crate::types::ws_frame_json("snapshot", &scope_rotated).unwrap();
    assert!(!snapshot_frame_unchanged(&f1, &f_scope));

    let mut changed = s2;
    changed.ticker.last_price = dec!(999);
    let f3 = crate::types::ws_frame_json("snapshot", &changed).unwrap();
    assert!(!snapshot_frame_unchanged(&f1, &f3));
}
