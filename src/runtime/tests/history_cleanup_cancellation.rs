use super::*;
use crate::server::create_managed_router;
use crate::types::{GridStats, OrderSide, TradeRecord, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration;

async fn cancelled_cleanup_preserves_membership(path: &str, payload: Value, scoped: bool) {
    let runtime = fixture();
    let handle = runtime.handle();
    let states = handle.states.read().await.clone();
    for (index, state) in states.iter().enumerate() {
        let trade = TradeRecord {
            trade_id: format!("cancelled-cleanup-{index}"),
            client_order_id: format!("gb_cancelled_cleanup_{index}"),
            symbol: state.pair_scope().unwrap().into(),
            mode: TradingMode::Paper,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: Decimal::ONE,
            amount_usdc: dec!(100),
            realized_pnl: dec!(2),
            commission: Decimal::ZERO,
            pnl_verified: true,
            is_maker: true,
            timestamp: chrono::Utc::now(),
            note: String::new(),
        };
        handle
            .db
            .insert_trade_with_stats(
                &trade,
                &GridStats {
                    total_trades: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        state.refresh_current_scope().await;
        assert_eq!(state.stats.read().await.total_trades, 1);
    }
    let router = create_managed_router(handle.clone());
    let stats_guard = states[0].stats.write().await;
    let revision = handle.db.trade_data_revision();
    let path = path.to_owned();
    let cleanup = tokio::spawn(async move { request(&router, &path, "POST", payload, true).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while handle.db.trade_data_revision() == revision {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(handle.membership_lock.try_lock().is_err());
    cleanup.abort();
    assert!(cleanup.await.unwrap_err().is_cancelled());
    // The SQLite commit has finished, but publishing the cleared cache is blocked.
    let membership_still_locked = handle.membership_lock.try_lock().is_err();
    drop(stats_guard);
    let _membership = tokio::time::timeout(Duration::from_secs(5), handle.membership_lock.lock())
        .await
        .unwrap();
    assert_eq!(states[0].snapshot().await.stats.total_trades, 0);
    assert_eq!(
        states[1].snapshot().await.stats.total_trades,
        usize::from(scoped)
    );
    assert_eq!(
        handle.db.get_recent_trades(10).unwrap().len(),
        usize::from(scoped)
    );
    assert!(
        membership_still_locked,
        "request cancellation must not admit pair changes before cleanup publication finishes"
    );
}

#[tokio::test]
async fn cancelled_legacy_history_cleanup_keeps_membership_locked() {
    cancelled_cleanup_preserves_membership("/api/trades/clear", json!({"confirm": true}), false)
        .await;
}

#[tokio::test]
async fn cancelled_scoped_history_cleanup_keeps_membership_locked() {
    cancelled_cleanup_preserves_membership(
        "/api/pairs/SOLUSDC/trades/clear",
        json!({"confirm": true}),
        true,
    )
    .await;
}

#[tokio::test]
async fn cancelled_selected_history_cleanup_keeps_membership_locked() {
    cancelled_cleanup_preserves_membership(
        "/api/database/clear",
        json!({"targets": ["trade_history"], "confirm": true}),
        false,
    )
    .await;
}

#[tokio::test]
async fn cancelled_scoped_request_releases_membership_before_its_body_is_accepted() {
    let runtime = fixture();
    let handle = runtime.handle();
    let router = create_managed_router(handle.clone());
    let body = Body::from_stream(futures_util::stream::pending::<
        Result<&'static str, std::io::Error>,
    >());
    let cleanup = tokio::spawn(async move {
        router
            .oneshot(
                Request::builder()
                    .uri("/api/pairs/SOLUSDC/trades/clear")
                    .method("POST")
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer configuration-test-token")
                    .body(body)
                    .unwrap(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while handle.membership_lock.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cleanup.abort();
    assert!(cleanup.await.unwrap_err().is_cancelled());
    assert!(handle.membership_lock.try_lock().is_ok());
    assert!(handle.db.get_recent_trades(10).unwrap().is_empty());
}
