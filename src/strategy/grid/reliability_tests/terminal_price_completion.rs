use super::*;

#[tokio::test]
async fn filled_zero_price_placeholder_without_average_is_not_booked_at_zero() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (_, mut response) = managed_price_fixture();
    response.client_order_id = "gb_zero_price_fill".into();
    response.price = Decimal::ZERO;
    response.avg_price = None;
    fake.orders
        .lock()
        .unwrap()
        .insert(response.client_order_id.clone(), response.clone());
    let mut first = engine(db.clone(), &url).await;
    assert!(!first.sync_live_orders().await);
    {
        let mut orders = fake.orders.lock().unwrap();
        let filled = orders.get_mut(&response.client_order_id).unwrap();
        filled.status = "FILLED".into();
        filled.executed_qty = filled.orig_qty;
    }
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn terminal_missing_average_never_reprices_prior_partial_fill_after_restart() {
    for terminal_average in [None, Some(Decimal::ZERO), Some(dec!(-1))] {
        for terminal_quantity in [dec!(0.5), dec!(0.75)] {
            let fake = Exchange::default();
            let (url, server) = exchange_server(fake.clone()).await;
            let db = database();
            let (mut order, mut partial) = managed_price_fixture();
            order.price = dec!(100);
            order.quantity = dec!(1.5);
            order.amount_usdc = dec!(150);
            order.status = OrderStatus::PartiallyFilled;
            db.save_managed_order(&order).unwrap();
            partial.status = "PARTIALLY_FILLED".into();
            partial.price = dec!(100);
            partial.orig_qty = dec!(2);
            partial.executed_qty = dec!(0.5);
            partial.avg_price = Some(dec!(98));
            partial.update_time = Some(1000);
            let mut first = engine(db.clone(), &url).await;
            let account = first.client.account_identity_key();
            db.save_identity_observation(
                account,
                &order,
                &crate::types::ObservedOrderIdentity {
                    client_order_id: order.client_order_id.clone(),
                    symbol: order.symbol.clone(),
                    side: "BUY".into(),
                    order_id: partial.order_id,
                },
            )
            .unwrap();
            assert!(
                first
                    .record_initial_order_response(
                        account,
                        "SOLUSDC",
                        &order.client_order_id,
                        &partial
                    )
                    .await
            );
            drop(first);
            let mut terminal = partial.clone();
            terminal.status = "CANCELED".into();
            terminal.executed_qty = terminal_quantity;
            terminal.avg_price = terminal_average;
            terminal.update_time = Some(2000);
            fake.orders
                .lock()
                .unwrap()
                .insert(order.client_order_id.clone(), terminal);
            let mut restarted = engine(db.clone(), &url).await;
            let success = restarted.sync_live_orders().await;
            let trades = db.get_recent_trades(10).unwrap();
            if success {
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].quantity, terminal_quantity);
                if terminal_quantity == dec!(0.5) {
                    assert_eq!(trades[0].price, dec!(98));
                } else {
                    assert_ne!(trades[0].price, dec!(98));
                }
            } else {
                assert!(restarted.reconciliation_blocked);
                assert!(trades.is_empty());
            }
            drop(restarted);
            let mut again = engine(db.clone(), &url).await;
            assert_eq!(again.sync_live_orders().await, success);
            assert_eq!(
                db.get_recent_trades(10).unwrap().len(),
                usize::from(success)
            );
            server.abort();
        }
    }
}

#[tokio::test]
async fn terminal_fill_requires_exchange_price_when_average_is_invalid() {
    for terminal_price in [dec!(98.25), Decimal::ZERO] {
        for average in [None, Some(Decimal::ZERO)] {
            let fake = Exchange::default();
            let (url, server) = exchange_server(fake.clone()).await;
            let db = database();
            let (mut order, mut response) = managed_price_fixture();
            order.price = dec!(120.375);
            order.amount_usdc = order.price * order.quantity;
            db.save_managed_order(&order).unwrap();
            response.status = "FILLED".into();
            response.price = terminal_price;
            response.avg_price = average;
            response.executed_qty = response.orig_qty;
            fake.orders
                .lock()
                .unwrap()
                .insert(order.client_order_id.clone(), response);
            let mut engine = engine(db.clone(), &url).await;
            let success = engine.sync_live_orders().await;
            let trades = db.get_recent_trades(10).unwrap();
            if terminal_price > Decimal::ZERO {
                assert!(success);
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].price, terminal_price);
                assert_eq!(trades[0].amount_usdc, terminal_price * order.quantity);
            } else {
                assert!(!success);
                assert!(engine.reconciliation_blocked);
                assert!(trades.is_empty());
                assert_eq!(
                    db.load_managed_orders("SOLUSDC").unwrap()[0].price,
                    order.price
                );
            }
            server.abort();
        }
    }
}

#[tokio::test]
async fn terminal_zero_price_with_positive_average_books_at_exchange_average() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.status = "FILLED".into();
    response.price = Decimal::ZERO;
    response.avg_price = Some(dec!(98.625));
    response.executed_qty = response.orig_qty;
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut first = engine(db.clone(), &url).await;
    assert!(first.sync_live_orders().await);
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].price, dec!(98.625));
    assert_eq!(trades[0].amount_usdc, dec!(98.625) * order.quantity);
    server.abort();
}

#[tokio::test]
async fn terminal_average_correction_after_restart_preserves_real_disagreements() {
    for initial_average in [None, Some(Decimal::ZERO), Some(dec!(97.5))] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (order, mut response) = managed_price_fixture();
        db.save_managed_order(&order).unwrap();
        response.status = "FILLED".into();
        response.price = Decimal::ZERO;
        response.executed_qty = response.orig_qty;
        response.avg_price = initial_average;
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &order,
            &crate::types::ObservedOrderIdentity {
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                side: "BUY".into(),
                order_id: response.order_id,
            },
        )
        .unwrap();
        assert!(
            first
                .record_initial_order_response(
                    account,
                    "SOLUSDC",
                    &order.client_order_id,
                    &response
                )
                .await
        );
        let initial = db
            .load_initial_order_responses(account, "SOLUSDC", &order.client_order_id)
            .unwrap();
        assert!(initial.iter().any(|found| found.status == "FILLED"
            && found.price == Decimal::ZERO
            && found.executed_qty == order.quantity
            && found.avg_price == initial_average));
        drop(first);
        response.avg_price = Some(dec!(98.625));
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response);
        let mut restarted = engine(db.clone(), &url).await;
        let corrected = initial_average.is_none_or(|price| price <= Decimal::ZERO);
        assert_eq!(restarted.sync_live_orders().await, corrected);
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        let trades = db.get_recent_trades(10).unwrap();
        if corrected {
            assert!(disagreements.is_empty());
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].price, dec!(98.625));
            assert_eq!(trades[0].amount_usdc, dec!(98.625) * order.quantity);
        } else {
            assert!(restarted.reconciliation_blocked);
            assert!(trades.is_empty());
            assert!(disagreements
                .iter()
                .any(|(id, a, b)| id == &order.client_order_id
                    && a.avg_price == Some(dec!(97.5))
                    && b.avg_price == Some(dec!(98.625))));
        }
        drop(restarted);
        let mut again = engine(db.clone(), &url).await;
        assert_eq!(again.sync_live_orders().await, corrected);
        assert_eq!(
            db.get_recent_trades(10).unwrap().len(),
            usize::from(corrected)
        );
        assert_eq!(
            db.load_terminal_order_disagreements(account, "SOLUSDC")
                .unwrap()
                .is_empty(),
            corrected
        );
        server.abort();
    }
}

#[tokio::test]
async fn terminal_price_correction_after_restart_rejects_time_regression() {
    for later_time in [2000, 500] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (order, mut response) = managed_price_fixture();
        db.save_managed_order(&order).unwrap();
        response.status = "FILLED".into();
        response.price = Decimal::ZERO;
        response.avg_price = None;
        response.executed_qty = response.orig_qty;
        response.update_time = Some(1000);
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &order,
            &crate::types::ObservedOrderIdentity {
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                side: "BUY".into(),
                order_id: response.order_id,
            },
        )
        .unwrap();
        assert!(
            first
                .record_initial_order_response(
                    account,
                    "SOLUSDC",
                    &order.client_order_id,
                    &response
                )
                .await
        );
        drop(first);
        response.price = dec!(98);
        response.update_time = Some(later_time);
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response);
        let mut restarted = engine(db.clone(), &url).await;
        let corrected = later_time > 1000;
        assert_eq!(restarted.sync_live_orders().await, corrected);
        let trades = db.get_recent_trades(10).unwrap();
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        if corrected {
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].price, dec!(98));
            assert_eq!(trades[0].amount_usdc, dec!(98) * order.quantity);
            assert!(disagreements.is_empty());
        } else {
            assert!(restarted.reconciliation_blocked);
            assert!(trades.is_empty());
            assert!(disagreements
                .iter()
                .any(|(id, first, later)| id == &order.client_order_id
                    && first.price == Decimal::ZERO
                    && first.update_time == Some(1000)
                    && later.price == dec!(98)
                    && later.update_time == Some(500)));
        }
        drop(restarted);
        let mut again = engine(db.clone(), &url).await;
        assert_eq!(again.sync_live_orders().await, corrected);
        assert_eq!(
            db.get_recent_trades(10).unwrap().len(),
            usize::from(corrected)
        );
        assert_eq!(
            db.load_terminal_order_disagreements(account, "SOLUSDC")
                .unwrap()
                .is_empty(),
            corrected
        );
        server.abort();
    }
}

#[tokio::test]
async fn terminal_price_and_average_can_be_completed_in_stages_across_restarts() {
    for average_first in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (order, mut response) = managed_price_fixture();
        db.save_managed_order(&order).unwrap();
        response.status = "FILLED".into();
        response.executed_qty = response.orig_qty;
        response.price = Decimal::ZERO;
        response.avg_price = None;
        response.update_time = Some(1000);
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &order,
            &crate::types::ObservedOrderIdentity {
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                side: "BUY".into(),
                order_id: response.order_id,
            },
        )
        .unwrap();
        assert!(
            first
                .record_initial_order_response(
                    account,
                    "SOLUSDC",
                    &order.client_order_id,
                    &response
                )
                .await
        );
        drop(first);
        if average_first {
            response.avg_price = Some(dec!(98));
        } else {
            response.price = dec!(100);
        }
        response.update_time = Some(2000);
        let mut second = engine(db.clone(), &url).await;
        assert!(
            second
                .record_initial_order_response(
                    account,
                    "SOLUSDC",
                    &order.client_order_id,
                    &response
                )
                .await
        );
        drop(second);
        let evidence = db
            .load_initial_order_responses(account, "SOLUSDC", &order.client_order_id)
            .unwrap();
        assert!(evidence
            .iter()
            .any(|found| found.price == Decimal::ZERO && found.update_time == Some(1000)));
        assert!(evidence.iter().any(|found| found.update_time == Some(2000)
            && if average_first {
                found.price == Decimal::ZERO && found.avg_price == Some(dec!(98))
            } else {
                found.price == dec!(100) && found.avg_price.is_none()
            }));
        response.price = dec!(100);
        response.avg_price = Some(dec!(98));
        response.update_time = Some(3000);
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response);
        let mut recovered = engine(db.clone(), &url).await;
        assert!(recovered.sync_live_orders().await);
        let trades = db.get_recent_trades(10).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price, dec!(98));
        assert_eq!(trades[0].amount_usdc, dec!(98) * order.quantity);
        assert!(db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        drop(recovered);
        let mut again = engine(db.clone(), &url).await;
        assert!(again.sync_live_orders().await);
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        server.abort();
    }
}

#[test]
fn terminal_fill_evidence_completion_comparison_table() {
    let cases = [
        (
            "complete price",
            dec!(0),
            None,
            dec!(100),
            None,
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "complete average",
            dec!(0),
            None,
            dec!(0),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "complete both",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "price after average",
            dec!(0),
            Some(dec!(98)),
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "average after price",
            dec!(100),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "zero average completed",
            dec!(100),
            Some(dec!(0)),
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "positive price changed",
            dec!(100),
            Some(dec!(98)),
            dec!(101),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "positive average changed",
            dec!(100),
            Some(dec!(98)),
            dec!(100),
            Some(dec!(99)),
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "price and average changed",
            dec!(100),
            Some(dec!(98)),
            dec!(101),
            Some(dec!(99)),
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "price lost",
            dec!(100),
            Some(dec!(98)),
            dec!(0),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "average lost",
            dec!(100),
            Some(dec!(98)),
            dec!(100),
            None,
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "same timestamp",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(1000),
            false,
        ),
        (
            "time reversed",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(500),
            true,
        ),
        (
            "known time lost",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            None,
            true,
        ),
        (
            "time supplied",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            None,
            Some(2000),
            false,
        ),
        (
            "both times missing",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            None,
            None,
            false,
        ),
    ];
    for (name, old_price, old_avg, new_price, new_avg, old_time, new_time, expected) in cases {
        let (_, mut old) = managed_price_fixture();
        old.status = "FILLED".into();
        old.executed_qty = old.orig_qty;
        old.price = old_price;
        old.avg_price = old_avg;
        old.update_time = old_time;
        let mut new = old.clone();
        new.price = new_price;
        new.avg_price = new_avg;
        new.update_time = new_time;
        assert_eq!(
            super::reconciliation::terminal_responses_disagree(&old, &new),
            expected,
            "{name}"
        );
    }
}

#[tokio::test]
async fn missing_terminal_correction_time_cannot_discard_reliable_ordering() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.status = "FILLED".into();
    response.executed_qty = response.orig_qty;
    response.price = Decimal::ZERO;
    response.avg_price = None;
    response.update_time = Some(1000);
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    db.save_identity_observation(
        account,
        &order,
        &crate::types::ObservedOrderIdentity {
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            side: "BUY".into(),
            order_id: response.order_id,
        },
    )
    .unwrap();
    assert!(
        first
            .record_initial_order_response(account, "SOLUSDC", &order.client_order_id, &response)
            .await
    );
    drop(first);
    response.price = dec!(98);
    response.update_time = None;
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut restarted = engine(db.clone(), &url).await;
    let success = restarted.sync_live_orders().await;
    let trades = db.get_recent_trades(10).unwrap();
    let evidence = db
        .load_initial_order_responses(account, "SOLUSDC", &order.client_order_id)
        .unwrap();
    if success {
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price, dec!(98));
        assert!(evidence.iter().any(|found| found.update_time == Some(1000)));
    } else {
        assert!(restarted.reconciliation_blocked);
        assert!(trades.is_empty());
        assert!(
            evidence.iter().any(|found| found.update_time == Some(1000))
                || !db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap()
                    .is_empty()
        );
    }
    drop(restarted);
    let mut again = engine(db.clone(), &url).await;
    assert_eq!(again.sync_live_orders().await, success);
    assert_eq!(
        db.get_recent_trades(10).unwrap().len(),
        usize::from(success)
    );
    server.abort();
}
