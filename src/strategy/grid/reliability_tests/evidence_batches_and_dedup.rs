use super::*;

#[tokio::test]
async fn duplicate_snapshot_responses_are_written_as_one_transaction() {
    let dir = crate::test_support::TempTestDir::new("snapshot-evidence-batch");
    let path = dir.join("snapshot-evidence-batch.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_second_snapshot BEFORE INSERT ON initial_order_responses WHEN NEW.response_json LIKE '%PARTIALLY_FILLED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let app = Router::new().route("/fapi/v1/openOrders", get(|| async {
        Json(json!([
            {"orderId":123,"clientOrderId":"gb_snapshot_batch","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"2","executedQty":"0","updateTime":1000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"},
            {"orderId":123,"clientOrderId":"gb_snapshot_batch","symbol":"SOLUSDC","status":"PARTIALLY_FILLED","price":"100","origQty":"2","executedQty":"0.5","updateTime":2000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}
        ]))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let key = (account, "gb_snapshot_batch".to_string());
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    assert!(db
        .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_batch")
        .unwrap()
        .is_empty());
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    assert!(first.evidence.has_conflict(key.0, &key.1));
    assert!(first
        .evidence
        .get_observations(key.0, &key.1)
        .unwrap()
        .iter()
        .any(|observed| observed.order_id == 123));
    assert_eq!(
        first.state.active_orders.read().await["gb_snapshot_batch"].quantity,
        dec!(2)
    );
    assert!(!first.sync_live_orders().await);
    assert!(db
        .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_batch")
        .unwrap()
        .is_empty());
    assert!(first.reconciliation_blocked);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    assert!(first.evidence.has_conflict(key.0, &key.1));
    assert!(first
        .evidence
        .get_observations(key.0, &key.1)
        .unwrap()
        .iter()
        .any(|observed| observed.order_id == 123));
    connection
        .execute_batch("DROP TRIGGER reject_second_snapshot")
        .unwrap();
    assert!(!first.sync_live_orders().await);
    let responses = db
        .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_batch")
        .unwrap();
    assert!(responses
        .iter()
        .any(|response| response.status == "NEW" && response.executed_qty == dec!(0)));
    assert!(responses.iter().any(
        |response| response.status == "PARTIALLY_FILLED" && response.executed_qty == dec!(0.5)
    ));
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_snapshot_batch"
                && observed.order_id == 123
        ));
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await["gb_snapshot_batch"].quantity,
        dec!(2)
    );
    let responses = db
        .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_batch")
        .unwrap();
    assert!(responses.iter().any(|response| response.status == "NEW"));
    assert!(responses.iter().any(
        |response| response.status == "PARTIALLY_FILLED" && response.executed_qty == dec!(0.5)
    ));
    assert!(restarted.evidence.has_conflict(key.0, &key.1));
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_snapshot_batch"
                && observed.order_id == 123
        ));
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
}

fn evidence_test_response(
    status: &str,
    executed_qty: Decimal,
    update_time: i64,
) -> BinanceOrderResponse {
    BinanceOrderResponse {
        order_id: 123,
        client_order_id: "gb_evidence_timeline".into(),
        symbol: "SOLUSDC".into(),
        status: status.into(),
        price: dec!(100),
        avg_price: None,
        orig_qty: dec!(2),
        executed_qty,
        side: "BUY".into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: Some(update_time),
    }
}

#[tokio::test]
async fn consecutive_snapshot_evidence_is_deduplicated_without_losing_timeline() {
    let db = database();
    let (url, server) = exchange_server(Exchange::default()).await;
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let client = "gb_evidence_timeline";
    let new = evidence_test_response("NEW", dec!(0), 1000);
    let partial = evidence_test_response("PARTIALLY_FILLED", dec!(0.5), 2000);
    let snapshot = vec![
        (client.to_string(), new.clone()),
        (client.to_string(), partial.clone()),
    ];
    let changed = vec![
        (client.to_string(), partial.clone()),
        (client.to_string(), new.clone()),
    ];

    assert!(
        first
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:first",
                &snapshot,
                &[]
            )
            .await
    );
    for id in ["snapshot:repeat1", "snapshot:repeat2"] {
        assert!(
            first
                .record_initial_order_response_snapshot(account, "SOLUSDC", id, &snapshot, &[])
                .await
        );
    }
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    assert_eq!(timeline.len(), 2);
    assert!(timeline.iter().all(|(batch, _)| batch == "snapshot:first"));
    drop(first);

    let mut restarted = engine(db.clone(), &url).await;
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:restart",
                &snapshot,
                &[]
            )
            .await
    );
    assert_eq!(
        db.load_initial_order_response_timeline(account, "SOLUSDC", client)
            .unwrap()
            .len(),
        2
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:after-query",
                &snapshot,
                &[]
            )
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &partial)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:after-second-query",
                &snapshot,
                &[]
            )
            .await
    );
    drop(restarted);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:second-restart",
                &snapshot,
                &[]
            )
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &partial)
            .await
    );
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    let statuses: Vec<_> = timeline
        .iter()
        .map(|(_, response)| response.status.as_str())
        .collect();
    assert_eq!(
        statuses,
        ["NEW", "PARTIALLY_FILLED", "NEW", "PARTIALLY_FILLED"]
    );
    assert_eq!(timeline[0].0, "snapshot:first");
    assert_eq!(timeline[1].0, "snapshot:first");
    assert!(!timeline[2].0.starts_with("snapshot:"));
    assert!(!timeline[3].0.starts_with("snapshot:"));
    assert_ne!(timeline[2].0, timeline[3].0);
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:changed",
                &changed,
                &[]
            )
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:after-change",
                &snapshot,
                &[]
            )
            .await
    );
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    let statuses: Vec<_> = timeline
        .iter()
        .map(|(_, response)| response.status.as_str())
        .collect();
    assert_eq!(
        statuses,
        [
            "NEW",
            "PARTIALLY_FILLED",
            "NEW",
            "PARTIALLY_FILLED",
            "NEW",
            "PARTIALLY_FILLED",
            "NEW",
            "NEW",
            "NEW",
            "PARTIALLY_FILLED"
        ]
    );
    assert!(!timeline[4].0.starts_with("snapshot:"));
    assert!(!timeline[7].0.starts_with("snapshot:"));
    for (index, batch) in [(5, "snapshot:changed"), (8, "snapshot:after-change")] {
        assert_eq!(timeline[index].0, batch);
        assert_eq!(timeline[index + 1].0, batch);
    }
    server.abort();
}

#[tokio::test]
async fn interleaved_lookup_evidence_is_deduplicated_per_observed_identity() {
    let db = database();
    let (url, server) = exchange_server(Exchange::default()).await;
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let client = "gb_evidence_timeline";
    let a = evidence_test_response("NEW", dec!(0), 1000);
    let mut b = a.clone();
    b.order_id = 456;
    let mut changed_a = a.clone();
    changed_a.status = "PARTIALLY_FILLED".into();
    changed_a.executed_qty = dec!(0.5);
    changed_a.update_time = Some(2000);
    let snapshot = vec![
        (client.to_string(), a.clone()),
        (client.to_string(), b.clone()),
    ];

    assert!(
        first
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:identities",
                &snapshot,
                &[]
            )
            .await
    );
    for (index, response) in [&a, &b, &a, &b].iter().enumerate() {
        assert!(
            first
                .record_initial_order_response(account, "SOLUSDC", client, response)
                .await
        );
        assert!(
            first
                .record_initial_order_response_snapshot(
                    account,
                    "SOLUSDC",
                    &format!("snapshot:identity-repeat-{index}"),
                    &snapshot,
                    &[]
                )
                .await
        );
    }
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    assert_eq!(timeline.len(), 4);
    assert_eq!(
        timeline
            .iter()
            .map(|(_, response)| response.order_id)
            .collect::<Vec<_>>(),
        [123, 456, 123, 456]
    );
    assert!(!timeline[2].0.starts_with("snapshot:"));
    assert!(!timeline[3].0.starts_with("snapshot:"));
    drop(first);

    let mut restarted = engine(db.clone(), &url).await;
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:identity-restart",
                &snapshot,
                &[]
            )
            .await
    );
    for response in [&a, &b] {
        assert!(
            restarted
                .record_initial_order_response(account, "SOLUSDC", client, response)
                .await
        );
    }
    assert_eq!(
        db.load_initial_order_response_timeline(account, "SOLUSDC", client)
            .unwrap()
            .len(),
        4
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &changed_a)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &b)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &a)
            .await
    );
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    assert_eq!(timeline.len(), 6);
    assert_eq!(timeline[4].1.order_id, 123);
    assert_eq!(timeline[4].1.status, "PARTIALLY_FILLED");
    assert_eq!(timeline[5].1.order_id, 123);
    assert_eq!(timeline[5].1.status, "NEW");
    assert!(!timeline[4].0.starts_with("snapshot:"));
    assert!(!timeline[5].0.starts_with("snapshot:"));
    server.abort();
}

#[tokio::test]
async fn failed_snapshot_batch_retries_atomically_and_only_once() {
    let dir = crate::test_support::TempTestDir::new("snapshot-retry");
    let path = dir.join("snapshot-retry.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_partial_retry BEFORE INSERT ON initial_order_responses WHEN NEW.response_json LIKE '%PARTIALLY_FILLED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let (url, server) = exchange_server(Exchange::default()).await;
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let client = "gb_evidence_timeline";
    let snapshot = vec![
        (
            client.to_string(),
            evidence_test_response("NEW", dec!(0), 1000),
        ),
        (
            client.to_string(),
            evidence_test_response("PARTIALLY_FILLED", dec!(0.5), 2000),
        ),
    ];
    for _ in 0..2 {
        assert!(
            !first
                .record_initial_order_response_snapshot(
                    account,
                    "SOLUSDC",
                    "snapshot:retry",
                    &snapshot,
                    &[]
                )
                .await
        );
        assert_eq!(
            first.evidence.initial_order_responses[&account]["SOLUSDC"].len(),
            1
        );
        assert!(db
            .load_initial_order_response_timeline(account, "SOLUSDC", client)
            .unwrap()
            .is_empty());
    }
    connection
        .execute_batch("DROP TRIGGER reject_partial_retry")
        .unwrap();
    assert!(
        first
            .flush_initial_order_responses(account, "SOLUSDC")
            .await
    );
    assert!(!first
        .evidence
        .has_pending_initial_responses(account, "SOLUSDC"));
    assert!(
        first
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:next",
                &snapshot,
                &[]
            )
            .await
    );
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    assert_eq!(timeline.len(), 2);
    assert_eq!(timeline[0].1.status, "NEW");
    assert_eq!(timeline[1].1.status, "PARTIALLY_FILLED");
    server.abort();
    drop(first);
    drop(connection);
    drop(db);
}

#[tokio::test]
async fn terminal_disagreement_queue_retries_failures_and_removes_successes() {
    let dir = crate::test_support::TempTestDir::new("terminal-retry");
    let path = dir.join("terminal-retry.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_terminal_retry BEFORE INSERT ON terminal_order_disagreements BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let (url, server) = exchange_server(Exchange::default()).await;
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let client = "gb_evidence_timeline";
    let filled = evidence_test_response("FILLED", dec!(2), 2000);
    let canceled = evidence_test_response("CANCELED", dec!(0), 3000);
    let expired = evidence_test_response("EXPIRED", dec!(0), 4000);
    assert!(
        !first
            .record_terminal_disagreement(account, "SOLUSDC", client, &filled, &canceled)
            .await
    );
    assert!(
        !first
            .record_terminal_disagreement(account, "SOLUSDC", client, &filled, &expired)
            .await
    );
    assert_eq!(
        first.evidence.terminal_order_disagreements[&account]["SOLUSDC"][client].len(),
        2
    );
    assert!(
        first
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert_eq!(
        first.evidence.terminal_order_disagreements[&account]["SOLUSDC"][client].len(),
        2
    );
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());

    connection.execute_batch("DROP TRIGGER reject_terminal_retry; CREATE TRIGGER reject_expired_retry BEFORE INSERT ON terminal_order_disagreements WHEN NEW.conflicting_json LIKE '%EXPIRED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    assert!(
        first
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert_eq!(
        first.evidence.terminal_order_disagreements[&account]["SOLUSDC"][client].len(),
        1
    );
    assert_eq!(
        first.evidence.terminal_order_disagreements[&account]["SOLUSDC"][client][0]
            .1
            .status,
        "EXPIRED"
    );
    assert_eq!(
        db.load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .len(),
        1
    );
    connection
        .execute_batch("DROP TRIGGER reject_expired_retry")
        .unwrap();
    assert!(
        first
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert!(!first
        .evidence
        .has_terminal_disagreements(account, "SOLUSDC"));
    assert_eq!(
        db.load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .len(),
        2
    );
    assert!(
        first
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert!(!first
        .evidence
        .has_terminal_disagreements(account, "SOLUSDC"));
    assert!(
        first
            .record_terminal_disagreement(account, "SOLUSDC", client, &canceled, &expired)
            .await
    );
    assert!(!first
        .evidence
        .has_terminal_disagreements(account, "SOLUSDC"));
    assert_eq!(
        db.load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .len(),
        3
    );
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(
        restarted
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        db.load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .len(),
        3
    );
    server.abort();
    drop(restarted);
    drop(connection);
    drop(db);
}

#[tokio::test]
async fn duplicate_snapshot_fill_evidence_survives_restart_and_preserves_order() {
    for case in ["partial", "filled_zero", "repeated_new"] {
        let db = database();
        let stage = Arc::new(AtomicU8::new(0));
        let snapshot = match case {
            "partial" => vec![("NEW", "0", 1000), ("PARTIALLY_FILLED", "0.5", 2000)],
            "filled_zero" => vec![("PARTIALLY_FILLED", "0.5", 2000), ("FILLED", "0", 3000)],
            _ => vec![("NEW", "0", 1000), ("PARTIALLY_FILLED", "0.5", 2000), ("NEW", "0", 1000)],
        }.into_iter().map(|(status, quantity, time)| json!({"orderId":456,"clientOrderId":"gb_snapshot_evidence",
            "symbol":"SOLUSDC","status":status,"price":"100","avgPrice":"100","origQty":"2",
            "executedQty":quantity,"updateTime":time,"side":"BUY","type":"LIMIT","timeInForce":"GTX"})).collect::<Vec<_>>();
        let app = Router::new()
            .route("/fapi/v1/openOrders", get({
                let stage = stage.clone();
                move || { let stage = stage.clone(); let snapshot = snapshot.clone(); async move {
                    Json(if stage.load(Ordering::SeqCst) == 0 {snapshot} else {Vec::new()})
                }}
            }))
            .route("/fapi/v1/order", get(|| async {
                Json(json!({"orderId":456,"clientOrderId":"gb_snapshot_evidence","symbol":"SOLUSDC",
                    "status":"CANCELED","price":"100","avgPrice":"100","origQty":"2",
                    "executedQty":"0","updateTime":3000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        assert!(!first.sync_live_orders().await, "case={case}");
        assert!(first.reconciliation_blocked);
        let responses = db
            .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_evidence")
            .unwrap();
        assert!(responses.iter().any(|response| response.order_id == 456
            && response.status == "PARTIALLY_FILLED"
            && response.executed_qty == dec!(0.5)
            && response.update_time == Some(2000)));
        if case == "partial" {
            assert!(responses
                .iter()
                .any(|response| response.status == "NEW" && response.executed_qty == dec!(0)));
        } else if case == "repeated_new" {
            let statuses: Vec<_> = responses
                .iter()
                .map(|response| response.status.as_str())
                .collect();
            assert_eq!(statuses, ["NEW", "PARTIALLY_FILLED", "NEW"]);
        } else {
            assert!(db
                .load_terminal_order_disagreements(account, "SOLUSDC")
                .unwrap()
                .is_empty());
        }
        drop(first);
        stage.store(1, Ordering::SeqCst);
        let mut restarted = engine(db.clone(), &url).await;
        for _ in 0..3 {
            assert!(!restarted.sync_live_orders().await, "case={case}");
            assert!(restarted.reconciliation_blocked);
            assert!(restarted
                .state
                .active_orders
                .read()
                .await
                .contains_key("gb_snapshot_evidence"));
            assert!(db.get_recent_trades(10).unwrap().is_empty());
        }
        let responses = db
            .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_evidence")
            .unwrap();
        assert!(responses
            .iter()
            .any(|response| response.status == "PARTIALLY_FILLED"
                && response.executed_qty == dec!(0.5)));
        assert!(!db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
    }
}
