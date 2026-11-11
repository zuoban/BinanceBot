use super::*;

#[tokio::test]
async fn duplicate_snapshot_responses_are_written_as_one_transaction() {
    let path = std::env::temp_dir().join(format!(
        "snapshot-evidence-batch-{}.db",
        uuid::Uuid::new_v4()
    ));
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
    let _ = std::fs::remove_file(path);
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
    let path = std::env::temp_dir().join(format!("snapshot-retry-{}.db", uuid::Uuid::new_v4()));
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
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn terminal_disagreement_queue_retries_failures_and_removes_successes() {
    let path = std::env::temp_dir().join(format!("terminal-retry-{}.db", uuid::Uuid::new_v4()));
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
    let _ = std::fs::remove_file(path);
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

#[tokio::test]
async fn duplicate_snapshot_details_must_agree_before_reconciliation() {
    for reversed in [false, true] {
        for mismatch in ["none", "status", "executed_qty", "orig_qty", "price"] {
            let db = database();
            let good = json!({"orderId":123,"clientOrderId":"gb_duplicate_details","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let bad = json!({"orderId":123,"clientOrderId":"gb_duplicate_details","symbol":"SOLUSDC",
                "status":if mismatch == "status" {"PARTIALLY_FILLED"} else {"NEW"},
                "price":if mismatch == "price" {"101"} else {"100"},
                "origQty":if mismatch == "orig_qty" {"3"} else {"2"},
                "executedQty":if mismatch == "status" || mismatch == "executed_qty" {"1"} else {"0"},
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let snapshot = if reversed {
                vec![bad, good]
            } else {
                vec![good, bad]
            };
            let app = Router::new()
                .route("/fapi/v1/openOrders", get(move || {
                    let snapshot = snapshot.clone();
                    async move { Json(snapshot) }
                }))
                .route("/fapi/v2/positionRisk", get(|| async {
                    Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"100","markPrice":"100",
                        "unRealizedProfit":"0","liquidationPrice":"0","leverage":"1"}]))
                }))
                .route("/fapi/v2/account", get(|| async {
                    Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                        "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                        "unrealizedProfit":"0","availableBalance":"1000"}]}))
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let order = GridOrder {
                client_order_id: "gb_duplicate_details".into(),
                order_id: Some(123),
                symbol: "SOLUSDC".into(),
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(2),
                amount_usdc: dec!(200),
                status: OrderStatus::New,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                grid_level: -1,
                paired_client_order_id: None,
                is_take_profit: false,
                purpose: crate::types::OrderPurpose::Grid,
                merge_sources: Vec::new(),
            };
            db.save_managed_order(&order).unwrap();
            let mut engine = engine(db.clone(), &url).await;
            assert_eq!(
                engine.sync_live_orders().await,
                mismatch == "none",
                "reversed={reversed}, mismatch={mismatch}"
            );
            assert_eq!(engine.reconciliation_blocked, mismatch != "none");
            if mismatch != "none" {
                assert!(engine.last_orders_sync.is_none());
                assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
            }
            let active = engine.state.active_orders.read().await;
            assert_eq!(active.len(), 1);
            let retained = &active[&order.client_order_id];
            assert_eq!(retained.order_id, order.order_id);
            assert_eq!(retained.quantity, order.quantity);
            assert_eq!(retained.price, order.price);
            assert_eq!(retained.status, order.status);
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].order_id, order.order_id);
            assert_eq!(saved[0].quantity, order.quantity);
            assert_eq!(saved[0].price, order.price);
            assert_eq!(saved[0].status, order.status);
            server.abort();
        }
    }
}

#[tokio::test]
async fn failed_managed_write_is_durably_recovered_when_next_snapshot_conflicts() {
    let path = std::env::temp_dir().join(format!("mixed-memory-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_discovered_order BEFORE INSERT ON managed_orders BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/openOrders", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            let mut orders = Vec::new();
            if stage.load(Ordering::SeqCst) == 0 {
                orders.push(json!({"orderId":99,"clientOrderId":"gb_memory_only","symbol":"SOLUSDC",
                    "status":"NEW","price":"95","origQty":"2","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}));
            } else {
                for id in [11, 12] {
                    orders.push(json!({"orderId":id,"clientOrderId":"gb_memory_conflict","symbol":"SOLUSDC",
                        "status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}));
                }
            }
            Json(orders)
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    assert_eq!(
        first.state.active_orders.read().await["gb_memory_only"].quantity,
        dec!(2)
    );
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    connection
        .execute_batch("DROP TRIGGER reject_discovered_order")
        .unwrap();
    stage.store(1, Ordering::SeqCst);
    assert!(!first.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert!(
        saved
            .iter()
            .any(|order| order.client_order_id == "gb_memory_only"
                && order.order_id == Some(99)
                && order.quantity == dec!(2)
                && order.price == dec!(95))
            || observations
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == "gb_memory_only"
                        && expected.quantity == dec!(2)
                        && expected.price == dec!(95)
                        && found.order_id == 99
                )
    );
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    let restored = &active["gb_memory_only"];
    assert_eq!(restored.order_id, Some(99));
    assert_eq!(restored.quantity, dec!(2));
    assert_eq!(restored.price, dec!(95));
    assert_eq!(restored.amount_usdc, dec!(190));
    drop(active);
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn managed_placeholder_does_not_hide_confirmed_memory_identity_on_restart() {
    let db = database();
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            Json(json!([
                {"orderId":11,"clientOrderId":"gb_placeholder_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":12,"clientOrderId":"gb_placeholder_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }))
        .route("/fapi/v1/order", get(|| async {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let order = GridOrder {
        client_order_id: "gb_placeholder_known".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(95),
        quantity: dec!(2),
        amount_usdc: dec!(190),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -3,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&order).unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let mut confirmed = order.clone();
    confirmed.order_id = Some(99);
    first
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), confirmed);
    assert!(!first.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert!(
        saved
            .iter()
            .any(|item| item.client_order_id == order.client_order_id && item.order_id == Some(99))
            || observations
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == order.client_order_id
                        && (expected.order_id == Some(99) || found.order_id == 99)
                )
    );
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    assert_eq!(active["gb_placeholder_known"].order_id, Some(99));
    assert_eq!(active["gb_placeholder_known"].quantity, dec!(2));
    server.abort();
}

#[tokio::test]
async fn memory_order_cannot_overwrite_existing_managed_identity_on_insert_if_absent() {
    for mismatch in ["symbol", "side", "order_id"] {
        let db = database();
        let app = Router::new().route("/fapi/v1/openOrders", get(|| async {
            Json(json!([
                {"orderId":11,"clientOrderId":"gb_collision_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":12,"clientOrderId":"gb_collision_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let durable = GridOrder {
            client_order_id: "gb_collision_memory".into(),
            order_id: Some(123),
            symbol: if mismatch == "symbol" {
                "BTCUSDC"
            } else {
                "SOLUSDC"
            }
            .into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -2,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&durable).unwrap();
        let mut engine = engine(db.clone(), &url).await;
        let mut memory = durable.clone();
        memory.symbol = "SOLUSDC".into();
        if mismatch == "side" {
            memory.side = OrderSide::Sell;
        }
        if mismatch == "order_id" {
            memory.order_id = Some(456);
        }
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(memory.client_order_id.clone(), memory);
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        assert!(engine.last_orders_sync.is_none());
        let saved = db.load_managed_orders(&durable.symbol).unwrap();
        let saved = saved
            .iter()
            .find(|item| item.client_order_id == durable.client_order_id)
            .unwrap();
        assert_eq!(saved.order_id, durable.order_id);
        assert_eq!(saved.symbol, durable.symbol);
        assert_eq!(saved.side, durable.side);
        assert_eq!(saved.grid_level, durable.grid_level);
        server.abort();
    }
}

#[tokio::test]
async fn second_lookup_partial_fill_survives_retry_and_restart() {
    for write_fails in [false, true] {
        let path =
            std::env::temp_dir().join(format!("second-response-write-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let partial_queries = Arc::new(AtomicU8::new(0));
        let partial_seen = Arc::new(AtomicBool::new(false));
        let partial_client_queries = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let stage = stage.clone();
                let partial_queries = partial_queries.clone();
                let partial_seen = partial_seen.clone();
                let partial_client_queries = partial_client_queries.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let stage = stage.clone();
                    let partial_queries = partial_queries.clone();
                    let partial_seen = partial_seen.clone();
                    let partial_client_queries = partial_client_queries.clone();
                    async move {
                        let by_client = query.contains_key("origClientOrderId");
                        let id = if by_client {456} else {query["orderId"].parse::<i64>().unwrap()};
                        let partial = stage.load(Ordering::SeqCst) == 0 && id == 456
                            && (!by_client || partial_seen.load(Ordering::SeqCst));
                        if partial && by_client {
                            partial_client_queries.fetch_add(1, Ordering::SeqCst);
                        } else if partial {
                            partial_seen.store(true, Ordering::SeqCst);
                            partial_queries.fetch_add(1, Ordering::SeqCst);
                        }
                        let status = if id == 123 {"CANCELED"} else if partial {"PARTIALLY_FILLED"}
                            else if stage.load(Ordering::SeqCst) == 0 {"NEW"} else {"CANCELED"};
                        Json(json!({"orderId":id,"clientOrderId":"gb_second_partial","symbol":"SOLUSDC",
                            "status":status,"price":"101","avgPrice":"100","origQty":"2",
                            "executedQty":if partial {"0.5"} else {"0"},
                            "updateTime":if partial {2000} else if status == "NEW" {1000} else {3000},
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_second_partial".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_second_partial_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_second_partial_parent".into(),
            client_order_id: "gb_second_partial_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        if write_fails {
            connection.execute_batch("CREATE TRIGGER reject_second_response BEFORE INSERT ON initial_order_responses WHEN NEW.response_json LIKE '%PARTIALLY_FILLED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        }
        assert!(!first.sync_live_orders().await);
        assert!(partial_queries.load(Ordering::SeqCst) > 0);
        let client_response = first
            .client
            .lookup_order_by_client_id("SOLUSDC", "gb_second_partial")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(client_response.status, "PARTIALLY_FILLED");
        assert_eq!(client_response.executed_qty, dec!(0.5));
        assert!(partial_client_queries.load(Ordering::SeqCst) > 0);
        assert!(first.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        if write_fails {
            assert!(!db
                .load_initial_order_responses(account, "SOLUSDC", &intent.client_order_id)
                .unwrap()
                .iter()
                .any(|response| response.status == "PARTIALLY_FILLED"));
            assert!(!first.sync_live_orders().await);
            drop(first);
            let mut failed_restart = engine(db.clone(), &url).await;
            assert!(!failed_restart.sync_live_orders().await);
            assert!(failed_restart.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            connection
                .execute_batch("DROP TRIGGER reject_second_response")
                .unwrap();
            assert!(!failed_restart.sync_live_orders().await);
            drop(failed_restart);
        } else {
            drop(first);
        }
        let responses = db
            .load_initial_order_responses(account, "SOLUSDC", &intent.client_order_id)
            .unwrap();
        assert!(responses.iter().any(|response| response.order_id == 456
            && response.status == "NEW"
            && response.executed_qty == dec!(0)
            && response.update_time == Some(1000)));
        assert!(responses.iter().any(|response| response.order_id == 456
            && response.status == "PARTIALLY_FILLED"
            && response.executed_qty == dec!(0.5)
            && response.update_time == Some(2000)));
        stage.store(1, Ordering::SeqCst);
        let mut restarted = engine(db.clone(), &url).await;
        for _ in 0..3 {
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        }
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        assert!(disagreements.iter().any(|(client, earlier, later)| client
            == &intent.client_order_id
            && earlier.order_id == 456
            && earlier.status == "PARTIALLY_FILLED"
            && earlier.executed_qty == dec!(0.5)
            && later.order_id == 456
            && later.status == "CANCELED"
            && later.executed_qty == dec!(0)));
        server.abort();
        drop(restarted);
        drop(connection);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn partial_client_response_survives_failed_id_lookup_and_restart() {
    for write_fails in [false, true] {
        let path = std::env::temp_dir().join(format!(
            "partial-response-write-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let failed_id_queries = Arc::new(AtomicU8::new(0));
        let saved_before_query = Arc::new(AtomicBool::new(false));
        let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            let failed_id_queries = failed_id_queries.clone();
            let saved_before_query = saved_before_query.clone();
            let path = path.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                let failed_id_queries = failed_id_queries.clone();
                let saved_before_query = saved_before_query.clone();
                let path = path.clone();
                async move {
                    let by_client = query.contains_key("origClientOrderId");
                    let id = if by_client {456} else {query["orderId"].parse::<i64>().unwrap()};
                    let initial = stage.load(Ordering::SeqCst) == 0;
                    if initial && !by_client && id == 456 {
                        failed_id_queries.fetch_add(1, Ordering::SeqCst);
                        let conn = rusqlite::Connection::open(&path).unwrap();
                        let count: i64 = conn.query_row("SELECT COUNT(*) FROM initial_order_responses", [], |row| row.get(0)).unwrap();
                        saved_before_query.store(count > 0, Ordering::SeqCst);
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                    }
                    let partial = initial && by_client;
                    (StatusCode::OK, Json(json!({"orderId":id,"clientOrderId":"gb_partial_interrupted",
                        "symbol":"SOLUSDC","status":if partial {"PARTIALLY_FILLED"} else {"CANCELED"},
                        "price":"101","avgPrice":"100","origQty":"2","executedQty":if partial {"0.5"} else {"0"},
                        "updateTime":if partial {1000} else {2000},"side":"SELL","type":"LIMIT","timeInForce":"GTX"})))
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_partial_interrupted".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_partial_interrupted_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_partial_interrupted_parent".into(),
            client_order_id: "gb_partial_interrupted_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        if write_fails {
            connection.execute_batch("CREATE TRIGGER reject_initial_response BEFORE INSERT ON initial_order_responses BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        }
        assert!(!first.sync_live_orders().await);
        if write_fails {
            assert_eq!(failed_id_queries.load(Ordering::SeqCst), 0);
            assert!(first.reconciliation_blocked);
            assert!(!first.sync_live_orders().await);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
            drop(first);
            let mut restarted = engine(db.clone(), &url).await;
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            assert_eq!(failed_id_queries.load(Ordering::SeqCst), 0);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
            server.abort();
            drop(restarted);
            drop(connection);
            drop(db);
            let _ = std::fs::remove_file(path);
            continue;
        }
        assert!(failed_id_queries.load(Ordering::SeqCst) > 0);
        assert!(saved_before_query.load(Ordering::SeqCst));
        assert!(first.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        stage.store(1, Ordering::SeqCst);
        for _ in 0..2 {
            assert!(!first.sync_live_orders().await);
            assert!(first.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        }
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        assert!(disagreements.iter().any(|(client, initial, later)| client
            == &intent.client_order_id
            && initial.order_id == 456
            && initial.status == "PARTIALLY_FILLED"
            && initial.executed_qty == dec!(0.5)
            && initial.update_time == Some(1000)
            && later.order_id == 456
            && later.status == "CANCELED"
            && later.executed_qty == dec!(0)
            && later.update_time == Some(2000)));
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        assert!(!db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
        drop(restarted);
        drop(connection);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn repeated_lookup_of_same_observed_id_preserves_disagreeing_terminal_responses() {
    let db = database();
    let queries = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let queries = queries.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let queries = queries.clone();
                async move {
                    assert_eq!(query.get("orderId").map(String::as_str), Some("456"));
                    let count = queries.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"orderId":456,"clientOrderId":"gb_repeated_observed_id","symbol":"SOLUSDC",
                        "status":"CANCELED","price":"100","avgPrice":"100","origQty":"2",
                        "executedQty":if count.is_multiple_of(2) {"0"} else {"0.5"},
                        "updateTime":1000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let expected = GridOrder {
        client_order_id: "gb_repeated_observed_id".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&expected).unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    for side in ["BUY", "SELL"] {
        db.save_identity_observation(
            account,
            &expected,
            &crate::types::ObservedOrderIdentity {
                client_order_id: expected.client_order_id.clone(),
                symbol: expected.symbol.clone(),
                side: side.into(),
                order_id: 456,
            },
        )
        .unwrap();
    }
    for _ in 0..3 {
        assert!(!first.sync_live_orders().await);
        assert!(first.reconciliation_blocked);
        assert_eq!(
            first.state.active_orders.read().await[&expected.client_order_id].order_id,
            Some(123)
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
        assert!(db.get_recent_trades(10).unwrap().is_empty());
    }
    assert!(queries.load(Ordering::SeqCst) >= 2);
    let disagreements = db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap();
    assert!(disagreements.iter().any(|(client, a, b)| {
        client == &expected.client_order_id
            && a.order_id == 456
            && b.order_id == 456
            && a.status == "CANCELED"
            && b.status == "CANCELED"
            && a.side == "BUY"
            && b.side == "BUY"
            && ((a.executed_qty == dec!(0) && b.executed_qty == dec!(0.5))
                || (a.executed_qty == dec!(0.5) && b.executed_qty == dec!(0)))
    }));
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await[&expected.client_order_id].order_id,
        Some(123)
    );
    assert!(!db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}
