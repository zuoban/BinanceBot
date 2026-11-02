use super::BinanceFuturesClient;
use crate::config::AppConfig;
use axum::{extract::Query, http::StatusCode, routing::get, Json, Router};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;

const SYMBOL: &str = "BTCUSDT";
const ORDER_ID: i64 = 42;

type Request = HashMap<String, String>;
type Reply = (StatusCode, Value);

struct Exchange {
    client: BinanceFuturesClient,
    requests: Arc<Mutex<Vec<Request>>>,
    server: JoinHandle<()>,
}

impl Drop for Exchange {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Exchange {
    async fn new(replies: Vec<Reply>) -> Self {
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/fapi/v1/userTrades",
            get({
                let replies = replies.clone();
                let requests = requests.clone();
                move |Query(query): Query<Request>| {
                    requests.lock().unwrap().push(query);
                    let reply = replies.lock().unwrap().pop_front().unwrap_or_else(|| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"msg": "unexpected request"}),
                        )
                    });
                    async move { (reply.0, Json(reply.1)) }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        Self {
            client,
            requests,
            server,
        }
    }

    async fn fetch(&self, expected: Decimal) -> anyhow::Result<Vec<super::BinanceUserTrade>> {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.client.get_user_trades(SYMBOL, ORDER_ID, expected, 0),
        )
        .await
        .expect("trade pagination did not terminate")
    }

    fn assert_cursors(&self, expected: &[Option<&str>]) {
        let requests = self.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            expected.len(),
            "unexpected number of requests: {requests:?}"
        );
        for (request, cursor) in requests.iter().zip(expected) {
            assert_eq!(request.get("symbol").map(String::as_str), Some(SYMBOL));
            assert_eq!(request.get("orderId").map(String::as_str), Some("42"));
            assert_eq!(request.get("limit").map(String::as_str), Some("1000"));
            assert_eq!(request.get("fromId").map(String::as_str), *cursor);
        }
    }
}

fn fill(id: i64) -> Value {
    json!({"id": id, "orderId": ORDER_ID, "qty": "1", "realizedPnl": "0.25",
           "commission": "0.01", "commissionAsset": "USDT", "maker": true})
}

fn page(rows: Vec<Value>) -> Reply {
    (StatusCode::OK, Value::Array(rows))
}

fn assert_totals(trades: &[super::BinanceUserTrade], count: usize, pnl: Decimal, fee: Decimal) {
    assert_eq!(trades.len(), count);
    assert_eq!(
        trades
            .iter()
            .map(|trade| trade.realized_pnl)
            .sum::<Decimal>(),
        pnl
    );
    assert_eq!(
        trades.iter().map(|trade| trade.commission).sum::<Decimal>(),
        fee
    );
    assert!(trades
        .iter()
        .all(|trade| trade.commission_asset == "USDT" && trade.maker));
}

#[tokio::test]
async fn exactly_one_thousand_fills_complete_on_default_page() {
    let exchange = Exchange::new(vec![page((1..=1000).map(fill).collect())]).await;
    let trades = exchange.fetch(dec!(1000)).await.unwrap();
    assert_totals(&trades, 1000, dec!(250), dec!(10));
    exchange.assert_cursors(&[None]);
}

#[tokio::test]
async fn incomplete_default_page_restarts_and_advances_from_page_maximum() {
    let exchange = Exchange::new(vec![
        page((2001..=3000).map(fill).collect()),
        // Deliberately unordered: the cursor must use the maximum, not the last ID.
        page((1..=1000).rev().map(fill).collect()),
        page((1001..=2000).map(fill).collect()),
    ])
    .await;
    let trades = exchange.fetch(dec!(3000)).await.unwrap();
    assert_totals(&trades, 3000, dec!(750), dec!(30));
    exchange.assert_cursors(&[None, Some("0"), Some("1001")]);
}

#[tokio::test]
async fn page_budget_preserves_progress_for_next_call() {
    let exchange = Exchange::new(vec![
        page((2501..=3500).map(fill).collect()),
        page((1..=1000).map(fill).collect()),
        page((1001..=2000).map(fill).collect()),
        // The final page overlaps the default page; those fills must not be counted twice.
        page((2001..=3000).map(fill).collect()),
    ])
    .await;

    let error = exchange.fetch(dec!(3500)).await.unwrap_err();
    assert!(error.to_string().contains("will continue later"));
    exchange.assert_cursors(&[None, Some("0"), Some("1001")]);

    let trades = exchange.fetch(dec!(3500)).await.unwrap();
    assert_totals(&trades, 3500, dec!(875), dec!(35));
    exchange.assert_cursors(&[None, Some("0"), Some("1001"), Some("2001")]);
}

#[tokio::test]
async fn later_page_error_preserves_cursor_and_fills_for_retry() {
    let exchange = Exchange::new(vec![
        page((2001..=3000).map(fill).collect()),
        page((1..=1000).map(fill).collect()),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"msg": "temporary error"}),
        ),
        page((1001..=2000).map(fill).collect()),
    ])
    .await;

    let error = exchange.fetch(dec!(3000)).await.unwrap_err();
    assert!(error.to_string().contains("500"));
    exchange.assert_cursors(&[None, Some("0"), Some("1001")]);

    let trades = exchange.fetch(dec!(3000)).await.unwrap();
    assert_totals(&trades, 3000, dec!(750), dec!(30));
    exchange.assert_cursors(&[None, Some("0"), Some("1001"), Some("1001")]);
}

#[tokio::test]
async fn duplicate_fills_do_not_inflate_quantity_or_financial_totals() {
    let exchange = Exchange::new(vec![
        page(vec![fill(2), fill(2)]),
        page(vec![fill(1), fill(2), fill(3)]),
    ])
    .await;
    let trades = exchange.fetch(dec!(3)).await.unwrap();
    assert_totals(&trades, 3, dec!(0.75), dec!(0.03));
    exchange.assert_cursors(&[None, Some("0")]);
}

#[tokio::test]
async fn missing_or_invalid_fill_fields_are_errors() {
    for field in [
        "id",
        "orderId",
        "qty",
        "realizedPnl",
        "commission",
        "commissionAsset",
        "maker",
    ] {
        let mut row = fill(1);
        row.as_object_mut().unwrap().remove(field);
        let exchange = Exchange::new(vec![page(vec![row])]).await;
        assert!(
            exchange.fetch(dec!(1)).await.is_err(),
            "missing {field} must fail"
        );
    }
    for (field, value) in [
        ("id", json!(-1)),
        ("orderId", json!(99)),
        ("qty", json!("0")),
        ("qty", json!("not-a-number")),
        ("realizedPnl", json!("invalid")),
        ("commission", json!("invalid")),
        ("maker", json!("yes")),
    ] {
        let mut row = fill(1);
        row[field] = value;
        let exchange = Exchange::new(vec![page(vec![row])]).await;
        assert!(
            exchange.fetch(dec!(1)).await.is_err(),
            "invalid {field} must fail"
        );
    }
}

#[tokio::test]
async fn conflicting_duplicate_and_exchange_error_are_rejected() {
    let mut conflicting = fill(1);
    conflicting["commission"] = json!("0.50");
    let exchange = Exchange::new(vec![page(vec![fill(1), conflicting])]).await;
    assert!(exchange.fetch(dec!(2)).await.is_err());

    let exchange = Exchange::new(vec![(
        StatusCode::BAD_REQUEST,
        json!({"code": -1121, "msg": "bad symbol"}),
    )])
    .await;
    assert!(exchange.fetch(dec!(1)).await.is_err());
}

#[tokio::test]
async fn insufficient_quantity_is_an_error_after_history_is_exhausted() {
    let exchange =
        Exchange::new(vec![page(vec![fill(1)]), page(vec![fill(1)]), page(vec![])]).await;
    assert!(exchange.fetch(dec!(2)).await.is_err());
    let requests = exchange.requests.lock().unwrap();
    assert!(requests.len() >= 2);
    assert_eq!(requests[0].get("fromId"), None);
    assert_eq!(requests[1].get("fromId").map(String::as_str), Some("0"));
}

#[tokio::test]
async fn incomplete_orders_do_not_block_a_new_order() {
    let mut replies = Vec::new();
    for order_id in 100..105 {
        let mut trade = fill(order_id);
        trade["orderId"] = json!(order_id);
        replies.push(page(vec![trade]));
        replies.push(page(vec![]));
    }
    let mut new_trade = fill(200);
    new_trade["orderId"] = json!(200);
    replies.push(page(vec![new_trade]));
    let exchange = Exchange::new(replies).await;

    for order_id in 100..105 {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            exchange
                .client
                .get_user_trades(SYMBOL, order_id, dec!(2), 0),
        )
        .await
        .expect("trade pagination did not terminate");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Incomplete trades"));
    }
    let trades = tokio::time::timeout(
        Duration::from_secs(5),
        exchange.client.get_user_trades(SYMBOL, 200, dec!(1), 0),
    )
    .await
    .expect("trade pagination did not terminate")
    .unwrap();
    assert_totals(&trades, 1, dec!(0.25), dec!(0.01));

    let requests = exchange.requests.lock().unwrap();
    assert_eq!(requests.len(), 11);
    for (index, request) in requests.iter().enumerate() {
        let order_id = if index < 10 { 100 + index / 2 } else { 200 };
        assert_eq!(request.get("orderId"), Some(&order_id.to_string()));
        assert_eq!(
            request.get("fromId").map(String::as_str),
            if index < 10 && index % 2 == 1 {
                Some("0")
            } else {
                None
            }
        );
    }
}

#[tokio::test]
async fn initial_request_failures_do_not_block_a_new_order() {
    let mut replies = vec![
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"msg": "temporary error"})
        );
        8
    ];
    let mut new_trade = fill(200);
    new_trade["orderId"] = json!(200);
    replies.push(page(vec![new_trade]));
    let exchange = Exchange::new(replies).await;

    for order_id in 100..108 {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            exchange
                .client
                .get_user_trades(SYMBOL, order_id, dec!(1), 0),
        )
        .await
        .expect("trade pagination did not terminate");
        assert!(result.unwrap_err().to_string().contains("500"));
    }
    let trades = tokio::time::timeout(
        Duration::from_secs(5),
        exchange.client.get_user_trades(SYMBOL, 200, dec!(1), 0),
    )
    .await
    .expect("trade pagination did not terminate")
    .unwrap();
    assert_totals(&trades, 1, dec!(0.25), dec!(0.01));

    let requests = exchange.requests.lock().unwrap();
    assert_eq!(requests.len(), 9);
    for (index, request) in requests.iter().enumerate() {
        let order_id = if index < 8 { 100 + index } else { 200 };
        assert_eq!(request.get("orderId"), Some(&order_id.to_string()));
        assert!(!request.contains_key("fromId"));
    }
}

#[tokio::test]
async fn full_progress_slots_queue_new_order_until_a_slot_is_free() {
    let requests = Arc::new(Mutex::new(Vec::<Request>::new()));
    let gates = Arc::new(
        (0..8)
            .map(|_| tokio::sync::Semaphore::new(0))
            .collect::<Vec<_>>(),
    );
    let (arrived_tx, mut arrived_rx) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new().route(
        "/fapi/v1/userTrades",
        get({
            let requests = Arc::clone(&requests);
            let gates = Arc::clone(&gates);
            move |Query(query): Query<Request>| {
                let requests = Arc::clone(&requests);
                let gates = Arc::clone(&gates);
                let arrived_tx = arrived_tx.clone();
                async move {
                    let order_id: i64 = query["orderId"].parse().unwrap();
                    requests.lock().unwrap().push(query.clone());
                    arrived_tx.send(order_id).unwrap();
                    let rows = if (100..108).contains(&order_id) {
                        gates[(order_id - 100) as usize]
                            .acquire()
                            .await
                            .unwrap()
                            .forget();
                        let mut trade = fill(order_id);
                        trade["orderId"] = json!(order_id);
                        vec![trade]
                    } else {
                        assert_eq!(order_id, 200);
                        let ids = match query.get("fromId").map(String::as_str) {
                            None => 3001..=4000,
                            Some("0") => 1..=1000,
                            Some("1001") => 1001..=2000,
                            Some("2001") => 2001..=3000,
                            cursor => panic!("unexpected cursor: {cursor:?}"),
                        };
                        ids.map(|id| {
                            let mut trade = fill(id);
                            trade["orderId"] = json!(order_id);
                            trade
                        })
                        .collect()
                    };
                    Json(rows)
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = Arc::new(BinanceFuturesClient::with_base_url(
        &AppConfig::default().exchange,
        url,
    ));

    let mut occupying = Vec::new();
    for order_id in 100..108 {
        let client = Arc::clone(&client);
        occupying.push(tokio::spawn(async move {
            client.get_user_trades(SYMBOL, order_id, dec!(1), 0).await
        }));
    }
    for _ in 0..8 {
        tokio::time::timeout(Duration::from_secs(5), arrived_rx.recv())
            .await
            .expect("occupying request did not arrive")
            .expect("server stopped");
    }

    for _ in 0..3 {
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            client.get_user_trades(SYMBOL, 200, dec!(4000), 0),
        )
        .await
        .expect("waiting order did not return")
        .unwrap_err();
        assert!(error.to_string().contains("waiting"));
        assert_eq!(requests.lock().unwrap().len(), 8);
    }

    let error = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_user_trades(SYMBOL, 201, dec!(1), 0),
    )
    .await
    .expect("second waiting order did not return")
    .unwrap_err();
    assert!(error.to_string().contains("waiting"));
    assert_eq!(requests.lock().unwrap().len(), 8);

    gates[0].add_permits(1);
    let completed = tokio::time::timeout(Duration::from_secs(5), occupying.remove(0))
        .await
        .expect("occupying order did not complete")
        .unwrap()
        .unwrap();
    assert_totals(&completed, 1, dec!(0.25), dec!(0.01));

    let error = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_user_trades(SYMBOL, 201, dec!(1), 0),
    )
    .await
    .expect("second waiting order did not return")
    .unwrap_err();
    assert!(error.to_string().contains("waiting"));
    assert_eq!(requests.lock().unwrap().len(), 8);

    let error = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_user_trades(SYMBOL, 200, dec!(4000), 0),
    )
    .await
    .expect("admitted order did not return")
    .unwrap_err();
    assert!(error.to_string().contains("will continue later"));
    let trades = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_user_trades(SYMBOL, 200, dec!(4000), 0),
    )
    .await
    .expect("admitted order did not resume")
    .unwrap();
    assert_totals(&trades, 4000, dec!(1000), dec!(40));

    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 12);
    let mut initial_orders = recorded[..8]
        .iter()
        .map(|request| request["orderId"].parse::<i64>().unwrap())
        .collect::<Vec<_>>();
    initial_orders.sort_unstable();
    assert_eq!(initial_orders, (100..108).collect::<Vec<_>>());
    for (request, cursor) in recorded[8..]
        .iter()
        .zip([None, Some("0"), Some("1001"), Some("2001")])
    {
        assert_eq!(request.get("symbol").map(String::as_str), Some(SYMBOL));
        assert_eq!(request.get("orderId").map(String::as_str), Some("200"));
        assert_eq!(request.get("limit").map(String::as_str), Some("1000"));
        assert_eq!(request.get("fromId").map(String::as_str), cursor);
    }
    drop(recorded);
    for task in occupying {
        task.abort();
    }
    server.abort();
}

#[tokio::test]
async fn cleared_generation_rejects_an_in_flight_http_response() {
    let (arrived_tx, mut arrived_rx) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let requests = Arc::new(Mutex::new(Vec::<Request>::new()));
    let app = Router::new().route(
        "/fapi/v1/userTrades",
        get({
            let gate = Arc::clone(&gate);
            let requests = Arc::clone(&requests);
            move |Query(query): Query<Request>| {
                let gate = Arc::clone(&gate);
                let requests = Arc::clone(&requests);
                let arrived_tx = arrived_tx.clone();
                async move {
                    let first = {
                        let mut requests = requests.lock().unwrap();
                        requests.push(query);
                        requests.len() == 1
                    };
                    if first {
                        arrived_tx.send(()).unwrap();
                        gate.acquire().await.unwrap().forget();
                    }
                    let mut trade = fill(if first { 1 } else { 2 });
                    if first {
                        trade["realizedPnl"] = json!("99");
                    }
                    Json(vec![trade])
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = Arc::new(BinanceFuturesClient::with_base_url(
        &AppConfig::default().exchange,
        url,
    ));

    let old = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.get_user_trades(SYMBOL, ORDER_ID, dec!(1), 0).await })
    };
    tokio::time::timeout(Duration::from_secs(5), arrived_rx.recv())
        .await
        .expect("old request did not reach the server")
        .expect("server stopped");
    let trades = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_user_trades(SYMBOL, ORDER_ID, dec!(1), 1),
    )
    .await
    .expect("new generation did not complete")
    .unwrap();
    assert_totals(&trades, 1, dec!(0.25), dec!(0.01));

    gate.add_permits(1);
    let error = tokio::time::timeout(Duration::from_secs(5), old)
        .await
        .expect("old request did not complete")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("cleared"));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| {
        request.get("symbol").map(String::as_str) == Some(SYMBOL)
            && request.get("orderId").map(String::as_str) == Some("42")
            && !request.contains_key("fromId")
    }));
    server.abort();
}

#[tokio::test]
async fn stale_generation_cannot_recreate_a_cleared_waiting_queue() {
    let (arrived_tx, mut arrived_rx) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let requests = Arc::new(Mutex::new(Vec::<Request>::new()));
    let app = Router::new().route(
        "/fapi/v1/userTrades",
        get({
            let gate = Arc::clone(&gate);
            let requests = Arc::clone(&requests);
            move |Query(query): Query<Request>| {
                let gate = Arc::clone(&gate);
                let requests = Arc::clone(&requests);
                let arrived_tx = arrived_tx.clone();
                async move {
                    let order_id: i64 = query["orderId"].parse().unwrap();
                    requests.lock().unwrap().push(query);
                    if (100..108).contains(&order_id) {
                        arrived_tx.send(order_id).unwrap();
                        gate.acquire().await.unwrap().forget();
                    }
                    let mut trade = fill(order_id);
                    trade["orderId"] = json!(order_id);
                    Json(vec![trade])
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = Arc::new(BinanceFuturesClient::with_base_url(
        &AppConfig::default().exchange,
        url,
    ));

    let mut old_requests = Vec::new();
    for order_id in 100..108 {
        let client = Arc::clone(&client);
        old_requests.push(tokio::spawn(async move {
            client.get_user_trades(SYMBOL, order_id, dec!(1), 0).await
        }));
    }
    for _ in 0..8 {
        tokio::time::timeout(Duration::from_secs(5), arrived_rx.recv())
            .await
            .expect("old request did not reach the server")
            .expect("server stopped");
    }
    let error = client
        .get_user_trades(SYMBOL, 200, dec!(1), 0)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("waiting"));

    let trades = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_user_trades(SYMBOL, 201, dec!(1), 1),
    )
    .await
    .expect("new generation remained blocked by the old queue")
    .unwrap();
    assert_totals(&trades, 1, dec!(0.25), dec!(0.01));
    for order_id in [200, 202] {
        let error = client
            .get_user_trades(SYMBOL, order_id, dec!(1), 0)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cleared"));
    }
    let trades = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_user_trades(SYMBOL, 203, dec!(1), 1),
    )
    .await
    .expect("stale requests recreated the waiting queue")
    .unwrap();
    assert_totals(&trades, 1, dec!(0.25), dec!(0.01));
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 10);
        assert_eq!(requests[8]["orderId"], "201");
        assert_eq!(requests[9]["orderId"], "203");
    }

    gate.add_permits(8);
    for old in old_requests {
        let error = tokio::time::timeout(Duration::from_secs(5), old)
            .await
            .expect("old request did not complete")
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("cleared"));
    }
    server.abort();
}
