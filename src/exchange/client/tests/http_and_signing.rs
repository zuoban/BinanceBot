use super::super::*;

#[tokio::test]
async fn shared_http_pool_keeps_api_headers_isolated_per_request() {
    use axum::{http::HeaderMap, routing::get, Router};
    let app = Router::new().route(
        "/",
        get(|headers: HeaderMap| async move {
            headers
                .get("X-MBX-APIKEY")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing")
                .to_owned()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let request = |key: &'static str| {
        let url = url.clone();
        async move {
            let mut config = crate::config::AppConfig::default().exchange;
            config.api_key = key.into();
            let client = BinanceFuturesClient::with_base_url(&config, url.clone());
            client
                .send(client.client.get(url))
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        }
    };
    let (first, second, public) =
        tokio::join!(request("first-key"), request("second-key"), request(""));
    assert_eq!(first, "first-key");
    assert_eq!(second, "second-key");
    assert_eq!(public, "missing");
    server.abort();
}

#[test]
fn exchange_environment_uses_mainnet_url() {
    let config = crate::config::AppConfig::default().exchange;
    assert_eq!(
        BinanceFuturesClient::new(&config).market_cache_key(),
        "https://fapi.binance.com"
    );
}

#[tokio::test]
async fn exchange_info_and_open_orders_preserve_binance_error_codes() {
    use axum::{http::StatusCode, routing::get, Json, Router};
    use serde_json::json;

    let app = Router::new()
        .route(
            "/fapi/v1/exchangeInfo",
            get(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"code": -1121, "msg": "Invalid symbol."})),
                )
            }),
        )
        .route(
            "/fapi/v1/ticker/price",
            get(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"code": -1121, "msg": "Invalid symbol."})),
                )
            }),
        )
        .route(
            "/fapi/v1/openOrders",
            get(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"code": -1121, "msg": "Invalid symbol."})),
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = crate::config::AppConfig::default().exchange;
    let client = BinanceFuturesClient::with_base_url(&config, url);
    for error in [
        client.get_exchange_info(Some("INVALID")).await.unwrap_err(),
        client.get_ticker_price("INVALID").await.unwrap_err(),
        client.get_open_orders("INVALID").await.unwrap_err(),
    ] {
        assert!(
            matches!(error.downcast_ref::<ExchangeError>(), Some(ExchangeError::ApiError { code: -1121, msg }) if msg == "Invalid symbol.")
        );
    }
    server.abort();
}

#[tokio::test]
async fn rate_limit_response_blocks_shared_clients_then_recovers() {
    use axum::{extract::Query, http::StatusCode, routing::get, Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get({
            let calls = calls.clone();
            move |Query(query): Query<std::collections::HashMap<String, String>>| {
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        (
                            StatusCode::TOO_MANY_REQUESTS,
                            [("retry-after", "1")],
                            Json(json!({})),
                        )
                    } else {
                        (
                            StatusCode::OK,
                            [("retry-after", "1")],
                            Json(json!({"symbol": query["symbol"], "markPrice":"100"})),
                        )
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = crate::config::AppConfig::default().exchange;
    let limiter = Arc::new(RateLimiter::default());
    let mut first = BinanceFuturesClient::with_base_url(&config, url.clone());
    let mut second = BinanceFuturesClient::with_base_url(&config, url);
    first.limiter = limiter.clone();
    second.limiter = limiter;
    assert!(first.get_mark_price("SOLUSDC").await.is_err());
    assert!(second.get_mark_price("BTCUSDT").await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(1050)).await;
    assert_eq!(
        second.get_mark_price("BTCUSDT").await.unwrap(),
        rust_decimal_macros::dec!(100)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn locally_blocked_order_is_known_not_to_have_been_submitted() {
    let config = crate::config::AppConfig::default().exchange;
    let client = BinanceFuturesClient::with_base_url(&config, "http://127.0.0.1:1".into());
    for _ in 0..100 {
        client.reserve(0, RequestClass::OpeningOrder).unwrap();
    }
    let error = client
        .place_order(NewOrderRequest {
            symbol: "SOLUSDC",
            side: "BUY",
            price: "100",
            quantity: "1",
            client_order_id: "gb_test",
            post_only: true,
            reduce_only: false,
        })
        .await
        .unwrap_err();
    assert!(matches!(error, ExchangeError::RequestRejected(_)));
    assert!(!error.execution_status_unknown());
}

#[test]
fn position_selection_accepts_one_way_short_and_rejects_hedge_mode() {
    let position = |side: &str, amount: &str| {
        serde_json::from_value::<BinancePositionRisk>(serde_json::json!({
            "symbol": "SOLUSDC", "positionSide": side, "positionAmt": amount,
            "entryPrice": "100", "markPrice": "99", "unRealizedProfit": "1",
            "liquidationPrice": "0", "leverage": "20"
        }))
        .unwrap()
    };
    let short = one_way_position(vec![position("BOTH", "-1")], "SOLUSDC")
        .unwrap()
        .unwrap();
    assert_eq!(short.position_amt, rust_decimal_macros::dec!(-1));
    assert!(one_way_position(vec![position("SHORT", "-1")], "SOLUSDC").is_err());
    assert!(one_way_position(
        vec![position("LONG", "0"), position("SHORT", "0")],
        "SOLUSDC"
    )
    .is_err());
}

#[test]
fn order_errors_distinguish_uncertain_execution_from_explicit_rejection() {
    let cases = [
        (
            400,
            r#"{"code":-1006,"msg":"Execution status unknown."}"#,
            true,
        ),
        (400, r#"{"code":-1007,"msg":"Backend timeout."}"#, true),
        (
            503,
            r#"{"code":-1000,"msg":"Unknown error, please check your request or try again later."}"#,
            true,
        ),
        (
            500,
            r#"{"code":-1100,"msg":"unexpected server error"}"#,
            true,
        ),
        (408, "timeout", true),
        (
            503,
            r#"{"code":-1008,"msg":"Request throttled by system-level protection."}"#,
            false,
        ),
        (503, r#"{"code":-1000,"msg":"Service Unavailable."}"#, false),
        (503, "Service Unavailable.", false),
        (
            503,
            r#"{"code":-1000,"msg":"Internal error; unable to process your request. Please try again."}"#,
            false,
        ),
        (400, r#"{"code":-5022,"msg":"GTX rejected"}"#, false),
        (429, r#"{"code":-1003,"msg":"Too many requests"}"#, false),
    ];
    for (status, body, uncertain) in cases {
        let error = order_response_error(reqwest::StatusCode::from_u16(status).unwrap(), body);
        assert_eq!(
            error.execution_status_unknown(),
            uncertain,
            "{status}: {body}"
        );
    }
}

#[tokio::test]
async fn signed_and_public_http_errors_strip_request_urls() {
    use axum::{http::StatusCode, routing::get, routing::post, Router};

    let mut config = crate::config::AppConfig::default().exchange;
    config.api_key = "test-key".into();
    config.api_secret = "test-secret".into();

    let unreachable = BinanceFuturesClient::with_base_url(&config, "http://127.0.0.1:1".into());
    let transport_err = unreachable.get_open_orders("SOLUSDC").await.unwrap_err();
    let rendered = format!("{transport_err} {transport_err:?}");
    assert!(!rendered.contains("signature="), "{rendered}");
    assert!(!rendered.contains("recvWindow="), "{rendered}");

    let app = Router::new()
        .route(
            "/fapi/v1/ticker/price",
            get(|| async { StatusCode::BAD_GATEWAY }),
        )
        .route("/fapi/v1/order", post(|| async { "not-json" }))
        .route("/fapi/v2/account", get(|| async { "not-json" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = BinanceFuturesClient::with_base_url(&config, url.clone());

    let public_err = client.get_ticker_price("SOLUSDC").await.unwrap_err();
    let public_rendered = format!("{public_err} {public_err:?}");
    assert!(!public_rendered.contains(&url), "{public_rendered}");

    let account_err = client.get_account().await.unwrap_err();
    let account_rendered = format!("{account_err} {account_err:?}");
    assert!(
        !account_rendered.contains("signature="),
        "{account_rendered}"
    );
    assert!(!account_rendered.contains(&url), "{account_rendered}");

    let order_err = client
        .place_order(NewOrderRequest {
            symbol: "SOLUSDC",
            side: "BUY",
            price: "100",
            quantity: "1",
            client_order_id: "gb_test",
            post_only: true,
            reduce_only: false,
        })
        .await
        .unwrap_err();
    let order_rendered = format!("{order_err} {order_err:?}");
    assert!(!order_rendered.contains("signature="), "{order_rendered}");
    assert!(!order_rendered.contains(&url), "{order_rendered}");

    server.abort();
}

#[tokio::test]
async fn reactive_invalid_timestamp_resyncs_and_throttles_repeat_requests() {
    use axum::{extract::Query, http::StatusCode, routing::get, Json, Router};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;

    let time_hits = Arc::new(AtomicUsize::new(0));
    let hits_for_route = Arc::clone(&time_hits);
    let skew_ms = 60_000_i64;
    let app = Router::new()
        .route(
            "/fapi/v1/time",
            get(move || {
                let hits = Arc::clone(&hits_for_route);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    Json(json!({ "serverTime": Utc::now().timestamp_millis() + skew_ms }))
                }
            }),
        )
        .route(
            "/fapi/v1/openOrders",
            get(move |Query(query): Query<HashMap<String, String>>| async move {
                let ts: i64 = query["timestamp"].parse().unwrap();
                let expected = Utc::now().timestamp_millis() + skew_ms;
                if (ts - expected).abs() > 5_000 {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "code": -1021, "msg": "Timestamp for this request is outside of the recvWindow." })),
                    )
                } else {
                    (StatusCode::OK, Json(json!([])))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = crate::config::AppConfig::default().exchange;
    config.api_key = "k".into();
    config.api_secret = "s".into();
    let client = BinanceFuturesClient::with_base_url(&config, url);

    // First signed call observes -1021 and triggers reactive sync_server_time().
    assert!(client.get_open_orders("SOLUSDC").await.is_err());
    assert_eq!(time_hits.load(Ordering::SeqCst), 1);
    // Next signed call succeeds using the updated offset.
    assert!(client.get_open_orders("SOLUSDC").await.unwrap().is_empty());
    // Another -1021 within 10s is throttled and does not hit /fapi/v1/time again.
    client
        .maybe_resync_on_invalid_timestamp(r#"{"code":-1021,"msg":"skew"}"#)
        .await;
    assert_eq!(time_hits.load(Ordering::SeqCst), 1);

    server.abort();
}

#[test]
fn reconfig_preserves_clock_offset_and_mainnet_client_seeds_shared_offset() {
    let mut config = crate::config::AppConfig::default().exchange;
    let client = BinanceFuturesClient::with_base_url(&config, "http://127.0.0.1:1".into());
    client.time_offset_ms.store(-4_321, Ordering::SeqCst);
    client.last_time_sync_ms.store(98_765, Ordering::SeqCst);
    config.recv_window = 9_000;
    let rebuilt = client.reconfig(&config);
    assert_eq!(rebuilt.time_offset_ms.load(Ordering::SeqCst), -4_321);
    assert_eq!(rebuilt.last_time_sync_ms.load(Ordering::SeqCst), 98_765);

    let previous_mainnet = MAINNET_TIME_OFFSET_MS.swap(2_500, Ordering::SeqCst);
    let mainnet_client = BinanceFuturesClient::new(&config);
    let seeded = mainnet_client.time_offset_ms.load(Ordering::SeqCst);
    MAINNET_TIME_OFFSET_MS.store(previous_mainnet, Ordering::SeqCst);
    assert_eq!(seeded, 2_500);
}

#[tokio::test]
async fn bounded_html_502_is_summarized_and_account_api_error_downcasts_cleanly() {
    use axum::{http::StatusCode, routing::get, Json, Router};
    use serde_json::json;

    let html_64kb = format!(
        "<!DOCTYPE html><html><body>502 Bad Gateway {}</body></html>",
        "X".repeat(64 * 1024)
    );
    let html_public = html_64kb.clone();
    let html_open = html_64kb.clone();
    let html_order_get = html_64kb.clone();
    let html_order_post = html_64kb.clone();
    let html_order_del = html_64kb.clone();
    let html_trades = html_64kb.clone();

    let app = Router::new()
        .route(
            "/fapi/v1/ticker/price",
            get(move || {
                let body = html_public.clone();
                async move { (StatusCode::BAD_GATEWAY, body) }
            }),
        )
        .route(
            "/fapi/v1/openOrders",
            get(move || {
                let body = html_open.clone();
                async move { (StatusCode::BAD_GATEWAY, body) }
            }),
        )
        .route(
            "/fapi/v1/order",
            get(move || {
                let body = html_order_get.clone();
                async move { (StatusCode::BAD_GATEWAY, body) }
            })
            .post(move || {
                let body = html_order_post.clone();
                async move { (StatusCode::BAD_GATEWAY, body) }
            })
            .delete(move || {
                let body = html_order_del.clone();
                async move { (StatusCode::BAD_GATEWAY, body) }
            }),
        )
        .route(
            "/fapi/v1/userTrades",
            get(move || {
                let body = html_trades.clone();
                async move { (StatusCode::BAD_GATEWAY, body) }
            }),
        )
        .route(
            "/fapi/v2/account",
            get(|| async {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"code": -2015, "msg": "Invalid API-key"})),
                )
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = crate::config::AppConfig::default().exchange;
    config.api_key = "k".into();
    config.api_secret = "s".into();
    let client = BinanceFuturesClient::with_base_url(&config, url);

    for err in [
        client.get_ticker_price("SOLUSDC").await.unwrap_err(),
        client.get_open_orders("SOLUSDC").await.unwrap_err(),
        client
            .lookup_order_by_client_id("SOLUSDC", "gb_b_test")
            .await
            .unwrap_err(),
        client
            .cancel_order("SOLUSDC", None, Some("gb_b_test"))
            .await
            .unwrap_err(),
        client
            .get_user_trades("SOLUSDC", 42, Decimal::ONE, 0)
            .await
            .unwrap_err(),
    ] {
        let ex = err
            .downcast_ref::<ExchangeError>()
            .expect("must downcast to ExchangeError");
        let rendered = format!("{ex} {err} {err:?}");
        assert!(
            rendered.contains("upstream HTML error response"),
            "{rendered}"
        );
        assert!(!rendered.contains("<!DOCTYPE"), "{rendered}");
        assert!(!rendered.contains("<html"), "{rendered}");
        assert!(rendered.len() < 1024, "error too large: {}", rendered.len());
    }

    let place_err = client
        .place_order(NewOrderRequest {
            symbol: "SOLUSDC",
            side: "BUY",
            price: "100",
            quantity: "1",
            client_order_id: "gb_b_test",
            post_only: true,
            reduce_only: false,
        })
        .await
        .unwrap_err();
    assert!(place_err.execution_status_unknown());
    let place_rendered = format!("{place_err} {place_err:?}");
    assert!(place_rendered.contains("upstream HTML error response"));
    assert!(!place_rendered.contains("<html"));

    let account_err = client.get_account().await.unwrap_err();
    assert!(matches!(
        account_err.downcast_ref::<ExchangeError>(),
        Some(ExchangeError::ApiError { code: -2015, msg }) if msg == "Invalid API-key"
    ));

    for bad_id in ["", "gb_b_bad&side=SELL", &"a".repeat(37)] {
        assert!(client
            .lookup_order_by_client_id("SOLUSDC", bad_id)
            .await
            .is_err());
        assert!(client
            .cancel_order("SOLUSDC", None, Some(bad_id))
            .await
            .is_err());
        let rejected = client
            .place_order(NewOrderRequest {
                symbol: "SOLUSDC",
                side: "BUY",
                price: "100",
                quantity: "1",
                client_order_id: bad_id,
                post_only: true,
                reduce_only: false,
            })
            .await
            .unwrap_err();
        assert!(matches!(rejected, ExchangeError::RequestRejected(_)));
        assert!(!rejected.execution_status_unknown());
    }

    server.abort();
}

#[tokio::test]
async fn place_order_oversized_200_response_preserves_unknown_execution_status() {
    use axum::{routing::post, Router};

    let oversized_json = format!(
        r#"{{"orderId":1,"clientOrderId":"gb_big","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX","pad":"{}"}}"#,
        "A".repeat(70 * 1024)
    );
    let app = Router::new().route(
        "/fapi/v1/order",
        post(move || {
            let body = oversized_json.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = crate::config::AppConfig::default().exchange;
    config.api_key = "k".into();
    config.api_secret = "s".into();
    let client = BinanceFuturesClient::with_base_url(&config, url);

    let err = client
        .place_order(NewOrderRequest {
            symbol: "SOLUSDC",
            side: "BUY",
            price: "100",
            quantity: "1",
            client_order_id: "gb_big",
            post_only: true,
            reduce_only: false,
        })
        .await
        .unwrap_err();
    assert!(matches!(&err, ExchangeError::Other(msg) if msg.contains("exceeded")));
    assert!(err.execution_status_unknown());

    server.abort();
}

#[tokio::test]
async fn http_client_refuses_redirects_and_never_leaks_api_key_cross_host() {
    use axum::{http::StatusCode, routing::get, Router};
    use std::sync::atomic::AtomicUsize;

    let evil_hits = Arc::new(AtomicUsize::new(0));
    let hits_clone = Arc::clone(&evil_hits);
    let evil_app = Router::new().route(
        "/steal",
        get(move || {
            let hits = Arc::clone(&hits_clone);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                "[]".to_owned()
            }
        }),
    );
    let evil_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let evil_target = format!("http://{}/steal", evil_listener.local_addr().unwrap());
    let evil_server =
        tokio::spawn(async move { axum::serve(evil_listener, evil_app).await.unwrap() });

    let target_open = evil_target.clone();
    let target_public = evil_target.clone();
    let origin_app = Router::new()
        .route(
            "/fapi/v1/openOrders",
            get(move || {
                let loc = target_open.clone();
                async move {
                    (
                        StatusCode::TEMPORARY_REDIRECT,
                        [(axum::http::header::LOCATION, loc)],
                        "redirect",
                    )
                }
            }),
        )
        .route(
            "/fapi/v1/ticker/price",
            get(move || {
                let loc = target_public.clone();
                async move {
                    (
                        StatusCode::TEMPORARY_REDIRECT,
                        [(axum::http::header::LOCATION, loc)],
                        "redirect",
                    )
                }
            }),
        );
    let origin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_url = format!("http://{}", origin_listener.local_addr().unwrap());
    let origin_server =
        tokio::spawn(async move { axum::serve(origin_listener, origin_app).await.unwrap() });

    let mut config = crate::config::AppConfig::default().exchange;
    config.api_key = "sensitive-key-must-not-follow".into();
    config.api_secret = "sensitive-secret".into();
    let client = BinanceFuturesClient::with_base_url(&config, origin_url);

    assert!(client.get_open_orders("SOLUSDC").await.is_err());
    assert!(client.get_ticker_price("SOLUSDC").await.is_err());
    assert_eq!(evil_hits.load(Ordering::SeqCst), 0);

    origin_server.abort();
    evil_server.abort();
}

#[tokio::test]
async fn sync_server_time_rejects_non_positive_timestamps_without_poisoning_offsets() {
    use axum::{routing::get, Json, Router};
    use serde_json::json;

    let app = Router::new().route(
        "/fapi/v1/time",
        get(|| async { Json(json!({ "serverTime": 0 })) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let config = crate::config::AppConfig::default().exchange;
    let client = BinanceFuturesClient::with_base_url(&config, url);
    client.time_offset_ms.store(4_321, Ordering::SeqCst);
    let before_mainnet = MAINNET_TIME_OFFSET_MS.load(Ordering::SeqCst);

    let err = client.sync_server_time().await.unwrap_err();
    assert!(err.to_string().contains("Invalid Binance server time: 0"));
    assert_eq!(client.time_offset_ms.load(Ordering::SeqCst), 4_321);
    assert_eq!(client.last_time_sync_ms.load(Ordering::SeqCst), 0);
    assert_eq!(
        MAINNET_TIME_OFFSET_MS.load(Ordering::SeqCst),
        before_mainnet
    );

    server.abort();
}
