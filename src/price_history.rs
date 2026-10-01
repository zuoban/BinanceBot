use crate::db::Database;
use crate::exchange::client::BinanceFuturesClient;
use crate::server::AppState;
use anyhow::Result;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tracing::warn;

pub fn beijing_midnight(now: DateTime<Utc>) -> DateTime<Utc> {
    let beijing = FixedOffset::east_opt(8 * 3600).unwrap();
    now.with_timezone(&beijing)
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(beijing)
        .single()
        .unwrap()
        .with_timezone(&Utc)
}

async fn refresh_history(
    db: &Arc<Database>,
    client: &BinanceFuturesClient,
    symbol: &str,
    is_testnet: bool,
    now: DateTime<Utc>,
) -> Result<bool> {
    let start = beijing_midnight(now) - Duration::days(6);
    let points = client.get_hourly_mark_prices(symbol, start, now).await?;
    anyhow::ensure!(!points.is_empty(), "Hourly mark-price history is not ready");
    let has_current_hour = points
        .iter()
        .any(|point| point.timestamp.timestamp() == now.timestamp().div_euclid(3600) * 3600);
    let symbol = symbol.to_string();
    db.run_blocking(move |db| db.save_price_points(&symbol, is_testnet, &points))
        .await?;
    Ok(has_current_hour)
}

impl AppState {
    /// Sync at startup and each new hour, including Beijing 00:00, even while trading is paused.
    pub async fn run_price_history_refresh(self: Arc<Self>) {
        let mut timer = tokio::time::interval(StdDuration::from_secs(1));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut completed = None;
        let mut attempted: Option<((String, bool, i64), Instant)> = None;
        let mut client: Option<(bool, BinanceFuturesClient)> = None;
        loop {
            timer.tick().await;
            let now = Utc::now();
            let mut exchange = self.config.read().await.exchange.clone();
            let key = (
                exchange.symbol.clone(),
                exchange.is_testnet,
                now.timestamp().div_euclid(3600),
            );
            if completed.as_ref() == Some(&key)
                || attempted.as_ref().is_some_and(|(previous, time)| {
                    previous == &key && time.elapsed() < StdDuration::from_secs(30)
                })
            {
                continue;
            }
            attempted = Some((key.clone(), Instant::now()));
            if client
                .as_ref()
                .is_none_or(|(is_testnet, _)| *is_testnet != exchange.is_testnet)
            {
                exchange.api_key.clear();
                exchange.api_secret.clear();
                client = Some((exchange.is_testnet, BinanceFuturesClient::new(&exchange)));
            }
            match refresh_history(
                &self.db,
                &client.as_ref().unwrap().1,
                &exchange.symbol,
                exchange.is_testnet,
                now,
            )
            .await
            {
                Ok(true) => completed = Some(key),
                Ok(false) => {} // A just-opened hour may not be available from the exchange yet.
                Err(error) => warn!(
                    "Could not sync {} mark-price history: {}",
                    exchange.symbol, error
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::types::PricePoint;
    use axum::{extract::Query, extract::State, http::HeaderMap, routing::get, Json, Router};
    use rust_decimal_macros::dec;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn test_time() -> DateTime<Utc> {
        "2026-10-01T05:30:00Z".parse().unwrap()
    }

    #[test]
    fn snapshots_are_immutable_persistent_and_scoped_to_seven_beijing_days() {
        let path = std::env::temp_dir().join(format!("price-history-{}.db", uuid::Uuid::new_v4()));
        let now = test_time();
        let midnight = beijing_midnight(now);
        assert_eq!(
            midnight,
            "2026-09-30T16:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        let start = midnight - Duration::days(6);
        let points: Vec<_> = [
            midnight,
            start - Duration::hours(1),
            now + Duration::minutes(30),
            start,
            midnight - Duration::hours(1),
            now - Duration::minutes(30),
        ]
        .into_iter()
        .map(|timestamp| PricePoint {
            timestamp,
            price: dec!(120),
        })
        .collect();
        {
            let db = Database::open(&path).unwrap();
            db.save_price_points("SOLUSDC", false, &points).unwrap();
            let replacement = [PricePoint {
                timestamp: midnight,
                price: dec!(999),
            }];
            db.save_price_points("SOLUSDC", false, &replacement)
                .unwrap();
            db.save_price_points("SOLUSDC", true, &replacement).unwrap();
            db.save_price_points("BTCUSDT", false, &replacement)
                .unwrap();
        }
        {
            let db = Database::open(&path).unwrap();
            let history = db.get_price_history("SOLUSDC", false, now).unwrap();
            assert_eq!(history.window_start, start);
            assert_eq!(history.window_end, now);
            assert_eq!(history.midnight_price, Some(dec!(120)));
            assert_eq!(history.points.len(), 4);
            assert_eq!(history.points[0].timestamp, start);
            assert!(history
                .points
                .windows(2)
                .all(|pair| pair[0].timestamp < pair[1].timestamp));
            for (symbol, is_testnet) in [("SOLUSDC", true), ("BTCUSDT", false)] {
                let scoped = db.get_price_history(symbol, is_testnet, now).unwrap();
                assert_eq!(scoped.points.len(), 1);
                assert_eq!(scoped.midnight_price, Some(dec!(999)));
            }
            let next_day = db
                .get_price_history("SOLUSDC", false, midnight + Duration::days(1))
                .unwrap();
            assert_eq!(next_day.day_start, midnight + Duration::days(1));
            assert_eq!(next_day.midnight_price, None);
            assert!(next_day.points.iter().all(|p| p.timestamp > start));
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn missing_midnight_is_not_substituted_and_invalid_batches_roll_back() {
        let db = Database::open(":memory:").unwrap();
        let now = test_time();
        let midnight = beijing_midnight(now);
        db.save_price_points(
            "SOLUSDC",
            false,
            &[PricePoint {
                timestamp: midnight + Duration::hours(1),
                price: dec!(123),
            }],
        )
        .unwrap();
        assert_eq!(
            db.get_price_history("SOLUSDC", false, now)
                .unwrap()
                .midnight_price,
            None
        );
        for invalid in [
            PricePoint {
                timestamp: midnight,
                price: dec!(0),
            },
            PricePoint {
                timestamp: midnight + Duration::nanoseconds(1),
                price: dec!(123),
            },
        ] {
            assert!(db
                .save_price_points(
                    "SOLUSDC",
                    false,
                    &[
                        PricePoint {
                            timestamp: midnight,
                            price: dec!(120)
                        },
                        invalid,
                    ]
                )
                .is_err());
            let history = db.get_price_history("SOLUSDC", false, now).unwrap();
            assert_eq!(history.points.len(), 1);
            assert_eq!(history.midnight_price, None);
        }
    }

    #[derive(Default)]
    struct MockExchange {
        response: Mutex<Value>,
        requests: Mutex<Vec<HashMap<String, String>>>,
    }

    async fn klines(
        State(state): State<Arc<MockExchange>>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Json<Value> {
        assert!(!headers.contains_key("X-MBX-APIKEY"));
        state.requests.lock().unwrap().push(query);
        Json(state.response.lock().unwrap().clone())
    }

    async fn mock_client() -> (
        BinanceFuturesClient,
        Arc<MockExchange>,
        tokio::task::JoinHandle<()>,
    ) {
        let state = Arc::new(MockExchange::default());
        let app = Router::new()
            .route("/fapi/v1/markPriceKlines", get(klines))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = BinanceFuturesClient::with_base_url(
            &AppConfig::default().exchange,
            format!("http://{address}"),
        );
        (client, state, server)
    }

    #[tokio::test]
    async fn startup_backfills_real_midnight_open_and_retries_missing_current_hour() {
        let (client, exchange, server) = mock_client().await;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let now = test_time();
        let midnight = beijing_midnight(now);
        let start = midnight - Duration::days(6);
        let current_hour = now - Duration::minutes(30);
        *exchange.response.lock().unwrap() = json!([
            [
                current_hour.timestamp_millis(),
                "121.5",
                "130",
                "110",
                "999"
            ],
            [midnight.timestamp_millis(), "120.25", "130", "110", "888"],
            [(start - Duration::hours(1)).timestamp_millis(), "100"],
            [start.timestamp_millis(), "109"],
            [
                (current_hour + Duration::hours(1)).timestamp_millis(),
                "122"
            ]
        ]);
        assert!(refresh_history(&db, &client, "SOLUSDC", false, now)
            .await
            .unwrap());
        let history = db.get_price_history("SOLUSDC", false, now).unwrap();
        assert_eq!(history.points.len(), 3);
        assert_eq!(history.midnight_price, Some(dec!(120.25)));
        assert_eq!(history.points[0].price, dec!(109));
        let requests = exchange.requests.lock().unwrap().clone();
        assert_eq!(requests[0].get("symbol").unwrap(), "SOLUSDC");
        assert_eq!(requests[0].get("interval").unwrap(), "1h");
        assert_eq!(
            requests[0].get("startTime").unwrap(),
            &start.timestamp_millis().to_string()
        );
        assert_eq!(
            requests[0].get("endTime").unwrap(),
            &now.timestamp_millis().to_string()
        );
        assert_eq!(requests[0].get("limit").unwrap(), "200");
        *exchange.response.lock().unwrap() = json!([[midnight.timestamp_millis(), "666"]]);
        assert!(!refresh_history(&db, &client, "SOLUSDC", false, now)
            .await
            .unwrap());
        assert_eq!(
            db.get_price_history("SOLUSDC", false, now)
                .unwrap()
                .midnight_price,
            Some(dec!(120.25))
        );
        *exchange.response.lock().unwrap() = json!([]);
        assert!(refresh_history(&db, &client, "SOLUSDC", false, now)
            .await
            .is_err());
        assert_eq!(
            db.get_price_history("SOLUSDC", false, now)
                .unwrap()
                .points
                .len(),
            3
        );
        server.abort();
    }

    #[tokio::test]
    async fn malformed_exchange_prices_never_become_snapshots() {
        let (client, exchange, server) = mock_client().await;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let now = test_time();
        let midnight = beijing_midnight(now);
        for rows in [
            json!([[midnight.timestamp_millis(), "0"]]),
            json!([[midnight.timestamp_millis() + 1, "120"]]),
            json!([[midnight.timestamp_millis()]]),
            json!([["invalid", "120"]]),
            json!({"code": -1000, "msg": "Unavailable"}),
        ] {
            *exchange.response.lock().unwrap() = rows;
            assert!(refresh_history(&db, &client, "SOLUSDC", false, now)
                .await
                .is_err());
            assert!(db
                .get_price_history("SOLUSDC", false, now)
                .unwrap()
                .points
                .is_empty());
        }
        server.abort();
    }
}
