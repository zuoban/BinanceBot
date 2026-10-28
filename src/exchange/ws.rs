use crate::exchange::model::{BinanceWs24hrTicker, BinanceWsMarkPrice};
use crate::types::TickerInfo;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

pub struct BinanceWsStream {
    config_rx: watch::Receiver<(String, bool)>,
    ticker_tx: broadcast::Sender<TickerInfo>,
}

fn stream_url(symbol: &str, is_testnet: bool) -> String {
    let host = if is_testnet {
        "wss://fstream.binancefuture.com"
    } else {
        "wss://fstream.binance.com"
    };
    let symbol = symbol.to_lowercase();
    format!("{host}/market/stream?streams={symbol}@ticker/{symbol}@markPrice@1s")
}

#[derive(Default)]
struct EventTimes {
    ticker: Option<i64>,
    mark: Option<i64>,
}

fn parse_market_update(
    text: &str,
    symbol: &str,
    now: DateTime<Utc>,
    times: &mut EventTimes,
) -> Option<TickerInfo> {
    let mut payload: Value = serde_json::from_str(text).ok()?;
    let stream = payload
        .get("stream")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if stream.is_some() {
        payload = payload.get("data")?.clone();
    }
    let event_type = payload.get("e")?.as_str()?;
    let (event_time, info, last_time) = match event_type {
        "24hrTicker" => {
            if stream
                .as_deref()
                .is_some_and(|s| s != format!("{}@ticker", symbol.to_lowercase()))
            {
                return None;
            }
            let msg: BinanceWs24hrTicker = serde_json::from_value(payload).ok()?;
            if msg.event_type != "24hrTicker"
                || !msg.symbol.eq_ignore_ascii_case(symbol)
                || msg.close_price <= Decimal::ZERO
            {
                return None;
            }
            let change = msg.close_price - msg.open_price;
            let change_pct = if msg.open_price.is_zero() {
                Decimal::ZERO
            } else {
                change / msg.open_price * rust_decimal_macros::dec!(100)
            };
            (
                msg.event_time,
                TickerInfo {
                    symbol: symbol.to_string(),
                    last_price: msg.close_price,
                    high_24h: msg.high_price,
                    low_24h: msg.low_price,
                    change_24h: change,
                    change_percent_24h: change_pct,
                    volume_24h: msg.volume,
                    ..Default::default()
                },
                &mut times.ticker,
            )
        }
        "markPriceUpdate" => {
            if stream
                .as_deref()
                .is_some_and(|s| s != format!("{}@markPrice@1s", symbol.to_lowercase()))
            {
                return None;
            }
            let msg: BinanceWsMarkPrice = serde_json::from_value(payload).ok()?;
            if msg.event_type != "markPriceUpdate"
                || !msg.symbol.eq_ignore_ascii_case(symbol)
                || msg.mark_price <= Decimal::ZERO
            {
                return None;
            }
            (
                msg.event_time,
                TickerInfo {
                    symbol: symbol.to_string(),
                    mark_price: msg.mark_price,
                    ..Default::default()
                },
                &mut times.mark,
            )
        }
        _ => return None,
    };
    let event_at = DateTime::from_timestamp_millis(event_time)?;
    let age = now.signed_duration_since(event_at);
    if age < chrono::Duration::seconds(-2)
        || age >= chrono::Duration::seconds(15)
        || last_time.is_some_and(|last| event_time <= last)
    {
        return None;
    }
    *last_time = Some(event_time);
    let mut info = info;
    let observed_at = event_at.min(now);
    if info.last_price > Decimal::ZERO {
        info.update_time = observed_at;
    }
    if info.mark_price > Decimal::ZERO {
        info.mark_update_time = observed_at;
    }
    Some(info)
}

impl BinanceWsStream {
    pub fn new(
        config_rx: watch::Receiver<(String, bool)>,
        ticker_tx: broadcast::Sender<TickerInfo>,
    ) -> Self {
        Self {
            config_rx,
            ticker_tx,
        }
    }

    pub async fn run(self) {
        self.run_with_url(stream_url).await;
    }

    async fn run_with_url<F>(mut self, build_url: F)
    where
        F: Fn(&str, bool) -> String,
    {
        let mut backoff_secs = 1u64;
        let mut previous_config = None;
        let mut event_times = EventTimes::default();
        'connect: loop {
            let (symbol, is_testnet) = self.config_rx.borrow_and_update().clone();
            if previous_config.as_ref() != Some(&(symbol.clone(), is_testnet)) {
                event_times = EventTimes::default();
                previous_config = Some((symbol.clone(), is_testnet));
            }
            let ws_url = build_url(&symbol, is_testnet);
            info!("Starting Binance WebSocket stream for {}", ws_url);

            let connection = tokio::select! {
                result = connect_async(&ws_url) => result,
                changed = self.config_rx.changed() => {
                    if changed.is_err() { return; }
                    continue 'connect;
                }
            };
            match connection {
                Ok((ws_stream, _)) => {
                    info!("Connected to Binance WebSocket: {}", ws_url);
                    backoff_secs = 1;
                    let (_, mut read) = ws_stream.split();
                    loop {
                        let msg_res = tokio::select! {
                            msg = read.next() => msg,
                            changed = self.config_rx.changed() => {
                                if changed.is_err() { return; }
                                continue 'connect;
                            }
                        };
                        let Some(msg_res) = msg_res else { break };
                        match msg_res {
                            Ok(msg) if msg.is_text() => {
                                if *self.config_rx.borrow() != (symbol.clone(), is_testnet) {
                                    continue 'connect;
                                }
                                if let Ok(text) = msg.to_text() {
                                    if let Some(update) = parse_market_update(
                                        text,
                                        &symbol,
                                        Utc::now(),
                                        &mut event_times,
                                    ) {
                                        let _ = self.ticker_tx.send(update);
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("WebSocket read error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!("Failed to connect to Binance WebSocket: {}", e),
            }
            warn!(
                "WebSocket disconnected. Reconnecting in {} seconds...",
                backoff_secs
            );
            tokio::select! {
                _ = sleep(Duration::from_millis(rand::random::<u64>() % (backoff_secs * 500 + 1) + backoff_secs * 500)) => {},
                changed = self.config_rx.changed() => {
                    if changed.is_err() { return; }
                    continue 'connect;
                }
            }
            backoff_secs = (backoff_secs * 2).min(30);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    use rust_decimal_macros::dec;
    use tokio_tungstenite::tungstenite::Message;

    #[test]
    fn combined_streams_are_routed_and_decoded_independently() {
        assert_eq!(
            stream_url("BTCUSDT", false),
            "wss://fstream.binance.com/market/stream?streams=btcusdt@ticker/btcusdt@markPrice@1s"
        );
        assert!(stream_url("BTCUSDT", true)
            .starts_with("wss://fstream.binancefuture.com/market/stream?"));
        let now = Utc::now();
        let mut times = EventTimes::default();
        let mark = format!(
            r#"{{"stream":"btcusdt@markPrice@1s","data":{{"e":"markPriceUpdate","E":{},"s":"BTCUSDT","p":"100.5"}}}}"#,
            now.timestamp_millis()
        );
        let update = parse_market_update(&mark, "BTCUSDT", now, &mut times).unwrap();
        assert_eq!(update.mark_price, dec!(100.5));
        assert_eq!(update.last_price, Decimal::ZERO);
        let ticker = format!(
            r#"{{"stream":"btcusdt@ticker","data":{{"e":"24hrTicker","E":{},"s":"BTCUSDT","c":"101","o":"100","h":"102","l":"99","v":"3","q":"300"}}}}"#,
            now.timestamp_millis()
        );
        let update = parse_market_update(&ticker, "BTCUSDT", now, &mut times).unwrap();
        assert_eq!(update.last_price, dec!(101));
        assert_eq!(update.mark_price, Decimal::ZERO);
    }

    #[test]
    fn rejects_wrong_symbol_stream_invalid_price_and_old_or_future_events() {
        let now = Utc::now();
        let mut times = EventTimes::default();
        let event = |stream: &str, symbol: &str, price: &str, ms: i64| {
            format!(
                r#"{{"stream":"{stream}","data":{{"e":"markPriceUpdate","E":{ms},"s":"{symbol}","p":"{price}"}}}}"#
            )
        };
        let ms = now.timestamp_millis();
        assert!(parse_market_update(
            &event("ethusdt@markPrice@1s", "BTCUSDT", "1", ms),
            "BTCUSDT",
            now,
            &mut times
        )
        .is_none());
        assert!(parse_market_update(
            &event("btcusdt@markPrice@1s", "ETHUSDT", "1", ms),
            "BTCUSDT",
            now,
            &mut times
        )
        .is_none());
        assert!(parse_market_update(
            &event("btcusdt@markPrice@1s", "BTCUSDT", "0", ms),
            "BTCUSDT",
            now,
            &mut times
        )
        .is_none());
        assert!(parse_market_update(
            &event("btcusdt@markPrice@1s", "BTCUSDT", "1", ms - 16000),
            "BTCUSDT",
            now,
            &mut times
        )
        .is_none());
        assert!(parse_market_update(
            &event("btcusdt@markPrice@1s", "BTCUSDT", "1", ms + 3000),
            "BTCUSDT",
            now,
            &mut times
        )
        .is_none());
        assert!(parse_market_update(
            &event("btcusdt@markPrice@1s", "BTCUSDT", "1", ms),
            "BTCUSDT",
            now,
            &mut times
        )
        .is_some());
        assert!(parse_market_update(
            &event("btcusdt@markPrice@1s", "BTCUSDT", "2", ms - 1),
            "BTCUSDT",
            now,
            &mut times
        )
        .is_none());
    }

    #[tokio::test]
    async fn reconnects_and_switches_symbol_without_delivering_old_market_updates() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (index, symbol) in ["BTCUSDT", "BTCUSDT", "ETHUSDT"].iter().enumerate() {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
                if index == 2 {
                    let wrong = serde_json::json!({"stream": "btcusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": Utc::now().timestamp_millis(), "s": "BTCUSDT", "p": "1"}});
                    socket.send(Message::Text(wrong.to_string())).await.unwrap();
                }
                let event = serde_json::json!({"stream": format!("{}@markPrice@1s", symbol.to_lowercase()), "data": {"e": "markPriceUpdate", "E": Utc::now().timestamp_millis(), "s": symbol, "p": "100"}});
                socket.send(Message::Text(event.to_string())).await.unwrap();
                if index == 1 {
                    while socket.next().await.is_some() {}
                }
            }
        });
        let (config_tx, config_rx) = watch::channel(("BTCUSDT".to_string(), false));
        let (ticker_tx, mut ticker_rx) = broadcast::channel(8);
        let client = tokio::spawn(BinanceWsStream::new(config_rx, ticker_tx).run_with_url(
            move |symbol, is_testnet| {
                format!("ws://{address}/market/stream?symbol={symbol}&testnet={is_testnet}")
            },
        ));
        async fn receive(receiver: &mut broadcast::Receiver<TickerInfo>) -> TickerInfo {
            tokio::time::timeout(Duration::from_secs(4), receiver.recv())
                .await
                .unwrap()
                .unwrap()
        }
        assert_eq!(receive(&mut ticker_rx).await.symbol, "BTCUSDT");
        assert_eq!(receive(&mut ticker_rx).await.symbol, "BTCUSDT");
        config_tx.send(("ETHUSDT".into(), true)).unwrap();
        assert_eq!(receive(&mut ticker_rx).await.symbol, "ETHUSDT");
        assert!(ticker_rx.try_recv().is_err());
        client.abort();
        server.abort();
    }
}
