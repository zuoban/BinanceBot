use crate::types::TickerInfo;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
#[cfg(test)]
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::borrow::Cow;
use std::time::Duration;
#[cfg(test)]
use tokio::sync::{broadcast, watch};
#[cfg(test)]
use tokio::time::sleep;
#[cfg(test)]
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tracing::warn;
#[cfg(test)]
use tracing::{error, info};

pub(crate) mod market_hub;
pub(crate) mod user_data;

pub(crate) use market_hub::MarketStreamHub;
pub(crate) use user_data::{UserDataHub, UserDataScope};

pub(super) const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const WS_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const WS_PONG_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const WS_MAX_FRAME_BYTES: usize = 64 * 1024;

#[allow(deprecated)]
pub(super) fn upstream_ws_config() -> WebSocketConfig {
    WebSocketConfig {
        max_message_size: Some(WS_MAX_FRAME_BYTES),
        max_frame_size: Some(WS_MAX_FRAME_BYTES),
        ..Default::default()
    }
}
#[cfg(test)]
pub(crate) struct BinanceWsStream {
    config_rx: watch::Receiver<String>,
    ticker_tx: broadcast::Sender<TickerInfo>,
}

#[cfg(test)]
fn stream_url(symbol: &str) -> String {
    let host = "wss://fstream.binance.com";
    let symbol = symbol.to_lowercase();
    format!("{host}/market/stream?streams={symbol}@ticker/{symbol}@markPrice@1s")
}

#[derive(Default)]
pub(super) struct EventTimes {
    ticker: Option<i64>,
    mark: Option<i64>,
}

fn stream_matches_symbol(stream: Option<&str>, suffix: &str, symbol: &str) -> bool {
    stream.is_none_or(|stream| {
        stream
            .strip_suffix(suffix)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(symbol))
    })
}

#[derive(Deserialize)]
struct WsMarketFields<'a> {
    #[serde(borrow, rename = "e")]
    event_type: Cow<'a, str>,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(borrow, rename = "s")]
    symbol: Cow<'a, str>,
    #[serde(default, rename = "c")]
    close_price: Option<Decimal>,
    #[serde(default, rename = "o")]
    open_price: Option<Decimal>,
    #[serde(default, rename = "h")]
    high_price: Option<Decimal>,
    #[serde(default, rename = "l")]
    low_price: Option<Decimal>,
    #[serde(default, rename = "v")]
    volume: Option<Decimal>,
    #[serde(default, rename = "q")]
    quote_volume: Option<Decimal>,
    #[serde(default, rename = "p")]
    price_field: Option<Decimal>,
}

#[derive(Deserialize)]
struct WsRawFrame<'a> {
    #[serde(borrow, default)]
    stream: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    data: Option<WsMarketFields<'a>>,
    #[serde(borrow, default, rename = "e")]
    event_type: Option<Cow<'a, str>>,
    #[serde(default, rename = "E")]
    event_time: Option<i64>,
    #[serde(borrow, default, rename = "s")]
    symbol: Option<Cow<'a, str>>,
    #[serde(default, rename = "c")]
    close_price: Option<Decimal>,
    #[serde(default, rename = "o")]
    open_price: Option<Decimal>,
    #[serde(default, rename = "h")]
    high_price: Option<Decimal>,
    #[serde(default, rename = "l")]
    low_price: Option<Decimal>,
    #[serde(default, rename = "v")]
    volume: Option<Decimal>,
    #[serde(default, rename = "q")]
    quote_volume: Option<Decimal>,
    #[serde(default, rename = "p")]
    price_field: Option<Decimal>,
}

fn extract_ws_market_fields(
    raw: WsRawFrame<'_>,
) -> Option<(Option<Cow<'_, str>>, WsMarketFields<'_>)> {
    let stream = raw.stream;
    let fields = if stream.is_some() {
        raw.data?
    } else {
        WsMarketFields {
            event_type: raw.event_type?,
            event_time: raw.event_time?,
            symbol: raw.symbol?,
            close_price: raw.close_price,
            open_price: raw.open_price,
            high_price: raw.high_price,
            low_price: raw.low_price,
            volume: raw.volume,
            quote_volume: raw.quote_volume,
            price_field: raw.price_field,
        }
    };
    Some((stream, fields))
}

fn build_24hr_ticker(
    fields: &WsMarketFields<'_>,
    symbol: &str,
    observed_at: DateTime<Utc>,
    times: &mut EventTimes,
) -> Option<TickerInfo> {
    let close_price = fields.close_price?;
    let open_price = fields.open_price?;
    let high_price = fields.high_price?;
    let low_price = fields.low_price?;
    let volume = fields.volume?;
    let _ = fields.quote_volume?;
    if close_price <= Decimal::ZERO || times.ticker.is_some_and(|last| fields.event_time <= last) {
        return None;
    }
    times.ticker = Some(fields.event_time);
    let change = close_price.checked_sub(open_price).unwrap_or(Decimal::ZERO);
    let change_pct = if open_price.is_zero() {
        Decimal::ZERO
    } else {
        change
            .checked_div(open_price)
            .and_then(|ratio| ratio.checked_mul(rust_decimal_macros::dec!(100)))
            .unwrap_or(Decimal::ZERO)
    };
    Some(TickerInfo {
        symbol: symbol.to_string(),
        last_price: close_price,
        high_24h: high_price,
        low_24h: low_price,
        change_24h: change,
        change_percent_24h: change_pct,
        volume_24h: volume,
        update_time: observed_at,
        ..Default::default()
    })
}

#[cfg(test)]
fn parse_market_update(
    text: &str,
    symbol: &str,
    now: DateTime<Utc>,
    times: &mut EventTimes,
) -> Option<TickerInfo> {
    let raw: WsRawFrame<'_> = serde_json::from_str(text).ok()?;
    let (stream, fields) = extract_ws_market_fields(raw)?;
    if !fields.symbol.eq_ignore_ascii_case(symbol) {
        return None;
    }
    parse_extracted_market_fields(stream.as_deref(), &fields, symbol, now, times)
}

pub(super) fn parse_multiplexed_market_update(
    text: &str,
    now: DateTime<Utc>,
    times_by_symbol: &mut std::collections::HashMap<String, EventTimes>,
) -> Option<(String, TickerInfo)> {
    let raw: WsRawFrame<'_> = serde_json::from_str(text).ok()?;
    let (stream, fields) = extract_ws_market_fields(raw)?;
    let canonical_symbol = fields.symbol.to_ascii_uppercase();
    if canonical_symbol.is_empty() {
        return None;
    }
    let times = times_by_symbol.get_mut(&canonical_symbol)?;
    let update =
        parse_extracted_market_fields(stream.as_deref(), &fields, &canonical_symbol, now, times)?;
    Some((canonical_symbol, update))
}

fn parse_extracted_market_fields(
    stream: Option<&str>,
    fields: &WsMarketFields<'_>,
    symbol: &str,
    now: DateTime<Utc>,
    times: &mut EventTimes,
) -> Option<TickerInfo> {
    let event_at = DateTime::from_timestamp_millis(fields.event_time)?;
    let age = now.signed_duration_since(event_at);
    if age < chrono::Duration::seconds(-2) || age >= chrono::Duration::seconds(15) {
        return None;
    }
    let observed_at = event_at.min(now);
    match fields.event_type.as_ref() {
        "24hrTicker" if stream_matches_symbol(stream, "@ticker", symbol) => {
            build_24hr_ticker(fields, symbol, observed_at, times)
        }
        "markPriceUpdate" if stream_matches_symbol(stream, "@markPrice@1s", symbol) => {
            let mark_price = fields.price_field?;
            if mark_price <= Decimal::ZERO
                || times.mark.is_some_and(|last| fields.event_time <= last)
            {
                return None;
            }
            times.mark = Some(fields.event_time);
            Some(TickerInfo {
                symbol: symbol.to_string(),
                mark_price,
                mark_update_time: observed_at,
                ..Default::default()
            })
        }
        _ => None,
    }
}

pub(super) async fn send_ws_pong<W>(write: &mut W, payload: Vec<u8>, ws_url: &str) -> bool
where
    W: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout(WS_PONG_WRITE_TIMEOUT, write.send(Message::Pong(payload))).await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            warn!("WebSocket pong send error: {}", e);
            false
        }
        Err(_) => {
            warn!("WebSocket pong send timed out for {}", ws_url);
            false
        }
    }
}

fn local_observed_time(
    local_now: DateTime<Utc>,
    exchange_now: DateTime<Utc>,
    exchange_time: DateTime<Utc>,
) -> DateTime<Utc> {
    let age = exchange_now.signed_duration_since(exchange_time);
    local_now.checked_sub_signed(age).unwrap_or(local_now)
}

pub(super) fn current_local_and_exchange_now() -> (DateTime<Utc>, DateTime<Utc>) {
    let local_now = Utc::now();
    let exchange_now = local_now
        .checked_add_signed(chrono::Duration::milliseconds(
            super::client::mainnet_time_offset_ms(),
        ))
        .unwrap_or(local_now);
    (local_now, exchange_now)
}

pub(super) fn apply_local_observed_times(
    update: &mut TickerInfo,
    local_now: DateTime<Utc>,
    exchange_now: DateTime<Utc>,
) {
    if update.last_price > Decimal::ZERO {
        update.update_time = local_observed_time(local_now, exchange_now, update.update_time);
    }
    if update.mark_price > Decimal::ZERO {
        update.mark_update_time =
            local_observed_time(local_now, exchange_now, update.mark_update_time);
    }
}

#[cfg(test)]
impl BinanceWsStream {
    pub(crate) fn new(
        config_rx: watch::Receiver<String>,
        ticker_tx: broadcast::Sender<TickerInfo>,
    ) -> Self {
        Self {
            config_rx,
            ticker_tx,
        }
    }

    async fn run_with_url<F>(self, build_url: F)
    where
        F: Fn(&str) -> String,
    {
        self.run_with_url_and_timeout(build_url, WS_READ_IDLE_TIMEOUT)
            .await;
    }

    fn forward_ws_text_frame(&self, text: &str, symbol: &str, event_times: &mut EventTimes) {
        let (local_now, exchange_now) = current_local_and_exchange_now();
        let Some(mut update) = parse_market_update(text, symbol, exchange_now, event_times) else {
            return;
        };
        apply_local_observed_times(&mut update, local_now, exchange_now);
        let _ = self.ticker_tx.send(update);
    }

    async fn read_ws_session<S>(
        &mut self,
        ws_stream: S,
        ws_url: &str,
        symbol: &str,
        read_timeout: Duration,
        event_times: &mut EventTimes,
    ) -> WsSessionExit
    where
        S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
            + futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
            + Unpin,
    {
        let (mut write, mut read) = ws_stream.split();
        loop {
            let next_msg = tokio::select! {
                msg = tokio::time::timeout(read_timeout, read.next()) => msg,
                changed = self.config_rx.changed() => {
                    return if changed.is_err() { WsSessionExit::Shutdown } else { WsSessionExit::ConfigChanged };
                }
            };
            let msg_res = match next_msg {
                Ok(Some(msg_res)) => msg_res,
                Ok(None) => return WsSessionExit::Reconnect,
                Err(_) => {
                    warn!(
                        "WebSocket read timed out after {:?} for {}; reconnecting",
                        read_timeout, ws_url
                    );
                    return WsSessionExit::Reconnect;
                }
            };
            match msg_res {
                Ok(Message::Ping(payload)) => {
                    if !send_ws_pong(&mut write, payload, ws_url).await {
                        return WsSessionExit::Reconnect;
                    }
                }
                Ok(Message::Close(_)) => return WsSessionExit::Reconnect,
                Ok(msg) if msg.is_text() => {
                    if *self.config_rx.borrow() != symbol {
                        return WsSessionExit::ConfigChanged;
                    }
                    if let Ok(text) = msg.to_text() {
                        self.forward_ws_text_frame(text, symbol, event_times);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("WebSocket read error: {}", e);
                    return WsSessionExit::Reconnect;
                }
            }
        }
    }

    async fn run_with_url_and_timeout<F>(mut self, build_url: F, read_timeout: Duration)
    where
        F: Fn(&str) -> String,
    {
        let mut backoff_secs = 1u64;
        let mut previous_config = None;
        let mut event_times = EventTimes::default();
        'connect: loop {
            let symbol = self.config_rx.borrow_and_update().clone();
            if previous_config.as_ref() != Some(&symbol) {
                event_times = EventTimes::default();
                backoff_secs = 1;
                previous_config = Some(symbol.clone());
            }
            let ws_url = build_url(&symbol);
            info!("Starting Binance WebSocket stream for {}", ws_url);

            let connection = tokio::select! {
                result = tokio::time::timeout(
                    WS_CONNECT_TIMEOUT,
                    connect_async_with_config(&ws_url, Some(upstream_ws_config()), false),
                ) => result,
                changed = self.config_rx.changed() => {
                    if changed.is_err() { return; }
                    continue 'connect;
                }
            };
            match connection {
                Ok(Ok((ws_stream, _))) => {
                    info!("Connected to Binance WebSocket: {}", ws_url);
                    backoff_secs = 1;
                    match self
                        .read_ws_session(
                            ws_stream,
                            &ws_url,
                            &symbol,
                            read_timeout,
                            &mut event_times,
                        )
                        .await
                    {
                        WsSessionExit::Reconnect => {}
                        WsSessionExit::ConfigChanged => continue 'connect,
                        WsSessionExit::Shutdown => return,
                    }
                }
                Ok(Err(e)) => error!("Failed to connect to Binance WebSocket: {}", e),
                Err(_) => error!("Timed out connecting to Binance WebSocket: {}", ws_url),
            }
            warn!(
                "WebSocket disconnected. Reconnecting in {} seconds...",
                backoff_secs
            );
            crate::metrics::MARKET_WS_RECONNECTS.inc();
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
enum WsSessionExit {
    Reconnect,
    ConfigChanged,
    Shutdown,
}

#[cfg(test)]
mod tests;
