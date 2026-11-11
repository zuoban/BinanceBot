use super::Candle;
use anyhow::{bail, ensure, Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::time::Duration;

const SUPPORTED_KLINE_INTERVALS: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M",
];
pub(super) const MAX_BACKTEST_DAYS: u32 = 3650;
const MAX_KLINE_SPAN_MS: i64 = MAX_BACKTEST_DAYS as i64 * 24 * 60 * 60 * 1000;

pub(super) fn validate_kline_interval(interval: &str) -> Result<()> {
    ensure!(
        SUPPORTED_KLINE_INTERVALS.contains(&interval),
        "不支持的 K 线周期: {interval}（支持: {}）",
        SUPPORTED_KLINE_INTERVALS.join(", ")
    );
    Ok(())
}

/// `time_ms,price` rows are ticks; `time_ms,open,high,low,close[,...]` rows are candles
/// (Binance kline CSV exports work as-is). A leading non-numeric row is treated as a header.
pub(crate) fn parse_csv(text: &str) -> Result<Vec<Candle>> {
    let mut candles = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut cols = line.split(',').map(str::trim);
        let first = cols.next().unwrap_or_default();
        if candles.is_empty() && first.parse::<i64>().is_err() {
            continue;
        }
        let time_ms: i64 = first
            .parse()
            .with_context(|| format!("第 {} 行：时间必须是毫秒时间戳", index + 1))?;
        let parse_price = |raw: &str| -> Result<Decimal> {
            Decimal::from_str(raw)
                .with_context(|| format!("第 {} 行：无法解析价格 {:?}", index + 1, raw))
        };
        let Some(c1) = cols.next() else {
            bail!(
                "第 {} 行：需要 2 列（时间,价格）或至少 5 列（时间,开,高,低,收），实际 1 列",
                index + 1
            );
        };
        match cols.next() {
            None => candles.push(Candle::tick(time_ms, parse_price(c1)?)),
            Some(c2) => {
                let c3 = cols.next();
                let c4 = cols.next();
                match (c3, c4) {
                    (Some(c3), Some(c4)) => candles.push(Candle {
                        time_ms,
                        open: parse_price(c1)?,
                        high: parse_price(c2)?,
                        low: parse_price(c3)?,
                        close: parse_price(c4)?,
                    }),
                    (Some(_), None) => bail!(
                        "第 {} 行：需要 2 列（时间,价格）或至少 5 列（时间,开,高,低,收），实际 4 列",
                        index + 1
                    ),
                    (None, _) => bail!(
                        "第 {} 行：需要 2 列（时间,价格）或至少 5 列（时间,开,高,低,收），实际 3 列",
                        index + 1
                    ),
                }
            }
        }
    }
    Ok(candles)
}

pub(crate) async fn fetch_klines(
    symbol: &str,
    interval: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<Candle>> {
    crate::config::validate_symbol(symbol)?;
    validate_kline_interval(interval)?;
    ensure!(
        end_ms > start_ms && end_ms.saturating_sub(start_ms) <= MAX_KLINE_SPAN_MS,
        "K 线时间范围必须在 1..={MAX_BACKTEST_DAYS} 天之间"
    );
    let client = crate::exchange::client::shared_http_client();
    let end_str = end_ms.to_string();
    let mut start = start_ms;
    let mut candles: Vec<Candle> = Vec::new();
    loop {
        let start_str = start.to_string();
        let rows: Vec<Vec<serde_json::Value>> = client
            .get("https://fapi.binance.com/fapi/v1/klines")
            .timeout(Duration::from_secs(15))
            .query(&[
                ("symbol", symbol),
                ("interval", interval),
                ("startTime", start_str.as_str()),
                ("endTime", end_str.as_str()),
                ("limit", "1500"),
            ])
            .send()
            .await
            .map_err(reqwest::Error::without_url)?
            .error_for_status()
            .map_err(reqwest::Error::without_url)
            .context("K 线请求失败，请检查交易对和周期")?
            .json()
            .await
            .map_err(reqwest::Error::without_url)?;
        let received = rows.len();
        for row in rows {
            let price = |position: usize| -> Result<Decimal> {
                Decimal::from_str(
                    row.get(position)
                        .and_then(|v| v.as_str())
                        .unwrap_or_default(),
                )
                .context("无法解析 K 线价格")
            };
            candles.push(Candle {
                time_ms: row
                    .first()
                    .and_then(|v| v.as_i64())
                    .context("无法解析 K 线时间")?,
                open: price(1)?,
                high: price(2)?,
                low: price(3)?,
                close: price(4)?,
            });
        }
        match candles.last() {
            Some(last) if received >= 1500 => start = last.time_ms.saturating_add(1),
            _ => break,
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    Ok(candles)
}
