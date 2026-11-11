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
const MAX_BACKTEST_CANDLES: usize = 1_500_000;

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

pub(super) fn next_kline_page_start(
    start: i64,
    received: usize,
    last: Option<&Candle>,
    end_ms: i64,
) -> Result<Option<i64>> {
    match last {
        Some(last) if received >= 1500 && last.time_ms < end_ms => {
            let next_start = last.time_ms.saturating_add(1);
            ensure!(next_start > start, "K 线分页游标未推进");
            Ok(Some(next_start))
        }
        _ => Ok(None),
    }
}

struct KlineRow(Candle);

impl<'de> serde::Deserialize<'de> for KlineRow {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = KlineRow;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a Binance kline array [time_ms, open, high, low, close, ...]")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                use serde::de::Error;
                let time_ms: i64 = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let open: Decimal = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(1, &self))?;
                let high: Decimal = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(2, &self))?;
                let low: Decimal = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(3, &self))?;
                let close: Decimal = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(4, &self))?;
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(KlineRow(Candle {
                    time_ms,
                    open,
                    high,
                    low,
                    close,
                }))
            }
        }

        deserializer.deserialize_seq(Visitor)
    }
}

pub(crate) async fn fetch_klines(
    symbol: &str,
    interval: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<Candle>> {
    fetch_klines_from_url(
        "https://fapi.binance.com/fapi/v1/klines",
        symbol,
        interval,
        start_ms,
        end_ms,
    )
    .await
}

pub(super) async fn fetch_klines_from_url(
    url: &str,
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
        let resp = client
            .get(url)
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
            .context("K 线请求失败，请检查交易对和周期")?;
        let rows =
            crate::exchange::client::read_bounded_json::<Vec<KlineRow>>(resp, 4 * 1024 * 1024)
                .await
                .context("无法解析 K 线响应")?;
        let received = rows.len();
        candles.extend(rows.into_iter().map(|row| row.0));
        ensure!(
            candles.len() <= MAX_BACKTEST_CANDLES,
            "K 线数量超过上限 ({MAX_BACKTEST_CANDLES})"
        );
        let Some(next_start) = next_kline_page_start(start, received, candles.last(), end_ms)?
        else {
            break;
        };
        ensure!(next_start > start, "K 线分页游标未推进");
        start = next_start;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    Ok(candles)
}

#[cfg(test)]
mod kline_row_tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn kline_row_deserializes_binance_12_column_arrays_and_rejects_short_rows() {
        let json = r#"[
            [1700000000000,"100.10","105.50","99.25","103.75","1234.5",1700000059999,"126000.0",42,"600.0","61200.0","0"],
            [1700000060000,"103.75","104.00","101.00","102.00"]
        ]"#;
        let rows: Vec<KlineRow> = serde_json::from_str(json).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].0,
            Candle {
                time_ms: 1_700_000_000_000,
                open: dec!(100.10),
                high: dec!(105.50),
                low: dec!(99.25),
                close: dec!(103.75),
            }
        );
        assert_eq!(rows[1].0.close, dec!(102.00));
        assert!(
            serde_json::from_str::<Vec<KlineRow>>(r#"[[1700000000000,"100","101","99"]]"#).is_err()
        );
    }
}
