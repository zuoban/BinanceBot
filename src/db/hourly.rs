use super::beijing_midnight;
use crate::types::{HourlyTradeCount, HourlyTradeStats, TradeRecord, TradingMode};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

fn decimal_col(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Decimal> {
    Decimal::from_str(&row.get::<_, String>(index)?).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(err))
    })
}

#[derive(Default)]
pub(super) struct Rollup {
    pub(super) buys: i64,
    pub(super) sells: i64,
    pub(super) pnl: Decimal,
    pub(super) pending: i64,
    pub(super) latest: Option<DateTime<Utc>>,
}

impl Rollup {
    fn add(&mut self, timestamp: DateTime<Utc>, side: &str, pnl: Decimal, verified: bool) {
        self.buys = self.buys.saturating_add(i64::from(side == "BUY"));
        self.sells = self.sells.saturating_add(i64::from(side == "SELL"));
        if verified {
            self.pnl = self.pnl.saturating_add(pnl);
        } else {
            self.pending = self.pending.saturating_add(1);
        }
        self.latest = Some(
            self.latest
                .map_or(timestamp, |latest| latest.max(timestamp)),
        );
    }

    pub(super) fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(i64, Self)> {
        Ok((
            row.get(0)?,
            Self {
                buys: row.get(1)?,
                sells: row.get(2)?,
                pnl: decimal_col(row, 3)?,
                pending: row.get(4)?,
                latest: Some(row.get(5)?),
            },
        ))
    }

    fn add_row(&mut self, timestamp: DateTime<Utc>, row: &rusqlite::Row<'_>) -> Result<()> {
        let verified: bool = row.get(3)?;
        let pnl = if verified {
            Decimal::from_str(&row.get::<_, String>(2)?)?
        } else {
            Decimal::ZERO
        };
        self.add(timestamp, &row.get::<_, String>(1)?, pnl, verified);
        Ok(())
    }
}

#[derive(Default)]
struct PnlRollup {
    pnl: Decimal,
    commission: Decimal,
    pending: i64,
}

fn load_pnl_on(conn: &Connection, symbol: &str, mode: &str) -> Result<PnlRollup> {
    Ok(conn
        .prepare_cached(
            "SELECT realized_pnl, commission, pending_pnl_count FROM trade_pnl_rollups WHERE symbol = ?1 AND mode = ?2",
        )?
        .query_row(params![symbol, mode], |row| {
            Ok(PnlRollup {
                pnl: decimal_col(row, 0)?,
                commission: decimal_col(row, 1)?,
                pending: row.get(2)?,
            })
        })
        .optional()?
        .unwrap_or_default())
}

fn save_pnl_on(conn: &Connection, symbol: &str, mode: &str, value: &PnlRollup) -> Result<()> {
    let pnl = value.pnl.to_string();
    let fee = value.commission.to_string();
    conn.prepare_cached(
        "INSERT INTO trade_pnl_rollups (symbol, mode, realized_pnl, commission, pending_pnl_count)
         VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(symbol, mode) DO UPDATE SET
         realized_pnl = excluded.realized_pnl, commission = excluded.commission, pending_pnl_count = excluded.pending_pnl_count",
    )?
    .execute(params![symbol, mode, pnl, fee, value.pending])?;
    Ok(())
}

fn migrate_pnl_on(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS trade_pnl_rollups (
             symbol TEXT NOT NULL, mode TEXT NOT NULL, realized_pnl TEXT NOT NULL,
             commission TEXT NOT NULL, pending_pnl_count INTEGER NOT NULL, PRIMARY KEY (symbol, mode)
         );
         CREATE TABLE IF NOT EXISTS trade_pnl_rollup_state (id INTEGER PRIMARY KEY CHECK (id = 1));",
    )?;
    if conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM trade_pnl_rollup_state WHERE id = 1)",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        return Ok(());
    }
    conn.execute("DELETE FROM trade_pnl_rollups", [])?;
    let mut rollups: HashMap<(String, String), PnlRollup> = HashMap::new();
    {
        let mut statement = conn
            .prepare("SELECT symbol, mode, realized_pnl, commission, pnl_verified FROM trades")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let symbol: String = row.get(0)?;
            let mode: String = row.get(1)?;
            let verified: bool = row.get(4)?;
            let entry = rollups.entry((symbol, mode)).or_default();
            if verified {
                entry.pnl = entry
                    .pnl
                    .saturating_add(Decimal::from_str(&row.get::<_, String>(2)?)?);
                entry.commission = entry
                    .commission
                    .saturating_add(Decimal::from_str(&row.get::<_, String>(3)?)?);
            } else {
                entry.pending = entry.pending.saturating_add(1);
            }
        }
    }
    for ((symbol, mode), rollup) in rollups {
        save_pnl_on(conn, &symbol, &mode, &rollup)?;
    }
    conn.execute("INSERT INTO trade_pnl_rollup_state (id) VALUES (1)", [])?;
    Ok(())
}

pub(super) fn pnl_totals_on(
    conn: &Connection,
    symbol: &str,
    mode: TradingMode,
) -> Result<(Decimal, Decimal, usize)> {
    let rollup = load_pnl_on(conn, symbol, mode.as_str())?;
    Ok((
        rollup.pnl,
        rollup.commission,
        rollup.pending.max(0) as usize,
    ))
}
pub(super) fn hour(timestamp: DateTime<Utc>) -> i64 {
    timestamp.timestamp().div_euclid(3600) * 3600
}

// Called after legacy modes have been normalized, within the schema transaction.
// The durable marker makes restart independent of the size of retained history.
pub(super) fn migrate_on(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS trade_hourly_rollups (
             symbol TEXT NOT NULL, mode TEXT NOT NULL, hour INTEGER NOT NULL,
             buy_count INTEGER NOT NULL, sell_count INTEGER NOT NULL, realized_pnl TEXT NOT NULL,
             pending_pnl_count INTEGER NOT NULL, latest_timestamp TEXT NOT NULL,
             PRIMARY KEY (symbol, mode, hour)
         );
         CREATE TABLE IF NOT EXISTS trade_hourly_rollup_state (id INTEGER PRIMARY KEY CHECK (id = 1));",
    )?;
    if conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM trade_hourly_rollup_state WHERE id = 1)",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        return migrate_pnl_on(conn);
    }
    if !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('trades') WHERE name = 'stats_hour')",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        conn.execute_batch("ALTER TABLE trades ADD COLUMN stats_hour INTEGER;")?;
    }
    conn.execute("DELETE FROM trade_hourly_rollups", [])?;
    let mut rollups: HashMap<(String, String, i64), Rollup> = HashMap::new();
    {
        let mut statement = conn.prepare(
            "SELECT timestamp, side, realized_pnl, pnl_verified, trade_id, symbol, mode FROM trades",
        )?;
        let mut update = conn.prepare("UPDATE trades SET stats_hour = ?1 WHERE trade_id = ?2")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let timestamp: DateTime<Utc> = row.get(0)?;
            let bucket = hour(timestamp);
            rollups
                .entry((row.get(5)?, row.get(6)?, bucket))
                .or_default()
                .add_row(timestamp, row)?;
            update.execute(params![bucket, row.get::<_, String>(4)?])?;
        }
    }
    for ((symbol, mode, bucket), rollup) in rollups {
        save_on(conn, &symbol, &mode, bucket, &rollup)?;
    }
    // The integer hour also gives historical/future queries an exact, indexed
    // boundary-hour scan independent of timestamp text format or UTC offset.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_trades_stats_hour ON trades (symbol, mode, stats_hour);
         INSERT INTO trade_hourly_rollup_state (id) VALUES (1);",
    )?;
    migrate_pnl_on(conn)
}

fn load_on(conn: &Connection, symbol: &str, mode: &str, bucket: i64) -> Result<Rollup> {
    Ok(conn
        .prepare_cached(
            "SELECT hour, buy_count, sell_count, realized_pnl, pending_pnl_count, latest_timestamp
             FROM trade_hourly_rollups WHERE symbol = ?1 AND mode = ?2 AND hour = ?3",
        )?
        .query_row(params![symbol, mode, bucket], Rollup::from_row)
        .optional()?
        .map_or_else(Rollup::default, |(_, rollup)| rollup))
}

fn save_on(conn: &Connection, symbol: &str, mode: &str, bucket: i64, v: &Rollup) -> Result<()> {
    let latest = v.latest.context("Hourly rollup has no timestamp")?;
    let pnl = v.pnl.to_string();
    conn.prepare_cached(
        "INSERT INTO trade_hourly_rollups (symbol, mode, hour, buy_count, sell_count, realized_pnl, pending_pnl_count, latest_timestamp)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(symbol, mode, hour) DO UPDATE SET
         buy_count = excluded.buy_count, sell_count = excluded.sell_count,
         realized_pnl = excluded.realized_pnl, pending_pnl_count = excluded.pending_pnl_count,
         latest_timestamp = excluded.latest_timestamp",
    )?
    .execute(params![symbol, mode, bucket, v.buys, v.sells, pnl, v.pending, latest])?;
    Ok(())
}

pub(super) fn insert_on(conn: &Connection, trade: &TradeRecord) -> Result<()> {
    let bucket = hour(trade.timestamp);
    let mut rollup = load_on(conn, &trade.symbol, trade.mode.as_str(), bucket)?;
    rollup.add(
        trade.timestamp,
        trade.side.as_str(),
        trade.realized_pnl,
        trade.pnl_verified,
    );
    save_on(conn, &trade.symbol, trade.mode.as_str(), bucket, &rollup)?;
    let mut pnl_rollup = load_pnl_on(conn, &trade.symbol, trade.mode.as_str())?;
    if trade.pnl_verified {
        pnl_rollup.pnl = pnl_rollup.pnl.saturating_add(trade.realized_pnl);
        pnl_rollup.commission = pnl_rollup.commission.saturating_add(trade.commission);
    } else {
        pnl_rollup.pending = pnl_rollup.pending.saturating_add(1);
    }
    save_pnl_on(conn, &trade.symbol, trade.mode.as_str(), &pnl_rollup)
}

pub(super) fn verify_on(
    conn: &Connection,
    symbol: &str,
    mode: TradingMode,
    timestamp: DateTime<Utc>,
    pnl: Decimal,
    commission: Decimal,
) -> Result<()> {
    let bucket = hour(timestamp);
    let mut rollup = load_on(conn, symbol, mode.as_str(), bucket)?;
    anyhow::ensure!(rollup.pending > 0, "Missing pending trade in hourly rollup");
    rollup.pending -= 1;
    rollup.pnl = rollup.pnl.saturating_add(pnl);
    save_on(conn, symbol, mode.as_str(), bucket, &rollup)?;
    let mut pnl_rollup = load_pnl_on(conn, symbol, mode.as_str())?;
    anyhow::ensure!(
        pnl_rollup.pending > 0,
        "Missing pending trade in PnL rollup"
    );
    pnl_rollup.pending -= 1;
    pnl_rollup.pnl = pnl_rollup.pnl.saturating_add(pnl);
    pnl_rollup.commission = pnl_rollup.commission.saturating_add(commission);
    save_pnl_on(conn, symbol, mode.as_str(), &pnl_rollup)
}
pub(super) fn stats_on(
    conn: &Connection,
    symbol: &str,
    mode: TradingMode,
    now: DateTime<Utc>,
) -> Result<HourlyTradeStats> {
    let window_start = beijing_midnight(now);
    let first_hour = window_start.timestamp();
    let last_hour = hour(now);
    let mut result = HourlyTradeStats {
        window_start,
        window_end: now,
        buckets: (first_hour..=last_hour)
            .step_by(3600)
            .filter_map(|timestamp| {
                DateTime::from_timestamp(timestamp, 0).map(|hour_start| HourlyTradeCount {
                    hour_start,
                    buy_count: 0,
                    sell_count: 0,
                })
            })
            .collect(),
        realized_pnl: Decimal::ZERO,
        pending_pnl_count: 0,
    };
    let mut statement = conn.prepare_cached(
        "SELECT hour, buy_count, sell_count, realized_pnl, pending_pnl_count, latest_timestamp
         FROM trade_hourly_rollups
         WHERE symbol = ?1 AND mode = ?2 AND hour >= ?3 AND hour <= ?4 ORDER BY hour",
    )?;
    let mut rows = statement.query(params![symbol, mode.as_str(), first_hour, last_hour])?;
    while let Some(row) = rows.next()? {
        let (bucket, stored) = Rollup::from_row(row)?;
        let value = if stored.latest.is_some_and(|latest| latest <= now) {
            stored
        } else {
            load_boundary_rollup_on(conn, symbol, mode.as_str(), bucket, window_start, now, true)?
        };
        if let Some(slot) = usize::try_from((bucket - first_hour) / 3600)
            .ok()
            .and_then(|i| result.buckets.get_mut(i))
        {
            slot.buy_count = value.buys.max(0) as u64;
            slot.sell_count = value.sells.max(0) as u64;
        }
        result.realized_pnl = result.realized_pnl.saturating_add(value.pnl);
        result.pending_pnl_count = result
            .pending_pnl_count
            .saturating_add(value.pending.max(0) as u64);
    }
    Ok(result)
}

#[derive(Default)]
pub(super) struct ScopedWindowRollup {
    pub(super) buys: u64,
    pub(super) sells: u64,
    pub(super) realized_pnl: Decimal,
    pub(super) pending_pnl_count: u64,
}

fn is_unaligned_hour(t: DateTime<Utc>) -> bool {
    t.timestamp() % 3600 != 0 || t.timestamp_subsec_nanos() != 0
}

pub(super) fn scan_window_rollups_on(
    conn: &Connection,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    inclusive_end: bool,
) -> Result<BTreeMap<(String, String), ScopedWindowRollup>> {
    if end < start || (!inclusive_end && end == start) {
        return Ok(BTreeMap::new());
    }
    let (start_unaligned, end_unaligned) = (is_unaligned_hour(start), is_unaligned_hour(end));
    let first_hour = hour(start);
    let last_hour = if inclusive_end || end_unaligned {
        hour(end)
    } else {
        hour(end).saturating_sub(3600)
    };
    let mut stmt = conn.prepare_cached(
        "SELECT hour, buy_count, sell_count, realized_pnl, pending_pnl_count, latest_timestamp, symbol, mode
         FROM trade_hourly_rollups WHERE hour >= ?1 AND hour <= ?2",
    )?;
    let mut rows = stmt.query(params![first_hour, last_hour])?;
    let mut groups: BTreeMap<(String, String), ScopedWindowRollup> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let (bucket, stored) = Rollup::from_row(row)?;
        let (symbol, mode): (String, String) = (row.get(6)?, row.get(7)?);
        let latest_outside =
            stored
                .latest
                .is_none_or(|t| if inclusive_end { t > end } else { t >= end });
        let scan_boundary = (start_unaligned && bucket == first_hour)
            || (!inclusive_end && end_unaligned && bucket == last_hour)
            || latest_outside;
        let v = if scan_boundary {
            load_boundary_rollup_on(conn, &symbol, &mode, bucket, start, end, inclusive_end)?
        } else {
            stored
        };
        let (buys, sells, pending) = (
            v.buys.max(0) as u64,
            v.sells.max(0) as u64,
            v.pending.max(0) as u64,
        );
        if buys == 0 && sells == 0 && pending == 0 && v.pnl.is_zero() {
            continue;
        }
        let entry = groups.entry((mode, symbol)).or_default();
        entry.buys = entry.buys.saturating_add(buys);
        entry.sells = entry.sells.saturating_add(sells);
        entry.realized_pnl = entry.realized_pnl.saturating_add(v.pnl);
        entry.pending_pnl_count = entry.pending_pnl_count.saturating_add(pending);
    }
    Ok(groups)
}

pub(super) fn today_pnl_on(
    conn: &Connection,
    now: DateTime<Utc>,
) -> Result<crate::types::TodayPnl> {
    let window_start = beijing_midnight(now);
    let groups = scan_window_rollups_on(conn, window_start, now, true)?
        .into_iter()
        .map(|((mode, symbol), rollup)| crate::types::TodayPnlGroup {
            symbol,
            mode: TradingMode::from_db(&mode),
            realized_pnl: rollup.realized_pnl,
            pending_pnl_count: rollup.pending_pnl_count,
        })
        .collect();
    Ok(crate::types::TodayPnl {
        window_start,
        window_end: now,
        groups,
    })
}

/// Only boundary hours can contain entries outside the requested bounds.
/// Parse timestamps before comparing so nanoseconds remain exact.
pub(super) fn load_boundary_rollup_on(
    conn: &Connection,
    symbol: &str,
    mode: &str,
    bucket: i64,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    inclusive_end: bool,
) -> Result<Rollup> {
    let mut boundary = conn.prepare_cached(
        "SELECT timestamp, side, realized_pnl, pnl_verified FROM trades
         WHERE symbol = ?1 AND mode = ?2 AND stats_hour = ?3",
    )?;
    let mut entries = boundary.query(params![symbol, mode, bucket])?;
    let mut value = Rollup::default();
    while let Some(entry) = entries.next()? {
        let timestamp: DateTime<Utc> = entry.get(0)?;
        let before_end = if inclusive_end {
            timestamp <= window_end
        } else {
            timestamp < window_end
        };
        if timestamp >= window_start && before_end {
            value.add_row(timestamp, entry)?;
        }
    }
    Ok(value)
}

#[cfg(test)]
#[path = "hourly_tests.rs"]
mod tests;
