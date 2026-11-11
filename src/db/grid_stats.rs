use super::{hourly, Database};
use crate::types::{GridStats, HourlyTradeStats, TradingMode};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::Decimal;
use std::str::FromStr;
#[cfg(test)]
use std::sync::atomic::Ordering;

impl Database {
    #[cfg(test)]
    pub(crate) fn hourly_stats_query_count(&self) -> u64 {
        self.hourly_stats_queries.load(Ordering::Relaxed)
    }

    /// Count today's persisted orders from Beijing midnight to now, including empty hours.
    pub(crate) fn get_hourly_trade_stats(
        &self,
        symbol: &str,
        mode: TradingMode,
        now: DateTime<Utc>,
    ) -> Result<HourlyTradeStats> {
        #[cfg(test)]
        self.hourly_stats_queries.fetch_add(1, Ordering::Relaxed);
        let conn = self.lock_connection("get_hourly_trade_stats");
        hourly::stats_on(&conn, symbol, mode, now)
    }

    pub fn get_pnl_totals(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<(Decimal, Decimal, usize)> {
        let conn = self.lock_connection("get_pnl_totals");
        hourly::pnl_totals_on(&conn, symbol, mode)
    }

    pub(crate) fn load_scoped_stats(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<Option<(usize, usize, Decimal, Decimal)>> {
        let conn = self.lock_connection("load_scoped_stats");
        let mut stmt = conn.prepare_cached(
            "SELECT total_trades, completed_cycles, total_realized_profit, total_volume_usdc FROM scoped_grid_stats WHERE symbol = ?1 AND mode = ?2;",
        )?;
        let row = stmt
            .query_row(params![symbol, mode.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .optional()?;
        row.map(parse_stats_row).transpose()
    }

    /// Legacy un-scoped grid stats accessor retained for backward-compatibility tests.
    pub fn load_stats(&self) -> Result<Option<(usize, usize, Decimal, Decimal)>> {
        let conn = self.lock_connection("load_stats");
        conn.query_row(
            "SELECT total_trades, completed_cycles, total_realized_profit, total_volume_usdc FROM grid_stats WHERE id = 1;",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?
        .map(parse_stats_row)
        .transpose()
    }

    /// Legacy un-scoped grid stats writer retained for backward-compatibility tests.
    pub fn save_stats(&self, stats: &GridStats) -> Result<()> {
        let conn = self.lock_connection("save_stats");
        conn.execute(
            "INSERT INTO grid_stats (id, total_trades, completed_cycles, total_realized_profit, total_volume_usdc, updated_at)
             VALUES (1, ?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET
                 total_trades = excluded.total_trades, completed_cycles = excluded.completed_cycles,
                 total_realized_profit = excluded.total_realized_profit,
                 total_volume_usdc = excluded.total_volume_usdc, updated_at = excluded.updated_at;",
            params![
                i64::try_from(stats.total_trades).unwrap_or(i64::MAX),
                i64::try_from(stats.completed_cycles).unwrap_or(i64::MAX),
                stats.total_realized_profit.to_string(),
                stats.total_volume_usdc.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )
        .context("Failed to save stats to SQLite database")?;
        Ok(())
    }
}

fn parse_stats_row(
    (trades, cycles, profit, volume): (i64, i64, String, String),
) -> Result<(usize, usize, Decimal, Decimal)> {
    Ok((
        usize::try_from(trades.max(0)).unwrap_or(usize::MAX),
        usize::try_from(cycles.max(0)).unwrap_or(usize::MAX),
        Decimal::from_str(&profit)?,
        Decimal::from_str(&volume)?,
    ))
}

pub(super) fn save_scoped_stats_on(
    conn: &Connection,
    symbol: &str,
    mode: TradingMode,
    stats: &GridStats,
) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO scoped_grid_stats (symbol, mode, total_trades, completed_cycles, total_realized_profit, total_volume_usdc, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(symbol, mode) DO UPDATE SET total_trades = excluded.total_trades, completed_cycles = excluded.completed_cycles, total_realized_profit = excluded.total_realized_profit, total_volume_usdc = excluded.total_volume_usdc, updated_at = excluded.updated_at;",
    )?
    .execute(params![
        symbol,
        mode.as_str(),
        i64::try_from(stats.total_trades).unwrap_or(i64::MAX),
        i64::try_from(stats.completed_cycles).unwrap_or(i64::MAX),
        stats.total_realized_profit.to_string(),
        stats.total_volume_usdc.to_string(),
        Utc::now().to_rfc3339()
    ])?;
    Ok(())
}
