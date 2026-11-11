use super::{
    config_runtime::save_pair_status_on, hourly, migrations::has_legacy_paper_history_on, Database,
};
use crate::types::{
    AccountInfo, BotStatus, GridOrder, GridStats, HourlyTradeStats, OrderSide, PositionInfo,
    TradeRecord, TradingMode,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::Decimal;
use std::str::FromStr;
#[cfg(test)]
use std::sync::atomic::Ordering;

impl Database {
    pub(crate) fn trade_data_revision(&self) -> u64 {
        self.trade_revisions.current()
    }

    #[cfg(test)]
    fn before_revision_publish(&self) {
        if let Some((reached, resume)) = self.revision_publish_hook.lock().unwrap().as_ref() {
            reached.send(()).unwrap();
            resume.recv().unwrap();
        }
    }

    pub(crate) fn symbol_trade_revision(&self, symbol: &str) -> u64 {
        self.trade_revisions.for_symbol(symbol)
    }

    fn publish_trade_revision(&self, symbol: &str) {
        #[cfg(test)]
        self.before_revision_publish();
        self.trade_revisions.publish(symbol);
    }

    #[cfg(test)]
    pub(crate) fn hourly_stats_query_count(&self) -> u64 {
        self.hourly_stats_queries.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn insert_trade(&self, trade: &TradeRecord) -> Result<bool> {
        let mut conn = self.lock_connection("insert_trade");
        // Acquire write intent before deduplication reads so contention can use
        // SQLite's busy handler instead of failing a deferred lock upgrade.
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let inserted = insert_trade_on(&tx, trade)?;
        tx.commit()?;
        if inserted {
            if matches!(trade.mode, TradingMode::Paper | TradingMode::Unknown) {
                if let Some(ledger) = self
                    .paper_ledger
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_mut()
                {
                    ledger.record_legacy(&trade.symbol);
                }
            }
            self.publish_trade_revision(&trade.symbol);
        }
        Ok(inserted)
    }

    /// Delete analysis history atomically while preserving order deduplication
    /// and the execution frontier needed by a running or restarted strategy.
    #[cfg(test)]
    pub(crate) fn clear_trade_history(&self) -> Result<usize> {
        Ok(self.clear_selected_database(true, false)?.0)
    }

    pub(crate) fn clear_selected_database(
        &self,
        trades: bool,
        snapshots: bool,
    ) -> Result<(usize, usize)> {
        let mut conn = self.lock_connection("clear_selected_database");
        let tx = conn.transaction()?;
        let deleted_trades = if trades {
            clear_trades_on(&tx, None)?
        } else {
            0
        };
        let deleted_snapshots = if snapshots {
            let hourly = tx.execute("DELETE FROM account_hourly_snapshots", [])?;
            let daily = tx.execute("DELETE FROM daily_snapshots", [])?;
            hourly + daily
        } else {
            0
        };
        tx.commit()?;
        if trades {
            #[cfg(test)]
            self.before_revision_publish();
            self.trade_revisions.clear_all();
            self.trade_clear_revisions.clear_all();
        }
        Ok((deleted_trades, deleted_snapshots))
    }

    /// Clear every mode for one symbol while retaining recovery metadata.
    pub(crate) fn clear_trade_history_for_symbol(&self, symbol: &str) -> Result<usize> {
        let mut conn = self.lock_connection("clear_trade_history_for_symbol");
        let tx = conn.transaction()?;
        let deleted = clear_trades_on(&tx, Some(symbol))?;
        tx.commit()?;
        self.publish_trade_revision(symbol);
        self.trade_clear_revisions.publish(symbol);
        Ok(deleted)
    }

    pub fn insert_trade_with_stats(&self, trade: &TradeRecord, stats: &GridStats) -> Result<bool> {
        self.insert_trade_with_paper_recovery(trade, stats, None, None, None)
    }

    #[cfg(test)]
    pub(crate) fn insert_trade_with_recovery(
        &self,
        trade: &TradeRecord,
        stats: &GridStats,
        pair_intent: Option<&GridOrder>,
        consumed_pair_source: Option<&str>,
    ) -> Result<bool> {
        self.insert_trade_with_paper_recovery(trade, stats, pair_intent, consumed_pair_source, None)
    }

    pub(crate) fn insert_trade_with_paper_recovery(
        &self,
        trade: &TradeRecord,
        stats: &GridStats,
        pair_intent: Option<&GridOrder>,
        consumed_pair_source: Option<&str>,
        paper_state: Option<&(PositionInfo, AccountInfo)>,
    ) -> Result<bool> {
        anyhow::ensure!(
            paper_state.is_none() || trade.mode == TradingMode::Paper,
            "Paper state requires a paper trade"
        );
        let mut conn = self.lock_connection("insert_trade_with_recovery");
        let tx = conn.transaction()?;
        let inserted = insert_trade_on(&tx, trade)?;
        if inserted {
            save_scoped_stats_on(&tx, &trade.symbol, trade.mode, stats)?;
            if let Some((position, account)) = paper_state {
                tx.execute(
                    "INSERT INTO paper_accounts (symbol, position_json, account_json) VALUES (?1, ?2, ?3) \
                     ON CONFLICT(symbol) DO UPDATE SET position_json = excluded.position_json, account_json = excluded.account_json",
                    params![trade.symbol, serde_json::to_string(position)?, serde_json::to_string(account)?],
                )?;
            }
            if let Some(intent) = pair_intent {
                let parent = intent
                    .paired_client_order_id
                    .as_deref()
                    .context("Pair intent is missing its source order")?;
                tx.execute(
                    "INSERT INTO pair_intents (parent_client_order_id, symbol, mode, order_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5);",
                    params![parent, trade.symbol, trade.mode.as_str(), serde_json::to_string(intent)?, Utc::now().to_rfc3339()],
                )?;
            }
        }
        if trade.mode != TradingMode::Paper {
            tx.execute(
                "DELETE FROM managed_orders WHERE client_order_id = ?1;",
                params![trade.client_order_id],
            )?;
        }
        if let Some(parent) = consumed_pair_source {
            tx.execute(
                "DELETE FROM pair_intents WHERE parent_client_order_id = ?1;",
                params![parent],
            )?;
        }
        // `conn` remains locked until after `paper_ledger` updates, so no reader can
        // observe the committed SQLite transaction before the cached contribution lands,
        // while async readers never block on SQLite `fsync` during `tx.commit()`.
        tx.commit()?;
        if inserted {
            if matches!(trade.mode, TradingMode::Paper | TradingMode::Unknown) {
                if let Some(ledger) = self
                    .paper_ledger
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_mut()
                {
                    if let Some((position, account)) = paper_state {
                        ledger.commit(&trade.symbol, position, account);
                    } else {
                        ledger.record_legacy(&trade.symbol);
                    }
                }
            }
            self.publish_trade_revision(&trade.symbol);
        }
        Ok(inserted)
    }

    pub(crate) fn load_paper_state(
        &self,
        symbol: &str,
    ) -> Result<Option<(PositionInfo, AccountInfo)>> {
        let conn = self.lock_connection("load_paper_state");
        let saved: Option<(String, String)> = conn
            .query_row(
                "SELECT position_json, account_json FROM paper_accounts WHERE symbol = ?1",
                params![symbol],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((position, account)) = saved {
            return Ok(Some((
                serde_json::from_str(&position)?,
                serde_json::from_str(&account)?,
            )));
        }
        // Older versions never stored the account. Cleared history and earlier
        // restarts make replay ambiguous, so do not invent a balance or position.
        let legacy = has_legacy_paper_history_on(&conn, symbol)?;
        anyhow::ensure!(
            !legacy,
            "旧模拟盘状态无法从成交历史可靠恢复：{}，请先人工核对并迁移仓位和余额",
            symbol
        );
        Ok(None)
    }

    /// Explicitly establish a new paper baseline; this cannot recover old holdings.
    pub fn initialize_legacy_paper_account(&self, symbol: &str) -> Result<()> {
        let mut conn = self.lock_connection("initialize_legacy_paper_account");
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM paper_accounts WHERE symbol = ?1)",
            params![symbol],
            |row| row.get(0),
        )?;
        anyhow::ensure!(!exists, "该交易对已有模拟盘状态，拒绝覆盖");
        anyhow::ensure!(
            has_legacy_paper_history_on(&tx, symbol)?,
            "该交易对没有需要迁移的旧模拟盘记录"
        );
        let position = PositionInfo {
            symbol: symbol.to_string(),
            ..PositionInfo::default()
        };
        let account = AccountInfo::initial_paper(symbol, Some(Utc::now()));
        tx.execute(
            "INSERT INTO paper_accounts (symbol, position_json, account_json) VALUES (?1, ?2, ?3)",
            params![
                symbol,
                serde_json::to_string(&position)?,
                serde_json::to_string(&account)?
            ],
        )?;
        save_pair_status_on(&tx, symbol, TradingMode::Paper, BotStatus::Paused)?;
        tx.commit()?;
        if let Some(ledger) = self
            .paper_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
        {
            ledger.commit(symbol, &position, &account);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_trade_by_client_id(
        &self,
        client_order_id: &str,
        mode: TradingMode,
    ) -> Result<Option<TradeRecord>> {
        let conn = self.lock_connection("get_trade_by_client_id");
        let mut stmt = conn.prepare(select_trades_sql!(
            "WHERE client_order_id = ?1 AND mode = ?2 ORDER BY timestamp DESC LIMIT 1;"
        ))?;
        let mut rows = stmt.query(params![client_order_id, mode.as_str()])?;
        rows.next()?
            .map(trade_from_row)
            .transpose()
            .map_err(Into::into)
    }

    /// Look up the data required by a paired cycle, including cleared history.
    pub(crate) fn get_pair_opening(
        &self,
        symbol: &str,
        client_order_id: &str,
        mode: TradingMode,
    ) -> Result<Option<(OrderSide, Decimal, Decimal)>> {
        let conn = self.lock_connection("get_pair_opening");
        let value: Option<(String, String, String)> = conn
            .prepare_cached(
                "SELECT side, price, quantity FROM (
                     SELECT * FROM (
                         SELECT side, price, quantity, 0 AS priority, timestamp FROM trades
                         WHERE symbol = ?1 AND client_order_id = ?2 AND mode = ?3
                         ORDER BY timestamp DESC LIMIT 1
                     )
                     UNION ALL
                     SELECT side, price, quantity, 1 AS priority, '' AS timestamp FROM cleared_pair_openings
                     WHERE symbol = ?1 AND client_order_id = ?2 AND mode = ?3
                 ) ORDER BY priority, timestamp DESC LIMIT 1",
            )?
            .query_row(params![symbol, client_order_id, mode.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()?;
        value
            .map(|(side, price, quantity)| {
                Ok((
                    parse_order_side(&side)?,
                    Decimal::from_str(&price)?,
                    Decimal::from_str(&quantity)?,
                ))
            })
            .transpose()
    }

    pub(crate) fn has_recorded_order(
        &self,
        client_order_id: &str,
        mode: TradingMode,
    ) -> Result<bool> {
        let conn = self.lock_connection("has_recorded_order");
        let mut stmt = conn.prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM trades WHERE client_order_id = ?1 AND mode = ?2)
                 OR EXISTS(SELECT 1 FROM cleared_trade_ids WHERE client_order_id = ?1 AND mode = ?2);",
        )?;
        Ok(stmt.query_row(params![client_order_id, mode.as_str()], |row| row.get(0))?)
    }

    /// Restore the execution frontier for this symbol and mode.
    pub fn last_fill_for(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<Option<(OrderSide, Decimal)>> {
        let conn = self.lock_connection("last_fill_for");
        let mut stmt = conn.prepare_cached(
            "SELECT side, price FROM (
                 SELECT * FROM (
                     SELECT side, price, timestamp, rowid AS sequence FROM trades
                     WHERE symbol = ?1 AND mode = ?2
                     ORDER BY timestamp DESC, rowid DESC LIMIT 1
                 )
                 UNION ALL
                 SELECT side, price, timestamp, 0 AS sequence FROM execution_frontiers
                 WHERE symbol = ?1 AND mode = ?2
             ) ORDER BY timestamp DESC, sequence DESC LIMIT 1;",
        )?;
        let value: Option<(String, String)> = stmt
            .query_row(params![symbol, mode.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .optional()?;
        value
            .map(|(side, price)| {
                let side = parse_order_side(&side)?;
                let price = Decimal::from_str(&price)?;
                anyhow::ensure!(price > Decimal::ZERO, "invalid saved fill price: {}", price);
                Ok((side, price))
            })
            .transpose()
    }

    /// Return true only for the first successful PnL verification of a saved trade.
    pub(crate) fn verify_trade_pnl(
        &self,
        trade: &TradeRecord,
        expected_clear_revision: u64,
    ) -> Result<bool> {
        let mut conn = self.lock_connection("verify_trade_pnl");
        let saved_symbol: Option<String> = conn
            .prepare_cached(
                "SELECT symbol FROM trades WHERE trade_id = ?1 AND mode = ?2 AND pnl_verified = 0",
            )?
            .query_row(params![trade.trade_id, trade.mode.as_str()], |row| {
                row.get(0)
            })
            .optional()?;
        let Some(saved_symbol) = saved_symbol else {
            return Ok(false);
        };
        if self.trade_clear_revisions.for_symbol(&saved_symbol) != expected_clear_revision {
            return Ok(false);
        }
        let tx = conn.transaction()?;
        let updated_trade: Option<(String, DateTime<Utc>)> = {
            let mut statement = tx.prepare_cached(
                "UPDATE trades SET realized_pnl = ?1, commission = ?2, is_maker = ?3, pnl_verified = 1 WHERE trade_id = ?4 AND mode = ?5 AND pnl_verified = 0 RETURNING symbol, timestamp;",
            )?;
            let mut rows = statement.query(params![
                trade.realized_pnl.to_string(),
                trade.commission.to_string(),
                if trade.is_maker { 1 } else { 0 },
                trade.trade_id,
                trade.mode.as_str(),
            ])?;
            let saved = rows
                .next()?
                .map(|row| -> rusqlite::Result<_> { Ok((row.get(0)?, row.get(1)?)) })
                .transpose()?;
            // Step through completion so errors after RETURNING are propagated.
            while rows.next()?.is_some() {}
            saved
        };
        if let Some((symbol, timestamp)) = &updated_trade {
            hourly::verify_on(
                &tx,
                symbol,
                trade.mode,
                *timestamp,
                trade.realized_pnl,
                trade.commission,
            )?;
        }
        tx.commit()?;
        if let Some((symbol, _)) = updated_trade {
            self.publish_trade_revision(&symbol);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get_recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>> {
        let conn = self.lock_connection("get_recent_trades");
        let mut stmt = conn.prepare_cached(select_trades_sql!(
            "ORDER BY timestamp DESC, rowid DESC LIMIT ?1;"
        ))?;

        let rows = stmt.query_map(params![limit as i64], trade_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub(crate) fn get_recent_trades_for(
        &self,
        symbol: &str,
        mode: TradingMode,
        limit: usize,
    ) -> Result<Vec<TradeRecord>> {
        let conn = self.lock_connection("get_recent_trades_for");
        let mut stmt = conn.prepare_cached(select_trades_sql!(
            "WHERE symbol = ?1 AND mode = ?2 ORDER BY timestamp DESC, rowid DESC LIMIT ?3;"
        ))?;
        let rows = stmt.query_map(params![symbol, mode.as_str(), limit as i64], trade_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
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

    #[cfg(test)]
    pub(crate) fn get_unverified_trades(
        &self,
        symbol: &str,
        mode: TradingMode,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TradeRecord>> {
        self.get_unverified_trades_with_clear_revision(symbol, mode, limit, offset)
            .map(|(trades, _)| trades)
    }

    pub(crate) fn get_unverified_trades_with_clear_revision(
        &self,
        symbol: &str,
        mode: TradingMode,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<TradeRecord>, u64)> {
        let conn = self.lock_connection("get_unverified_trades_with_clear_revision");
        let mut stmt = conn.prepare_cached(select_trades_sql!(
            "WHERE symbol = ?1 AND mode = ?2 AND pnl_verified = 0 ORDER BY timestamp DESC LIMIT ?3 OFFSET ?4;"
        ))?;
        let rows = stmt.query_map(
            params![symbol, mode.as_str(), limit as i64, offset as i64],
            trade_from_row,
        )?;
        let trades = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let revision = self.trade_clear_revisions.for_symbol(symbol);
        Ok((trades, revision))
    }

    pub fn symbol_trade_clear_revision(&self, symbol: &str) -> u64 {
        let _conn = self.lock_connection("symbol_trade_clear_revision");
        self.trade_clear_revisions.for_symbol(symbol)
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
                stats.total_trades as i64,
                stats.completed_cycles as i64,
                stats.total_realized_profit.to_string(),
                stats.total_volume_usdc.to_string(),
                Utc::now().to_rfc3339(),
            ],
        )
        .context("Failed to save stats to SQLite database")?;
        Ok(())
    }
}

fn parse_order_side(side: &str) -> Result<OrderSide> {
    OrderSide::from_exchange_str(side).with_context(|| format!("Invalid order side: {}", side))
}

fn parse_stats_row(
    (trades, cycles, profit, volume): (i64, i64, String, String),
) -> Result<(usize, usize, Decimal, Decimal)> {
    Ok((
        trades as usize,
        cycles as usize,
        Decimal::from_str(&profit)?,
        Decimal::from_str(&volume)?,
    ))
}

fn clear_trades_on(tx: &Connection, symbol: Option<&str>) -> Result<usize> {
    tx.execute(
        "INSERT INTO execution_frontiers (symbol, mode, side, price, timestamp)
         SELECT symbol, mode, side, price, timestamp FROM (
             SELECT symbol, mode, side, price, timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, mode ORDER BY timestamp DESC, rowid DESC
                    ) AS rank
             FROM trades WHERE (?1 IS NULL OR symbol = ?1)
         ) WHERE rank = 1
         ON CONFLICT(symbol, mode) DO UPDATE SET
             side = excluded.side, price = excluded.price, timestamp = excluded.timestamp
         WHERE excluded.timestamp >= execution_frontiers.timestamp",
        params![symbol],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO cleared_trade_ids (trade_id, client_order_id, mode)
         SELECT trade_id, client_order_id, mode FROM trades WHERE (?1 IS NULL OR symbol = ?1)",
        params![symbol],
    )?;
    preserve_pair_openings_on(tx, symbol)?;
    let deleted = tx.execute(
        "DELETE FROM trades WHERE (?1 IS NULL OR symbol = ?1)",
        params![symbol],
    )?;
    tx.execute(
        "DELETE FROM trade_hourly_rollups WHERE (?1 IS NULL OR symbol = ?1)",
        params![symbol],
    )?;
    tx.execute(
        "DELETE FROM trade_pnl_rollups WHERE (?1 IS NULL OR symbol = ?1)",
        params![symbol],
    )?;
    tx.execute(
        "DELETE FROM scoped_grid_stats WHERE (?1 IS NULL OR symbol = ?1)",
        params![symbol],
    )?;
    if symbol.is_none() {
        tx.execute("DELETE FROM grid_stats", [])?;
    }
    Ok(deleted)
}

fn preserve_pair_openings_on(conn: &Connection, symbol: Option<&str>) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO cleared_pair_openings (symbol, mode, client_order_id, side, price, quantity)
         SELECT symbol, mode, client_order_id, side, price, quantity FROM (
             SELECT symbol, mode, client_order_id, side, price, quantity,
                    ROW_NUMBER() OVER (PARTITION BY symbol, mode, client_order_id
                                       ORDER BY timestamp DESC, rowid DESC) AS rank
             FROM trades WHERE (?1 IS NULL OR symbol = ?1)
         ) WHERE rank = 1",
        params![symbol],
    )?;
    Ok(())
}

fn insert_trade_on(conn: &Connection, trade: &TradeRecord) -> Result<bool> {
    let cleared: bool = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM cleared_trade_ids WHERE trade_id = ?1)
                 OR EXISTS(SELECT 1 FROM cleared_trade_ids WHERE client_order_id = ?2 AND mode = ?3);",
        )?
        .query_row(
            params![trade.trade_id, trade.client_order_id, trade.mode.as_str()],
            |row| row.get(0),
        )?;
    if cleared {
        return Ok(false);
    }
    let inserted = conn
        .prepare_cached(
            r#"
            INSERT OR IGNORE INTO trades (
                trade_id, client_order_id, symbol, mode, side, price, quantity,
                amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note,
                stats_hour
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15);
            "#,
        )?
        .execute(params![
            trade.trade_id,
            trade.client_order_id,
            trade.symbol,
            trade.mode.as_str(),
            trade.side.as_str(),
            trade.price.to_string(),
            trade.quantity.to_string(),
            trade.amount_usdc.to_string(),
            trade.realized_pnl.to_string(),
            trade.commission.to_string(),
            if trade.pnl_verified { 1 } else { 0 },
            if trade.is_maker { 1 } else { 0 },
            trade.timestamp,
            trade.note,
            hourly::hour(trade.timestamp),
        ])
        .context("Failed to insert trade into SQLite database")?;
    if inserted == 1 {
        hourly::insert_on(conn, trade)?;
    }
    Ok(inserted == 1)
}

fn save_scoped_stats_on(
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
        stats.total_trades as i64,
        stats.completed_cycles as i64,
        stats.total_realized_profit.to_string(),
        stats.total_volume_usdc.to_string(),
        Utc::now().to_rfc3339()
    ])?;
    Ok(())
}

fn decimal_col(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Decimal> {
    let raw: String = row.get(index)?;
    Decimal::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(err))
    })
}

fn trade_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TradeRecord> {
    let side_raw: String = row.get(3)?;
    let side = OrderSide::from_exchange_str(&side_raw).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid order side in trades table: {side_raw}"),
            )),
        )
    })?;
    Ok(TradeRecord {
        trade_id: row.get(0)?,
        client_order_id: row.get(1)?,
        symbol: row.get(2)?,
        mode: TradingMode::from_db(&row.get::<_, String>(13)?),
        side,
        price: decimal_col(row, 4)?,
        quantity: decimal_col(row, 5)?,
        amount_usdc: decimal_col(row, 6)?,
        realized_pnl: decimal_col(row, 7)?,
        commission: decimal_col(row, 8)?,
        pnl_verified: row.get::<_, i32>(9)? != 0,
        is_maker: row.get::<_, i32>(10)? != 0,
        timestamp: row.get(11)?,
        note: row.get(12)?,
    })
}
