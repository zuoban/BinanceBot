use super::{immediate_tx, Database};
use anyhow::Result;
use rusqlite::{params, Connection};

pub(crate) const MAX_CLEARED_TRADE_IDS_PER_MODE: i64 = 4096;
pub(crate) const MAX_CLEARED_PAIR_OPENINGS_PER_SCOPE: i64 = 4096;

impl Database {
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
        let tx = immediate_tx(&mut conn)?;
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
        let _commit_guard = trades.then(|| self.begin_trade_commit());
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
        let tx = immediate_tx(&mut conn)?;
        let deleted = clear_trades_on(&tx, Some(symbol))?;
        let _commit_guard = self.begin_trade_commit();
        tx.commit()?;
        self.publish_trade_revision(symbol);
        self.trade_clear_revisions.publish(symbol);
        Ok(deleted)
    }
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
         SELECT trade_id, client_order_id, mode FROM trades
         WHERE (?1 IS NULL OR symbol = ?1)
         ORDER BY timestamp ASC, rowid ASC",
        params![symbol],
    )?;
    tx.execute(
        "DELETE FROM cleared_trade_ids
         WHERE rowid IN (
             SELECT rowid FROM (
                 SELECT rowid, ROW_NUMBER() OVER (PARTITION BY mode ORDER BY rowid DESC) AS rn
                 FROM cleared_trade_ids
             ) WHERE rn > ?1
         )",
        params![MAX_CLEARED_TRADE_IDS_PER_MODE],
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
             SELECT symbol, mode, client_order_id, side, price, quantity, timestamp, rowid AS src_rowid,
                    ROW_NUMBER() OVER (PARTITION BY symbol, mode, client_order_id
                                       ORDER BY timestamp DESC, rowid DESC) AS rank
             FROM trades WHERE (?1 IS NULL OR symbol = ?1)
         ) WHERE rank = 1
         ORDER BY timestamp ASC, src_rowid ASC",
        params![symbol],
    )?;
    conn.execute(
        "DELETE FROM cleared_pair_openings
         WHERE rowid IN (
             SELECT rowid FROM (
                 SELECT rowid, ROW_NUMBER() OVER (PARTITION BY symbol, mode ORDER BY rowid DESC) AS rn
                 FROM cleared_pair_openings
                 WHERE (?1 IS NULL OR symbol = ?1)
             ) WHERE rn > ?2
         )",
        params![symbol, MAX_CLEARED_PAIR_OPENINGS_PER_SCOPE],
    )?;
    Ok(())
}
