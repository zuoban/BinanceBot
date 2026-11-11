use super::{config_runtime::load_config_on, hourly, Database};
use crate::config::AppConfig;
use crate::types::TradingMode;
use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::Decimal;
#[cfg(unix)]
use std::path::Path;

impl Database {
    pub(crate) fn cached_history_account_id(&self, identity: &[u8; 32]) -> Option<String> {
        self.history_account_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(identity)
            .cloned()
    }

    pub(crate) fn history_account_id(&self, identity: [u8; 32]) -> Result<String> {
        if let Some(cached) = self
            .history_account_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&identity)
        {
            return Ok(cached.clone());
        }
        let id: String = {
            let conn = self.lock_connection("history_account_id");
            let existing: Option<String> = {
                let mut stmt = conn.prepare_cached(
                    "SELECT account_id FROM history_account_ids WHERE identity = ?1",
                )?;
                stmt.query_row(params![identity.as_slice()], |row| row.get(0))
                    .optional()?
            };
            if let Some(id) = existing {
                id
            } else {
                {
                    let mut insert = conn.prepare_cached(
                        "INSERT OR IGNORE INTO history_account_ids (identity, account_id) VALUES (?1, ?2)",
                    )?;
                    insert.execute(params![
                        identity.as_slice(),
                        uuid::Uuid::new_v4().to_string()
                    ])?;
                }
                let mut stmt = conn.prepare_cached(
                    "SELECT account_id FROM history_account_ids WHERE identity = ?1",
                )?;
                stmt.query_row(params![identity.as_slice()], |row| row.get(0))?
            }
        };
        let mut cache = self
            .history_account_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if cache.len() >= 256 {
            cache.clear();
        }
        cache.insert(identity, id.clone());
        Ok(id)
    }

    const HOURLY_SNAPSHOT_RETENTION_SECS: i64 = 30 * 24 * 3600;

    pub fn save_hourly_account_snapshot(
        &self,
        hour: i64,
        groups: &[crate::snapshots::SnapshotGroup],
    ) -> Result<()> {
        let payload = serde_json::to_string(groups)?;
        let mut conn = self.lock_connection("save_hourly_account_snapshot");
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO account_hourly_snapshots (hour, payload) VALUES (?1, ?2)",
            params![hour, payload],
        )?;
        tx.execute(
            "DELETE FROM account_hourly_snapshots WHERE hour < ?1",
            params![hour - Self::HOURLY_SNAPSHOT_RETENTION_SECS],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn cleanup_hourly_account_snapshots(&self, now: i64) -> Result<()> {
        self.lock_connection("cleanup_hourly_account_snapshots")
            .execute(
                "DELETE FROM account_hourly_snapshots WHERE hour < ?1",
                params![now - Self::HOURLY_SNAPSHOT_RETENTION_SECS],
            )?;
        Ok(())
    }

    fn query_hourly_snapshot_rows(
        &self,
        since: i64,
        operation: &'static str,
    ) -> Result<Vec<(i64, String)>> {
        let conn = self.lock_connection(operation);
        let mut statement = conn.prepare_cached(
            "SELECT hour, payload FROM account_hourly_snapshots WHERE hour >= ?1 ORDER BY hour",
        )?;
        let rows = statement.query_map(params![since], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn hourly_account_snapshots(
        &self,
        since: i64,
    ) -> Result<Vec<crate::snapshots::HourlySnapshot>> {
        self.query_hourly_snapshot_rows(since, "hourly_account_snapshots")?
            .into_iter()
            .map(|(hour, payload)| {
                Ok(crate::snapshots::HourlySnapshot {
                    hour,
                    groups: serde_json::from_str(&payload)?,
                })
            })
            .collect()
    }

    pub(crate) fn hourly_account_snapshots_raw(
        &self,
        since: i64,
    ) -> Result<Vec<crate::snapshots::RawHourlySnapshot>> {
        self.query_hourly_snapshot_rows(since, "hourly_account_snapshots_raw")?
            .into_iter()
            .map(|(hour, payload)| {
                Ok(crate::snapshots::RawHourlySnapshot {
                    hour,
                    groups: serde_json::value::RawValue::from_string(payload)?,
                })
            })
            .collect()
    }

    const DAILY_SNAPSHOT_RETENTION_DAYS: i64 = 90;

    pub fn save_daily_snapshot(&self, snapshot: &crate::snapshots::DailySnapshot) -> Result<bool> {
        let payload = serde_json::to_string(snapshot)?;
        let cutoff = chrono::NaiveDate::parse_from_str(&snapshot.date, "%Y-%m-%d")
            .ok()
            .map(|date| {
                (date - ChronoDuration::days(Self::DAILY_SNAPSHOT_RETENTION_DAYS)).to_string()
            });
        let mut conn = self.lock_connection("save_daily_snapshot");
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO daily_snapshots (date, payload) VALUES (?1, ?2)",
            params![snapshot.date, payload],
        )?;
        if let Some(cutoff) = cutoff {
            tx.execute(
                "DELETE FROM daily_snapshots WHERE date < ?1",
                params![cutoff],
            )?;
        }
        tx.commit()?;
        Ok(inserted == 1)
    }

    pub fn cleanup_daily_snapshots(&self, now: DateTime<Utc>) -> Result<()> {
        let today = now
            .with_timezone(&crate::types::beijing_offset())
            .date_naive();
        let cutoff =
            (today - ChronoDuration::days(Self::DAILY_SNAPSHOT_RETENTION_DAYS)).to_string();
        self.lock_connection("cleanup_daily_snapshots").execute(
            "DELETE FROM daily_snapshots WHERE date < ?1",
            params![cutoff],
        )?;
        Ok(())
    }

    fn query_daily_snapshot_payloads(
        &self,
        limit: usize,
        operation: &'static str,
    ) -> Result<Vec<String>> {
        let conn = self.lock_connection(operation);
        let mut statement =
            conn.prepare_cached("SELECT payload FROM daily_snapshots ORDER BY date DESC LIMIT ?1")?;
        let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    #[cfg(test)]
    pub(crate) fn daily_snapshots(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::snapshots::DailySnapshot>> {
        self.query_daily_snapshot_payloads(limit, "daily_snapshots")?
            .into_iter()
            .map(|payload| Ok(serde_json::from_str(&payload)?))
            .collect()
    }

    pub(crate) fn daily_snapshots_raw(
        &self,
        limit: usize,
    ) -> Result<Vec<Box<serde_json::value::RawValue>>> {
        self.query_daily_snapshot_payloads(limit, "daily_snapshots_raw")?
            .into_iter()
            .map(|payload| Ok(serde_json::value::RawValue::from_string(payload)?))
            .collect()
    }

    pub fn daily_trade_summaries(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<Vec<crate::snapshots::DailyTradeGroup>> {
        if window_end <= window_start {
            return Ok(Vec::new());
        }
        let start_unaligned =
            window_start.timestamp() % 3600 != 0 || window_start.timestamp_subsec_nanos() != 0;
        let end_unaligned =
            window_end.timestamp() % 3600 != 0 || window_end.timestamp_subsec_nanos() != 0;
        let first_hour = hourly::hour(window_start);
        let last_hour = if end_unaligned {
            hourly::hour(window_end)
        } else {
            hourly::hour(window_end).saturating_sub(3600)
        };
        let conn = self.lock_connection("daily_trade_summaries");
        let mut statement = conn.prepare_cached(
            "SELECT hour, buy_count, sell_count, realized_pnl, pending_pnl_count, latest_timestamp, symbol, mode
             FROM trade_hourly_rollups
             WHERE hour >= ?1 AND hour <= ?2
             ORDER BY mode, symbol, hour",
        )?;
        let mut rows = statement.query(params![first_hour, last_hour])?;
        let mut groups: std::collections::BTreeMap<
            (String, String),
            crate::snapshots::DailyTradeGroup,
        > = std::collections::BTreeMap::new();
        while let Some(row) = rows.next()? {
            let (bucket, stored) = hourly::Rollup::from_row(row)?;
            let symbol: String = row.get(6)?;
            let mode: String = row.get(7)?;
            let needs_boundary_scan = (start_unaligned && bucket == first_hour)
                || (end_unaligned && bucket == last_hour)
                || stored.latest.is_none_or(|latest| latest >= window_end);
            let value = if needs_boundary_scan {
                hourly::load_boundary_rollup_on(
                    &conn,
                    &symbol,
                    &mode,
                    bucket,
                    window_start,
                    window_end,
                    false,
                )?
            } else {
                stored
            };
            let buys = value.buys.max(0) as u64;
            let sells = value.sells.max(0) as u64;
            let pending = value.pending.max(0) as u64;
            if buys == 0 && sells == 0 && pending == 0 && value.pnl.is_zero() {
                continue;
            }
            let entry = groups
                .entry((mode, symbol))
                .or_insert_with_key(|(mode, symbol)| crate::snapshots::DailyTradeGroup {
                    symbol: symbol.clone(),
                    mode: TradingMode::from_db(mode),
                    buy_count: 0,
                    sell_count: 0,
                    realized_pnl: Decimal::ZERO,
                    pending_pnl_count: 0,
                });
            entry.buy_count += buys;
            entry.sell_count += sells;
            entry.realized_pnl += value.pnl;
            entry.pending_pnl_count += pending;
        }
        Ok(groups.into_values().collect())
    }

    /// A consistent analysis snapshot, including committed WAL transactions.
    /// Redaction happens only on the copy; never expose a raw live database file.
    pub fn export_analysis_snapshot(&self) -> Result<Vec<u8>> {
        let _export = self.try_analysis_export()?;
        let snapshot = self.backup_to_memory()?;

        if let Some(mut config) = load_config_on(&snapshot)? {
            redact_config_credentials(&mut config);
            snapshot.execute(
                "UPDATE app_config SET config_json = ?1 WHERE id = 1",
                params![serde_json::to_string(&config)?],
            )?;
        }
        snapshot
            .execute_batch("DELETE FROM auth_sessions; DELETE FROM history_account_ids; VACUUM;")?;
        Ok(snapshot.serialize(rusqlite::DatabaseName::Main)?.to_vec())
    }

    /// Export only rows that can be attributed to this symbol.
    pub fn export_analysis_snapshot_for_symbol(&self, symbol: &str) -> Result<Vec<u8>> {
        let _export = self.try_analysis_export()?;
        let snapshot = self.analysis_snapshot_with_tables(&[
            "trades",
            "scoped_grid_stats",
            "execution_frontiers",
        ])?;
        for table in ["trades", "scoped_grid_stats", "execution_frontiers"] {
            snapshot.execute(
                &format!("DELETE FROM {table} WHERE symbol != ?1"),
                params![symbol],
            )?;
        }
        snapshot.execute_batch("VACUUM;")?;
        Ok(snapshot.serialize(rusqlite::DatabaseName::Main)?.to_vec())
    }

    /// Export only the selected analysis data categories.
    pub fn export_selected_analysis_snapshot(
        &self,
        trade_history: bool,
        account_hourly_snapshots: bool,
    ) -> Result<Vec<u8>> {
        let _export = self.try_analysis_export()?;
        let mut tables = Vec::new();
        if trade_history {
            // Legacy grid_stats has no symbol or mode and cannot be attributed
            // to history retained after a per-symbol cleanup.
            tables.extend(["trades", "scoped_grid_stats"]);
        }
        if account_hourly_snapshots {
            tables.extend(["account_hourly_snapshots", "daily_snapshots"]);
        }
        let snapshot = self.analysis_snapshot_with_tables(&tables)?;
        snapshot.execute_batch("VACUUM;")?;
        Ok(snapshot.serialize(rusqlite::DatabaseName::Main)?.to_vec())
    }

    fn try_analysis_export(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.analysis_export
            .try_lock()
            .map_err(|_| anyhow::anyhow!("Database analysis export is busy; retry the export"))
    }

    fn backup_to_memory(&self) -> Result<Connection> {
        let mut snapshot = Connection::open_in_memory()?;
        if self.path != ":memory:" {
            #[cfg(unix)]
            if let Some(expected_id) = self.file_identity {
                use std::os::unix::fs::MetadataExt;
                let effective_uid = unsafe { libc::geteuid() } as u32;
                super::validate_database_file(Path::new(&self.path), effective_uid)?;
                let meta = std::fs::symlink_metadata(&self.path)?;
                anyhow::ensure!(
                    (meta.dev(), meta.ino()) == expected_id,
                    "SQLite database file changed on disk; retry the export"
                );
            }
            let reader = Connection::open_with_flags(
                &self.path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            reader.busy_timeout(std::time::Duration::from_secs(5))?;
            {
                let backup = rusqlite::backup::Backup::new(&reader, &mut snapshot)?;
                anyhow::ensure!(
                    backup.step(-1)? == rusqlite::backup::StepResult::Done,
                    "Database is busy; retry the export"
                );
            }
            return Ok(snapshot);
        }
        {
            let source = self.lock_connection("backup_to_memory");
            let backup = rusqlite::backup::Backup::new(&source, &mut snapshot)?;
            anyhow::ensure!(
                backup.step(-1)? == rusqlite::backup::StepResult::Done,
                "Database is busy; retry the export"
            );
        }
        Ok(snapshot)
    }

    fn analysis_snapshot_with_tables(&self, tables: &[&str]) -> Result<Connection> {
        let snapshot = self.backup_to_memory()?;
        // Remove triggers and views before clearing unselected tables so their
        // contents cannot affect the export or reveal data through a view.
        let objects = {
            let mut statement = snapshot.prepare(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' \
                 AND type IN ('table', 'view', 'trigger') \
                 ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'view' THEN 1 ELSE 2 END",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (kind, name) in objects {
            let quoted = name.replace('"', "\"\"");
            if kind == "table" {
                if !tables.contains(&name.as_str()) {
                    snapshot.execute_batch(&format!("DELETE FROM \"{}\";", quoted))?;
                }
            } else {
                snapshot.execute_batch(&format!(
                    "DROP {} IF EXISTS \"{}\";",
                    kind.to_ascii_uppercase(),
                    quoted
                ))?;
            }
        }
        Ok(snapshot)
    }
}

fn redact_config_credentials(config: &mut AppConfig) {
    // Typed serialization also drops unrecognized config fields.
    config.exchange.api_key.clear();
    config.exchange.api_secret.clear();
    config.telegram.bot_token.clear();
    config.telegram.chat_id.clear();
    for pair in &mut config.pairs {
        pair.exchange.api_key = None;
        pair.exchange.api_secret = None;
        if let Some(telegram) = &mut pair.telegram {
            telegram.bot_token.clear();
            telegram.chat_id.clear();
        }
    }
}
