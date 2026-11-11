use super::{beijing_midnight, config_runtime::load_config_on, hourly, Database};
use crate::config::AppConfig;
use crate::sync_ext::MutexUnpoisonedExt;
use crate::types::TradingMode;
use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
#[cfg(unix)]
use std::path::Path;

const MAX_ANALYSIS_EXPORT_DB_BYTES: u64 = 256 * 1024 * 1024;
const TRADE_ANALYSIS_TABLES: &[&str] = &[
    "trades",
    "scoped_grid_stats",
    "execution_frontiers",
    "trade_hourly_rollups",
    "trade_pnl_rollups",
    "trade_hourly_rollup_state",
    "trade_pnl_rollup_state",
];
const SYMBOL_SCOPED_ANALYSIS_TABLES: &[&str] = &[
    "trades",
    "scoped_grid_stats",
    "execution_frontiers",
    "trade_hourly_rollups",
    "trade_pnl_rollups",
];

impl Database {
    pub(crate) fn cached_history_account_id(&self, identity: &[u8; 32]) -> Option<String> {
        self.history_account_ids
            .lock_unpoisoned()
            .get(identity)
            .cloned()
    }

    pub(crate) fn history_account_id(&self, identity: [u8; 32]) -> Result<String> {
        if let Some(cached) = self.history_account_ids.lock_unpoisoned().get(&identity) {
            return Ok(cached.clone());
        }
        let id: String = {
            let mut conn = self.lock_connection("history_account_id");
            let tx = super::immediate_tx(&mut conn)?;
            let existing: Option<String> = tx
                .prepare_cached("SELECT account_id FROM history_account_ids WHERE identity = ?1")?
                .query_row(params![identity.as_slice()], |row| row.get(0))
                .optional()?;
            let id = if let Some(id) = existing {
                id
            } else {
                tx.prepare_cached(
                    "INSERT OR IGNORE INTO history_account_ids (identity, account_id) VALUES (?1, ?2)",
                )?
                .execute(params![
                    identity.as_slice(),
                    uuid::Uuid::new_v4().to_string()
                ])?;
                tx.prepare_cached("SELECT account_id FROM history_account_ids WHERE identity = ?1")?
                    .query_row(params![identity.as_slice()], |row| row.get(0))?
            };
            tx.commit()?;
            id
        };
        let mut cache = self.history_account_ids.lock_unpoisoned();
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
        let tx = super::immediate_tx(&mut conn)?;
        tx.prepare_cached(
            "INSERT OR IGNORE INTO account_hourly_snapshots (hour, payload) VALUES (?1, ?2)",
        )?
        .execute(params![hour, payload])?;
        let cutoff = hour.saturating_sub(Self::HOURLY_SNAPSHOT_RETENTION_SECS);
        tx.prepare_cached("DELETE FROM account_hourly_snapshots WHERE hour < ?1")?
            .execute(params![cutoff])?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn cleanup_hourly_account_snapshots(&self, now: i64) -> Result<()> {
        let conn = self.lock_connection("cleanup_hourly_account_snapshots");
        let cutoff = now.saturating_sub(Self::HOURLY_SNAPSHOT_RETENTION_SECS);
        conn.prepare_cached("DELETE FROM account_hourly_snapshots WHERE hour < ?1")?
            .execute(params![cutoff])?;
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

    pub(crate) fn save_daily_snapshot(
        &self,
        snapshot: &crate::snapshots::DailySnapshot,
    ) -> Result<bool> {
        let payload = serde_json::to_string(snapshot)?;
        let cutoff = chrono::NaiveDate::parse_from_str(&snapshot.date, "%Y-%m-%d")
            .ok()
            .and_then(|date| {
                date.checked_sub_signed(ChronoDuration::days(
                    Self::DAILY_SNAPSHOT_RETENTION_DAYS - 1,
                ))
            })
            .map(|date| date.to_string());
        let mut conn = self.lock_connection("save_daily_snapshot");
        let tx = super::immediate_tx(&mut conn)?;
        let inserted = tx
            .prepare_cached(
                "INSERT OR IGNORE INTO daily_snapshots (date, payload) VALUES (?1, ?2)",
            )?
            .execute(params![snapshot.date, payload])?;
        if let Some(cutoff) = cutoff {
            tx.prepare_cached("DELETE FROM daily_snapshots WHERE date < ?1")?
                .execute(params![cutoff])?;
        }
        tx.commit()?;
        Ok(inserted == 1)
    }

    pub(crate) fn cleanup_daily_snapshots(&self, now: DateTime<Utc>) -> Result<()> {
        let today = now
            .with_timezone(&crate::types::beijing_offset())
            .date_naive();
        if let Some(cutoff) =
            today.checked_sub_signed(ChronoDuration::days(Self::DAILY_SNAPSHOT_RETENTION_DAYS))
        {
            let conn = self.lock_connection("cleanup_daily_snapshots");
            conn.prepare_cached("DELETE FROM daily_snapshots WHERE date < ?1")?
                .execute(params![cutoff.to_string()])?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn daily_snapshots(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::snapshots::DailySnapshot>> {
        let conn = self.lock_connection("daily_snapshots");
        super::query_json_vec(
            &conn,
            "SELECT payload FROM daily_snapshots ORDER BY date DESC LIMIT ?1",
            params![i64::try_from(limit).unwrap_or(i64::MAX)],
            "daily_snapshots",
        )
    }

    pub(crate) fn daily_snapshots_raw(
        &self,
        limit: usize,
    ) -> Result<Vec<Box<serde_json::value::RawValue>>> {
        let conn = self.lock_connection("daily_snapshots_raw");
        super::query_json_vec(
            &conn,
            "SELECT payload FROM daily_snapshots ORDER BY date DESC LIMIT ?1",
            params![i64::try_from(limit).unwrap_or(i64::MAX)],
            "daily_snapshots_raw",
        )
    }

    pub(crate) fn daily_trade_summaries(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<Vec<crate::snapshots::DailyTradeGroup>> {
        let conn = self.lock_connection("daily_trade_summaries");
        Ok(
            hourly::scan_window_rollups_on(&conn, window_start, window_end, false)?
                .into_iter()
                .map(
                    |((mode, symbol), rollup)| crate::snapshots::DailyTradeGroup {
                        symbol,
                        mode: TradingMode::from_db(&mode),
                        buy_count: rollup.buys,
                        sell_count: rollup.sells,
                        realized_pnl: rollup.realized_pnl,
                        pending_pnl_count: rollup.pending_pnl_count,
                    },
                )
                .collect(),
        )
    }

    /// Aggregate persisted trades across all modes and symbols for the Beijing day.
    pub(crate) fn get_today_pnl(&self, now: DateTime<Utc>) -> Result<crate::types::TodayPnl> {
        let conn = self.lock_connection("get_today_pnl");
        hourly::today_pnl_on(&conn, now)
    }

    pub(crate) async fn cached_today_pnl(
        self: &std::sync::Arc<Self>,
        now: DateTime<Utc>,
    ) -> Option<crate::types::TodayPnl> {
        let mut cache = self.today_pnl_cache.lock().await;
        let revision = self.trade_data_revision();
        let day = beijing_midnight(now);
        if let Some((cached_revision, cached_day, loaded, value)) = cache.as_ref() {
            let ttl = if value.is_some() {
                std::time::Duration::from_secs(2)
            } else {
                std::time::Duration::from_secs(1)
            };
            if *cached_revision == revision
                && *cached_day == day
                && loaded.elapsed() < ttl
                && value.as_ref().is_none_or(|v| v.window_end <= now)
            {
                return value.clone();
            }
        }
        let value = self
            .run_blocking(move |db| db.get_today_pnl(now))
            .await
            .map_err(|error| tracing::warn!("Failed to load overview today PnL: {}", error))
            .ok();
        *cache = Some((revision, day, std::time::Instant::now(), value.clone()));
        value
    }

    /// A consistent analysis snapshot, including committed WAL transactions.
    /// Redaction happens only on the copy; never expose a raw live database file.
    pub(crate) fn export_analysis_snapshot(&self) -> Result<Vec<u8>> {
        let _export = self.try_analysis_export()?;
        let snapshot = self.backup_to_memory()?;
        drop_views_and_triggers(&snapshot)?;

        if let Some(mut config) = load_config_on(&snapshot)? {
            redact_config_credentials(&mut config);
            snapshot.execute(
                "UPDATE app_config SET config_json = ?1 WHERE id = 1",
                params![serde_json::to_string(&config)?],
            )?;
        }
        snapshot.execute_batch(
            "DELETE FROM app_config WHERE id != 1; DELETE FROM auth_sessions; DELETE FROM history_account_ids;",
        )?;
        clear_sqlite_metadata_tables(&snapshot);
        snapshot.execute_batch("VACUUM;")?;
        Ok(snapshot.serialize(rusqlite::DatabaseName::Main)?.to_vec())
    }

    /// Export only rows that can be attributed to this symbol.
    pub(crate) fn export_analysis_snapshot_for_symbol(&self, symbol: &str) -> Result<Vec<u8>> {
        let _export = self.try_analysis_export()?;
        let snapshot = self.analysis_snapshot_with_tables(TRADE_ANALYSIS_TABLES)?;
        for table in SYMBOL_SCOPED_ANALYSIS_TABLES {
            snapshot.execute(
                &format!("DELETE FROM {table} WHERE symbol != ?1"),
                params![symbol],
            )?;
        }
        snapshot.execute_batch("VACUUM;")?;
        Ok(snapshot.serialize(rusqlite::DatabaseName::Main)?.to_vec())
    }

    /// Export only the selected analysis data categories.
    pub(crate) fn export_selected_analysis_snapshot(
        &self,
        trade_history: bool,
        account_hourly_snapshots: bool,
    ) -> Result<Vec<u8>> {
        let _export = self.try_analysis_export()?;
        let mut tables = Vec::new();
        if trade_history {
            // Legacy grid_stats has no symbol or mode and cannot be attributed
            // to history retained after a per-symbol cleanup.
            tables.extend_from_slice(TRADE_ANALYSIS_TABLES);
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

    #[cfg(unix)]
    fn verify_on_disk_identity(&self) -> Result<()> {
        if let Some(expected_id) = self.file_identity {
            use std::os::unix::fs::MetadataExt;
            super::validate_database_and_sidecars(Path::new(&self.path))?;
            let meta = std::fs::symlink_metadata(&self.path)?;
            anyhow::ensure!(
                (meta.dev(), meta.ino()) == expected_id,
                "SQLite database file changed on disk; retry the export"
            );
        }
        Ok(())
    }

    fn backup_to_memory(&self) -> Result<Connection> {
        let mut snapshot = Connection::open_in_memory()?;
        if self.path != ":memory:" {
            #[cfg(unix)]
            self.verify_on_disk_identity()?;
            let db_bytes = std::fs::symlink_metadata(&self.path)?.len();
            let wal_bytes = match std::fs::symlink_metadata(format!("{}-wal", self.path)) {
                Ok(meta) => meta.len(),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
                Err(err) => return Err(err.into()),
            };
            anyhow::ensure!(
                db_bytes.saturating_add(wal_bytes) <= MAX_ANALYSIS_EXPORT_DB_BYTES,
                "SQLite database exceeds 256 MiB analysis export limit"
            );
            let reader = Connection::open_with_flags(
                &self.path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                    | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            reader.busy_timeout(std::time::Duration::from_secs(5))?;
            ensure_logical_export_size_within_limit(&reader)?;
            {
                let backup = rusqlite::backup::Backup::new(&reader, &mut snapshot)?;
                anyhow::ensure!(
                    backup.step(-1)? == rusqlite::backup::StepResult::Done,
                    "Database is busy; retry the export"
                );
            }
            ensure_logical_export_size_within_limit(&snapshot)?;
            #[cfg(unix)]
            self.verify_on_disk_identity()?;
            return Ok(snapshot);
        }
        {
            let source = self.lock_connection("backup_to_memory");
            ensure_logical_export_size_within_limit(&source)?;
            let backup = rusqlite::backup::Backup::new(&source, &mut snapshot)?;
            anyhow::ensure!(
                backup.step(-1)? == rusqlite::backup::StepResult::Done,
                "Database is busy; retry the export"
            );
        }
        ensure_logical_export_size_within_limit(&snapshot)?;
        Ok(snapshot)
    }

    fn analysis_snapshot_with_tables(&self, tables: &[&str]) -> Result<Connection> {
        let snapshot = self.backup_to_memory()?;
        // Remove triggers and views before clearing unselected tables so their
        // contents cannot affect the export or reveal data through a view.
        drop_views_and_triggers(&snapshot)?;
        let table_names = {
            let mut statement = snapshot.prepare(
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND type = 'table'",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for name in table_names {
            if !tables.contains(&name.as_str()) {
                let quoted = name.replace('"', "\"\"");
                snapshot.execute_batch(&format!("DELETE FROM \"{}\";", quoted))?;
            }
        }
        clear_sqlite_metadata_tables(&snapshot);
        Ok(snapshot)
    }
}

fn ensure_logical_export_size_within_limit(conn: &Connection) -> Result<()> {
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let logical_bytes = u64::try_from(page_count.max(0))
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(page_size.max(0)).unwrap_or(u64::MAX));
    anyhow::ensure!(
        logical_bytes <= MAX_ANALYSIS_EXPORT_DB_BYTES,
        "SQLite database exceeds 256 MiB analysis export limit"
    );
    Ok(())
}

fn clear_sqlite_metadata_tables(conn: &Connection) {
    for table in [
        "sqlite_stat1",
        "sqlite_stat3",
        "sqlite_stat4",
        "sqlite_sequence",
    ] {
        let _ = conn.execute(&format!("DELETE FROM {table}"), []);
    }
}

fn drop_views_and_triggers(conn: &Connection) -> Result<()> {
    let objects = {
        let mut statement = conn.prepare(
            "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' \
             AND type IN ('view', 'trigger') \
             ORDER BY CASE type WHEN 'trigger' THEN 0 ELSE 1 END",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (kind, name) in objects {
        let quoted = name.replace('"', "\"\"");
        conn.execute_batch(&format!(
            "DROP {} IF EXISTS \"{}\";",
            kind.to_ascii_uppercase(),
            quoted
        ))?;
    }
    Ok(())
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
