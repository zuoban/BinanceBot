use crate::config::AppConfig;
use crate::types::{
    AccountInfo, BotStatus, GridOrder, GridStats, HourlyTradeCount, HourlyTradeStats, OrderSide,
    PositionInfo, RemainderPlan, TradeRecord, TradingMode,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, FixedOffset, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tracing::{info, warn};

mod authentication;

pub(crate) fn beijing_midnight(now: DateTime<Utc>) -> DateTime<Utc> {
    let beijing = FixedOffset::east_opt(8 * 3600).unwrap();
    now.with_timezone(&beijing)
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(beijing)
        .single()
        .unwrap()
        .with_timezone(&Utc)
}

type TodayPnlCacheEntry = (
    u64,
    DateTime<Utc>,
    std::time::Instant,
    Option<crate::types::TodayPnl>,
);

#[derive(Default)]
struct TradeRevisions {
    cleared: u64,
    symbols: HashMap<String, u64>,
}

impl TradeRevisions {
    fn for_symbol(&self, symbol: &str) -> u64 {
        self.symbols.get(symbol).copied().unwrap_or(self.cleared)
    }

    fn changed(&mut self, symbol: &str, revision: u64) {
        self.symbols.insert(symbol.to_owned(), revision);
    }

    fn clear_all(&mut self, revision: u64) {
        self.cleared = revision;
        self.symbols.clear();
    }
}

// Enabled only by the local benchmark; no global collector or production overhead.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct DatabaseWaitSamples {
    pub(crate) queue: Mutex<Vec<f64>>,
    pub(crate) connection: Mutex<Vec<f64>>,
}

#[cfg(test)]
type MembershipHook = (
    &'static str,
    tokio::sync::oneshot::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

pub struct Database {
    conn: Mutex<Connection>,
    path: String,
    trade_revision: AtomicU64,
    trade_revisions: Mutex<TradeRevisions>,
    trade_clear_revision: AtomicU64,
    trade_clear_revisions: Mutex<TradeRevisions>,
    #[cfg(test)]
    revision_publish_hook:
        Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    #[cfg(test)]
    membership_commit_hook: Mutex<Option<MembershipHook>>,
    today_pnl_cache: tokio::sync::Mutex<Option<TodayPnlCacheEntry>>,
    #[cfg(test)]
    hourly_stats_queries: AtomicU64,
    #[cfg(test)]
    benchmark_wait_samples: std::sync::OnceLock<std::sync::Arc<DatabaseWaitSamples>>,
}

#[cfg(unix)]
fn validate_database_file(path: &Path, expected_uid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "SQLite database and sidecar paths must not be symbolic links",
        ));
    }
    if metadata.uid() != expected_uid {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "file owner {} differs from service effective user {}",
                metadata.uid(),
                expected_uid
            ),
        ));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "remove group and other permissions before starting",
        ));
    }
    Ok(())
}

fn prepare_database_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(parent)
            .with_context(|| format!("Failed to create database directory: {:?}", parent))?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        // Pre-create the database privately so SQLite also creates its WAL and
        // shared-memory files with private permissions, without changing umask.
        match std::fs::symlink_metadata(path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                {
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("Failed to prepare SQLite database at {:?}", path)
                        })
                    }
                }
            }
            Err(error) => {
                return Err(error).with_context(|| format!("Failed to inspect {:?}", path))
            }
        }
        // Inspect without opening existing files: closing a descriptor can
        // release another SQLite connection's process-wide POSIX locks.
        let effective_uid = unsafe { libc::geteuid() } as u32;
        let validate = |path: &Path| validate_database_file(path, effective_uid);
        validate(path).with_context(|| format!("Insecure SQLite database path {:?}; ensure it is owned by the service user, remove group and other permissions and avoid symbolic links", path))?;

        // SQLite resolves parent symlinks before choosing sidecar names.
        let canonical = path
            .canonicalize()
            .context("Failed to resolve database path")?;
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut name = canonical.as_os_str().to_os_string();
            name.push(suffix);
            let sidecar = Path::new(&name);
            match validate(sidecar) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).with_context(|| format!("Insecure SQLite sidecar {:?}; ensure it is owned by the service user, remove group and other permissions and avoid symbolic links", sidecar)),
            }
        }
    }
    Ok(())
}

impl Database {
    pub async fn run_blocking<T, F>(self: &std::sync::Arc<Self>, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> Result<T> + Send + 'static,
    {
        let db = std::sync::Arc::clone(self);
        #[cfg(test)]
        let benchmark_wait = self
            .benchmark_wait_samples
            .get()
            .cloned()
            .map(|samples| (samples, std::time::Instant::now()));
        let queued_at = tracing::enabled!(tracing::Level::DEBUG).then(std::time::Instant::now);
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some((samples, queued_at)) = benchmark_wait {
                let elapsed = queued_at.elapsed().as_secs_f64() * 1000.0;
                samples.queue.lock().unwrap().push(elapsed);
            }
            if let Some(queued_at) = queued_at {
                tracing::debug!(
                    queue_wait_us = queued_at.elapsed().as_micros() as u64,
                    "SQLite blocking worker started"
                );
            }
            operation(&db)
        })
        .await
        .context("Database worker stopped")?
    }

    fn lock_connection(&self, operation: &'static str) -> std::sync::MutexGuard<'_, Connection> {
        let waiting_since = tracing::enabled!(tracing::Level::DEBUG).then(std::time::Instant::now);
        #[cfg(test)]
        let benchmark_wait = self
            .benchmark_wait_samples
            .get()
            .map(|samples| (samples, std::time::Instant::now()));
        let conn = self.conn.lock().unwrap();
        #[cfg(test)]
        if let Some((samples, waiting_since)) = benchmark_wait {
            let elapsed = waiting_since.elapsed().as_secs_f64() * 1000.0;
            samples.connection.lock().unwrap().push(elapsed);
        }
        if let Some(waiting_since) = waiting_since {
            tracing::debug!(
                operation,
                lock_wait_us = waiting_since.elapsed().as_micros() as u64,
                "SQLite connection lock acquired"
            );
        }
        conn
    }
    // These methods already lock the connection directly in production; the test
    // build routes them through the benchmark collector when enabled.
    fn lock_reconciliation_connection(
        &self,
        operation: &'static str,
    ) -> std::sync::MutexGuard<'_, Connection> {
        #[cfg(test)]
        {
            self.lock_connection(operation)
        }
        #[cfg(not(test))]
        {
            let _ = operation;
            self.conn.lock().unwrap()
        }
    }

    #[cfg(test)]
    pub(crate) fn enable_benchmark_wait_samples(&self) -> std::sync::Arc<DatabaseWaitSamples> {
        let samples = std::sync::Arc::new(DatabaseWaitSamples::default());
        assert!(self.benchmark_wait_samples.set(samples.clone()).is_ok());
        samples
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        if path_str != ":memory:" {
            // URI filenames can redirect SQLite to a different path or VFS;
            // they cannot be secured by preparing this filesystem path.
            anyhow::ensure!(
                !path_str.starts_with("file:"),
                "SQLite URI database paths are not supported; use a filesystem path"
            );
            prepare_database_path(path_ref)?;
        }

        let conn = Connection::open(path_ref)
            .with_context(|| format!("Failed to open SQLite database at {:?}", path_ref))?;

        // Persist committed order intents across power loss before sending orders.
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .context("Failed to enable SQLite WAL journal mode")?;
        anyhow::ensure!(
            journal_mode.eq_ignore_ascii_case(if path_str == ":memory:" {
                "memory"
            } else {
                "wal"
            }),
            "SQLite journal mode is {journal_mode}, expected WAL"
        );
        conn.execute_batch("PRAGMA synchronous = FULL;")
            .context("Failed to configure SQLite synchronous mode")?;
        let synchronous: i64 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .context("Failed to read SQLite synchronous mode")?;
        anyhow::ensure!(synchronous == 2, "SQLite synchronous mode is not FULL");

        let db = Self {
            conn: Mutex::new(conn),
            path: path_str,
            trade_revision: AtomicU64::new(0),
            trade_revisions: Mutex::new(TradeRevisions::default()),
            trade_clear_revision: AtomicU64::new(0),
            trade_clear_revisions: Mutex::new(TradeRevisions::default()),
            #[cfg(test)]
            revision_publish_hook: Mutex::new(None),
            #[cfg(test)]
            membership_commit_hook: Mutex::new(None),
            today_pnl_cache: tokio::sync::Mutex::new(None),
            #[cfg(test)]
            hourly_stats_queries: AtomicU64::new(0),
            #[cfg(test)]
            benchmark_wait_samples: std::sync::OnceLock::new(),
        };

        db.init_tables()?;
        Ok(db)
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub(crate) fn history_account_id(&self, identity: [u8; 32]) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        if let Some(id) = conn
            .query_row(
                "SELECT account_id FROM history_account_ids WHERE identity = ?1",
                params![identity.as_slice()],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(id);
        }
        conn.execute(
            "INSERT OR IGNORE INTO history_account_ids (identity, account_id) VALUES (?1, ?2)",
            params![identity.as_slice(), uuid::Uuid::new_v4().to_string()],
        )?;
        Ok(conn.query_row(
            "SELECT account_id FROM history_account_ids WHERE identity = ?1",
            params![identity.as_slice()],
            |row| row.get(0),
        )?)
    }

    pub fn save_hourly_account_snapshot(
        &self,
        hour: i64,
        groups: &[crate::snapshots::SnapshotGroup],
    ) -> Result<()> {
        let payload = serde_json::to_string(groups)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO account_hourly_snapshots (hour, payload) VALUES (?1, ?2)",
            params![hour, payload],
        )?;
        tx.execute(
            "DELETE FROM account_hourly_snapshots WHERE hour < ?1",
            params![hour - 30 * 24 * 3600],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn cleanup_hourly_account_snapshots(&self, now: i64) -> Result<()> {
        self.conn.lock().unwrap().execute(
            "DELETE FROM account_hourly_snapshots WHERE hour < ?1",
            params![now - 30 * 24 * 3600],
        )?;
        Ok(())
    }

    pub fn hourly_account_snapshots(
        &self,
        since: i64,
    ) -> Result<Vec<crate::snapshots::HourlySnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut statement = conn.prepare(
            "SELECT hour, payload FROM account_hourly_snapshots WHERE hour >= ?1 ORDER BY hour",
        )?;
        let rows = statement.query_map(params![since], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.map(|row| {
            let (hour, payload) = row?;
            Ok(crate::snapshots::HourlySnapshot {
                hour,
                groups: serde_json::from_str(&payload)?,
            })
        })
        .collect()
    }

    pub(crate) fn trade_data_revision(&self) -> u64 {
        self.trade_revision.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn before_revision_publish(&self) {
        if let Some((reached, resume)) = self.revision_publish_hook.lock().unwrap().as_ref() {
            reached.send(()).unwrap();
            resume.recv().unwrap();
        }
    }

    pub(crate) fn symbol_trade_revision(&self, symbol: &str) -> u64 {
        self.trade_revisions.lock().unwrap().for_symbol(symbol)
    }

    #[cfg(test)]
    pub(crate) fn hourly_stats_query_count(&self) -> u64 {
        self.hourly_stats_queries.load(Ordering::Relaxed)
    }

    /// A consistent analysis snapshot, including committed WAL transactions.
    /// Redaction happens only on the copy; never expose a raw live database file.
    pub fn export_analysis_snapshot(&self) -> Result<Vec<u8>> {
        let mut snapshot = Connection::open_in_memory()?;
        {
            let source = self.conn.lock().unwrap();
            let backup = rusqlite::backup::Backup::new(&source, &mut snapshot)?;
            anyhow::ensure!(
                backup.step(-1)? == rusqlite::backup::StepResult::Done,
                "Database is busy; retry the export"
            );
        }

        let config_json: Option<String> = snapshot
            .query_row(
                "SELECT config_json FROM app_config WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(json) = config_json {
            // Typed serialization also drops unrecognized config fields.
            let mut config: AppConfig = serde_json::from_str(&json)?;
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
            snapshot.execute(
                "UPDATE app_config SET config_json = ?1 WHERE id = 1",
                params![serde_json::to_string(&config)?],
            )?;
        }
        snapshot
            .execute_batch("DELETE FROM auth_sessions; DELETE FROM history_account_ids; VACUUM;")?;
        // VACUUM removes credentials from freed pages and old config payloads too.
        Ok(snapshot.serialize(rusqlite::DatabaseName::Main)?.to_vec())
    }

    /// Export only rows that can be attributed to this symbol.
    pub fn export_analysis_snapshot_for_symbol(&self, symbol: &str) -> Result<Vec<u8>> {
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
        let mut tables = Vec::new();
        if trade_history {
            // Legacy grid_stats has no symbol or mode and cannot be attributed
            // to history retained after a per-symbol cleanup.
            tables.extend(["trades", "scoped_grid_stats"]);
        }
        if account_hourly_snapshots {
            tables.push("account_hourly_snapshots");
        }
        let snapshot = self.analysis_snapshot_with_tables(&tables)?;
        snapshot.execute_batch("VACUUM;")?;
        Ok(snapshot.serialize(rusqlite::DatabaseName::Main)?.to_vec())
    }

    fn analysis_snapshot_with_tables(&self, tables: &[&str]) -> Result<Connection> {
        let mut snapshot = Connection::open_in_memory()?;
        {
            let source = self.conn.lock().unwrap();
            let backup = rusqlite::backup::Backup::new(&source, &mut snapshot)?;
            anyhow::ensure!(
                backup.step(-1)? == rusqlite::backup::StepResult::Done,
                "Database is busy; retry the export"
            );
        }
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

    pub fn load_bot_status(&self) -> Result<Option<BotStatus>> {
        let conn = self.conn.lock().unwrap();
        let value: Option<String> = conn
            .query_row("SELECT status FROM bot_runtime WHERE id = 1;", [], |row| {
                row.get(0)
            })
            .optional()?;
        value
            .map(|status| serde_json::from_str(&format!("\"{}\"", status)).map_err(Into::into))
            .transpose()
    }

    pub fn save_bot_status(&self, status: BotStatus) -> Result<()> {
        save_bot_status_on(&self.conn.lock().unwrap(), status)
    }

    pub fn load_pair_status(&self, symbol: &str, mode: TradingMode) -> Result<Option<BotStatus>> {
        let value: Option<String> = {
            let conn = self.conn.lock().unwrap();
            conn.query_row(
                "SELECT status FROM pair_runtime WHERE symbol = ?1 AND mode = ?2",
                params![symbol, mode.as_str()],
                |row| row.get(0),
            )
            .optional()?
        };
        if let Some(value) = value {
            return Ok(Some(serde_json::from_str(&value)?));
        }
        // Only the former single strategy inherits the legacy global status.
        if let Some(config) = self.load_config()? {
            if config.exchange.symbol == symbol && config.exchange.trading_mode() == mode {
                return self.load_bot_status();
            }
        }
        Ok(None)
    }

    pub fn save_pair_status(
        &self,
        symbol: &str,
        mode: TradingMode,
        status: BotStatus,
    ) -> Result<()> {
        save_pair_status_on(&self.conn.lock().unwrap(), symbol, mode, status)
    }

    pub fn init_tables(&self) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS app_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                symbol TEXT NOT NULL,
                config_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS account_hourly_snapshots (
                hour INTEGER PRIMARY KEY,
                payload TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS history_account_ids (
                identity BLOB PRIMARY KEY,
                account_id TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS pair_runtime (
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, mode)
            );
            -- Durable account state is independent of deletable trade history.
            CREATE TABLE IF NOT EXISTS paper_accounts (
                symbol TEXT PRIMARY KEY,
                position_json TEXT NOT NULL,
                account_json TEXT NOT NULL
            );


            CREATE TABLE IF NOT EXISTS pair_execution_guard (
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                dirty INTEGER NOT NULL,
                PRIMARY KEY (symbol, mode)
            );

            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                client_order_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT 'UNKNOWN',
                side TEXT NOT NULL,
                price TEXT NOT NULL,
                quantity TEXT NOT NULL,
                amount_usdc TEXT NOT NULL,
                realized_pnl TEXT NOT NULL,
                commission TEXT NOT NULL,
                pnl_verified INTEGER NOT NULL DEFAULT 0,
                is_maker INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                note TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades (timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol);
            CREATE INDEX IF NOT EXISTS idx_trades_client_order_id ON trades (client_order_id);

            -- Minimal recovery metadata survives history cleanup.
            CREATE TABLE IF NOT EXISTS cleared_trade_ids (
                trade_id TEXT PRIMARY KEY,
                client_order_id TEXT NOT NULL,
                mode TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_cleared_trade_order
                ON cleared_trade_ids (client_order_id, mode);

            -- A paired order can remain only in memory, so preserve the fields
            -- needed to complete a cycle for every cleared order.
            CREATE TABLE IF NOT EXISTS cleared_pair_openings (
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                side TEXT NOT NULL,
                price TEXT NOT NULL,
                quantity TEXT NOT NULL,
                PRIMARY KEY (symbol, mode, client_order_id)
            );

            CREATE TABLE IF NOT EXISTS execution_frontiers (
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                side TEXT NOT NULL,
                price TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                PRIMARY KEY (symbol, mode)
            );

            CREATE TABLE IF NOT EXISTS managed_orders (
                client_order_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                order_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_managed_orders_symbol ON managed_orders (symbol);

            CREATE TABLE IF NOT EXISTS order_identity_observations (
                account_key BLOB NOT NULL,
                client_order_id TEXT NOT NULL,
                expected_symbol TEXT NOT NULL,
                observed_client_order_id TEXT NOT NULL,
                observed_symbol TEXT NOT NULL,
                observed_side TEXT NOT NULL,
                observed_order_id INTEGER NOT NULL,
                expected_json TEXT NOT NULL,
                observed_json TEXT NOT NULL,
                PRIMARY KEY (account_key, client_order_id, expected_symbol, observed_client_order_id, observed_symbol, observed_side, observed_order_id)
            );

            CREATE TABLE IF NOT EXISTS initial_order_responses (
                account_key BLOB NOT NULL,
                expected_symbol TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                observed_client_order_id TEXT NOT NULL,
                observed_symbol TEXT NOT NULL,
                observed_side TEXT NOT NULL,
                observed_order_id INTEGER NOT NULL,
                response_json TEXT NOT NULL,
                observation_id TEXT NOT NULL UNIQUE,
                batch_id TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS terminal_order_disagreements (
                account_key BLOB NOT NULL,
                expected_symbol TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                first_json TEXT NOT NULL,
                conflicting_json TEXT NOT NULL,
                PRIMARY KEY (account_key, expected_symbol, client_order_id, first_json, conflicting_json)
            );

            CREATE TABLE IF NOT EXISTS pair_intents (
                parent_client_order_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                order_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pair_intents_scope ON pair_intents (symbol, mode);

            CREATE TABLE IF NOT EXISTS remainder_plans (
                target_client_order_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                active INTEGER NOT NULL,
                plan_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_active_remainder_plan
                ON remainder_plans (symbol, mode) WHERE active = 1;

            CREATE TABLE IF NOT EXISTS bot_runtime (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS grid_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_trades INTEGER NOT NULL,
                completed_cycles INTEGER NOT NULL,
                total_realized_profit TEXT NOT NULL,
                total_volume_usdc TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scoped_grid_stats (
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                total_trades INTEGER NOT NULL,
                completed_cycles INTEGER NOT NULL,
                total_realized_profit TEXT NOT NULL,
                total_volume_usdc TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, mode)
            );

            CREATE TABLE IF NOT EXISTS auth_sessions (
                token TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            );

            "#,
        )
        .context("Failed to initialize SQLite tables")?;

        let has_legacy_admin_auth: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'admin_auth')",
            [],
            |row| row.get(0),
        )?;
        if has_legacy_admin_auth {
            conn.execute_batch(
                "PRAGMA secure_delete = ON;
                 DROP TABLE admin_auth;
                 VACUUM;
                 PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .context("Failed to remove the legacy administrator password from SQLite")?;
            warn!(
                "Removed the legacy administrator password from SQLite; \
                 set ADMIN_PASSWORD or --admin-password to require console authentication"
            );
        }

        // Rebuild the old key atomically, retaining the identities in the stored JSON.
        let tx = conn.transaction()?;
        let has_observed_client_id: i64 = tx.query_row(
            "SELECT count(*) FROM pragma_table_info('order_identity_observations') WHERE name = 'observed_client_order_id'",
            [],
            |row| row.get(0),
        )?;
        if has_observed_client_id == 0 {
            tx.execute_batch(
                r#"
                CREATE TABLE order_identity_observations_new (
                    account_key BLOB NOT NULL,
                    client_order_id TEXT NOT NULL,
                    expected_symbol TEXT NOT NULL,
                    observed_client_order_id TEXT NOT NULL,
                    observed_symbol TEXT NOT NULL,
                    observed_side TEXT NOT NULL,
                    observed_order_id INTEGER NOT NULL,
                    expected_json TEXT NOT NULL,
                    observed_json TEXT NOT NULL,
                    PRIMARY KEY (account_key, client_order_id, expected_symbol, observed_client_order_id, observed_symbol, observed_side, observed_order_id)
                );
                INSERT INTO order_identity_observations_new
                    (account_key, client_order_id, expected_symbol, observed_client_order_id, observed_symbol, observed_side, observed_order_id, expected_json, observed_json)
                SELECT account_key, client_order_id, expected_symbol,
                    json_extract(observed_json, '$.client_order_id'), observed_symbol,
                    json_extract(observed_json, '$.side'), observed_order_id, expected_json, observed_json
                FROM order_identity_observations;
                DROP TABLE order_identity_observations;
                ALTER TABLE order_identity_observations_new RENAME TO order_identity_observations;
                "#,
            )?;
        }
        // Older versions deduplicated by response content. Preserve the order
        // of surviving rows; observations already discarded cannot be restored.
        let has_observation_id: i64 = tx.query_row(
            "SELECT count(*) FROM pragma_table_info('initial_order_responses') WHERE name = 'observation_id'",
            [],
            |row| row.get(0),
        )?;
        if has_observation_id == 0 {
            tx.execute_batch(
                r#"
                CREATE TABLE initial_order_responses_new (
                    account_key BLOB NOT NULL,
                    expected_symbol TEXT NOT NULL,
                    client_order_id TEXT NOT NULL,
                    observed_client_order_id TEXT NOT NULL,
                    observed_symbol TEXT NOT NULL,
                    observed_side TEXT NOT NULL,
                    observed_order_id INTEGER NOT NULL,
                    response_json TEXT NOT NULL,
                    observation_id TEXT NOT NULL UNIQUE
                );
                INSERT INTO initial_order_responses_new
                    (account_key, expected_symbol, client_order_id, observed_client_order_id,
                     observed_symbol, observed_side, observed_order_id, response_json, observation_id)
                SELECT account_key, expected_symbol, client_order_id, observed_client_order_id,
                    observed_symbol, observed_side, observed_order_id, response_json,
                    'legacy:' || rowid
                FROM initial_order_responses ORDER BY rowid;
                DROP TABLE initial_order_responses;
                ALTER TABLE initial_order_responses_new RENAME TO initial_order_responses;
                "#,
            )?;
        }
        let has_batch_id: i64 = tx.query_row(
            "SELECT count(*) FROM pragma_table_info('initial_order_responses') WHERE name = 'batch_id'",
            [],
            |row| row.get(0),
        )?;
        if has_batch_id == 0 {
            tx.execute_batch(
                "ALTER TABLE initial_order_responses ADD COLUMN batch_id TEXT NOT NULL DEFAULT 'legacy';",
            )?;
        }
        let terminal_key_columns: i64 = tx.query_row(
            "SELECT count(*) FROM pragma_table_info('terminal_order_disagreements') WHERE pk > 0",
            [],
            |row| row.get(0),
        )?;
        if terminal_key_columns == 3 {
            tx.execute_batch(
                r#"
                CREATE TABLE terminal_order_disagreements_new (
                    account_key BLOB NOT NULL,
                    expected_symbol TEXT NOT NULL,
                    client_order_id TEXT NOT NULL,
                    first_json TEXT NOT NULL,
                    conflicting_json TEXT NOT NULL,
                    PRIMARY KEY (account_key, expected_symbol, client_order_id, first_json, conflicting_json)
                );
                INSERT INTO terminal_order_disagreements_new
                    (account_key, expected_symbol, client_order_id, first_json, conflicting_json)
                SELECT account_key, expected_symbol, client_order_id, first_json, conflicting_json
                FROM terminal_order_disagreements;
                DROP TABLE terminal_order_disagreements;
                ALTER TABLE terminal_order_disagreements_new RENAME TO terminal_order_disagreements;
                "#,
            )?;
        }
        tx.commit()?;

        let has_pnl_verified: i64 = conn.query_row(
            "SELECT count(*) FROM pragma_table_info('trades') WHERE name = 'pnl_verified';",
            [],
            |row| row.get(0),
        )?;
        if has_pnl_verified == 0 {
            conn.execute(
                "ALTER TABLE trades ADD COLUMN pnl_verified INTEGER NOT NULL DEFAULT 0;",
                [],
            )?;
        }

        let has_mode: i64 = conn.query_row(
            "SELECT count(*) FROM pragma_table_info('trades') WHERE name = 'mode';",
            [],
            |row| row.get(0),
        )?;
        if has_mode == 0 {
            conn.execute(
                "ALTER TABLE trades ADD COLUMN mode TEXT NOT NULL DEFAULT 'UNKNOWN';",
                [],
            )?;
        }
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trades_scope ON trades (symbol, mode, timestamp DESC);",
            [],
        )?;
        retire_legacy_testnet_config(&conn)?;
        retire_legacy_dry_run_intent(&conn)?;
        retire_legacy_testnet_rows(&conn)?;

        info!("SQLite schema verified for database: {}", self.path);
        Ok(())
    }

    pub fn load_config(&self) -> Result<Option<AppConfig>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT config_json FROM app_config WHERE id = 1;")?;
        let mut rows = stmt.query([])?;

        if let Some(row) = rows.next()? {
            let json_str: String = row.get(0)?;
            let config: AppConfig = serde_json::from_str(&json_str)
                .context("Failed to deserialize AppConfig from SQLite database")?;
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    pub fn save_config(&self, config: &AppConfig) -> Result<()> {
        save_config_on(&self.conn.lock().unwrap(), config)
    }

    /// Merge while holding the database mutex so simultaneous pair updates cannot lose changes.
    pub fn save_pair_config(&self, symbol: &str, config: &AppConfig) -> Result<()> {
        save_pair_config_on(&self.conn.lock().unwrap(), symbol, config)
    }

    pub fn save_global_config(&self, global: &crate::config::GlobalConfig) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut saved = load_required_config_on(&conn)?;
        saved.ensure_pair_list()?;
        global.apply_to(&mut saved);
        saved.validate()?;
        save_config_on(&conn, &saved)
    }

    #[cfg(test)]
    pub(crate) fn set_membership_commit_hook(
        &self,
        operation: &'static str,
        reached: tokio::sync::oneshot::Sender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) {
        *self.membership_commit_hook.lock().unwrap() = Some((operation, reached, resume));
    }

    #[cfg(test)]
    fn before_membership_commit(&self, operation: &'static str) {
        let hook = {
            let mut hook = self.membership_commit_hook.lock().unwrap();
            if hook
                .as_ref()
                .is_some_and(|(expected, _, _)| *expected == operation)
            {
                hook.take()
            } else {
                None
            }
        };
        if let Some((_, reached, resume)) = hook {
            let _ = reached.send(());
            let _ = resume.recv();
        }
    }

    /// Add a paused pair and arm its guard atomically; returns whether it was already dirty.
    /// The caller must use this result for startup recovery and must not arm it again.
    pub fn add_pair_config(&self, config: &AppConfig) -> Result<bool> {
        config.validate()?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let mut saved = load_required_config_on(&tx)?;
        saved.ensure_pair_list()?;
        anyhow::ensure!(
            !saved
                .pairs
                .iter()
                .any(|pair| pair.symbol == config.exchange.symbol),
            "交易对已存在"
        );
        let mut pair = crate::config::TradingPairConfig::new(config.exchange.symbol.clone());
        pair.update_from(config);
        saved.pairs.push(pair);
        save_config_on(&tx, &saved)?;
        let mode = config.exchange.trading_mode();
        save_pair_status_on(&tx, &config.exchange.symbol, mode, BotStatus::Paused)?;
        let was_dirty = arm_execution_guard_on(&tx, &config.exchange.symbol, mode)?;
        #[cfg(test)]
        self.before_membership_commit("add");
        tx.commit()?;
        #[cfg(test)]
        {
            drop(conn);
            self.before_membership_commit("add_after");
        }
        Ok(was_dirty)
    }

    pub fn remove_pair_config(&self, symbol: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut saved = load_required_config_on(&conn)?;
        anyhow::ensure!(saved.pairs.len() > 1, "至少保留一个交易对");
        let index = saved
            .pairs
            .iter()
            .position(|pair| pair.symbol == symbol)
            .context("交易对不存在")?;
        saved.pairs.remove(index);
        #[cfg(test)]
        self.before_membership_commit("remove");
        save_config_on(&conn, &saved)?;
        #[cfg(test)]
        {
            drop(conn);
            self.before_membership_commit("remove_after");
        }
        Ok(())
    }

    pub fn save_server_config_checked(
        &self,
        server: &crate::config::ServerConfig,
        baseline_version: Option<&str>,
    ) -> Result<()> {
        server.socket_addr()?;
        let conn = self.conn.lock().unwrap();
        let mut saved = load_required_config_on(&conn)?;
        if baseline_version.is_some_and(|version| version != config_version(&saved.server)) {
            anyhow::bail!("服务设置已被其他请求修改，请刷新后重试");
        }
        saved.server = server.clone();
        save_config_on(&conn, &saved)
    }

    /// A new configuration must never become durable without its current execution status.
    pub fn save_runtime_config(
        &self,
        config: &AppConfig,
        pair_scope: Option<&str>,
        status: BotStatus,
        previous_scope: (&str, TradingMode),
    ) -> Result<(BotStatus, bool)> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        match pair_scope {
            Some(symbol) => save_pair_config_on(&tx, symbol, config)?,
            None => save_config_on(&tx, config)?,
        }
        let mode = config.exchange.trading_mode();
        let switching = previous_scope != (config.exchange.symbol.as_str(), mode);
        arm_execution_guard_on(&tx, &config.exchange.symbol, mode)?;
        // A different account scope always requires a fresh manual resume, even
        // when its execution guard has never been armed before.
        let guarded = switching;
        if switching {
            // Never let the abandoned mode keep a Running status: the mode now
            // derives from globally persisted credentials, so a crash between
            // this pair commit and the global commit must not resume it live.
            save_pair_status_on(&tx, previous_scope.0, previous_scope.1, BotStatus::Paused)?;
            tx.execute(
                "UPDATE pair_execution_guard SET dirty = 0 WHERE symbol = ?1 AND mode = ?2",
                params![previous_scope.0, previous_scope.1.as_str()],
            )?;
        }
        let status = if guarded { BotStatus::Paused } else { status };
        save_pair_status_on(&tx, &config.exchange.symbol, mode, status)?;
        if pair_scope.is_none() {
            save_bot_status_on(&tx, status)?;
        }
        tx.commit()?;
        Ok((status, guarded))
    }

    /// Legacy writers also update the scoped row so switching configuration formats is reversible.
    pub fn save_runtime_status(
        &self,
        symbol: &str,
        mode: TradingMode,
        status: BotStatus,
        legacy: bool,
    ) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        save_pair_status_on(&tx, symbol, mode, status)?;
        if legacy {
            save_bot_status_on(&tx, status)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Arm before starting an engine. An armed guard means the previous engine
    /// did not finish a clean shutdown, so its saved Running state is unsafe.
    pub fn arm_execution_guard(&self, symbol: &str, mode: TradingMode) -> Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let dirty = arm_execution_guard_on(&tx, symbol, mode)?;
        tx.commit()?;
        Ok(dirty)
    }

    pub fn clear_execution_guard(&self, symbol: &str, mode: TradingMode) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE pair_execution_guard SET dirty = 0 WHERE symbol = ?1 AND mode = ?2",
            params![symbol, mode.as_str()],
        )?;
        Ok(())
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<bool> {
        let conn = self.lock_connection("insert_trade");
        let inserted = insert_trade_on(&conn, trade)?;
        if inserted {
            #[cfg(test)]
            self.before_revision_publish();
            let mut revisions = self.trade_revisions.lock().unwrap();
            let revision = self.trade_revision.fetch_add(1, Ordering::Release) + 1;
            revisions.changed(&trade.symbol, revision);
        }
        Ok(inserted)
    }

    /// Delete analysis history atomically while preserving order deduplication
    /// and the execution frontier needed by a running or restarted strategy.
    pub fn clear_trade_history(&self) -> Result<usize> {
        Ok(self.clear_selected_database(true, false)?.0)
    }

    pub fn clear_selected_database(&self, trades: bool, snapshots: bool) -> Result<(usize, usize)> {
        let mut conn = self.lock_connection("clear_selected_database");
        let tx = conn.transaction()?;
        let deleted_trades = if trades {
            tx.execute_batch(
                "INSERT INTO execution_frontiers (symbol, mode, side, price, timestamp)
                 SELECT symbol, mode, side, price, timestamp FROM (
                     SELECT symbol, mode, side, price, timestamp,
                            ROW_NUMBER() OVER (
                                PARTITION BY symbol, mode ORDER BY timestamp DESC, rowid DESC
                            ) AS rank
                     FROM trades
                 ) WHERE rank = 1
                 ON CONFLICT(symbol, mode) DO UPDATE SET
                     side = excluded.side, price = excluded.price, timestamp = excluded.timestamp
                 WHERE excluded.timestamp >= execution_frontiers.timestamp;
                 INSERT OR IGNORE INTO cleared_trade_ids (trade_id, client_order_id, mode)
                     SELECT trade_id, client_order_id, mode FROM trades;",
            )?;
            preserve_pair_openings_on(&tx, None)?;
            let deleted = tx.execute("DELETE FROM trades", [])?;
            tx.execute_batch("DELETE FROM scoped_grid_stats; DELETE FROM grid_stats;")?;
            deleted
        } else {
            0
        };
        let deleted_snapshots = if snapshots {
            tx.execute("DELETE FROM account_hourly_snapshots", [])?
        } else {
            0
        };
        tx.commit()?;
        if trades {
            #[cfg(test)]
            self.before_revision_publish();
            let mut revisions = self.trade_revisions.lock().unwrap();
            let revision = self.trade_revision.fetch_add(1, Ordering::Release) + 1;
            revisions.clear_all(revision);
            let mut clear_revisions = self.trade_clear_revisions.lock().unwrap();
            let clear_revision = self.trade_clear_revision.fetch_add(1, Ordering::Release) + 1;
            clear_revisions.clear_all(clear_revision);
        }
        Ok((deleted_trades, deleted_snapshots))
    }

    /// Clear every mode for one symbol while retaining recovery metadata.
    pub fn clear_trade_history_for_symbol(&self, symbol: &str) -> Result<usize> {
        let mut conn = self.lock_connection("clear_trade_history_for_symbol");
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO execution_frontiers (symbol, mode, side, price, timestamp)
             SELECT symbol, mode, side, price, timestamp FROM (
                 SELECT symbol, mode, side, price, timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol, mode ORDER BY timestamp DESC, rowid DESC
                        ) AS rank
                 FROM trades WHERE symbol = ?1
             ) WHERE rank = 1
             ON CONFLICT(symbol, mode) DO UPDATE SET
                 side = excluded.side, price = excluded.price, timestamp = excluded.timestamp
             WHERE excluded.timestamp >= execution_frontiers.timestamp",
            params![symbol],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO cleared_trade_ids (trade_id, client_order_id, mode)
             SELECT trade_id, client_order_id, mode FROM trades WHERE symbol = ?1",
            params![symbol],
        )?;
        preserve_pair_openings_on(&tx, Some(symbol))?;
        let deleted = tx.execute("DELETE FROM trades WHERE symbol = ?1", params![symbol])?;
        tx.execute(
            "DELETE FROM scoped_grid_stats WHERE symbol = ?1",
            params![symbol],
        )?;
        tx.commit()?;
        #[cfg(test)]
        self.before_revision_publish();
        let mut revisions = self.trade_revisions.lock().unwrap();
        let revision = self.trade_revision.fetch_add(1, Ordering::Release) + 1;
        revisions.changed(symbol, revision);
        let mut clear_revisions = self.trade_clear_revisions.lock().unwrap();
        let clear_revision = self.trade_clear_revision.fetch_add(1, Ordering::Release) + 1;
        clear_revisions.changed(symbol, clear_revision);
        Ok(deleted)
    }

    pub fn insert_trade_with_stats(&self, trade: &TradeRecord, stats: &GridStats) -> Result<bool> {
        self.insert_trade_with_recovery(trade, stats, None, None)
    }

    pub fn insert_trade_with_recovery(
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
        tx.commit()?;
        if inserted {
            #[cfg(test)]
            self.before_revision_publish();
            let mut revisions = self.trade_revisions.lock().unwrap();
            let revision = self.trade_revision.fetch_add(1, Ordering::Release) + 1;
            revisions.changed(&trade.symbol, revision);
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
        let balance = rust_decimal_macros::dec!(10000);
        let account = AccountInfo {
            asset: crate::types::quote_asset(symbol)
                .unwrap_or(symbol)
                .to_string(),
            total_wallet_balance: balance,
            available_balance: balance,
            margin_balance: balance,
            unrealized_profit: Decimal::ZERO,
            update_time: Utc::now(),
        };
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
        Ok(())
    }

    pub fn load_pair_intents(&self, symbol: &str, mode: TradingMode) -> Result<Vec<GridOrder>> {
        let conn = self.lock_reconciliation_connection("load_pair_intents");
        let mut stmt = conn.prepare(
            "SELECT order_json FROM pair_intents WHERE symbol = ?1 AND mode = ?2 ORDER BY created_at;",
        )?;
        let rows = stmt.query_map(params![symbol, mode.as_str()], |row| {
            row.get::<_, String>(0)
        })?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn delete_pair_intent(&self, parent_client_order_id: &str) -> Result<()> {
        let conn = self.lock_reconciliation_connection("delete_pair_intent");
        conn.execute(
            "DELETE FROM pair_intents WHERE parent_client_order_id = ?1;",
            params![parent_client_order_id],
        )?;
        Ok(())
    }

    pub fn clear_pair_intents(&self, symbol: &str, mode: TradingMode) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM pair_intents WHERE symbol = ?1 AND mode = ?2;",
            params![symbol, mode.as_str()],
        )?;
        Ok(())
    }

    pub fn save_remainder_plan(&self, plan: &RemainderPlan) -> Result<()> {
        let active = matches!(
            plan.phase,
            crate::types::RemainderPhase::Canceling | crate::types::RemainderPhase::Submitting
        );
        let conn = self.lock_reconciliation_connection("save_remainder_plan");
        conn.execute(
            "INSERT INTO remainder_plans (target_client_order_id, symbol, mode, active, plan_json, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(target_client_order_id) DO UPDATE SET active = excluded.active, plan_json = excluded.plan_json, updated_at = excluded.updated_at;",
            params![plan.target.client_order_id, plan.symbol, plan.mode.as_str(), active, serde_json::to_string(plan)?, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn load_remainder_plan(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<Option<RemainderPlan>> {
        let conn = self.lock_reconciliation_connection("load_remainder_plan");
        let json: Option<String> = conn.query_row(
            "SELECT plan_json FROM remainder_plans WHERE symbol = ?1 AND mode = ?2 AND active = 1;",
            params![symbol, mode.as_str()], |row| row.get(0),
        ).optional()?;
        json.map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }

    pub fn save_managed_order(&self, order: &GridOrder) -> Result<()> {
        let json = serde_json::to_string(order)?;
        let conn = self.lock_reconciliation_connection("save_managed_order");
        conn.execute(
            "INSERT INTO managed_orders (client_order_id, symbol, order_json, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(client_order_id) DO UPDATE SET symbol = excluded.symbol, order_json = excluded.order_json, updated_at = excluded.updated_at;",
            params![order.client_order_id, order.symbol, json, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn save_managed_orders(&self, orders: &[GridOrder]) -> Result<()> {
        let mut conn = self.lock_reconciliation_connection("save_managed_orders");
        let tx = conn.transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO managed_orders (client_order_id, symbol, order_json, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(client_order_id) DO UPDATE SET symbol = excluded.symbol, order_json = excluded.order_json, updated_at = excluded.updated_at;",
            )?;
            for order in orders {
                statement.execute(params![
                    order.client_order_id,
                    order.symbol,
                    serde_json::to_string(order)?,
                    Utc::now().to_rfc3339(),
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn save_managed_orders_if_absent(&self, orders: &[GridOrder]) -> Result<()> {
        let mut conn = self.lock_reconciliation_connection("save_managed_orders_if_absent");
        let tx = conn.transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO managed_orders (client_order_id, symbol, order_json, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(client_order_id) DO NOTHING;",
            )?;
            for order in orders {
                let inserted = statement.execute(params![
                    order.client_order_id,
                    order.symbol,
                    serde_json::to_string(order)?,
                    Utc::now().to_rfc3339(),
                ])?;
                if inserted == 0 {
                    let (stored_symbol, stored_json): (String, String) = tx.query_row(
                        "SELECT symbol, order_json FROM managed_orders WHERE client_order_id = ?1;",
                        params![order.client_order_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )?;
                    let stored: GridOrder = serde_json::from_str(&stored_json)?;
                    if stored.client_order_id != order.client_order_id
                        || stored_symbol != order.symbol
                        || stored.symbol != order.symbol
                        || stored.side != order.side
                        || stored.order_id != order.order_id
                    {
                        anyhow::bail!(
                            "managed order identity conflict for client order ID {}",
                            order.client_order_id
                        );
                    }
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn load_managed_orders(&self, symbol: &str) -> Result<Vec<GridOrder>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT order_json FROM managed_orders WHERE symbol = ?1;")?;
        let json_rows = stmt.query_map(params![symbol], |row| row.get::<_, String>(0))?;
        let mut orders = Vec::new();
        for row in json_rows {
            orders.push(serde_json::from_str(&row?)?);
        }
        Ok(orders)
    }
    #[cfg(test)]
    pub(crate) fn save_initial_order_responses(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
        responses: &[crate::exchange::BinanceOrderResponse],
    ) -> Result<()> {
        let observations: Vec<_> = responses
            .iter()
            .map(|response| {
                let id = uuid::Uuid::new_v4().to_string();
                (id.clone(), id, response.clone())
            })
            .collect();
        self.save_identified_initial_order_responses(
            account,
            symbol,
            client_order_id,
            &observations,
        )
    }

    // Reuse each observation ID on retry; allocate a fresh ID for every new observation.
    #[cfg(test)]
    pub(crate) fn save_identified_initial_order_responses(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
        observations: &[(String, String, crate::exchange::BinanceOrderResponse)],
    ) -> Result<()> {
        self.save_initial_order_response_rows(
            account,
            symbol,
            &[],
            false,
            observations.iter().map(|(id, batch, response)| {
                (client_order_id, id.as_str(), batch.as_str(), response)
            }),
        )
    }

    pub(crate) fn save_initial_order_response_snapshot(
        &self,
        account: [u8; 32],
        symbol: &str,
        observations: &[(
            String,
            String,
            String,
            crate::exchange::BinanceOrderResponse,
        )],
        identities: &[(GridOrder, crate::types::ObservedOrderIdentity)],
    ) -> Result<()> {
        self.save_initial_order_response_rows(
            account,
            symbol,
            identities,
            true,
            observations.iter().map(|(client_id, id, batch, response)| {
                (client_id.as_str(), id.as_str(), batch.as_str(), response)
            }),
        )
    }

    fn save_initial_order_response_rows<'a>(
        &self,
        account: [u8; 32],
        symbol: &str,
        identities: &[(GridOrder, crate::types::ObservedOrderIdentity)],
        deduplicate_snapshot: bool,
        observations: impl IntoIterator<
            Item = (
                &'a str,
                &'a str,
                &'a str,
                &'a crate::exchange::BinanceOrderResponse,
            ),
        >,
    ) -> Result<()> {
        let mut conn = self.lock_reconciliation_connection("save_initial_order_responses");
        let tx = conn.transaction()?;
        Self::save_identity_observation_rows(&tx, account, identities)?;
        let observations: Vec<_> = observations.into_iter().collect();
        // An earlier identical snapshot already precedes every later individual
        // query, so it supplies the same comparison evidence. Start a new
        // snapshot batch whenever the snapshot changes.
        let duplicate_snapshot = if deduplicate_snapshot
            && !observations.is_empty()
            && observations[0].2.starts_with("snapshot:")
            && observations.iter().all(|row| row.2 == observations[0].2)
        {
            let last_batch: Option<String> = tx.query_row(
                "SELECT batch_id FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND substr(batch_id,1,9)='snapshot:' ORDER BY rowid DESC LIMIT 1",
                params![&account[..], symbol],
                |row| row.get(0),
            ).optional()?;
            if let Some(last_batch) = last_batch.filter(|batch| {
                batch.starts_with("snapshot:") && batch.as_str() != observations[0].2
            }) {
                let mut statement = tx.prepare("SELECT client_order_id, response_json FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND batch_id=?3 ORDER BY rowid")?;
                let previous: Vec<(String, String)> = statement
                    .query_map(params![&account[..], symbol, last_batch], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<rusqlite::Result<_>>()?;
                if previous.len() != observations.len() {
                    false
                } else {
                    let mut same = true;
                    for ((client, json), (new_client, _, _, response)) in
                        previous.iter().zip(&observations)
                    {
                        if client.as_str() != *new_client
                            || json.as_str() != serde_json::to_string(response)?
                        {
                            same = false;
                            break;
                        }
                    }
                    same
                }
            } else {
                false
            }
        } else {
            false
        };
        // Compare the latest evidence for this exact order identity. Queries
        // for other identities cannot affect its transition history.
        let duplicate_query = if deduplicate_snapshot && observations.len() == 1 {
            let (client, observation_id, batch_id, response) = observations[0];
            if uuid::Uuid::parse_str(observation_id).is_ok()
                && uuid::Uuid::parse_str(batch_id).is_ok()
            {
                let previous: Option<(String, String)> = tx.query_row(
                    "SELECT batch_id, response_json FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND client_order_id=?3 AND observed_client_order_id=?4 AND observed_symbol=?5 AND observed_side=?6 AND observed_order_id=?7 ORDER BY rowid DESC LIMIT 1",
                    params![&account[..], symbol, client, response.client_order_id, response.symbol, response.side, response.order_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ).optional()?;
                if let Some((previous_batch, previous_json)) = previous {
                    let same = uuid::Uuid::parse_str(&previous_batch).is_ok()
                        && previous_batch != batch_id
                        && previous_json == serde_json::to_string(response)?;
                    if same {
                        let safe: bool = tx.query_row(
                            "SELECT (SELECT count(*) FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND batch_id=?3)=1 AND NOT EXISTS(SELECT 1 FROM initial_order_responses WHERE observation_id=?4 OR (account_key=?1 AND expected_symbol=?2 AND batch_id=?5))",
                            params![&account[..], symbol, previous_batch, observation_id, batch_id],
                            |row| row.get(0),
                        )?;
                        safe
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };
        {
            let mut statement = tx.prepare(
                "INSERT INTO initial_order_responses (account_key, expected_symbol, client_order_id, observed_client_order_id, observed_symbol, observed_side, observed_order_id, response_json, observation_id, batch_id) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(observation_id) DO NOTHING"
            )?;
            for (client_order_id, observation_id, batch_id, response) in observations
                .into_iter()
                .filter(|_| !duplicate_snapshot && !duplicate_query)
            {
                let json = serde_json::to_string(response)?;
                let inserted = statement.execute(params![
                    &account[..],
                    symbol,
                    client_order_id,
                    response.client_order_id,
                    response.symbol,
                    response.side,
                    response.order_id,
                    &json,
                    observation_id,
                    batch_id
                ])?;
                if inserted == 0 {
                    let matches: bool = tx.query_row(
                        "SELECT account_key=?1 AND expected_symbol=?2 AND client_order_id=?3 AND response_json=?4 AND batch_id=?5 FROM initial_order_responses WHERE observation_id=?6",
                        params![&account[..], symbol, client_order_id, &json, batch_id, observation_id],
                        |row| row.get(0),
                    )?;
                    anyhow::ensure!(
                        matches,
                        "initial order response observation ID reused with different data"
                    );
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn load_initial_order_responses(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Vec<crate::exchange::BinanceOrderResponse>> {
        Ok(self
            .load_initial_order_response_timeline(account, symbol, client_order_id)?
            .into_iter()
            .map(|(_, response)| response)
            .collect())
    }

    pub(crate) fn load_initial_order_response_timeline(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
    ) -> Result<Vec<(String, crate::exchange::BinanceOrderResponse)>> {
        let conn = self.lock_reconciliation_connection("load_initial_order_responses");
        let mut statement = conn.prepare("SELECT batch_id, response_json FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND client_order_id=?3 ORDER BY rowid")?;
        let rows = statement.query_map(params![&account[..], symbol, client_order_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut responses = Vec::new();
        for row in rows {
            let (batch_id, json) = row?;
            responses.push((batch_id, serde_json::from_str(&json)?));
        }
        Ok(responses)
    }

    pub(crate) fn delete_order_evidence_groups(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_ids: &[String],
    ) -> Result<()> {
        if client_ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock_reconciliation_connection("delete_order_evidence_groups");
        let tx = conn.transaction()?;
        {
            let mut initial = tx.prepare("DELETE FROM initial_order_responses WHERE account_key=?1 AND expected_symbol=?2 AND client_order_id=?3")?;
            let mut observations = tx.prepare("DELETE FROM order_identity_observations WHERE account_key=?1 AND expected_symbol=?2 AND client_order_id=?3")?;
            for id in client_ids {
                initial.execute(params![&account[..], symbol, id])?;
                observations.execute(params![&account[..], symbol, id])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn save_terminal_order_disagreement(
        &self,
        account: [u8; 32],
        symbol: &str,
        client_order_id: &str,
        first: &crate::exchange::BinanceOrderResponse,
        conflicting: &crate::exchange::BinanceOrderResponse,
    ) -> Result<()> {
        let conn = self.lock_reconciliation_connection("save_terminal_order_disagreement");
        conn.execute(
            "INSERT INTO terminal_order_disagreements (account_key, expected_symbol, client_order_id, first_json, conflicting_json) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(account_key, expected_symbol, client_order_id, first_json, conflicting_json) DO NOTHING",
            params![&account[..], symbol, client_order_id, serde_json::to_string(first)?, serde_json::to_string(conflicting)?],
        )?;
        Ok(())
    }

    pub(crate) fn load_terminal_order_disagreements(
        &self,
        account: [u8; 32],
        symbol: &str,
    ) -> Result<
        Vec<(
            String,
            crate::exchange::BinanceOrderResponse,
            crate::exchange::BinanceOrderResponse,
        )>,
    > {
        let conn = self.lock_reconciliation_connection("load_terminal_order_disagreements");
        let mut stmt = conn.prepare("SELECT client_order_id, first_json, conflicting_json FROM terminal_order_disagreements WHERE account_key=?1 AND expected_symbol=?2")?;
        let rows = stmt.query_map(params![&account[..], symbol], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut records = Vec::new();
        for row in rows {
            let (client_order_id, first, conflicting) = row?;
            records.push((
                client_order_id,
                serde_json::from_str(&first)?,
                serde_json::from_str(&conflicting)?,
            ));
        }
        Ok(records)
    }

    pub(crate) fn save_identity_observations(
        &self,
        account: [u8; 32],
        records: &[(GridOrder, crate::types::ObservedOrderIdentity)],
    ) -> Result<()> {
        let mut conn = self.lock_reconciliation_connection("save_identity_observations");
        let tx = conn.transaction()?;
        Self::save_identity_observation_rows(&tx, account, records)?;
        tx.commit()?;
        Ok(())
    }

    fn save_identity_observation_rows(
        tx: &rusqlite::Transaction<'_>,
        account: [u8; 32],
        records: &[(GridOrder, crate::types::ObservedOrderIdentity)],
    ) -> Result<()> {
        {
            let mut statement = tx.prepare(
                "INSERT INTO order_identity_observations (account_key, client_order_id, expected_symbol, observed_client_order_id, observed_symbol, observed_side, observed_order_id, expected_json, observed_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(account_key,client_order_id,expected_symbol,observed_client_order_id,observed_symbol,observed_side,observed_order_id) DO UPDATE SET expected_json=excluded.expected_json, observed_json=excluded.observed_json"
            )?;
            for (expected, observed) in records {
                statement.execute(params![
                    &account[..],
                    expected.client_order_id,
                    expected.symbol,
                    observed.client_order_id,
                    observed.symbol,
                    observed.side,
                    observed.order_id,
                    serde_json::to_string(expected)?,
                    serde_json::to_string(observed)?
                ])?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn save_identity_observation(
        &self,
        account: [u8; 32],
        expected: &GridOrder,
        observed: &crate::types::ObservedOrderIdentity,
    ) -> Result<()> {
        self.save_identity_observations(account, &[(expected.clone(), observed.clone())])
    }

    pub(crate) fn load_identity_observations(
        &self,
        account: [u8; 32],
        symbol: &str,
    ) -> Result<Vec<(GridOrder, crate::types::ObservedOrderIdentity)>> {
        let conn = self.lock_reconciliation_connection("load_identity_observations");
        let mut stmt = conn.prepare("SELECT expected_json, observed_json FROM order_identity_observations WHERE account_key=?1 AND expected_symbol=?2")?;
        let rows = stmt.query_map(params![&account[..], symbol], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut observations = Vec::new();
        for row in rows {
            let (expected, observed) = row?;
            observations.push((
                serde_json::from_str(&expected)?,
                serde_json::from_str(&observed)?,
            ));
        }
        Ok(observations)
    }

    pub fn delete_managed_order(&self, client_order_id: &str) -> Result<()> {
        let conn = self.lock_reconciliation_connection("delete_managed_order");
        conn.execute(
            "DELETE FROM managed_orders WHERE client_order_id = ?1;",
            params![client_order_id],
        )?;
        Ok(())
    }

    pub fn clear_managed_orders(&self, symbol: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM managed_orders WHERE symbol = ?1;",
            params![symbol],
        )?;
        Ok(())
    }

    pub fn get_trade_by_client_id(
        &self,
        client_order_id: &str,
        mode: TradingMode,
    ) -> Result<Option<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note, mode FROM trades WHERE client_order_id = ?1 AND mode = ?2 ORDER BY timestamp DESC LIMIT 1;",
        )?;
        let mut rows = stmt.query(params![client_order_id, mode.as_str()])?;
        rows.next()?
            .map(trade_from_row)
            .transpose()
            .map_err(Into::into)
    }

    /// Look up the data required by a paired cycle, including cleared history.
    pub fn get_pair_opening(
        &self,
        symbol: &str,
        client_order_id: &str,
        mode: TradingMode,
    ) -> Result<Option<(OrderSide, Decimal, Decimal)>> {
        let conn = self.lock_reconciliation_connection("get_pair_opening");
        let value: Option<(String, String, String)> = conn.query_row(
            "SELECT side, price, quantity FROM (
                 SELECT side, price, quantity, 0 AS priority, timestamp FROM trades
                 WHERE symbol = ?1 AND client_order_id = ?2 AND mode = ?3
                 UNION ALL
                 SELECT side, price, quantity, 1 AS priority, '' AS timestamp FROM cleared_pair_openings
                 WHERE symbol = ?1 AND client_order_id = ?2 AND mode = ?3
             ) ORDER BY priority, timestamp DESC LIMIT 1",
            params![symbol, client_order_id, mode.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).optional()?;
        value
            .map(|(side, price, quantity)| {
                let side = match side.as_str() {
                    "BUY" => OrderSide::Buy,
                    "SELL" => OrderSide::Sell,
                    _ => anyhow::bail!("Invalid paired opening side: {}", side),
                };
                Ok((
                    side,
                    Decimal::from_str(&price)?,
                    Decimal::from_str(&quantity)?,
                ))
            })
            .transpose()
    }

    pub fn has_recorded_order(&self, client_order_id: &str, mode: TradingMode) -> Result<bool> {
        let conn = self.lock_reconciliation_connection("has_recorded_order");
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM trades WHERE client_order_id = ?1 AND mode = ?2)
                 OR EXISTS(SELECT 1 FROM cleared_trade_ids WHERE client_order_id = ?1 AND mode = ?2);",
            params![client_order_id, mode.as_str()],
            |row| row.get(0),
        )?)
    }

    /// Restore the execution frontier for this symbol and mode.
    pub fn last_fill_for(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<Option<(OrderSide, Decimal)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT side, price FROM (
                 SELECT side, price, timestamp, rowid AS sequence FROM trades WHERE symbol = ?1 AND mode = ?2
                 UNION ALL
                 SELECT side, price, timestamp, 0 AS sequence FROM execution_frontiers WHERE symbol = ?1 AND mode = ?2
             ) ORDER BY timestamp DESC, sequence DESC LIMIT 1;",
        )?;
        let value: Option<(String, String)> = stmt
            .query_row(params![symbol, mode.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .optional()?;
        value
            .map(|(side, price)| {
                let side = match side.as_str() {
                    "BUY" => OrderSide::Buy,
                    "SELL" => OrderSide::Sell,
                    _ => anyhow::bail!("invalid saved fill side: {}", side),
                };
                let price = Decimal::from_str(&price)?;
                anyhow::ensure!(price > Decimal::ZERO, "invalid saved fill price: {}", price);
                Ok((side, price))
            })
            .transpose()
    }

    /// Return true only for the first successful PnL verification of a saved trade.
    pub fn verify_trade_pnl(&self, trade: &TradeRecord) -> Result<bool> {
        let mut conn = self.lock_connection("verify_trade_pnl");
        let tx = conn.transaction()?;
        let updated_symbol: Option<String> = {
            let mut statement = tx.prepare(
                "UPDATE trades SET realized_pnl = ?1, commission = ?2, is_maker = ?3, pnl_verified = 1 WHERE trade_id = ?4 AND mode = ?5 AND pnl_verified = 0 RETURNING symbol;",
            )?;
            let mut rows = statement.query(params![
                trade.realized_pnl.to_string(),
                trade.commission.to_string(),
                if trade.is_maker { 1 } else { 0 },
                trade.trade_id,
                trade.mode.as_str(),
            ])?;
            let symbol = rows.next()?.map(|row| row.get(0)).transpose()?;
            // Step through completion so errors after RETURNING are propagated.
            while rows.next()?.is_some() {}
            symbol
        };
        tx.commit()?;
        if let Some(symbol) = updated_symbol {
            #[cfg(test)]
            self.before_revision_publish();
            let mut revisions = self.trade_revisions.lock().unwrap();
            let revision = self.trade_revision.fetch_add(1, Ordering::Release) + 1;
            revisions.changed(&symbol, revision);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get_recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT trade_id, client_order_id, symbol, side, price, quantity,
                   amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note, mode
            FROM trades
            ORDER BY timestamp DESC, rowid DESC
            LIMIT ?1;
            "#,
        )?;

        let rows = stmt.query_map(params![limit as i64], trade_from_row)?;

        let mut list = Vec::new();
        for item in rows {
            list.push(item?);
        }
        Ok(list)
    }

    pub fn get_recent_trades_for(
        &self,
        symbol: &str,
        mode: TradingMode,
        limit: usize,
    ) -> Result<Vec<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note, mode FROM trades WHERE symbol = ?1 AND mode = ?2 ORDER BY timestamp DESC, rowid DESC LIMIT ?3;",
        )?;
        let rows = stmt.query_map(params![symbol, mode.as_str(), limit as i64], trade_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Count today's persisted orders from Beijing midnight to now, including empty hours.
    pub fn get_hourly_trade_stats(
        &self,
        symbol: &str,
        mode: TradingMode,
        now: DateTime<Utc>,
    ) -> Result<HourlyTradeStats> {
        #[cfg(test)]
        self.hourly_stats_queries.fetch_add(1, Ordering::Relaxed);
        let window_start = beijing_midnight(now);
        let first_hour = window_start.timestamp();
        let last_hour = now.timestamp().div_euclid(3600) * 3600;
        let mut buckets: Vec<_> = (first_hour..=last_hour)
            .step_by(3600)
            .map(|timestamp| HourlyTradeCount {
                hour_start: DateTime::from_timestamp(timestamp, 0).unwrap(),
                buy_count: 0,
                sell_count: 0,
            })
            .collect();

        let mut realized_pnl = Decimal::ZERO;
        let mut pending_pnl_count = 0;
        let conn = self.lock_reconciliation_connection("get_hourly_trade_stats");
        // Stored timestamps are UTC RFC3339. Coarse day bounds use the scope index;
        // parse before the exact comparison to preserve fractional-second precision.
        let mut stmt = conn.prepare(
            "SELECT timestamp, side, realized_pnl, pnl_verified FROM trades
             WHERE symbol = ?1 AND mode = ?2 AND timestamp >= ?3 AND timestamp < ?4;",
        )?;
        let rows = stmt.query_map(
            params![
                symbol,
                mode.as_str(),
                window_start.date_naive().to_string(),
                (now + ChronoDuration::days(1)).date_naive().to_string(),
            ],
            |row| {
                Ok((
                    row.get::<_, DateTime<Utc>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            },
        )?;
        for row in rows {
            let (timestamp, side, pnl, verified) = row?;
            if timestamp < window_start || timestamp > now {
                continue;
            }
            if verified {
                realized_pnl += Decimal::from_str(&pnl)?;
            } else {
                pending_pnl_count += 1;
            }
            let index = ((timestamp.timestamp() - first_hour) / 3600) as usize;
            match side.as_str() {
                "BUY" => buckets[index].buy_count += 1,
                "SELL" => buckets[index].sell_count += 1,
                _ => {}
            }
        }
        Ok(HourlyTradeStats {
            window_start,
            window_end: now,
            buckets,
            realized_pnl,
            pending_pnl_count,
        })
    }

    /// Aggregate persisted trades across all modes and symbols for the Beijing day.
    pub fn get_today_pnl(&self, now: DateTime<Utc>) -> Result<crate::types::TodayPnl> {
        let window_start = beijing_midnight(now);
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT symbol, mode, timestamp, realized_pnl, pnl_verified FROM trades
             WHERE timestamp >= ?1 AND timestamp < ?2 ORDER BY mode, symbol",
        )?;
        let rows = stmt.query_map(
            params![
                window_start.date_naive().to_string(),
                (now + ChronoDuration::days(1)).date_naive().to_string()
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, DateTime<Utc>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            },
        )?;
        let mut groups = std::collections::BTreeMap::new();
        for row in rows {
            let (symbol, mode, timestamp, pnl, verified) = row?;
            if timestamp < window_start || timestamp > now {
                continue;
            }
            let group = groups
                .entry((mode.clone(), symbol.clone()))
                .or_insert_with(|| crate::types::TodayPnlGroup {
                    symbol,
                    mode: TradingMode::from_db(&mode),
                    realized_pnl: Decimal::ZERO,
                    pending_pnl_count: 0,
                });
            if verified {
                group.realized_pnl += Decimal::from_str(&pnl)?;
            } else {
                group.pending_pnl_count += 1;
            }
        }
        Ok(crate::types::TodayPnl {
            window_start,
            window_end: now,
            groups: groups.into_values().collect(),
        })
    }

    pub async fn cached_today_pnl(
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

    pub fn get_unverified_trades(
        &self,
        symbol: &str,
        mode: TradingMode,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TradeRecord>> {
        self.get_unverified_trades_with_clear_revision(symbol, mode, limit, offset)
            .map(|(trades, _)| trades)
    }

    pub fn get_unverified_trades_with_clear_revision(
        &self,
        symbol: &str,
        mode: TradingMode,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<TradeRecord>, u64)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note, mode FROM trades WHERE symbol = ?1 AND mode = ?2 AND pnl_verified = 0 ORDER BY timestamp DESC LIMIT ?3 OFFSET ?4;",
        )?;
        let rows = stmt.query_map(
            params![symbol, mode.as_str(), limit as i64, offset as i64],
            trade_from_row,
        )?;
        let trades = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let revision = self
            .trade_clear_revisions
            .lock()
            .unwrap()
            .for_symbol(symbol);
        Ok((trades, revision))
    }

    pub fn symbol_trade_clear_revision(&self, symbol: &str) -> u64 {
        let _conn = self.conn.lock().unwrap();
        self.trade_clear_revisions
            .lock()
            .unwrap()
            .for_symbol(symbol)
    }

    pub fn get_pnl_totals(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<(Decimal, Decimal, usize)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT realized_pnl, commission, pnl_verified FROM trades WHERE symbol = ?1 AND mode = ?2;",
        )?;
        let rows = stmt.query_map(params![symbol, mode.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i32>(2)?,
            ))
        })?;
        let (mut pnl, mut fees, mut pending) = (Decimal::ZERO, Decimal::ZERO, 0);
        for row in rows {
            let (gross, commission, verified) = row?;
            if verified != 0 {
                pnl += Decimal::from_str(&gross)?;
                fees += Decimal::from_str(&commission)?;
            } else {
                pending += 1;
            }
        }
        Ok((pnl, fees, pending))
    }

    pub fn load_scoped_stats(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<Option<(usize, usize, Decimal, Decimal)>> {
        let conn = self.conn.lock().unwrap();
        let row: Option<(i64, i64, String, String)> = conn
            .query_row(
                "SELECT total_trades, completed_cycles, total_realized_profit, total_volume_usdc FROM scoped_grid_stats WHERE symbol = ?1 AND mode = ?2;",
                params![symbol, mode.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        row.map(|(trades, cycles, profit, volume)| {
            Ok((
                trades as usize,
                cycles as usize,
                Decimal::from_str(&profit)?,
                Decimal::from_str(&volume)?,
            ))
        })
        .transpose()
    }

    pub fn load_stats(&self) -> Result<Option<(usize, usize, Decimal, Decimal)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT total_trades, completed_cycles, total_realized_profit, total_volume_usdc FROM grid_stats WHERE id = 1;",
        )?;
        let mut rows = stmt.query([])?;

        if let Some(row) = rows.next()? {
            let total_trades: i64 = row.get(0)?;
            let completed_cycles: i64 = row.get(1)?;
            let profit_str: String = row.get(2)?;
            let volume_str: String = row.get(3)?;

            Ok(Some((
                total_trades as usize,
                completed_cycles as usize,
                Decimal::from_str(&profit_str).unwrap_or(Decimal::ZERO),
                Decimal::from_str(&volume_str).unwrap_or(Decimal::ZERO),
            )))
        } else {
            Ok(None)
        }
    }

    pub fn save_stats(&self, stats: &GridStats) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        save_stats_on(&conn, stats)
    }
}

fn load_required_config_on(conn: &Connection) -> Result<AppConfig> {
    let json: String = conn.query_row(
        "SELECT config_json FROM app_config WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

fn save_config_on(conn: &Connection, config: &AppConfig) -> Result<()> {
    let json =
        serde_json::to_string_pretty(config).context("Failed to serialize AppConfig to JSON")?;
    conn.execute(
        "INSERT INTO app_config (id, symbol, config_json, updated_at) VALUES (1, ?1, ?2, ?3)
         ON CONFLICT(id) DO UPDATE SET symbol = excluded.symbol,
         config_json = excluded.config_json, updated_at = excluded.updated_at",
        params![config.exchange.symbol, json, Utc::now().to_rfc3339()],
    )
    .context("Failed to save config to SQLite database")?;
    Ok(())
}

/// Process-local keyed version: configuration secrets cannot be guessed from this value.
pub(crate) fn config_version<T: serde::Serialize>(value: &T) -> String {
    use hmac::{Hmac, Mac};
    use rand::RngCore;
    use sha2::Sha256;
    static KEY: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();
    let key = KEY.get_or_init(|| {
        let mut key = [0; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);
        key
    });
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("valid HMAC key");
    let serialized = serde_json::to_vec(value).unwrap_or_default();
    mac.update(&serialized);
    hex::encode(mac.finalize().into_bytes())
}

fn save_pair_config_on(conn: &Connection, symbol: &str, config: &AppConfig) -> Result<()> {
    anyhow::ensure!(
        symbol == config.exchange.symbol,
        "交易对不能直接重命名，请在页面中添加新交易对"
    );
    let mut saved = load_required_config_on(conn)?;
    let pair = saved
        .pairs
        .iter_mut()
        .find(|pair| pair.symbol == symbol)
        .with_context(|| format!("交易对 {} 不在已保存的配置中", symbol))?;
    pair.update_from(config);
    save_config_on(conn, &saved)
}

/// Testnet trading was removed. A saved strategy pinned to the retired Binance
/// testnet must not silently start trading on mainnet, so it is switched to
/// paper mode and paused until an operator reviews it.
fn retire_legacy_testnet_config(conn: &Connection) -> Result<()> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT config_json FROM app_config WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let Some(stored) = stored else {
        return Ok(());
    };
    let Ok(mut config) = serde_json::from_str::<serde_json::Value>(&stored) else {
        return Ok(());
    };
    let Some(root) = config.as_object_mut() else {
        return Ok(());
    };
    let mut symbols = Vec::new();
    let mut retired = false;
    let mut paper_required = false;
    let mut global_testnet = false;
    if let Some(exchange) = root
        .get_mut("exchange")
        .and_then(serde_json::Value::as_object_mut)
    {
        if let Some(flag) = exchange
            .remove("is_testnet")
            .and_then(|value| value.as_bool())
        {
            retired = true;
            global_testnet = flag;
            paper_required |= flag;
        }
    }
    if let Some(pairs) = root
        .get_mut("pairs")
        .and_then(serde_json::Value::as_array_mut)
    {
        for pair in pairs.iter_mut() {
            let Some(pair) = pair.as_object_mut() else {
                continue;
            };
            let flag = pair
                .get_mut("exchange")
                .and_then(serde_json::Value::as_object_mut)
                .and_then(|exchange| exchange.remove("is_testnet"))
                .and_then(|value| value.as_bool());
            retired |= flag.is_some();
            if !flag.unwrap_or(global_testnet) {
                continue;
            }
            if !pair.contains_key("exchange") {
                pair.insert(
                    "exchange".into(),
                    serde_json::Value::Object(Default::default()),
                );
            }
            if let Some(exchange) = pair
                .get_mut("exchange")
                .and_then(serde_json::Value::as_object_mut)
            {
                let testnet = flag.unwrap_or(global_testnet);
                paper_required |= testnet;
                if testnet {
                    exchange.remove("api_key");
                    exchange.remove("api_secret");
                }
            }
            if let Some(symbol) = pair.get("symbol").and_then(|value| value.as_str()) {
                symbols.push(symbol.to_ascii_uppercase());
            }
        }
    }
    if !retired {
        return Ok(());
    }
    if paper_required {
        if let Some(exchange) = root
            .get_mut("exchange")
            .and_then(serde_json::Value::as_object_mut)
        {
            exchange.remove("api_key");
            exchange.remove("api_secret");
        }
    }
    if global_testnet {
        if let Some(symbol) = root
            .get("exchange")
            .and_then(|exchange| exchange.get("symbol"))
            .and_then(|value| value.as_str())
        {
            symbols.push(symbol.to_ascii_uppercase());
        }
    }
    conn.execute(
        "UPDATE app_config SET config_json = ?1, updated_at = ?2 WHERE id = 1",
        params![
            serde_json::to_string_pretty(&config)?,
            Utc::now().to_rfc3339()
        ],
    )?;
    symbols.sort();
    symbols.dedup();
    if symbols.is_empty() {
        return Ok(());
    }
    for symbol in &symbols {
        save_pair_status_on(conn, symbol, TradingMode::Paper, BotStatus::Paused)?;
    }
    warn!(
        "Retired testnet configuration now uses paper mode and stays paused until resumed: {}",
        symbols.join(", ")
    );
    Ok(())
}

/// Run mode used to be stored per pair. A saved paper-mode configuration could
/// still carry API credentials, which now select live mode. Force that derived
/// live scope to start paused and drop the retired flag so an operator's later
/// resume is not undone on the next restart.
fn retire_legacy_dry_run_intent(conn: &Connection) -> Result<()> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT config_json FROM app_config WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let Some(stored) = stored else {
        return Ok(());
    };
    let Ok(mut config) = serde_json::from_str::<serde_json::Value>(&stored) else {
        return Ok(());
    };
    let Some(root) = config.as_object_mut() else {
        return Ok(());
    };
    let mut removed = false;
    let global_intent = root
        .get_mut("exchange")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|exchange| {
            let value = exchange.remove("dry_run");
            removed |= value.is_some();
            value
        })
        .and_then(|value| value.as_bool());
    // Older configs defaulted both fields to paper when omitted.
    let default_intent = global_intent.unwrap_or(true);
    let mut pair_intents = std::collections::HashMap::new();
    if let Some(pairs) = root
        .get_mut("pairs")
        .and_then(serde_json::Value::as_array_mut)
    {
        for pair in pairs {
            let symbol = pair
                .get("symbol")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let intent = pair
                .get_mut("exchange")
                .and_then(serde_json::Value::as_object_mut)
                .and_then(|exchange| {
                    let value = exchange.remove("dry_run");
                    removed |= value.is_some();
                    value
                })
                .and_then(|value| value.as_bool());
            if let Some(symbol) = symbol {
                pair_intents.insert(symbol, intent.unwrap_or(default_intent));
            }
        }
    }
    if !removed {
        return Ok(());
    }
    let mut app: AppConfig = serde_json::from_value(config.clone())?;
    // Match the promotion performed by AppConfig::ensure_pair_list at startup.
    app.ensure_pair_list()?;
    let live_symbols: Vec<String> = app
        .strategy_configs()?
        .into_iter()
        .filter(|strategy| {
            strategy.exchange.trading_mode() == TradingMode::Live
                && pair_intents
                    .get(&strategy.exchange.symbol)
                    .copied()
                    .unwrap_or(default_intent)
        })
        .map(|strategy| strategy.exchange.symbol)
        .collect();
    // Persist the pause before consuming the legacy flag: a crash in between
    // re-applies the safe pause, never an automatic live resume.
    for symbol in &live_symbols {
        save_pair_status_on(conn, symbol, TradingMode::Live, BotStatus::Paused)?;
    }
    conn.execute(
        "UPDATE app_config SET config_json = ?1, updated_at = ?2 WHERE id = 1",
        params![
            serde_json::to_string_pretty(&config)?,
            Utc::now().to_rfc3339()
        ],
    )?;
    if !live_symbols.is_empty() {
        warn!(
            "Legacy paper configuration had API credentials; now live and paused until resumed: {}",
            live_symbols.join(", ")
        );
    }
    Ok(())
}

/// Drop the state of the retired testnet mode. Those orders can never be
/// reconciled against mainnet, so keeping their runtime rows would only leave
/// unreachable intent guards; trade history is kept and reads back as `UNKNOWN`.
fn retire_legacy_testnet_rows(conn: &Connection) -> Result<()> {
    conn.execute(
        "UPDATE trades SET mode = 'UNKNOWN' WHERE mode = 'TESTNET'",
        [],
    )?;
    for table in [
        "pair_runtime",
        "pair_execution_guard",
        "pair_intents",
        "remainder_plans",
        "cleared_trade_ids",
        "cleared_pair_openings",
        "execution_frontiers",
        "scoped_grid_stats",
    ] {
        conn.execute(&format!("DELETE FROM {table} WHERE mode = 'TESTNET'"), [])?;
    }
    Ok(())
}

fn has_legacy_paper_history_on(conn: &Connection, symbol: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM trades WHERE symbol = ?1 AND mode IN ('PAPER', 'UNKNOWN')) \
         OR EXISTS(SELECT 1 FROM execution_frontiers WHERE symbol = ?1 AND mode IN ('PAPER', 'UNKNOWN')) \
         OR EXISTS(SELECT 1 FROM cleared_pair_openings WHERE symbol = ?1 AND mode IN ('PAPER', 'UNKNOWN'))",
        params![symbol], |row| row.get(0),
    )?)
}

fn arm_execution_guard_on(conn: &Connection, symbol: &str, mode: TradingMode) -> Result<bool> {
    let dirty: Option<bool> = conn
        .query_row(
            "SELECT dirty FROM pair_execution_guard WHERE symbol = ?1 AND mode = ?2",
            params![symbol, mode.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    conn.execute(
        "INSERT INTO pair_execution_guard (symbol, mode, dirty) VALUES (?1, ?2, 1)
         ON CONFLICT(symbol, mode) DO UPDATE SET dirty = 1",
        params![symbol, mode.as_str()],
    )?;
    Ok(dirty.unwrap_or(false))
}

fn save_pair_status_on(
    conn: &Connection,
    symbol: &str,
    mode: TradingMode,
    status: BotStatus,
) -> Result<()> {
    conn.execute(
        "INSERT INTO pair_runtime (symbol, mode, status, updated_at) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(symbol, mode) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at",
        params![symbol, mode.as_str(), serde_json::to_string(&status)?, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn save_bot_status_on(conn: &Connection, status: BotStatus) -> Result<()> {
    let value = serde_json::to_value(status)?;
    let name = value
        .as_str()
        .context("Invalid bot status representation")?;
    conn.execute(
        "INSERT INTO bot_runtime (id, status, updated_at) VALUES (1, ?1, ?2)
         ON CONFLICT(id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at",
        params![name, Utc::now().to_rfc3339()],
    )?;
    Ok(())
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
    let cleared: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM cleared_trade_ids WHERE trade_id = ?1)
             OR EXISTS(SELECT 1 FROM cleared_trade_ids WHERE client_order_id = ?2 AND mode = ?3);",
        params![trade.trade_id, trade.client_order_id, trade.mode.as_str()],
        |row| row.get(0),
    )?;
    if cleared {
        return Ok(false);
    }
    let side_str = match trade.side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    };
    let inserted = conn
        .execute(
            r#"
            INSERT OR IGNORE INTO trades (
                trade_id, client_order_id, symbol, mode, side, price, quantity,
                amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14);
            "#,
            params![
                trade.trade_id,
                trade.client_order_id,
                trade.symbol,
                trade.mode.as_str(),
                side_str,
                trade.price.to_string(),
                trade.quantity.to_string(),
                trade.amount_usdc.to_string(),
                trade.realized_pnl.to_string(),
                trade.commission.to_string(),
                if trade.pnl_verified { 1 } else { 0 },
                if trade.is_maker { 1 } else { 0 },
                trade.timestamp,
                trade.note,
            ],
        )
        .context("Failed to insert trade into SQLite database")?;
    Ok(inserted == 1)
}

fn save_scoped_stats_on(
    conn: &Connection,
    symbol: &str,
    mode: TradingMode,
    stats: &GridStats,
) -> Result<()> {
    conn.execute(
        "INSERT INTO scoped_grid_stats (symbol, mode, total_trades, completed_cycles, total_realized_profit, total_volume_usdc, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(symbol, mode) DO UPDATE SET total_trades = excluded.total_trades, completed_cycles = excluded.completed_cycles, total_realized_profit = excluded.total_realized_profit, total_volume_usdc = excluded.total_volume_usdc, updated_at = excluded.updated_at;",
        params![symbol, mode.as_str(), stats.total_trades as i64, stats.completed_cycles as i64, stats.total_realized_profit.to_string(), stats.total_volume_usdc.to_string(), Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn save_stats_on(conn: &Connection, stats: &GridStats) -> Result<()> {
    conn.execute(
        r#"
        INSERT INTO grid_stats (id, total_trades, completed_cycles, total_realized_profit, total_volume_usdc, updated_at)
        VALUES (1, ?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(id) DO UPDATE SET
            total_trades = excluded.total_trades,
            completed_cycles = excluded.completed_cycles,
            total_realized_profit = excluded.total_realized_profit,
            total_volume_usdc = excluded.total_volume_usdc,
            updated_at = excluded.updated_at;
        "#,
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

fn trade_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TradeRecord> {
    let side: String = row.get(3)?;
    let price: String = row.get(4)?;
    let quantity: String = row.get(5)?;
    let amount: String = row.get(6)?;
    let pnl: String = row.get(7)?;
    let commission: String = row.get(8)?;
    Ok(TradeRecord {
        trade_id: row.get(0)?,
        client_order_id: row.get(1)?,
        symbol: row.get(2)?,
        mode: TradingMode::from_db(&row.get::<_, String>(13)?),
        side: if side == "BUY" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        price: Decimal::from_str(&price).unwrap_or(Decimal::ZERO),
        quantity: Decimal::from_str(&quantity).unwrap_or(Decimal::ZERO),
        amount_usdc: Decimal::from_str(&amount).unwrap_or(Decimal::ZERO),
        realized_pnl: Decimal::from_str(&pnl).unwrap_or(Decimal::ZERO),
        commission: Decimal::from_str(&commission).unwrap_or(Decimal::ZERO),
        pnl_verified: row.get::<_, i32>(9)? != 0,
        is_maker: row.get::<_, i32>(10)? != 0,
        timestamp: row.get(11)?,
        note: row.get(12)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn revision_test_trade() -> TradeRecord {
        TradeRecord {
            trade_id: "revision-trade".into(),
            client_order_id: "revision-order".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            pnl_verified: false,
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn revision_read_does_not_block_runtime_during_sql_write() {
        use std::sync::{mpsc, Arc, LazyLock};
        use std::time::Duration;

        type BusyHandshake = (tokio::sync::oneshot::Sender<()>, mpsc::Receiver<()>);
        static BUSY: LazyLock<Mutex<Option<BusyHandshake>>> = LazyLock::new(|| Mutex::new(None));
        fn busy_handler(_: i32) -> bool {
            let (reached, resume) = BUSY.lock().unwrap().take().unwrap();
            reached.send(()).unwrap();
            resume.recv().unwrap();
            true
        }

        let path = std::env::temp_dir().join(format!("revision-lock-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        let external = Connection::open(&path).unwrap();
        external.execute_batch("BEGIN IMMEDIATE").unwrap();
        let (busy_tx, busy_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        *BUSY.lock().unwrap() = Some((busy_tx, resume_rx));
        db.conn
            .lock()
            .unwrap()
            .busy_handler(Some(busy_handler))
            .unwrap();
        let writer_db = Arc::clone(&db);
        let writer =
            tokio::task::spawn_blocking(move || writer_db.insert_trade(&revision_test_trade()));
        busy_rx.await.unwrap();

        // Release SQLite even if a regression blocks the runtime thread.
        let (read_tx, read_rx) = mpsc::channel();
        let watchdog = std::thread::spawn(move || {
            let completed = read_rx.recv_timeout(Duration::from_secs(5)).is_ok();
            external.execute_batch("ROLLBACK").unwrap();
            resume_tx.send(()).unwrap();
            completed
        });
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 0);
        read_tx.send(()).unwrap();
        assert!(
            watchdog.join().unwrap(),
            "revision read blocked the Tokio thread"
        );
        assert!(writer.await.unwrap().unwrap());
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 1);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn committed_write_publishes_revision_before_releasing_connection() {
        use std::sync::{mpsc, Arc};

        let db = Arc::new(Database::open(":memory:").unwrap());
        let (reached_tx, reached_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        *db.revision_publish_hook.lock().unwrap() = Some((reached_tx, resume_rx));
        let writer_db = Arc::clone(&db);
        let writer = std::thread::spawn(move || writer_db.insert_trade(&revision_test_trade()));
        reached_rx.recv().unwrap();
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 0);
        assert_eq!(db.trade_data_revision(), 0);
        assert!(db.conn.try_lock().is_err());
        resume_tx.send(()).unwrap();
        assert!(writer.join().unwrap().unwrap());
        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM trades", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 1);
        assert_eq!(db.trade_data_revision(), 1);
    }

    #[test]
    fn pnl_commit_failure_rolls_back_without_publishing_revision() {
        let db = Database::open(":memory:").unwrap();
        let trade = revision_test_trade();
        assert!(db.insert_trade(&trade).unwrap());
        db.conn.lock().unwrap().execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE revision_parent (id INTEGER PRIMARY KEY);
             CREATE TABLE revision_child (parent_id INTEGER REFERENCES revision_parent(id) DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER reject_pnl_commit AFTER UPDATE ON trades BEGIN
                 INSERT INTO revision_child VALUES (1);
             END;"
        ).unwrap();
        assert!(db.verify_trade_pnl(&trade).is_err());
        assert_eq!(db.trade_data_revision(), 1);
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 1);
        assert!(!db.get_recent_trades(1).unwrap()[0].pnl_verified);
        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_pnl_commit")
            .unwrap();
        assert!(db.verify_trade_pnl(&trade).unwrap());
        assert_eq!(db.trade_data_revision(), 2);
    }

    #[test]
    fn revisions_follow_successful_trade_changes_and_rollback_failures() {
        let db = Database::open(":memory:").unwrap();
        let trade = revision_test_trade();
        let stats = GridStats::default();
        db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_insert BEFORE INSERT ON trades BEGIN SELECT RAISE(ABORT, 'reject'); END;"
        ).unwrap();
        assert!(db.insert_trade(&trade).is_err());
        assert!(db.insert_trade_with_stats(&trade, &stats).is_err());
        assert_eq!(db.trade_data_revision(), 0);
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 0);
        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_insert")
            .unwrap();
        assert!(db.insert_trade_with_stats(&trade, &stats).unwrap());
        assert!(!db.insert_trade(&trade).unwrap());
        assert!(!db.insert_trade_with_stats(&trade, &stats).unwrap());
        assert_eq!(db.trade_data_revision(), 1);

        db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_update BEFORE UPDATE ON trades BEGIN SELECT RAISE(ABORT, 'reject'); END;
             CREATE TRIGGER reject_delete BEFORE DELETE ON trades BEGIN SELECT RAISE(ABORT, 'reject'); END;"
        ).unwrap();
        assert!(db.verify_trade_pnl(&trade).is_err());
        assert!(db.clear_trade_history_for_symbol("SOLUSDC").is_err());
        assert!(db.clear_selected_database(true, false).is_err());
        assert_eq!(db.trade_data_revision(), 1);
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 1);
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_update; DROP TRIGGER reject_delete;")
            .unwrap();

        let mut other_symbol = trade.clone();
        other_symbol.symbol = "BTCUSDC".into();
        assert!(db.verify_trade_pnl(&other_symbol).unwrap());
        assert!(!db.verify_trade_pnl(&other_symbol).unwrap());
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 2);
        assert_eq!(db.symbol_trade_revision("BTCUSDC"), 0);
        db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 3);
        assert_eq!(db.symbol_trade_revision("BTCUSDC"), 0);
        db.clear_selected_database(false, true).unwrap();
        assert_eq!(db.trade_data_revision(), 3);
        db.clear_selected_database(true, false).unwrap();
        assert_eq!(db.trade_data_revision(), 4);
        assert_eq!(db.symbol_trade_revision("SOLUSDC"), 4);
        assert_eq!(db.symbol_trade_revision("BTCUSDC"), 4);
    }

    #[test]
    fn clear_revisions_track_successful_clears_by_symbol() {
        let db = Database::open(":memory:").unwrap();
        let sol = revision_test_trade();
        let mut btc = sol.clone();
        btc.trade_id = "btc-trade".into();
        btc.client_order_id = "btc-order".into();
        btc.symbol = "BTCUSDC".into();
        assert!(db.insert_trade(&sol).unwrap());
        assert!(db.insert_trade(&btc).unwrap());

        let (pending, revision) = db
            .get_unverified_trades_with_clear_revision("SOLUSDC", TradingMode::Paper, 10, 0)
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(revision, 0);
        assert_eq!(db.symbol_trade_clear_revision("BTCUSDC"), 0);

        db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_clear BEFORE DELETE ON trades BEGIN SELECT RAISE(ABORT, 'reject'); END;"
        ).unwrap();
        assert!(db.clear_trade_history_for_symbol("SOLUSDC").is_err());
        assert!(db.clear_trade_history().is_err());
        assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 0);
        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_clear")
            .unwrap();

        db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
        let (pending, sol_revision) = db
            .get_unverified_trades_with_clear_revision("SOLUSDC", TradingMode::Paper, 10, 0)
            .unwrap();
        assert!(pending.is_empty());
        assert_eq!(sol_revision, 1);
        let (pending, btc_revision) = db
            .get_unverified_trades_with_clear_revision("BTCUSDC", TradingMode::Paper, 10, 0)
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(btc_revision, 0);

        db.clear_trade_history_for_symbol("SOLUSDC").unwrap();
        assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 2);
        db.clear_selected_database(false, true).unwrap();
        assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 2);
        db.clear_trade_history().unwrap();
        assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 3);
        assert_eq!(db.symbol_trade_clear_revision("BTCUSDC"), 3);
        assert_eq!(db.symbol_trade_clear_revision("ETHUSDC"), 3);
        db.clear_trade_history_for_symbol("BTCUSDC").unwrap();
        assert_eq!(db.symbol_trade_clear_revision("BTCUSDC"), 4);
        assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), 3);
    }

    #[test]
    fn committed_clear_publishes_revision_before_releasing_connection() {
        use std::sync::{mpsc, Arc};

        let db = Arc::new(Database::open(":memory:").unwrap());
        assert!(db.insert_trade(&revision_test_trade()).unwrap());
        let (reached_tx, reached_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        *db.revision_publish_hook.lock().unwrap() = Some((reached_tx, resume_rx));
        let writer_db = Arc::clone(&db);
        let writer =
            std::thread::spawn(move || writer_db.clear_trade_history_for_symbol("SOLUSDC"));
        reached_rx.recv().unwrap();
        assert!(db.conn.try_lock().is_err());
        resume_tx.send(()).unwrap();
        assert_eq!(writer.join().unwrap().unwrap(), 1);
        let (pending, revision) = db
            .get_unverified_trades_with_clear_revision("SOLUSDC", TradingMode::Paper, 10, 0)
            .unwrap();
        assert!(pending.is_empty());
        assert_eq!(revision, 1);
        assert_eq!(db.symbol_trade_clear_revision("SOLUSDC"), revision);
    }

    #[cfg(unix)]
    #[test]
    fn database_files_are_private_without_changing_existing_parent() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("db-permissions-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        let directory = root.join("new").join("nested");
        let path = directory.join("bot.db");
        let db = Database::open(&path).unwrap();
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&root), 0o755);
        assert_eq!(mode(&root.join("new")) & 0o077, 0);
        assert_eq!(mode(&directory) & 0o077, 0);
        assert_eq!(mode(&path) & 0o077, 0);
        let wal = directory.join("bot.db-wal");
        let shm = directory.join("bot.db-shm");
        assert_eq!(mode(&wal) & 0o077, 0);
        assert_eq!(mode(&shm) & 0o077, 0);
        let uid = unsafe { libc::geteuid() } as u32;
        for file in [&path, &wal, &shm] {
            validate_database_file(file, uid).unwrap();
            let error = validate_database_file(file, uid.wrapping_add(1)).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            assert!(error
                .to_string()
                .contains("differs from service effective user"));
        }

        // Existing insecure files must be repaired by the operator, without
        // changing any of their permissions during a failed open.
        for file in [&path, &wal, &shm] {
            std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o644)).unwrap();
            let error = match Database::open(&path) {
                Ok(_) => panic!("accepted insecure {}", file.display()),
                Err(error) => error,
            };
            assert!(error
                .to_string()
                .contains("remove group and other permissions"));
            assert_eq!(mode(file), 0o644);
            std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(mode(&root), 0o755);
        let reopened = Database::open(&path).unwrap();
        drop(reopened);
        drop(db);
        let journal = directory.join("bot.db-journal");
        std::fs::write(&journal, b"old journal").unwrap();
        std::fs::set_permissions(&journal, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(prepare_database_path(&path).is_err());
        assert_eq!(mode(&journal), 0o644);
        std::fs::set_permissions(&journal, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(validate_database_file(&journal, uid.wrapping_add(1)).is_err());
        prepare_database_path(&path).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn database_and_sidecar_symlinks_are_rejected_without_changing_targets() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let root = std::env::temp_dir().join(format!("db-symlinks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("bot.db");
        let target = root.join("target");
        std::fs::write(&target, b"unrelated").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        let target_mode = || std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;

        symlink(&target, &path).unwrap();
        assert!(prepare_database_path(&path).is_err());
        assert_eq!(target_mode(), 0o644);
        std::fs::remove_file(&path).unwrap();
        symlink(root.join("missing"), &path).unwrap();
        assert!(prepare_database_path(&path).is_err());
        assert!(!root.join("missing").exists());
        std::fs::remove_file(&path).unwrap();

        prepare_database_path(&path).unwrap();
        for suffix in ["-wal", "-shm", "-journal"] {
            let sidecar = root.join(format!("bot.db{suffix}"));
            symlink(&target, &sidecar).unwrap();
            assert!(prepare_database_path(&path).is_err());
            assert_eq!(target_mode(), 0o644);
            std::fs::remove_file(&sidecar).unwrap();
            symlink(root.join("missing"), &sidecar).unwrap();
            assert!(prepare_database_path(&path).is_err());
            std::fs::remove_file(&sidecar).unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn database_rejects_sqlite_uri_paths() {
        assert!(Database::open("file:other.db?mode=rwc").is_err());
    }

    #[test]
    fn database_uses_durable_journal_settings() {
        let path = std::env::temp_dir().join(format!("durable-{}.db", uuid::Uuid::new_v4()));
        let db = Database::open(&path).unwrap();
        {
            let conn = db.conn.lock().unwrap();
            let journal: String = conn
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap();
            let synchronous: i64 = conn
                .query_row("PRAGMA synchronous", [], |row| row.get(0))
                .unwrap();
            assert_eq!(journal, "wal");
            assert_eq!(synchronous, 2);
        }
        drop(db);
        std::fs::remove_file(path).unwrap();

        let memory = Database::open(":memory:").unwrap();
        let conn = memory.conn.lock().unwrap();
        let journal: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal, "memory");
    }

    #[test]
    fn symbol_cleanup_preserves_unattributed_legacy_stats_but_selected_export_omits_them() {
        let db = Database::open(":memory:").unwrap();
        db.conn.lock().unwrap().execute_batch(
            "INSERT INTO grid_stats VALUES (1, 7, 3, '4', '500', '2026-10-10');
             INSERT INTO scoped_grid_stats VALUES ('SOLUSDC', 'LIVE', 1, 0, '0', '100', '2026-10-10');
             INSERT INTO scoped_grid_stats VALUES ('BTCUSDC', 'LIVE', 2, 1, '4', '400', '2026-10-10');"
        ).unwrap();
        assert_eq!(db.clear_trade_history_for_symbol("SOLUSDC").unwrap(), 0);
        assert!(db
            .load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_none());
        assert_eq!(
            db.load_scoped_stats("BTCUSDC", TradingMode::Live)
                .unwrap()
                .unwrap()
                .0,
            2
        );
        assert_eq!(db.load_stats().unwrap().unwrap().0, 7);

        let directory =
            std::env::temp_dir().join(format!("legacy-export-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        for (name, bytes, expected) in [
            (
                "selected.db",
                db.export_selected_analysis_snapshot(true, false).unwrap(),
                0,
            ),
            ("full.db", db.export_analysis_snapshot().unwrap(), 1),
        ] {
            let path = directory.join(name);
            std::fs::write(&path, bytes).unwrap();
            let snapshot =
                Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                    .unwrap();
            let legacy: i64 = snapshot
                .query_row("SELECT COUNT(*) FROM grid_stats", [], |row| row.get(0))
                .unwrap();
            let other: i64 = snapshot
                .query_row(
                    "SELECT COUNT(*) FROM scoped_grid_stats WHERE symbol = 'BTCUSDC'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(legacy, expected);
            assert_eq!(other, 1);
        }
        std::fs::remove_dir_all(directory).unwrap();
        db.clear_trade_history().unwrap();
        assert!(db.load_stats().unwrap().is_none());
    }

    #[test]
    fn cleanup_preserves_frontiers_and_deduplication_across_restarts() {
        let path = std::env::temp_dir().join(format!("cleanup-{}.db", uuid::Uuid::new_v4()));
        let db = Database::open(&path).unwrap();
        let mut trade = TradeRecord {
            trade_id: "cleanup-buy".into(),
            client_order_id: "gb_b_cleanup".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(119.2),
            quantity: dec!(1),
            amount_usdc: dec!(119.2),
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            pnl_verified: false,
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        };
        db.insert_trade(&trade).unwrap();
        let mut latest = trade.clone();
        latest.trade_id = "cleanup-sell".into();
        latest.client_order_id = "gb_s_cleanup".into();
        latest.side = OrderSide::Sell;
        latest.price = dec!(119.3);
        // Equal timestamps must retain the later inserted fill.
        db.insert_trade(&latest).unwrap();
        assert_eq!(db.clear_trade_history().unwrap(), 2);
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.get_pair_opening("SOLUSDC", "gb_b_cleanup", TradingMode::Live)
                .unwrap(),
            Some((OrderSide::Buy, dec!(119.2), dec!(1)))
        );
        assert_eq!(
            db.get_pair_opening("BTCUSDC", "gb_b_cleanup", TradingMode::Live)
                .unwrap(),
            None
        );
        assert_eq!(
            db.get_pair_opening("SOLUSDC", "gb_b_cleanup", TradingMode::Paper)
                .unwrap(),
            None
        );
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        assert_eq!(
            db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
            Some((OrderSide::Sell, dec!(119.3)))
        );
        assert_eq!(
            db.last_fill_for("SOLUSDC", TradingMode::Paper).unwrap(),
            None
        );
        assert!(db
            .has_recorded_order(&trade.client_order_id, TradingMode::Live)
            .unwrap());
        assert!(!db
            .has_recorded_order(&trade.client_order_id, TradingMode::Paper)
            .unwrap());
        assert!(!db.insert_trade(&trade).unwrap());
        trade.trade_id = "different-replayed-id".into();
        assert!(!db.insert_trade(&trade).unwrap());
        trade.trade_id = "cleanup-buy".into();
        trade.pnl_verified = true;
        assert!(!db.verify_trade_pnl(&trade).unwrap());
        assert_eq!(db.clear_trade_history().unwrap(), 0);
        assert_eq!(
            db.get_pair_opening("SOLUSDC", "gb_b_cleanup", TradingMode::Live)
                .unwrap(),
            Some((OrderSide::Buy, dec!(119.2), dec!(1)))
        );
        assert_eq!(
            db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
            Some((OrderSide::Sell, dec!(119.3)))
        );

        trade.trade_id = "cleanup-new-buy".into();
        trade.client_order_id = "gb_b_new_cleanup".into();
        assert!(db.insert_trade(&trade).unwrap());
        assert_eq!(
            db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
            Some((OrderSide::Buy, dec!(119.2)))
        );
        assert_eq!(db.clear_trade_history().unwrap(), 1);
        // Late historical data must not move the saved frontier backwards.
        trade.trade_id = "late-buy".into();
        trade.client_order_id = "gb_b_late_cleanup".into();
        trade.timestamp -= ChronoDuration::hours(1);
        trade.price = dec!(90);
        db.insert_trade(&trade).unwrap();
        db.clear_trade_history().unwrap();
        assert_eq!(
            db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
            Some((OrderSide::Buy, dec!(119.2)))
        );
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn failed_cleanup_rolls_back_history_stats_and_recovery_metadata() {
        let db = Database::open(":memory:").unwrap();
        db.conn.lock().unwrap().execute_batch(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity,
                 amount_usdc, realized_pnl, commission, is_maker, timestamp, note)
             VALUES ('t1', 'gb_b_1', 'SOLUSDC', 'LIVE', 'BUY', '100', '1', '100', '0', '0', 1, '2026-10-10T00:00:00Z', 'test');
             INSERT INTO scoped_grid_stats VALUES ('SOLUSDC', 'LIVE', 1, 0, '0', '100', '2026-10-10');
             CREATE TRIGGER reject_cleanup BEFORE DELETE ON trades
                 BEGIN SELECT RAISE(ABORT, 'cleanup test failure'); END;"
        ).unwrap();
        assert!(db.clear_trade_history().is_err());
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        assert_eq!(
            db.load_scoped_stats("SOLUSDC", TradingMode::Live)
                .unwrap()
                .unwrap()
                .0,
            1
        );
        let conn = db.conn.lock().unwrap();
        for table in [
            "cleared_trade_ids",
            "execution_frontiers",
            "cleared_pair_openings",
        ] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn selected_cleanup_rolls_back_trade_changes_when_snapshot_deletion_fails() {
        let db = Database::open(":memory:").unwrap();
        db.conn.lock().unwrap().execute_batch(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity,
                 amount_usdc, realized_pnl, commission, is_maker, timestamp, note)
             VALUES ('t1', 'gb_b_1', 'SOLUSDC', 'LIVE', 'BUY', '100', '1', '100', '0', '0', 1, '2026-10-10T00:00:00Z', 'test');
             INSERT INTO scoped_grid_stats VALUES ('SOLUSDC', 'LIVE', 1, 0, '0', '100', '2026-10-10');
             INSERT INTO account_hourly_snapshots (hour, payload) VALUES (1, '[]');
             CREATE TRIGGER reject_snapshot_cleanup BEFORE DELETE ON account_hourly_snapshots
                 BEGIN SELECT RAISE(ABORT, 'snapshot cleanup test failure'); END;"
        ).unwrap();

        let revision = db.trade_revision.load(Ordering::Acquire);
        assert!(db.clear_selected_database(true, true).is_err());
        assert_eq!(db.trade_revision.load(Ordering::Acquire), revision);
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        assert_eq!(
            db.load_scoped_stats("SOLUSDC", TradingMode::Live)
                .unwrap()
                .unwrap()
                .0,
            1
        );
        let conn = db.conn.lock().unwrap();
        for (table, expected) in [
            ("account_hourly_snapshots", 1),
            ("cleared_trade_ids", 0),
            ("execution_frontiers", 0),
        ] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, expected, "{table}");
        }
    }

    #[test]
    fn history_identity_survives_reopen_and_is_removed_from_analysis_export() {
        let directory =
            std::env::temp_dir().join(format!("history-identities-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("source.db");
        let identity = [42_u8; 32];
        let other = [43_u8; 32];
        let first = {
            let db = Database::open(&path).unwrap();
            let first = db.history_account_id(identity).unwrap();
            assert!(uuid::Uuid::parse_str(&first).is_ok());
            assert_ne!(first, db.history_account_id(other).unwrap());
            let recovery_identity = [44_u8; 32];
            db.conn.lock().unwrap().execute(
                "INSERT INTO terminal_order_disagreements VALUES (?1, 'SOLUSDC', 'test', '{}', '{}')",
                params![recovery_identity.as_slice()],
            ).unwrap();
            let exported = db.export_analysis_snapshot().unwrap();
            assert!(!exported
                .windows(identity.len())
                .any(|bytes| bytes == identity));
            let exported_path = directory.join("export.db");
            std::fs::write(&exported_path, &exported).unwrap();
            let copy = Connection::open(&exported_path).unwrap();
            let preserved: Vec<u8> = copy
                .query_row(
                    "SELECT account_key FROM terminal_order_disagreements",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(preserved.as_slice(), recovery_identity.as_slice());
            first
        };
        let reopened = Database::open(&path).unwrap();
        assert_eq!(first, reopened.history_account_id(identity).unwrap());
        drop(reopened);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn analysis_export_captures_wal_and_preserves_records_without_credentials() {
        let directory =
            std::env::temp_dir().join(format!("binancebot-export-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let source_path = directory.join("source.db");
        let export_path = directory.join("analysis.db");
        let db = Database::open(&source_path).unwrap();
        db.conn
            .lock()
            .unwrap()
            .execute_batch("PRAGMA wal_autocheckpoint = 0")
            .unwrap();
        let mut config = AppConfig::default();
        config.exchange.api_key = "export-test-api-key".into();
        config.exchange.api_secret = "historical-secret-marker".repeat(500);
        db.save_config(&config).unwrap();
        config.exchange.api_secret = "export-test-api-secret".into();
        config.telegram.bot_token = "export-test-telegram-token".into();
        config.telegram.chat_id = "export-test-chat-id".into();
        db.save_config(&config).unwrap();
        db.create_session("export-test-session-token").unwrap();
        db.save_bot_status(BotStatus::Running).unwrap();
        {
            let conn = db.conn.lock().unwrap();
            for index in 0..65 {
                conn.execute(
                    "INSERT INTO trades (rowid, trade_id, client_order_id, symbol, mode, side, price, quantity, amount_usdc, realized_pnl, commission, is_maker, timestamp, note) VALUES (?1, ?2, ?2, ?3, ?4, 'BUY', '118.3', '2.53', '299.299', '0', '0', 1, '2026-09-30T00:00:00Z', 'export test')",
                    params![10 + index * 2, format!("gb_b_export_{index}"), if index % 2 == 0 { "SOLUSDC" } else { "UNIUSDC" }, if index % 2 == 0 { "LIVE" } else { "PAPER" }],
                ).unwrap();
            }
            conn.execute_batch(
                r#"INSERT INTO managed_orders VALUES ('gb_s_exit', 'SOLUSDC', '{"paired_client_order_id":"gb_b_export_0"}', '2026-09-30');
                INSERT INTO pair_intents VALUES ('gb_b_export_0', 'SOLUSDC', 'LIVE', '{"price":"118.4"}', '2026-09-30');
                INSERT INTO remainder_plans VALUES ('gb_s_remainder', 'SOLUSDC', 'LIVE', 1, '{"sources":[]}', '2026-09-30');
                INSERT INTO scoped_grid_stats VALUES ('SOLUSDC', 'LIVE', 65, 0, '0', '19454.435', '2026-09-30');"#,
            ).unwrap();
        }
        assert!(
            std::fs::metadata(directory.join("source.db-wal"))
                .unwrap()
                .len()
                > 0
        );
        let bytes = db.export_analysis_snapshot().unwrap();
        assert!(bytes.starts_with(b"SQLite format 3\0"));
        let raw = String::from_utf8_lossy(&bytes);
        for secret in [
            "export-test-api-key",
            "export-test-api-secret",
            "historical-secret-marker",
            "export-test-telegram-token",
            "export-test-chat-id",
            "export-test-session-token",
        ] {
            assert!(!raw.contains(secret), "Credential bytes leaked: {secret}");
        }
        std::fs::write(&export_path, bytes).unwrap();
        let snapshot =
            Connection::open_with_flags(&export_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let integrity: String = snapshot
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        for (table, expected) in [
            ("trades", 65),
            ("managed_orders", 1),
            ("pair_intents", 1),
            ("remainder_plans", 1),
            ("scoped_grid_stats", 1),
            ("app_config", 1),
            ("bot_runtime", 1),
            ("auth_sessions", 0),
        ] {
            let count: i64 = snapshot
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, expected, "Incorrect exported count for {table}");
        }
        let rows: Vec<(i64, String)> = snapshot
            .prepare("SELECT rowid, client_order_id FROM trades ORDER BY rowid")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        for (index, (rowid, id)) in rows.iter().enumerate() {
            assert_eq!(
                *rowid,
                10 + index as i64 * 2,
                "Fill journal cursors must be preserved"
            );
            assert_eq!(id, &format!("gb_b_export_{index}"));
        }
        // Preserve all analysis fields, especially paired IDs and numeric strings.
        for table in [
            "trades",
            "managed_orders",
            "pair_intents",
            "remainder_plans",
            "grid_stats",
            "scoped_grid_stats",
            "bot_runtime",
        ] {
            let read_rows = |connection: &Connection| {
                let mut statement = connection
                    .prepare(&format!("SELECT rowid, * FROM {table} ORDER BY rowid"))
                    .unwrap();
                let columns = statement.column_count();
                statement
                    .query_map([], |row| {
                        (0..columns)
                            .map(|column| row.get::<_, rusqlite::types::Value>(column))
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .unwrap()
                    .map(Result::unwrap)
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                read_rows(&snapshot),
                read_rows(&db.conn.lock().unwrap()),
                "Changed analysis data in {table}"
            );
        }
        let json: String = snapshot
            .query_row("SELECT config_json FROM app_config", [], |r| r.get(0))
            .unwrap();
        let exported: AppConfig = serde_json::from_str(&json).unwrap();
        assert!(exported.exchange.api_key.is_empty());
        assert!(exported.exchange.api_secret.is_empty());
        assert!(exported.telegram.bot_token.is_empty());
        assert!(exported.telegram.chat_id.is_empty());
        assert_eq!(exported.grid, config.grid);
        assert_eq!(db.load_config().unwrap().unwrap().exchange, config.exchange);
        assert_eq!(
            db.load_config().unwrap().unwrap().telegram.bot_token,
            config.telegram.bot_token
        );
        assert!(db.is_session_valid("export-test-session-token").unwrap());
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
        drop(snapshot);
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn analysis_export_rejects_unreadable_config_instead_of_exporting_secrets() {
        let db = Database::open(":memory:").unwrap();
        db.conn.lock().unwrap().execute(
            "INSERT INTO app_config VALUES (1, 'SOLUSDC', 'invalid-secret-config', '2026-09-30')", [],
        ).unwrap();
        assert!(db.export_analysis_snapshot().is_err());
    }

    #[test]
    fn managed_order_metadata_survives_reload() {
        let db = Database::open(":memory:").unwrap();
        let order = GridOrder {
            client_order_id: "gb_s_paired".into(),
            order_id: Some(42),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(115.1),
            quantity: dec!(1),
            amount_usdc: dec!(115.1),
            status: crate::types::OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_b_purchase".into()),
            is_take_profit: true,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&order).unwrap();
        let loaded = db.load_managed_orders("SOLUSDC").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].paired_client_order_id,
            order.paired_client_order_id
        );
        assert!(loaded[0].is_take_profit);
        db.delete_managed_order(&order.client_order_id).unwrap();
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    }

    #[test]
    fn expired_session_is_rejected() {
        let db = Database::open(":memory:").unwrap();
        db.create_session("old-token").unwrap();
        db.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE auth_sessions SET created_at = ?1 WHERE token = 'old-token';",
                params![(Utc::now() - ChronoDuration::hours(25)).to_rfc3339()],
            )
            .unwrap();
        assert!(!db.is_session_valid("old-token").unwrap());
        assert!(!db.is_session_valid("old-token").unwrap());
    }

    #[test]
    fn bot_pause_state_is_persistent() {
        let db = Database::open(":memory:").unwrap();
        assert_eq!(db.load_bot_status().unwrap(), None);
        db.save_bot_status(BotStatus::Paused).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        db.save_bot_status(BotStatus::Running).unwrap();
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
    }

    #[test]
    fn test_db_config_lifecycle() {
        let db = Database::open(":memory:").unwrap();
        assert!(db.load_config().unwrap().is_none());

        let mut config = AppConfig::default();
        config.exchange.symbol = "BTCUSDC".to_string();
        config.grid.grid_interval = dec!(50.0);

        db.save_config(&config).unwrap();
        let loaded = db.load_config().unwrap().expect("config should exist");
        assert_eq!(loaded.exchange.symbol, "BTCUSDC");
        assert_eq!(loaded.grid.grid_interval, dec!(50.0));
    }

    #[test]
    fn dashboard_membership_and_settings_survive_reopen_without_losing_history() {
        let path =
            std::env::temp_dir().join(format!("dashboard-settings-{}.db", uuid::Uuid::new_v4()));
        let db = Database::open(&path).unwrap();
        let mut initial = AppConfig::default();
        initial.exchange.recv_window = 9000;
        initial.ensure_pair_list().unwrap();
        db.save_config(&initial).unwrap();
        db.conn.lock().unwrap().execute_batch(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity, amount_usdc,
               realized_pnl, commission, is_maker, timestamp, note)
             VALUES ('history', 'gb_b_history', 'SOLUSDC', 'PAPER', 'BUY', '100', '1', '100', '0', '0', 1, '2026-10-10T00:00:00Z', 'test');"
        ).unwrap();
        let mut added = AppConfig::default();
        added.exchange.symbol = "BTCUSDT".into();
        added.grid.grid_interval = dec!(10);
        added.exchange.recv_window = 9000;
        db.add_pair_config(&added).unwrap();
        db.save_server_config_checked(
            &crate::config::ServerConfig {
                host: "127.0.0.1".into(),
                port: 9090,
            },
            None,
        )
        .unwrap();
        added.grid.buy_window = 3;
        db.save_pair_config("BTCUSDT", &added).unwrap();
        db.remove_pair_config("SOLUSDC").unwrap();
        assert!(db.remove_pair_config("BTCUSDT").is_err());
        drop(db);
        let db = Database::open(&path).unwrap();
        let config = db.load_config().unwrap().unwrap();
        assert_eq!(config.server.port, 9090);
        let strategies = config.strategy_configs().unwrap();
        assert_eq!(strategies.len(), 1);
        assert_eq!(strategies[0].exchange.symbol, "BTCUSDT");
        assert_eq!(strategies[0].exchange.recv_window, 9000);
        assert_eq!(strategies[0].grid.buy_window, 3);
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        assert_eq!(
            db.get_recent_trades_for("SOLUSDC", TradingMode::Paper, 10)
                .unwrap()
                .len(),
            1
        );
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn adding_a_pair_rolls_back_if_initial_pause_cannot_be_saved() {
        let db = Database::open(":memory:").unwrap();
        let mut initial = AppConfig::default();
        initial.ensure_pair_list().unwrap();
        db.save_config(&initial).unwrap();
        db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_pair_status BEFORE INSERT ON pair_runtime BEGIN SELECT RAISE(ABORT, 'test status failure'); END;"
        ).unwrap();
        let mut added = AppConfig::default();
        added.exchange.symbol = "BTCUSDT".into();
        assert!(db.add_pair_config(&added).is_err());
        assert_eq!(db.load_config().unwrap().unwrap().pairs.len(), 1);
    }

    #[test]
    fn hourly_trade_stats_cover_beijing_today_and_isolate_scope() {
        let db = Database::open(":memory:").unwrap();
        let now = DateTime::parse_from_rfc3339("2026-09-28T10:30:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        let start = DateTime::parse_from_rfc3339("2026-09-27T16:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let empty = db
            .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, now)
            .unwrap();
        assert_eq!(empty.realized_pnl, Decimal::ZERO);
        assert_eq!(empty.pending_pnl_count, 0);
        assert_eq!(empty.buckets.len(), 19);
        assert!(empty
            .buckets
            .iter()
            .all(|b| b.buy_count == 0 && b.sell_count == 0));

        let mut trade = TradeRecord {
            trade_id: String::new(),
            client_order_id: String::new(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            pnl_verified: false,
            is_maker: true,
            timestamp: start,
            note: String::new(),
        };
        // More than both the snapshot (50) and in-memory (200) history limits.
        for index in 0..250 {
            trade.trade_id = format!("buy-{index}");
            trade.client_order_id = format!("order-{index}");
            assert!(db.insert_trade(&trade).unwrap());
        }
        assert!(!db.insert_trade(&trade).unwrap());
        for (id, timestamp, side, symbol, mode) in [
            ("last", now, OrderSide::Sell, "SOLUSDC", TradingMode::Paper),
            (
                "utc-midnight",
                DateTime::parse_from_rfc3339("2026-09-28T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                OrderSide::Sell,
                "SOLUSDC",
                TradingMode::Paper,
            ),
            (
                "old",
                start - ChronoDuration::nanoseconds(1),
                OrderSide::Buy,
                "SOLUSDC",
                TradingMode::Paper,
            ),
            (
                "future",
                now + ChronoDuration::nanoseconds(1),
                OrderSide::Sell,
                "SOLUSDC",
                TradingMode::Paper,
            ),
            ("symbol", now, OrderSide::Buy, "BTCUSDC", TradingMode::Paper),
            ("live", now, OrderSide::Buy, "SOLUSDC", TradingMode::Live),
        ] {
            trade.trade_id = id.into();
            trade.client_order_id = id.into();
            trade.timestamp = timestamp;
            trade.side = side;
            trade.symbol = symbol.into();
            trade.mode = mode;
            trade.pnl_verified = true;
            trade.realized_pnl = match id {
                "last" => dec!(1.23456789),
                "utc-midnight" => dec!(-0.23456788),
                _ => dec!(999),
            };
            assert!(db.insert_trade(&trade).unwrap());
        }
        let stats = db
            .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, now)
            .unwrap();
        assert_eq!(stats.window_start, start);
        assert_eq!(stats.window_end, now);
        assert_eq!(stats.realized_pnl, dec!(1.00000001));
        assert_eq!(stats.pending_pnl_count, 250);
        assert_eq!(stats.buckets[0].buy_count, 250);
        assert_eq!(stats.buckets[8].sell_count, 1);
        assert_eq!(stats.buckets[18].sell_count, 1);
        assert_eq!(
            stats
                .buckets
                .iter()
                .map(|b| b.buy_count + b.sell_count)
                .sum::<u64>(),
            252
        );
        assert!(stats
            .buckets
            .windows(2)
            .all(|b| b[1].hour_start - b[0].hour_start == ChronoDuration::hours(1)));
        let later = db
            .get_hourly_trade_stats(
                "SOLUSDC",
                TradingMode::Paper,
                now + ChronoDuration::hours(1),
            )
            .unwrap();
        assert_eq!(later.buckets.len(), 20);
        assert_eq!(later.window_start, start);
        assert_eq!(later.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 250);
        assert_eq!(later.buckets.iter().map(|b| b.sell_count).sum::<u64>(), 3);

        // Beijing's next day begins at 16:00 UTC, before the UTC date changes.
        let next_midnight = start + ChronoDuration::days(1);
        trade.trade_id = "next-day".into();
        trade.client_order_id = "next-day".into();
        trade.symbol = "SOLUSDC".into();
        trade.mode = TradingMode::Paper;
        trade.side = OrderSide::Buy;
        trade.timestamp = next_midnight;
        assert!(db.insert_trade(&trade).unwrap());
        let next_day = db
            .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, next_midnight)
            .unwrap();
        assert_eq!(next_day.window_start, next_midnight);
        assert_eq!(next_day.window_end, next_midnight);
        assert_eq!(next_day.realized_pnl, dec!(999));
        assert_eq!(next_day.pending_pnl_count, 0);
        assert_eq!(next_day.buckets.len(), 1);
        assert_eq!(next_day.buckets[0].hour_start, next_midnight);
        assert_eq!(next_day.buckets[0].buy_count, 1);
        assert_eq!(next_day.buckets[0].sell_count, 0);

        let before_midnight = db
            .get_hourly_trade_stats(
                "SOLUSDC",
                TradingMode::Paper,
                next_midnight - ChronoDuration::nanoseconds(1),
            )
            .unwrap();
        assert_eq!(before_midnight.window_start, start);
        assert_eq!(before_midnight.buckets.len(), 24);
        assert_eq!(
            before_midnight
                .buckets
                .iter()
                .map(|b| b.buy_count + b.sell_count)
                .sum::<u64>(),
            253
        );
    }

    #[test]
    fn last_fill_uses_execution_time_scope_and_stable_ties() {
        let db = Database::open(":memory:").unwrap();
        let now = Utc::now();
        let mut trade = TradeRecord {
            trade_id: "latest-buy".into(),
            client_order_id: "gb_latest_buy".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            side: OrderSide::Buy,
            price: dec!(118.8),
            quantity: dec!(1),
            amount_usdc: dec!(118.8),
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            pnl_verified: false,
            is_maker: true,
            timestamp: now,
            note: String::new(),
        };
        db.insert_trade(&trade).unwrap();
        // An older execution discovered later must not overwrite the latest price.
        trade.trade_id = "late-old-buy".into();
        trade.price = dec!(118.6);
        trade.timestamp = now - chrono::Duration::seconds(10);
        db.insert_trade(&trade).unwrap();
        trade.side = OrderSide::Sell;
        trade.timestamp = now;
        for (id, price) in [("sell-a", dec!(118.9)), ("sell-b", dec!(119.1))] {
            trade.trade_id = id.into();
            trade.price = price;
            db.insert_trade(&trade).unwrap();
        }
        trade.trade_id = "other-mode".into();
        trade.mode = TradingMode::Live;
        trade.price = dec!(119.3);
        db.insert_trade(&trade).unwrap();
        trade.trade_id = "other-symbol".into();
        trade.symbol = "ETHUSDC".into();
        db.insert_trade(&trade).unwrap();
        assert_eq!(
            db.last_fill_for("SOLUSDC", TradingMode::Paper).unwrap(),
            Some((OrderSide::Sell, dec!(119.1)))
        );
        assert_eq!(
            db.last_fill_for("SOLUSDC", TradingMode::Live).unwrap(),
            Some((OrderSide::Sell, dec!(119.3)))
        );
        assert_eq!(
            db.get_recent_trades_for("SOLUSDC", TradingMode::Paper, 10)
                .unwrap()[0]
                .trade_id,
            "sell-b"
        );
        db.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE trades SET price = 'invalid' WHERE trade_id = 'sell-b'",
                [],
            )
            .unwrap();
        assert!(db.last_fill_for("SOLUSDC", TradingMode::Paper).is_err());
    }

    #[test]
    fn test_db_trade_lifecycle() {
        let db = Database::open(":memory:").unwrap();
        let trade = TradeRecord {
            trade_id: "trade_1".to_string(),
            client_order_id: "order_1".to_string(),
            symbol: "SOLUSDC".to_string(),
            mode: TradingMode::Paper,
            side: OrderSide::Buy,
            price: dec!(150.2),
            quantity: dec!(0.66),
            amount_usdc: dec!(99.132),
            realized_pnl: dec!(0.0),
            commission: dec!(0.0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "Test trade".to_string(),
        };

        db.insert_trade(&trade).unwrap();
        let trades = db.get_recent_trades(10).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, "trade_1");
        assert_eq!(trades[0].symbol, "SOLUSDC");
        assert_eq!(trades[0].price, dec!(150.2));
        assert!(trades[0].is_maker);
    }

    #[test]
    fn trade_and_stats_commit_together_and_duplicates_do_not_recount() {
        let db = Database::open(":memory:").unwrap();
        let trade = TradeRecord {
            trade_id: "atomic_trade".into(),
            client_order_id: "gb_b_atomic".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "atomic test".into(),
        };
        let stats = GridStats {
            total_trades: 1,
            total_volume_usdc: dec!(100),
            ..GridStats::default()
        };
        db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_stats BEFORE INSERT ON scoped_grid_stats BEGIN SELECT RAISE(ABORT, 'reject stats'); END;",
        ).unwrap();
        assert!(db.insert_trade_with_stats(&trade, &stats).is_err());
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        assert!(db
            .load_scoped_stats("SOLUSDC", TradingMode::Paper)
            .unwrap()
            .is_none());

        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_stats;")
            .unwrap();
        assert!(db.insert_trade_with_stats(&trade, &stats).unwrap());
        let double_stats = GridStats {
            total_trades: 2,
            total_volume_usdc: dec!(200),
            ..GridStats::default()
        };
        assert!(!db.insert_trade_with_stats(&trade, &double_stats).unwrap());
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        assert_eq!(
            db.load_scoped_stats("SOLUSDC", TradingMode::Paper)
                .unwrap()
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            db.load_scoped_stats("SOLUSDC", TradingMode::Paper)
                .unwrap()
                .unwrap()
                .3,
            dec!(100)
        );
    }

    #[test]
    fn pair_intent_and_fill_commit_atomically() {
        let db = Database::open(":memory:").unwrap();
        let parent = GridOrder {
            client_order_id: "gb_b_parent".into(),
            order_id: Some(41),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: crate::types::OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        let child = GridOrder {
            client_order_id: "gb_s_child".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(1),
            amount_usdc: dec!(101),
            status: crate::types::OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 0,
            paired_client_order_id: Some(parent.client_order_id.clone()),
            is_take_profit: true,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_b_parent".into(),
            client_order_id: parent.client_order_id.clone(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: parent.price,
            quantity: parent.quantity,
            amount_usdc: parent.amount_usdc,
            realized_pnl: Decimal::ZERO,
            commission: Decimal::ZERO,
            pnl_verified: false,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        let stats = GridStats {
            total_trades: 1,
            ..GridStats::default()
        };
        db.save_managed_order(&parent).unwrap();
        db.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_pair BEFORE INSERT ON pair_intents BEGIN SELECT RAISE(ABORT, 'reject pair'); END;",
        ).unwrap();
        assert!(db
            .insert_trade_with_recovery(&trade, &stats, Some(&child), None)
            .is_err());
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
        assert!(db
            .load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_none());
        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_pair;")
            .unwrap();

        assert!(db
            .insert_trade_with_recovery(&trade, &stats, Some(&child), None)
            .unwrap());
        assert!(!db
            .insert_trade_with_recovery(&trade, &stats, Some(&child), None)
            .unwrap());
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        let intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].client_order_id, child.client_order_id);
        assert_eq!(
            intents[0].paired_client_order_id,
            child.paired_client_order_id
        );
        assert_eq!(
            db.load_scoped_stats("SOLUSDC", TradingMode::Live)
                .unwrap()
                .unwrap()
                .0,
            1
        );
    }

    #[test]
    fn trading_modes_have_separate_history_and_totals() {
        let db = Database::open(":memory:").unwrap();
        for (id, mode, pnl) in [
            ("paper", TradingMode::Paper, dec!(12)),
            ("live", TradingMode::Live, dec!(-4)),
        ] {
            let trade = TradeRecord {
                trade_id: id.into(),
                client_order_id: format!("gb_b_{id}"),
                symbol: "SOLUSDC".into(),
                mode,
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(1),
                amount_usdc: dec!(100),
                realized_pnl: pnl,
                commission: dec!(0.1),
                pnl_verified: true,
                is_maker: true,
                timestamp: Utc::now(),
                note: String::new(),
            };
            let stats = GridStats {
                total_trades: 1,
                total_volume_usdc: dec!(100),
                ..GridStats::default()
            };
            assert!(db.insert_trade_with_stats(&trade, &stats).unwrap());
        }

        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Live).unwrap(),
            (dec!(-4), dec!(0.1), 0)
        );
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
            (dec!(12), dec!(0.1), 0)
        );
        assert_eq!(
            db.get_recent_trades_for("SOLUSDC", TradingMode::Live, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn legacy_trades_can_be_backfilled_without_double_counting() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE trades (trade_id TEXT PRIMARY KEY, client_order_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL, price TEXT NOT NULL, quantity TEXT NOT NULL, amount_usdc TEXT NOT NULL, realized_pnl TEXT NOT NULL, commission TEXT NOT NULL, is_maker INTEGER NOT NULL, timestamp TEXT NOT NULL, note TEXT NOT NULL);",
        ).unwrap();
        conn.execute(
            "INSERT INTO trades VALUES ('t1', 'gb_s_1', 'SOLUSDC', 'SELL', '115.1', '17.37', '1999.29', '0', '0', 1, ?1, 'old trade');",
            params![Utc::now().to_rfc3339()],
        ).unwrap();
        let db = Database {
            conn: Mutex::new(conn),
            path: ":memory:".into(),
            trade_revision: AtomicU64::new(0),
            trade_revisions: Mutex::new(TradeRevisions::default()),
            trade_clear_revision: AtomicU64::new(0),
            trade_clear_revisions: Mutex::new(TradeRevisions::default()),
            revision_publish_hook: Mutex::new(None),
            membership_commit_hook: Mutex::new(None),
            today_pnl_cache: tokio::sync::Mutex::new(None),
            hourly_stats_queries: AtomicU64::new(0),
            benchmark_wait_samples: std::sync::OnceLock::new(),
        };
        db.init_tables().unwrap();

        let mut pending = db
            .get_unverified_trades("SOLUSDC", TradingMode::Unknown, 10, 0)
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Unknown).unwrap(),
            (dec!(0), dec!(0), 1)
        );
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Live).unwrap(),
            (dec!(0), dec!(0), 0)
        );

        let mut trade = pending.pop().unwrap();
        trade.realized_pnl = dec!(-3.25);
        trade.commission = dec!(0.80);
        trade.pnl_verified = true;
        assert!(db.verify_trade_pnl(&trade).unwrap());
        assert!(!db.verify_trade_pnl(&trade).unwrap());
        assert!(db
            .get_unverified_trades("SOLUSDC", TradingMode::Unknown, 10, 0)
            .unwrap()
            .is_empty());
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Unknown).unwrap(),
            (dec!(-3.25), dec!(0.80), 0)
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    }

    #[test]
    fn test_db_stats_lifecycle() {
        let db = Database::open(":memory:").unwrap();
        assert!(db.load_stats().unwrap().is_none());

        let stats = GridStats {
            total_trades: 42,
            completed_cycles: 21,
            total_realized_profit: dec!(15.75),
            total_volume_usdc: dec!(4200.0),
            ..GridStats::default()
        };

        db.save_stats(&stats).unwrap();
        let (trades, cycles, profit, volume) =
            db.load_stats().unwrap().expect("stats should exist");
        assert_eq!(trades, 42);
        assert_eq!(cycles, 21);
        assert_eq!(profit, dec!(15.75));
        assert_eq!(volume, dec!(4200.0));
    }

    #[test]
    fn opening_database_removes_legacy_admin_password_table() {
        let directory =
            std::env::temp_dir().join(format!("binancebot-legacy-auth-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("legacy.db");
        {
            let db = Database::open(&path).unwrap();
            db.conn
                .lock()
                .unwrap()
                .execute_batch(
                    "CREATE TABLE admin_auth (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        password_hash TEXT NOT NULL,
                        salt TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    INSERT INTO admin_auth VALUES (1, 'legacy-password-hash-marker', 'legacy-salt-marker', '2026-01-01');",
                )
                .unwrap();
        }

        let db = Database::open(&path).unwrap();
        let remaining: i64 = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'admin_auth'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 0);
        drop(db);
        let mut bytes = std::fs::read(&path).unwrap();
        if let Ok(wal) = std::fs::read(path.with_extension("db-wal")) {
            bytes.extend(wal);
        }
        let raw = String::from_utf8_lossy(&bytes);
        assert!(!raw.contains("legacy-password-hash-marker"));
        assert!(!raw.contains("legacy-salt-marker"));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn test_db_sessions() {
        let db = Database::open(":memory:").unwrap();
        let token = "test_token_123456";
        assert!(!db.is_session_valid(token).unwrap());

        db.create_session(token).unwrap();
        assert!(db.is_session_valid(token).unwrap());

        db.delete_session(token).unwrap();
        assert!(!db.is_session_valid(token).unwrap());

        db.create_session("first-token").unwrap();
        db.create_session("second-token").unwrap();
        db.clear_all_sessions().unwrap();
        assert!(!db.is_session_valid("first-token").unwrap());
        assert!(!db.is_session_valid("second-token").unwrap());
    }

    #[test]
    fn concurrent_pair_updates_preserve_the_other_strategy_and_shared_defaults() {
        use std::sync::Arc;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let mut config = AppConfig::default();
        config.pairs = ["SOLUSDC", "BTCUSDT"]
            .map(|s| crate::config::TradingPairConfig::new(s.into()))
            .to_vec();
        db.save_config(&config).unwrap();
        let mut strategies = config.strategy_configs().unwrap();
        strategies[0].grid.buy_window = 2;
        strategies[1].grid.grid_interval = dec!(10);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let workers: Vec<_> = strategies
            .into_iter()
            .map(|config| {
                let db = db.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    db.save_pair_config(&config.exchange.symbol, &config)
                        .unwrap();
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let saved = db.load_config().unwrap().unwrap();
        assert_eq!(saved.grid.buy_window, 5);
        assert_eq!(saved.grid.grid_interval, dec!(0.1));
        let restored = saved.strategy_configs().unwrap();
        assert_eq!(restored[0].grid.buy_window, 2);
        assert_eq!(restored[1].grid.grid_interval, dec!(10));
        assert!(db.save_pair_config("SOLUSDC", &restored[1]).is_err());
    }

    #[test]
    fn a_failed_runtime_status_write_rolls_back_the_configuration_too() {
        let db = Database::open(":memory:").unwrap();
        let config = AppConfig {
            pairs: vec![crate::config::TradingPairConfig::new("SOLUSDC".into())],
            ..Default::default()
        };
        db.save_config(&config).unwrap();
        db.save_pair_status("SOLUSDC", TradingMode::Live, BotStatus::Running)
            .unwrap();
        let mut candidate = config.strategy_configs().unwrap().remove(0);
        candidate.exchange.api_key = "live-key".into();
        candidate.exchange.api_secret = "live-secret".into();
        candidate.grid.buy_window = 7;
        db.conn
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_status BEFORE UPDATE ON pair_runtime
             BEGIN SELECT RAISE(ABORT, 'status write failed'); END;",
            )
            .unwrap();
        assert!(db
            .save_runtime_config(
                &candidate,
                Some("SOLUSDC"),
                BotStatus::Paused,
                ("SOLUSDC", TradingMode::Paper)
            )
            .is_err());
        assert_eq!(
            serde_json::to_value(db.load_config().unwrap().unwrap()).unwrap(),
            serde_json::to_value(config).unwrap()
        );
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Running)
        );
    }

    #[test]
    fn switching_modes_clears_only_the_previous_guard_and_rolls_back_on_failure() {
        let db = Database::open(":memory:").unwrap();
        let paper = AppConfig::default();
        db.save_config(&paper).unwrap();
        let symbol = &paper.exchange.symbol;
        assert!(!db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
        assert!(!db.arm_execution_guard(symbol, TradingMode::Live).unwrap());
        let mut live = paper.clone();
        live.exchange.api_key = "live-key".into();
        live.exchange.api_secret = "live-secret".into();

        db.conn
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_old_guard BEFORE UPDATE ON pair_execution_guard
             WHEN OLD.mode = 'PAPER' AND NEW.dirty = 0
             BEGIN SELECT RAISE(ABORT, 'guard clear failed'); END;",
            )
            .unwrap();
        assert!(db
            .save_runtime_config(
                &live,
                None,
                BotStatus::Running,
                (symbol, TradingMode::Paper)
            )
            .is_err());
        assert!(db.load_config().unwrap().unwrap().exchange.is_paper());
        assert!(db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
        assert!(db.arm_execution_guard(symbol, TradingMode::Live).unwrap());
        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_old_guard")
            .unwrap();

        assert_eq!(
            db.save_runtime_config(
                &live,
                None,
                BotStatus::Running,
                (symbol, TradingMode::Paper)
            )
            .unwrap(),
            (BotStatus::Paused, true)
        );
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        assert!(!db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
        assert!(db.arm_execution_guard(symbol, TradingMode::Live).unwrap());
        db.clear_execution_guard(symbol, TradingMode::Paper)
            .unwrap();
        assert_eq!(
            db.save_runtime_config(
                &paper,
                None,
                BotStatus::Running,
                (symbol, TradingMode::Live)
            )
            .unwrap(),
            (BotStatus::Paused, true)
        );
        assert_eq!(
            db.load_pair_status(symbol, TradingMode::Live).unwrap(),
            Some(BotStatus::Paused)
        );
        assert!(!db.arm_execution_guard(symbol, TradingMode::Live).unwrap());
        assert!(db.arm_execution_guard(symbol, TradingMode::Paper).unwrap());
    }

    #[test]
    fn adding_pair_rolls_back_config_and_status_when_guard_write_fails() {
        let db = Database::open(":memory:").unwrap();
        let original = AppConfig::default();
        db.save_config(&original).unwrap();
        let mut added = original.clone();
        added.exchange.symbol = "BTCUSDT".into();
        db.conn
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_guard BEFORE INSERT ON pair_execution_guard
             BEGIN SELECT RAISE(ABORT, 'guard write failed'); END;",
            )
            .unwrap();
        assert!(db.add_pair_config(&added).is_err());
        assert_eq!(
            serde_json::to_value(db.load_config().unwrap().unwrap()).unwrap(),
            serde_json::to_value(original).unwrap()
        );
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            None
        );
    }
    #[test]
    fn legacy_testnet_configuration_is_retired_to_paused_paper_mode() {
        let path = std::env::temp_dir().join(format!("legacy-testnet-{}.db", uuid::Uuid::new_v4()));
        {
            let db = Database::open(&path).unwrap();
            db.save_config(&AppConfig::default()).unwrap();
            db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
                .unwrap();
            db.save_pair_status("BTCUSDT", TradingMode::Paper, BotStatus::Running)
                .unwrap();
            let mut legacy = serde_json::to_value(AppConfig::default()).unwrap();
            legacy["exchange"]["is_testnet"] = serde_json::json!(true);
            legacy["exchange"]["api_key"] = serde_json::json!("testnet-key");
            legacy["exchange"]["api_secret"] = serde_json::json!("testnet-secret");
            legacy["pairs"] = serde_json::json!([
                {"symbol": "SOLUSDC"},
                {"symbol": "BTCUSDT", "exchange": {"api_key": "pair-key", "api_secret": "pair-secret"}}
            ]);
            db.conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE app_config SET config_json = ?1 WHERE id = 1",
                    params![serde_json::to_string_pretty(&legacy).unwrap()],
                )
                .unwrap();
        }
        let db = Database::open(&path).unwrap();
        let config = db.load_config().unwrap().unwrap();
        assert!(config.exchange.is_paper());
        assert!(config.exchange.api_key.is_empty());
        assert!(config.exchange.api_secret.is_empty());
        assert!(config
            .strategy_configs()
            .unwrap()
            .iter()
            .all(|strategy| strategy.exchange.is_paper()));
        let stored: String = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT config_json FROM app_config WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!stored.contains("is_testnet"));
        for symbol in ["SOLUSDC", "BTCUSDT"] {
            assert_eq!(
                db.load_pair_status(symbol, TradingMode::Paper).unwrap(),
                Some(BotStatus::Paused)
            );
            assert_eq!(
                db.load_pair_status(symbol, TradingMode::Live).unwrap(),
                None
            );
        }
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_paper_intent_with_credentials_pauses_derived_live_mode_once() {
        let path =
            std::env::temp_dir().join(format!("legacy-paper-intent-{}.db", uuid::Uuid::new_v4()));
        let mut config = AppConfig::default();
        config.exchange.api_key = "legacy-key".into();
        config.exchange.api_secret = "legacy-secret".into();
        config.pairs = ["SOLUSDC", "BTCUSDT"]
            .map(|symbol| crate::config::TradingPairConfig::new(symbol.into()))
            .to_vec();
        {
            let db = Database::open(&path).unwrap();
            db.save_config(&config).unwrap();
            for symbol in ["SOLUSDC", "BTCUSDT"] {
                db.save_pair_status(symbol, TradingMode::Live, BotStatus::Running)
                    .unwrap();
            }
            let mut legacy = serde_json::to_value(&config).unwrap();
            legacy["exchange"]["api_key"] = serde_json::json!("");
            legacy["exchange"]["api_secret"] = serde_json::json!("");
            legacy["exchange"]["dry_run"] = serde_json::json!(true);
            legacy["pairs"][0]["exchange"] = serde_json::json!({
                "api_key": "pair-key",
                "api_secret": "pair-secret",
                "dry_run": true
            });
            legacy["pairs"][1]["exchange"] = serde_json::json!({"dry_run": false});
            db.conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE app_config SET config_json = ?1 WHERE id = 1",
                    params![serde_json::to_string_pretty(&legacy).unwrap()],
                )
                .unwrap();
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Paused)
        );
        // A pair that was explicitly live before the upgrade keeps its status.
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Live).unwrap(),
            Some(BotStatus::Running)
        );
        let stored: String = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT config_json FROM app_config WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!stored.contains("dry_run"));
        // A later manual resume must survive the next restart.
        db.save_pair_status("SOLUSDC", TradingMode::Live, BotStatus::Running)
            .unwrap();
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            Some(BotStatus::Running)
        );
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn retired_testnet_rows_are_removed_and_trade_history_becomes_unknown() {
        let path =
            std::env::temp_dir().join(format!("legacy-testnet-rows-{}.db", uuid::Uuid::new_v4()));
        {
            let db = Database::open(&path).unwrap();
            db.save_config(&AppConfig::default()).unwrap();
            db.insert_trade(&revision_test_trade()).unwrap();
            db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
                .unwrap();
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE trades SET mode = 'TESTNET' WHERE trade_id = 'revision-trade'",
                [],
            )
            .unwrap();
            conn.execute_batch(
                "INSERT INTO pair_runtime (symbol, mode, status, updated_at)
                    VALUES ('SOLUSDC', 'TESTNET', '\"Running\"', '2026-01-01T00:00:00Z');
                 INSERT INTO pair_execution_guard (symbol, mode, dirty)
                    VALUES ('SOLUSDC', 'TESTNET', 1);",
            )
            .unwrap();
        }
        let db = Database::open(&path).unwrap();
        let conn = db.conn.lock().unwrap();
        let mode: String = conn
            .query_row(
                "SELECT mode FROM trades WHERE trade_id = 'revision-trade'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mode, "UNKNOWN");
        for table in ["pair_runtime", "pair_execution_guard"] {
            let leftover: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE mode = 'TESTNET'"),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(leftover, 0, "{table}");
        }
        drop(conn);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn first_exchange_mode_switch_persists_pause_across_reopen() {
        let target = TradingMode::Live;
        for scoped in [false, true] {
            let path =
                std::env::temp_dir().join(format!("mode-switch-{}.db", uuid::Uuid::new_v4()));
            let mut config = AppConfig::default();
            if scoped {
                config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
            }
            {
                let db = Database::open(&path).unwrap();
                db.save_config(&config).unwrap();
                let mut candidate = if scoped {
                    config.strategy_configs().unwrap().remove(0)
                } else {
                    config.clone()
                };
                candidate.exchange.api_key = "live-key".into();
                candidate.exchange.api_secret = "live-secret".into();
                assert_eq!(
                    db.save_runtime_config(
                        &candidate,
                        scoped.then_some("SOLUSDC"),
                        BotStatus::Running,
                        ("SOLUSDC", TradingMode::Paper)
                    )
                    .unwrap(),
                    (BotStatus::Paused, true)
                );
            }
            let db = Database::open(&path).unwrap();
            assert_eq!(
                db.load_pair_status("SOLUSDC", target).unwrap(),
                Some(BotStatus::Paused)
            );
            if !scoped {
                assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
            }
            drop(db);
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn pair_status_survives_restart_and_only_the_original_pair_inherits_legacy_pause() {
        let path =
            std::env::temp_dir().join(format!("multi-pair-status-{}.db", uuid::Uuid::new_v4()));
        {
            let db = Database::open(&path).unwrap();
            db.save_config(&AppConfig::default()).unwrap();
            db.save_bot_status(BotStatus::Paused).unwrap();
            assert_eq!(
                db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
                Some(BotStatus::Paused)
            );
            assert_eq!(
                db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
                None
            );
            db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
                .unwrap();
            db.save_pair_status("BTCUSDT", TradingMode::Paper, BotStatus::Paused)
                .unwrap();
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
            Some(BotStatus::Running)
        );
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Live).unwrap(),
            None
        );
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_running_status_is_inherited_only_by_the_original_symbol_and_mode() {
        let db = Database::open(":memory:").unwrap();
        db.save_config(&AppConfig::default()).unwrap();
        db.save_bot_status(BotStatus::Running).unwrap();

        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
            Some(BotStatus::Running)
        );
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
            None
        );
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            None
        );

        db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Paused)
            .unwrap();
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Running));
    }

    #[test]
    fn adding_and_readding_a_pair_persists_paused_status() {
        let db = Database::open(":memory:").unwrap();
        let original = AppConfig::default();
        db.save_config(&original).unwrap();
        db.save_runtime_status("SOLUSDC", TradingMode::Paper, BotStatus::Running, true)
            .unwrap();

        let mut added = original.clone();
        added.exchange.symbol = "BTCUSDT".into();
        assert!(!db.add_pair_config(&added).unwrap());
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );

        db.save_pair_status("BTCUSDT", TradingMode::Paper, BotStatus::Running)
            .unwrap();
        db.remove_pair_config("BTCUSDT").unwrap();
        assert!(!db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .iter()
            .any(|pair| pair.symbol == "BTCUSDT"));
        assert!(db.add_pair_config(&added).unwrap());
        assert!(db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .iter()
            .any(|pair| pair.symbol == "BTCUSDT"));
        assert_eq!(
            db.load_pair_status("BTCUSDT", TradingMode::Paper).unwrap(),
            Some(BotStatus::Paused)
        );
        assert_eq!(
            db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
            Some(BotStatus::Running)
        );
    }

    #[test]
    fn analysis_export_removes_credentials_from_pair_overrides() {
        let db = Database::open(":memory:").unwrap();
        let mut config = AppConfig::default();
        let mut pair = crate::config::TradingPairConfig::new("BTCUSDT".into());
        pair.exchange.api_key = Some("pair-sensitive-key".into());
        pair.exchange.api_secret = Some("pair-sensitive-secret".into());
        pair.telegram = Some(crate::config::TelegramConfig {
            enabled: true,
            bot_token: "pair-sensitive-token".into(),
            chat_id: "pair-sensitive-chat".into(),
        });
        config.pairs.push(pair);
        db.save_config(&config).unwrap();
        let bytes = db.export_analysis_snapshot().unwrap();
        let exported = String::from_utf8_lossy(&bytes);
        for secret in [
            "pair-sensitive-key",
            "pair-sensitive-secret",
            "pair-sensitive-token",
            "pair-sensitive-chat",
        ] {
            assert!(!exported.contains(secret));
        }
        assert!(db.load_config().unwrap().unwrap().pairs[0]
            .exchange
            .api_secret
            .is_some());
    }
}
