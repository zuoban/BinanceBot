use crate::sync_ext::MutexUnpoisonedExt;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use tracing::info;

macro_rules! select_trades_sql {
    ($suffix:literal) => {
        concat!(
            "SELECT trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note, mode, commission_asset FROM trades ",
            $suffix
        )
    };
}

mod authentication;
mod config_runtime;
mod evidence;
mod grid_stats;
mod hourly;
mod migrations;
mod orders;
mod paper;
mod process_lock;
mod snapshots_export;
#[cfg(test)]
mod tests;
mod trades_cleanup;
mod trades_stats;

pub(crate) use config_runtime::config_version;
pub use process_lock::{
    read_active_listen_addr, read_saved_server_config, ActiveListenGuard, DatabaseProcessLock,
};
pub(crate) use snapshots_export::ExportedAnalysisFile;
pub(crate) use trades_stats::OrderFillDbOutcome;

pub(crate) fn beijing_midnight(now: DateTime<Utc>) -> DateTime<Utc> {
    let beijing = crate::types::beijing_offset();
    let local = now.with_timezone(&beijing);
    local
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .and_then(|midnight| midnight.and_local_timezone(beijing).single())
        .map_or(now, |midnight| midnight.with_timezone(&Utc))
}

pub(super) fn format_sqlite_timestamp(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

pub(super) fn immediate_tx(conn: &mut Connection) -> rusqlite::Result<rusqlite::Transaction<'_>> {
    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
}

pub(super) fn decimal_col(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<rust_decimal::Decimal> {
    let raw: String = row.get(index)?;
    std::str::FromStr::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(err))
    })
}

pub(super) fn query_json_vec<T, P>(
    conn: &Connection,
    sql: &str,
    params: P,
    context_label: &str,
) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned,
    P: rusqlite::Params,
{
    let mut stmt = conn
        .prepare_cached(sql)
        .with_context(|| format!("Failed to prepare {context_label} query"))?;
    let rows = stmt
        .query_map(params, |row| row.get::<_, String>(0))
        .with_context(|| format!("Failed to execute {context_label} query"))?;
    let mut items = Vec::new();
    for row in rows {
        let raw = row.with_context(|| format!("Failed to read {context_label} row"))?;
        items.push(
            serde_json::from_str(&raw)
                .with_context(|| format!("Failed to deserialize {context_label}"))?,
        );
    }
    Ok(items)
}

fn open_configured_connection(open_path: &Path, is_memory: bool) -> Result<Connection> {
    let conn = if is_memory {
        Connection::open_in_memory().context("Failed to open in-memory SQLite database")?
    } else {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        Connection::open_with_flags(open_path, flags)
            .with_context(|| format!("Failed to open SQLite database at {:?}", open_path))?
    };
    // Persist committed order intents across power loss before sending orders.
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .context("Failed to enable SQLite WAL journal mode")?;
    anyhow::ensure!(
        journal_mode.eq_ignore_ascii_case(if is_memory { "memory" } else { "wal" }),
        "SQLite journal mode is {journal_mode}, expected WAL"
    );
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .context("Failed to configure SQLite busy timeout")?;
    conn.set_prepared_statement_cache_capacity(64);
    conn.execute_batch(
        "PRAGMA synchronous = FULL;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -16000;",
    )
    .context("Failed to configure SQLite synchronous mode")?;
    if !is_memory {
        let _ = conn.execute_batch(
            "PRAGMA mmap_size = 67108864;
             PRAGMA journal_size_limit = 67108864;",
        );
    }
    let synchronous: i64 = conn
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .context("Failed to read SQLite synchronous mode")?;
    anyhow::ensure!(synchronous == 2, "SQLite synchronous mode is not FULL");
    Ok(conn)
}

// Jobs beyond this wait asynchronously instead of parking blocking-pool threads
// on the SQLite connection mutexes. Waiting never rejects a write.
const MAX_INFLIGHT_BLOCKING: usize = 32;
const MIN_READ_POOL_SIZE: usize = 2;
const MAX_READ_POOL_SIZE: usize = 8;

fn read_pool_size() -> usize {
    std::thread::available_parallelism()
        .map_or(4, std::num::NonZeroUsize::get)
        .clamp(MIN_READ_POOL_SIZE, MAX_READ_POOL_SIZE)
}

fn open_read_only_connection(open_path: &Path) -> Result<Connection> {
    // Open with VFS read-write flags so WAL shared-memory (`-shm`) can always
    // attach cleanly on freshly reopened databases, while enforcing SQL-level
    // immutability via `PRAGMA query_only = ON`.
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let conn = Connection::open_with_flags(open_path, flags)
        .with_context(|| format!("Failed to open SQLite read connection at {:?}", open_path))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .context("Failed to configure SQLite read connection busy timeout")?;
    conn.set_prepared_statement_cache_capacity(64);
    conn.execute_batch(
        "PRAGMA query_only = ON;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -4000;
         PRAGMA mmap_size = 67108864;",
    )
    .context("Failed to configure SQLite read-only connection")?;
    let query_only: i64 = conn
        .query_row("PRAGMA query_only", [], |row| row.get(0))
        .context("Failed to read SQLite query_only mode")?;
    anyhow::ensure!(query_only == 1, "SQLite read connection is not query_only");
    Ok(conn)
}

fn open_read_connection_pool(
    open_path: &Path,
    #[cfg(unix)] expected_identity: Option<(u64, u64)>,
) -> Result<Vec<Mutex<Connection>>> {
    let pool_size = read_pool_size();
    let mut pool = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        #[cfg(unix)]
        if let Some(expected_id) = expected_identity {
            use std::os::unix::fs::MetadataExt;
            validate_database_and_sidecars(open_path)?;
            let meta = std::fs::symlink_metadata(open_path)
                .with_context(|| format!("Failed to inspect {:?}", open_path))?;
            anyhow::ensure!(
                !meta.file_type().is_symlink() && (meta.dev(), meta.ino()) == expected_id,
                "SQLite database file changed while opening read pool at {:?}",
                open_path
            );
        }
        pool.push(Mutex::new(open_read_only_connection(open_path)?));
    }
    #[cfg(unix)]
    if let Some(expected_id) = expected_identity {
        use std::os::unix::fs::MetadataExt;
        validate_database_and_sidecars(open_path)?;
        let meta = std::fs::symlink_metadata(open_path)
            .with_context(|| format!("Failed to inspect {:?}", open_path))?;
        anyhow::ensure!(
            !meta.file_type().is_symlink() && (meta.dev(), meta.ino()) == expected_id,
            "SQLite database file changed after opening read pool at {:?}",
            open_path
        );
    }
    Ok(pool)
}

#[must_use]
pub(super) struct TradeCommitGuard<'a>(&'a AtomicU64);

impl Drop for TradeCommitGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

type TodayPnlCacheEntry = (
    u64,
    DateTime<Utc>,
    std::time::Instant,
    Option<crate::types::TodayPnl>,
);

#[derive(Default)]
struct RevisionTracker {
    revision: AtomicU64,
    by_symbol: Mutex<(u64, HashMap<String, u64>)>,
}

impl RevisionTracker {
    fn current(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    fn for_symbol(&self, symbol: &str) -> u64 {
        let (cleared, symbols) = &*self.by_symbol.lock_unpoisoned();
        symbols.get(symbol).copied().unwrap_or(*cleared)
    }

    fn publish(&self, symbol: &str) {
        let mut state = self.by_symbol.lock_unpoisoned();
        let revision = self.revision.fetch_add(1, Ordering::Release) + 1;
        state.1.insert(symbol.to_owned(), revision);
    }

    fn clear_all(&self) {
        let mut state = self.by_symbol.lock_unpoisoned();
        state.0 = self.revision.fetch_add(1, Ordering::Release) + 1;
        state.1.clear();
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
    // Declared before `conn` so read-only pool handles close first on Drop,
    // allowing the primary read-write handle to run SQLite's clean WAL close.
    read_conns: Vec<Mutex<Connection>>,
    next_reader: AtomicUsize,
    blocking_permits: std::sync::Arc<tokio::sync::Semaphore>,
    conn: Mutex<Connection>,
    path: String,
    #[cfg(unix)]
    file_identity: Option<(u64, u64)>,
    history_account_ids: Mutex<HashMap<[u8; 32], String>>,
    trade_commit_seq: AtomicU64,
    trade_revisions: RevisionTracker,
    trade_clear_revisions: RevisionTracker,
    #[cfg(test)]
    revision_publish_hook:
        Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    #[cfg(test)]
    membership_commit_hook: Mutex<Option<MembershipHook>>,
    today_pnl_cache: tokio::sync::Mutex<Option<TodayPnlCacheEntry>>,
    session_cache: authentication::SessionCache,
    paper_ledger: Mutex<Option<paper::PaperLedger>>,
    analysis_export: Mutex<()>,
    #[cfg(test)]
    hourly_stats_queries: AtomicU64,
    #[cfg(test)]
    benchmark_wait_samples: std::sync::OnceLock<std::sync::Arc<DatabaseWaitSamples>>,
}

use process_lock::prepare_database_path;
#[cfg(unix)]
use process_lock::validate_database_and_sidecars;
#[cfg(all(test, unix))]
use process_lock::validate_database_file;

impl Database {
    pub(crate) async fn run_blocking<T, F>(self: &std::sync::Arc<Self>, operation: F) -> Result<T>
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
        let queued_at = std::time::Instant::now();
        let permit = std::sync::Arc::clone(&self.blocking_permits)
            .acquire_owned()
            .await
            .context("Database worker queue closed")?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            #[cfg(test)]
            if let Some((samples, queued_at)) = benchmark_wait {
                let elapsed = queued_at.elapsed().as_secs_f64() * 1000.0;
                samples.queue.lock().unwrap().push(elapsed);
            }
            crate::metrics::SQLITE_BLOCKING_QUEUE_WAIT.observe(queued_at.elapsed());
            if tracing::enabled!(tracing::Level::DEBUG) {
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

    pub(super) fn lock_connection(
        &self,
        operation: &'static str,
    ) -> std::sync::MutexGuard<'_, Connection> {
        let waiting_since = std::time::Instant::now();
        #[cfg(test)]
        let benchmark_wait = self
            .benchmark_wait_samples
            .get()
            .map(|samples| (samples, std::time::Instant::now()));
        let conn = self.conn.lock_unpoisoned();
        crate::metrics::SQLITE_CONNECTION_LOCK_WAIT.observe(waiting_since.elapsed());
        #[cfg(test)]
        if let Some((samples, waiting_since)) = benchmark_wait {
            let elapsed = waiting_since.elapsed().as_secs_f64() * 1000.0;
            samples.connection.lock().unwrap().push(elapsed);
        }
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                operation,
                lock_wait_us = waiting_since.elapsed().as_micros() as u64,
                "SQLite connection lock acquired"
            );
        }
        conn
    }

    pub(super) fn lock_read_connection(
        &self,
        operation: &'static str,
    ) -> std::sync::MutexGuard<'_, Connection> {
        if self.read_conns.is_empty() {
            return self.lock_connection(operation);
        }
        let waiting_since = std::time::Instant::now();
        #[cfg(test)]
        let benchmark_wait = self
            .benchmark_wait_samples
            .get()
            .map(|samples| (samples, std::time::Instant::now()));
        let len = self.read_conns.len();
        let start = self.next_reader.fetch_add(1, Ordering::Relaxed);
        let mut guard = None;
        for offset in 0..len {
            let idx = start.wrapping_add(offset) % len;
            match self.read_conns[idx].try_lock() {
                Ok(conn) => {
                    guard = Some(conn);
                    break;
                }
                Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                    guard = Some(poisoned.into_inner());
                    break;
                }
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
        }
        let conn = guard.unwrap_or_else(|| self.read_conns[start % len].lock_unpoisoned());
        crate::metrics::SQLITE_CONNECTION_LOCK_WAIT.observe(waiting_since.elapsed());
        #[cfg(test)]
        if let Some((samples, waiting_since)) = benchmark_wait {
            let elapsed = waiting_since.elapsed().as_secs_f64() * 1000.0;
            samples.connection.lock().unwrap().push(elapsed);
        }
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                operation,
                lock_wait_us = waiting_since.elapsed().as_micros() as u64,
                "SQLite read connection lock acquired"
            );
        }
        conn
    }

    pub(super) fn begin_trade_commit(&self) -> TradeCommitGuard<'_> {
        self.trade_commit_seq.fetch_add(1, Ordering::AcqRel);
        TradeCommitGuard(&self.trade_commit_seq)
    }

    pub(crate) fn trade_commit_stable_token(&self) -> Option<u64> {
        let seq = self.trade_commit_seq.load(Ordering::Acquire);
        (seq & 1 == 0).then_some(seq)
    }

    #[cfg(test)]
    pub(crate) fn enable_benchmark_wait_samples(&self) -> std::sync::Arc<DatabaseWaitSamples> {
        let samples = std::sync::Arc::new(DatabaseWaitSamples::default());
        assert!(self.benchmark_wait_samples.set(samples.clone()).is_ok());
        samples
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let raw_path_str = path_ref.to_string_lossy().to_string();
        #[cfg(unix)]
        let mut expected_identity = None;
        let (open_path, path_str) = if raw_path_str != ":memory:" {
            // URI filenames can redirect SQLite to a different path or VFS;
            // they cannot be secured by preparing this filesystem path.
            anyhow::ensure!(
                !raw_path_str.starts_with("file:"),
                "SQLite URI database paths are not supported; use a filesystem path"
            );
            #[cfg(unix)]
            let pre_existing_id = std::fs::symlink_metadata(path_ref).ok().and_then(|meta| {
                use std::os::unix::fs::MetadataExt;
                (!meta.file_type().is_symlink()).then(|| (meta.dev(), meta.ino()))
            });
            #[cfg(unix)]
            let prepared_id = prepare_database_path(path_ref)?;
            #[cfg(not(unix))]
            prepare_database_path(path_ref)?;
            let canonical = path_ref
                .canonicalize()
                .with_context(|| format!("Failed to resolve database path {:?}", path_ref))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let canonical_meta = std::fs::symlink_metadata(&canonical)
                    .with_context(|| format!("Failed to inspect {:?}", canonical))?;
                let canonical_id = (canonical_meta.dev(), canonical_meta.ino());
                if let Some(existing_id) = pre_existing_id {
                    anyhow::ensure!(
                        existing_id == prepared_id && canonical_id == existing_id,
                        "SQLite database path {:?} changed during preparation",
                        path_ref
                    );
                } else {
                    anyhow::ensure!(
                        canonical_id == prepared_id,
                        "SQLite database path {:?} changed during resolution",
                        path_ref
                    );
                }
                expected_identity = Some(canonical_id);
            }
            let canonical_str = canonical.to_string_lossy().into_owned();
            (canonical, canonical_str)
        } else {
            (std::path::PathBuf::from(":memory:"), raw_path_str)
        };

        let is_memory = path_str == ":memory:";
        let conn = open_configured_connection(&open_path, is_memory)?;
        #[cfg(unix)]
        let file_identity = if let Some(expected_id) = expected_identity {
            use std::os::unix::fs::MetadataExt;
            validate_database_and_sidecars(&open_path)?;
            let meta = std::fs::symlink_metadata(&open_path)
                .with_context(|| format!("Failed to inspect {:?}", open_path))?;
            anyhow::ensure!(
                (meta.dev(), meta.ino()) == expected_id,
                "SQLite database file changed during open at {:?}",
                open_path
            );
            Some(expected_id)
        } else {
            None
        };
        let mut db = Self {
            read_conns: Vec::new(),
            blocking_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_INFLIGHT_BLOCKING,
            )),
            next_reader: AtomicUsize::new(0),
            conn: Mutex::new(conn),
            path: path_str,
            #[cfg(unix)]
            file_identity,
            history_account_ids: Mutex::new(HashMap::new()),
            trade_commit_seq: AtomicU64::new(0),
            trade_revisions: RevisionTracker::default(),
            trade_clear_revisions: RevisionTracker::default(),
            #[cfg(test)]
            revision_publish_hook: Mutex::new(None),
            #[cfg(test)]
            membership_commit_hook: Mutex::new(None),
            today_pnl_cache: tokio::sync::Mutex::new(None),
            session_cache: Default::default(),
            paper_ledger: Mutex::new(None),
            analysis_export: Mutex::new(()),
            #[cfg(test)]
            hourly_stats_queries: AtomicU64::new(0),
            #[cfg(test)]
            benchmark_wait_samples: std::sync::OnceLock::new(),
        };

        db.init_tables()?;
        if !is_memory {
            db.read_conns = open_read_connection_pool(
                &open_path,
                #[cfg(unix)]
                file_identity,
            )?;
        }
        Ok(db)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn init_tables(&self) -> Result<()> {
        let mut conn = self.lock_connection("init_tables");
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS app_config (id INTEGER PRIMARY KEY CHECK (id = 1), symbol TEXT NOT NULL, config_json TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS account_hourly_snapshots (hour INTEGER PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS daily_snapshots (date TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS history_account_ids (identity BLOB PRIMARY KEY, account_id TEXT NOT NULL UNIQUE);
            CREATE TABLE IF NOT EXISTS pair_runtime (symbol TEXT NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (symbol, mode));
            CREATE TABLE IF NOT EXISTS paper_accounts (symbol TEXT PRIMARY KEY, position_json TEXT NOT NULL, account_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS pair_execution_guard (symbol TEXT NOT NULL, mode TEXT NOT NULL, dirty INTEGER NOT NULL, PRIMARY KEY (symbol, mode));
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY, client_order_id TEXT NOT NULL, symbol TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT 'UNKNOWN', side TEXT NOT NULL, price TEXT NOT NULL,
                quantity TEXT NOT NULL, amount_usdc TEXT NOT NULL, realized_pnl TEXT NOT NULL,
                commission TEXT NOT NULL, pnl_verified INTEGER NOT NULL DEFAULT 0,
                is_maker INTEGER NOT NULL, timestamp TEXT NOT NULL, note TEXT NOT NULL,
                stats_hour INTEGER, commission_asset TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades (timestamp DESC);
            CREATE TABLE IF NOT EXISTS cleared_trade_ids (trade_id TEXT PRIMARY KEY, client_order_id TEXT NOT NULL, mode TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_cleared_trade_order ON cleared_trade_ids (client_order_id, mode);
            CREATE TABLE IF NOT EXISTS cleared_pair_openings (symbol TEXT NOT NULL, mode TEXT NOT NULL, client_order_id TEXT NOT NULL, side TEXT NOT NULL, price TEXT NOT NULL, quantity TEXT NOT NULL, PRIMARY KEY (symbol, mode, client_order_id));
            CREATE TABLE IF NOT EXISTS execution_frontiers (symbol TEXT NOT NULL, mode TEXT NOT NULL, side TEXT NOT NULL, price TEXT NOT NULL, timestamp TEXT NOT NULL, PRIMARY KEY (symbol, mode));
            CREATE TABLE IF NOT EXISTS managed_orders (client_order_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, order_json TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_managed_orders_symbol ON managed_orders (symbol);
            CREATE TABLE IF NOT EXISTS order_identity_observations (account_key BLOB NOT NULL, client_order_id TEXT NOT NULL, expected_symbol TEXT NOT NULL, observed_client_order_id TEXT NOT NULL, observed_symbol TEXT NOT NULL, observed_side TEXT NOT NULL, observed_order_id INTEGER NOT NULL, expected_json TEXT NOT NULL, observed_json TEXT NOT NULL, PRIMARY KEY (account_key, client_order_id, expected_symbol, observed_client_order_id, observed_symbol, observed_side, observed_order_id));
            CREATE TABLE IF NOT EXISTS initial_order_responses (account_key BLOB NOT NULL, expected_symbol TEXT NOT NULL, client_order_id TEXT NOT NULL, observed_client_order_id TEXT NOT NULL, observed_symbol TEXT NOT NULL, observed_side TEXT NOT NULL, observed_order_id INTEGER NOT NULL, response_json TEXT NOT NULL, observation_id TEXT NOT NULL UNIQUE, batch_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS terminal_order_disagreements (account_key BLOB NOT NULL, expected_symbol TEXT NOT NULL, client_order_id TEXT NOT NULL, first_json TEXT NOT NULL, conflicting_json TEXT NOT NULL, PRIMARY KEY (account_key, expected_symbol, client_order_id, first_json, conflicting_json));
            CREATE TABLE IF NOT EXISTS pair_intents (parent_client_order_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, mode TEXT NOT NULL, order_json TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS remainder_plans (target_client_order_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, mode TEXT NOT NULL, active INTEGER NOT NULL, plan_json TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_active_remainder_plan ON remainder_plans (symbol, mode) WHERE active = 1;
            CREATE TABLE IF NOT EXISTS bot_runtime (id INTEGER PRIMARY KEY CHECK (id = 1), status TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS grid_stats (id INTEGER PRIMARY KEY CHECK (id = 1), total_trades INTEGER NOT NULL, completed_cycles INTEGER NOT NULL, total_realized_profit TEXT NOT NULL, total_volume_usdc TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS scoped_grid_stats (symbol TEXT NOT NULL, mode TEXT NOT NULL, total_trades INTEGER NOT NULL, completed_cycles INTEGER NOT NULL, total_realized_profit TEXT NOT NULL, total_volume_usdc TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (symbol, mode));
            CREATE TABLE IF NOT EXISTS auth_sessions (token TEXT PRIMARY KEY, created_at TEXT NOT NULL);
            "#,
        )
        .context("Failed to initialize SQLite tables")?;

        migrations::run_schema_migrations(&mut conn)?;

        info!("SQLite schema verified for database: {}", self.path);
        Ok(())
    }
}
