use super::{
    config_runtime::{load_raw_config_value_on, save_pair_status_on, save_raw_config_value_on},
    hourly,
};
use crate::config::AppConfig;
use crate::types::{BotStatus, TradingMode};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::collections::HashMap;
use tracing::warn;

fn strip_legacy_testnet_flags(
    root: &mut serde_json::Map<String, serde_json::Value>,
) -> (bool, bool) {
    let mut retired = false;
    let mut paper_required = false;
    if let Some(exchange) = root
        .get_mut("exchange")
        .and_then(serde_json::Value::as_object_mut)
    {
        if let Some(flag) = exchange
            .remove("is_testnet")
            .and_then(|value| value.as_bool())
        {
            retired = true;
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
            paper_required |= flag.unwrap_or(false);
        }
    }
    (retired, paper_required)
}

fn clear_testnet_credentials_and_collect_symbols(
    root: &mut serde_json::Map<String, serde_json::Value>,
) -> Vec<String> {
    if let Some(exchange) = root
        .get_mut("exchange")
        .and_then(serde_json::Value::as_object_mut)
    {
        exchange.remove("api_key");
        exchange.remove("api_secret");
    }
    let mut symbols = Vec::new();
    if let Some(pairs) = root
        .get_mut("pairs")
        .and_then(serde_json::Value::as_array_mut)
    {
        for pair in pairs {
            // Startup promotes the first pair's credentials to the global
            // exchange. No legacy override may restore live mode.
            if let Some(exchange) = pair
                .get_mut("exchange")
                .and_then(serde_json::Value::as_object_mut)
            {
                exchange.remove("api_key");
                exchange.remove("api_secret");
            }
            if let Some(symbol) = pair.get("symbol").and_then(serde_json::Value::as_str) {
                symbols.push(symbol.to_ascii_uppercase());
            }
        }
    }
    if symbols.is_empty() {
        if let Some(symbol) = root
            .get("exchange")
            .and_then(|exchange| exchange.get("symbol"))
            .and_then(serde_json::Value::as_str)
        {
            symbols.push(symbol.to_ascii_uppercase());
        }
    }
    symbols
}

/// Testnet trading was removed. Credentials now select one global mode, so any
/// saved testnet strategy moves all configured strategies to paused paper mode.
/// Keep non-testnet recovery state intact for an operator to review.
fn retire_legacy_testnet_config(conn: &Connection) -> Result<()> {
    let Some(mut config) = load_raw_config_value_on(conn)? else {
        return Ok(());
    };
    let Some(root) = config.as_object_mut() else {
        return Ok(());
    };
    let (retired, paper_required) = strip_legacy_testnet_flags(root);
    if !retired {
        return Ok(());
    }
    let mut symbols = if paper_required {
        clear_testnet_credentials_and_collect_symbols(root)
    } else {
        Vec::new()
    };
    save_raw_config_value_on(conn, &config)?;
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

fn extract_legacy_dry_run_intents(
    root: &mut serde_json::Map<String, serde_json::Value>,
) -> Option<(bool, HashMap<String, bool>)> {
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
    let mut pair_intents = HashMap::new();
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
    removed.then_some((default_intent, pair_intents))
}

/// Run mode used to be stored per pair. A saved paper-mode configuration could
/// still carry API credentials, which now select live mode. Force that derived
/// live scope to start paused and drop the retired flag so an operator's later
/// resume is not undone on the next restart.
fn retire_legacy_dry_run_intent(conn: &Connection) -> Result<()> {
    let Some(mut config) = load_raw_config_value_on(conn)? else {
        return Ok(());
    };
    let Some(root) = config.as_object_mut() else {
        return Ok(());
    };
    let Some((default_intent, pair_intents)) = extract_legacy_dry_run_intents(root) else {
        return Ok(());
    };
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
    save_raw_config_value_on(conn, &config)?;
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

pub(super) fn has_legacy_paper_history_on(conn: &Connection, symbol: &str) -> Result<bool> {
    Ok(conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM trades WHERE symbol = ?1 AND mode IN ('PAPER', 'UNKNOWN')) \
             OR EXISTS(SELECT 1 FROM execution_frontiers WHERE symbol = ?1 AND mode IN ('PAPER', 'UNKNOWN')) \
             OR EXISTS(SELECT 1 FROM cleared_pair_openings WHERE symbol = ?1 AND mode IN ('PAPER', 'UNKNOWN'))",
        )?
        .query_row(params![symbol], |row| row.get(0))?)
}

fn remove_legacy_admin_auth(conn: &Connection) -> Result<()> {
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
    Ok(())
}

fn migrate_identity_observations_on(tx: &Connection) -> Result<()> {
    let has_observed_client_id: i64 = tx.query_row(
        "SELECT count(*) FROM pragma_table_info('order_identity_observations') WHERE name = 'observed_client_order_id'",
        [],
        |row| row.get(0),
    )?;
    if has_observed_client_id == 0 {
        tx.execute_batch(
            "CREATE TABLE order_identity_observations_new (
                account_key BLOB NOT NULL, client_order_id TEXT NOT NULL, expected_symbol TEXT NOT NULL,
                observed_client_order_id TEXT NOT NULL, observed_symbol TEXT NOT NULL,
                observed_side TEXT NOT NULL, observed_order_id INTEGER NOT NULL,
                expected_json TEXT NOT NULL, observed_json TEXT NOT NULL,
                PRIMARY KEY (account_key, client_order_id, expected_symbol, observed_client_order_id, observed_symbol, observed_side, observed_order_id)
            );
            INSERT INTO order_identity_observations_new
                (account_key, client_order_id, expected_symbol, observed_client_order_id, observed_symbol, observed_side, observed_order_id, expected_json, observed_json)
            SELECT account_key, client_order_id, expected_symbol,
                json_extract(observed_json, '$.client_order_id'), observed_symbol,
                json_extract(observed_json, '$.side'), observed_order_id, expected_json, observed_json
            FROM order_identity_observations;
            DROP TABLE order_identity_observations;
            ALTER TABLE order_identity_observations_new RENAME TO order_identity_observations;",
        )?;
    }
    Ok(())
}

fn migrate_initial_responses_and_disagreements_on(tx: &Connection) -> Result<()> {
    migrate_initial_order_responses_on(tx)?;
    migrate_terminal_disagreements_on(tx)
}

fn migrate_initial_order_responses_on(tx: &Connection) -> Result<()> {
    // Older versions deduplicated by response content. Preserve the order
    // of surviving rows; observations already discarded cannot be restored.
    let has_observation_id: i64 = tx.query_row(
        "SELECT count(*) FROM pragma_table_info('initial_order_responses') WHERE name = 'observation_id'",
        [],
        |row| row.get(0),
    )?;
    if has_observation_id == 0 {
        tx.execute_batch(
            "CREATE TABLE initial_order_responses_new (
                account_key BLOB NOT NULL, expected_symbol TEXT NOT NULL, client_order_id TEXT NOT NULL,
                observed_client_order_id TEXT NOT NULL, observed_symbol TEXT NOT NULL,
                observed_side TEXT NOT NULL, observed_order_id INTEGER NOT NULL,
                response_json TEXT NOT NULL, observation_id TEXT NOT NULL UNIQUE
            );
            INSERT INTO initial_order_responses_new
                (account_key, expected_symbol, client_order_id, observed_client_order_id,
                 observed_symbol, observed_side, observed_order_id, response_json, observation_id)
            SELECT account_key, expected_symbol, client_order_id, observed_client_order_id,
                observed_symbol, observed_side, observed_order_id, response_json, 'legacy:' || rowid
            FROM initial_order_responses ORDER BY rowid;
            DROP TABLE initial_order_responses;
            ALTER TABLE initial_order_responses_new RENAME TO initial_order_responses;",
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
    Ok(())
}

fn migrate_terminal_disagreements_on(tx: &Connection) -> Result<()> {
    let terminal_key_columns: i64 = tx.query_row(
        "SELECT count(*) FROM pragma_table_info('terminal_order_disagreements') WHERE pk > 0",
        [],
        |row| row.get(0),
    )?;
    if terminal_key_columns == 3 {
        tx.execute_batch(
            "CREATE TABLE terminal_order_disagreements_new (
                account_key BLOB NOT NULL, expected_symbol TEXT NOT NULL, client_order_id TEXT NOT NULL,
                first_json TEXT NOT NULL, conflicting_json TEXT NOT NULL,
                PRIMARY KEY (account_key, expected_symbol, client_order_id, first_json, conflicting_json)
            );
            INSERT INTO terminal_order_disagreements_new
                (account_key, expected_symbol, client_order_id, first_json, conflicting_json)
            SELECT account_key, expected_symbol, client_order_id, first_json, conflicting_json
            FROM terminal_order_disagreements;
            DROP TABLE terminal_order_disagreements;
            ALTER TABLE terminal_order_disagreements_new RENAME TO terminal_order_disagreements;",
        )?;
    }
    Ok(())
}

fn migrate_trades_columns_and_scope_index(conn: &Connection) -> Result<()> {
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
    let has_commission_asset: i64 = conn.query_row(
        "SELECT count(*) FROM pragma_table_info('trades') WHERE name = 'commission_asset';",
        [],
        |row| row.get(0),
    )?;
    if has_commission_asset == 0 {
        conn.execute("ALTER TABLE trades ADD COLUMN commission_asset TEXT;", [])?;
    }
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trades_scope ON trades (symbol, mode, timestamp DESC);",
        [],
    )?;
    Ok(())
}

fn parse_legacy_timestamp(row: &rusqlite::Row<'_>, index: usize) -> Result<DateTime<Utc>> {
    if let Ok(dt) = row.get::<_, DateTime<Utc>>(index) {
        return Ok(dt);
    }
    let mut raw: String = row.get(index)?;
    if raw.as_bytes().get(10) == Some(&b' ') {
        raw.replace_range(10..11, "T");
    }
    DateTime::parse_from_rfc3339(&raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("Invalid SQLite timestamp {raw:?}"))
}

fn normalize_legacy_timestamps_on(tx: &Connection) -> Result<()> {
    const FILTER: &str =
        "length(timestamp) != 30 OR substr(timestamp, 30, 1) != 'Z' OR substr(timestamp, 11, 1) != 'T'";
    let trade_updates = {
        let mut stmt = tx.prepare(&format!(
            "SELECT rowid, timestamp FROM trades WHERE {FILTER}"
        ))?;
        let mut rows = stmt.query([])?;
        let mut updates = Vec::new();
        while let Some(row) = rows.next()? {
            let canonical = super::format_sqlite_timestamp(parse_legacy_timestamp(row, 1)?);
            updates.push((row.get::<_, i64>(0)?, canonical));
        }
        updates
    };
    if !trade_updates.is_empty() {
        let mut update = tx.prepare("UPDATE trades SET timestamp = ?1 WHERE rowid = ?2")?;
        for (rowid, ts) in trade_updates {
            update.execute(params![ts, rowid])?;
        }
    }
    let frontier_updates = {
        let sql = format!("SELECT symbol, mode, timestamp FROM execution_frontiers WHERE {FILTER}");
        let mut stmt = tx.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        let mut updates = Vec::new();
        while let Some(row) = rows.next()? {
            let canonical = super::format_sqlite_timestamp(parse_legacy_timestamp(row, 2)?);
            updates.push((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                canonical,
            ));
        }
        updates
    };
    if !frontier_updates.is_empty() {
        let sql = "UPDATE execution_frontiers SET timestamp = ?1 WHERE symbol = ?2 AND mode = ?3";
        let mut update = tx.prepare(sql)?;
        for (symbol, mode, ts) in frontier_updates {
            update.execute(params![ts, symbol, mode])?;
        }
    }
    Ok(())
}

fn finalize_legacy_retirements_and_rollups(conn: &mut Connection) -> Result<()> {
    let tx = super::immediate_tx(conn)?;
    retire_legacy_testnet_config(&tx)?;
    retire_legacy_dry_run_intent(&tx)?;
    // Partial indexes keep normal inserts free while turning startup legacy scans into O(1) lookups.
    tx.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_trades_legacy_testnet ON trades (mode) WHERE mode = 'TESTNET';
         CREATE INDEX IF NOT EXISTS idx_trades_legacy_timestamp ON trades (timestamp) WHERE length(timestamp) != 30 OR substr(timestamp, 30, 1) != 'Z' OR substr(timestamp, 11, 1) != 'T';",
    )?;
    retire_legacy_testnet_rows(&tx)?;
    normalize_legacy_timestamps_on(&tx)?;
    tx.execute_batch(
        "DROP INDEX IF EXISTS idx_trades_symbol;
         DROP INDEX IF EXISTS idx_trades_client_order_id;
         DROP INDEX IF EXISTS idx_pair_intents_scope;
         CREATE INDEX IF NOT EXISTS idx_trades_unverified_scope ON trades (symbol, mode, timestamp DESC) WHERE pnl_verified = 0;
         CREATE INDEX IF NOT EXISTS idx_trades_client_order_mode ON trades (client_order_id, mode, timestamp DESC);
         CREATE INDEX IF NOT EXISTS idx_pair_intents_scope_created ON pair_intents (symbol, mode, created_at);
         CREATE INDEX IF NOT EXISTS idx_remainder_plans_inactive_scope ON remainder_plans (symbol, mode) WHERE active = 0;
         CREATE INDEX IF NOT EXISTS idx_auth_sessions_created_at ON auth_sessions (created_at);
         CREATE INDEX IF NOT EXISTS idx_order_identity_observations_scope ON order_identity_observations (account_key, expected_symbol, client_order_id);
         CREATE INDEX IF NOT EXISTS idx_initial_order_responses_order ON initial_order_responses (account_key, expected_symbol, client_order_id);
         CREATE INDEX IF NOT EXISTS idx_initial_order_responses_batch ON initial_order_responses (account_key, expected_symbol, batch_id);
         CREATE INDEX IF NOT EXISTS idx_initial_order_responses_snapshot_scope ON initial_order_responses (account_key, expected_symbol) WHERE batch_id GLOB 'snapshot:*';",
    )?;
    hourly::migrate_on(&tx)?;
    tx.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_trade_hourly_rollups_hour ON trade_hourly_rollups (hour);",
    )?;
    tx.commit()?;
    Ok(())
}

pub(super) fn run_schema_migrations(conn: &mut Connection) -> Result<()> {
    remove_legacy_admin_auth(conn)?;
    {
        // Rebuild old evidence keys atomically, retaining identities in the stored JSON.
        let tx = super::immediate_tx(conn)?;
        migrate_identity_observations_on(&tx)?;
        migrate_initial_responses_and_disagreements_on(&tx)?;
        tx.commit()?;
    }
    migrate_trades_columns_and_scope_index(conn)?;
    finalize_legacy_retirements_and_rollups(conn)
}
