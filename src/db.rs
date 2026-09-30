use crate::auth;
use crate::config::AppConfig;
use crate::types::{
    BotStatus, GridOrder, GridStats, HourlyTradeCount, HourlyTradeStats, OrderSide, RemainderPlan,
    TradeRecord, TradingMode,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::Decimal;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;
use tracing::info;

pub struct Database {
    conn: Mutex<Connection>,
    path: String,
}

impl Database {
    pub async fn run_blocking<T, F>(self: &std::sync::Arc<Self>, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> Result<T> + Send + 'static,
    {
        let db = std::sync::Arc::clone(self);
        tokio::task::spawn_blocking(move || operation(&db))
            .await
            .context("Database worker stopped")?
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        if path_str != ":memory:" {
            if let Some(parent) = path_ref.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("Failed to create database directory: {:?}", parent)
                    })?;
                }
            }
        }

        let conn = Connection::open(path_ref)
            .with_context(|| format!("Failed to open SQLite database at {:?}", path_ref))?;

        // Enable WAL mode and normal synchronous for better write throughput and concurrency
        let _ = conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;");

        let db = Self {
            conn: Mutex::new(conn),
            path: path_str,
        };

        db.init_tables()?;
        Ok(db)
    }

    pub fn path(&self) -> &str {
        &self.path
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
            snapshot.execute(
                "UPDATE app_config SET config_json = ?1 WHERE id = 1",
                params![serde_json::to_string(&config)?],
            )?;
        }
        snapshot.execute_batch("DELETE FROM admin_auth; DELETE FROM auth_sessions; VACUUM;")?;
        // VACUUM removes credentials from freed pages and old config payloads too.
        Ok(snapshot.serialize(rusqlite::DatabaseName::Main)?.to_vec())
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
        let value = serde_json::to_value(status)?;
        let name = value
            .as_str()
            .context("Invalid bot status representation")?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO bot_runtime (id, status, updated_at) VALUES (1, ?1, ?2) ON CONFLICT(id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at;",
            params![name, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn init_tables(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS app_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                symbol TEXT NOT NULL,
                config_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
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

            CREATE TABLE IF NOT EXISTS managed_orders (
                client_order_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                order_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_managed_orders_symbol ON managed_orders (symbol);

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

            CREATE TABLE IF NOT EXISTS admin_auth (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS auth_sessions (
                token TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            );
            "#,
        )
        .context("Failed to initialize SQLite tables")?;

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

        info!("SQLite schema verified for database: {}", self.path);
        Ok(())
    }

    pub fn is_admin_password_set(&self) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT count(*) FROM admin_auth WHERE id = 1;")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count > 0)
    }

    pub fn set_admin_password(&self, password: &str) -> Result<()> {
        let hash = auth::hash_password_argon2(password)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO admin_auth (id, password_hash, salt, updated_at)
            VALUES (1, ?1, ?2, ?3)
            ON CONFLICT(id) DO UPDATE SET
                password_hash = excluded.password_hash,
                salt = excluded.salt,
                updated_at = excluded.updated_at;
            "#,
            params![hash, "", Utc::now().to_rfc3339()],
        )
        .context("Failed to save admin password into SQLite database")?;

        Ok(())
    }

    pub fn initialize_admin_password(&self, password: &str) -> Result<bool> {
        let hash = auth::hash_password_argon2(password)?;
        let conn = self.conn.lock().unwrap();
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO admin_auth (id, password_hash, salt, updated_at) VALUES (1, ?1, '', ?2);",
            params![hash, Utc::now().to_rfc3339()],
        )?;
        Ok(inserted == 1)
    }

    pub fn verify_admin_password(&self, password: &str) -> Result<bool> {
        let stored: Option<(String, String)> = {
            let conn = self.conn.lock().unwrap();
            conn.query_row(
                "SELECT password_hash, salt FROM admin_auth WHERE id = 1;",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
        };
        let Some((stored_hash, salt)) = stored else {
            return Ok(false);
        };
        if stored_hash.starts_with("$argon2") {
            return Ok(auth::verify_password_argon2(password, &stored_hash));
        }
        let valid = auth::hash_password(password, &salt) == stored_hash;
        if valid {
            self.set_admin_password(password)?;
        }
        Ok(valid)
    }

    pub fn create_session(&self, token: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO auth_sessions (token, created_at) VALUES (?1, ?2);",
            params![token, Utc::now().to_rfc3339()],
        )
        .context("Failed to insert session token into SQLite database")?;
        Ok(())
    }

    pub fn is_session_valid(&self, token: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let created: Option<String> = conn
            .query_row(
                "SELECT created_at FROM auth_sessions WHERE token = ?1;",
                params![token],
                |row| row.get(0),
            )
            .optional()?;
        let Some(created) = created else {
            return Ok(false);
        };
        let valid = DateTime::parse_from_rfc3339(&created).is_ok_and(|time| {
            Utc::now().signed_duration_since(time.with_timezone(&Utc)) < ChronoDuration::hours(24)
        });
        if !valid {
            conn.execute(
                "DELETE FROM auth_sessions WHERE token = ?1;",
                params![token],
            )?;
        }
        Ok(valid)
    }

    pub fn delete_session(&self, token: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM auth_sessions WHERE token = ?1;",
            params![token],
        )
        .context("Failed to delete session token from SQLite database")?;
        Ok(())
    }

    pub fn clear_all_sessions(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM auth_sessions;", [])
            .context("Failed to clear sessions from SQLite database")?;
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
        let json_str = serde_json::to_string_pretty(config)
            .context("Failed to serialize AppConfig to JSON")?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO app_config (id, symbol, config_json, updated_at)
            VALUES (1, ?1, ?2, ?3)
            ON CONFLICT(id) DO UPDATE SET
                symbol = excluded.symbol,
                config_json = excluded.config_json,
                updated_at = excluded.updated_at;
            "#,
            params![config.exchange.symbol, json_str, Utc::now().to_rfc3339(),],
        )
        .context("Failed to save config to SQLite database")?;

        Ok(())
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        insert_trade_on(&conn, trade)
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
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let inserted = insert_trade_on(&tx, trade)?;
        if inserted {
            save_scoped_stats_on(&tx, &trade.symbol, trade.mode, stats)?;
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
        Ok(inserted)
    }

    pub fn load_pair_intents(&self, symbol: &str, mode: TradingMode) -> Result<Vec<GridOrder>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT order_json FROM pair_intents WHERE symbol = ?1 AND mode = ?2 ORDER BY created_at;",
        )?;
        let rows = stmt.query_map(params![symbol, mode.as_str()], |row| {
            row.get::<_, String>(0)
        })?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn delete_pair_intent(&self, parent_client_order_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
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
        let conn = self.conn.lock().unwrap();
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
        let conn = self.conn.lock().unwrap();
        let json: Option<String> = conn.query_row(
            "SELECT plan_json FROM remainder_plans WHERE symbol = ?1 AND mode = ?2 AND active = 1;",
            params![symbol, mode.as_str()], |row| row.get(0),
        ).optional()?;
        json.map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }

    pub fn save_managed_order(&self, order: &GridOrder) -> Result<()> {
        let json = serde_json::to_string(order)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO managed_orders (client_order_id, symbol, order_json, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(client_order_id) DO UPDATE SET symbol = excluded.symbol, order_json = excluded.order_json, updated_at = excluded.updated_at;",
            params![order.client_order_id, order.symbol, json, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn save_managed_orders(&self, orders: &[GridOrder]) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
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

    pub fn delete_managed_order(&self, client_order_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
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

    /// Restore the execution frontier for this symbol and mode.
    pub fn last_fill_for(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<Option<(OrderSide, Decimal)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT side, price FROM trades WHERE symbol = ?1 AND mode = ?2 ORDER BY timestamp DESC, rowid DESC LIMIT 1;",
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
        let conn = self.conn.lock().unwrap();
        let updated = conn.execute(
            "UPDATE trades SET realized_pnl = ?1, commission = ?2, is_maker = ?3, pnl_verified = 1 WHERE trade_id = ?4 AND mode = ?5 AND pnl_verified = 0;",
            params![
                trade.realized_pnl.to_string(),
                trade.commission.to_string(),
                if trade.is_maker { 1 } else { 0 },
                trade.trade_id,
                trade.mode.as_str(),
            ],
        )?;
        Ok(updated == 1)
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

    /// Count all persisted orders in the rolling 24-hour window, including empty hours.
    pub fn get_hourly_trade_stats(
        &self,
        symbol: &str,
        mode: TradingMode,
        now: DateTime<Utc>,
    ) -> Result<HourlyTradeStats> {
        let window_start = now - ChronoDuration::hours(24);
        let first_hour = window_start.timestamp().div_euclid(3600) * 3600;
        let last_hour = now.timestamp().div_euclid(3600) * 3600;
        let mut buckets: Vec<_> = (first_hour..=last_hour)
            .step_by(3600)
            .map(|timestamp| HourlyTradeCount {
                hour_start: DateTime::from_timestamp(timestamp, 0).unwrap(),
                buy_count: 0,
                sell_count: 0,
            })
            .collect();

        let conn = self.conn.lock().unwrap();
        // Stored timestamps are UTC RFC3339. Coarse day bounds use the scope index;
        // parse before the exact comparison to preserve fractional-second precision.
        let mut stmt = conn.prepare(
            "SELECT timestamp, side FROM trades
             WHERE symbol = ?1 AND mode = ?2 AND timestamp >= ?3 AND timestamp < ?4;",
        )?;
        let rows = stmt.query_map(
            params![
                symbol,
                mode.as_str(),
                window_start.date_naive().to_string(),
                (now + ChronoDuration::days(1)).date_naive().to_string(),
            ],
            |row| Ok((row.get::<_, DateTime<Utc>>(0)?, row.get::<_, String>(1)?)),
        )?;
        for row in rows {
            let (timestamp, side) = row?;
            if timestamp < window_start || timestamp > now {
                continue;
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
        })
    }

    pub fn get_unverified_trades(
        &self,
        symbol: &str,
        mode: TradingMode,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note, mode FROM trades WHERE symbol = ?1 AND mode = ?2 AND pnl_verified = 0 ORDER BY timestamp DESC LIMIT ?3 OFFSET ?4;",
        )?;
        let rows = stmt.query_map(
            params![symbol, mode.as_str(), limit as i64, offset as i64],
            trade_from_row,
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
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

fn insert_trade_on(conn: &Connection, trade: &TradeRecord) -> Result<bool> {
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
        db.initialize_admin_password("export-test-password")
            .unwrap();
        db.create_session("export-test-session-token").unwrap();
        db.save_bot_status(BotStatus::Running).unwrap();
        let password_hash: String;
        {
            let conn = db.conn.lock().unwrap();
            password_hash = conn
                .query_row("SELECT password_hash FROM admin_auth", [], |r| r.get(0))
                .unwrap();
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
            &password_hash,
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
            ("admin_auth", 0),
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
        assert!(db.verify_admin_password("export-test-password").unwrap());
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
    fn admin_setup_is_atomic_and_legacy_hashes_upgrade() {
        let db = Database::open(":memory:").unwrap();
        assert!(db.initialize_admin_password("first-password").unwrap());
        assert!(!db.initialize_admin_password("second-password").unwrap());
        assert!(db.verify_admin_password("first-password").unwrap());
        assert!(!db.verify_admin_password("second-password").unwrap());

        let salt = auth::generate_salt();
        let legacy_hash = auth::hash_password("legacy-password", &salt);
        db.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE admin_auth SET password_hash = ?1, salt = ?2 WHERE id = 1;",
                params![legacy_hash, salt],
            )
            .unwrap();
        assert!(db.verify_admin_password("legacy-password").unwrap());
        let upgraded: String = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT password_hash FROM admin_auth WHERE id = 1;",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(upgraded.starts_with("$argon2id$"));
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
    fn hourly_trade_stats_cover_full_window_and_isolate_scope() {
        let db = Database::open(":memory:").unwrap();
        let now = DateTime::parse_from_rfc3339("2026-09-28T10:30:00.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        let start = now - ChronoDuration::hours(24);
        let empty = db
            .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, now)
            .unwrap();
        assert_eq!(empty.buckets.len(), 25);
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
                "midnight",
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
            (
                "testnet",
                now,
                OrderSide::Buy,
                "SOLUSDC",
                TradingMode::Testnet,
            ),
        ] {
            trade.trade_id = id.into();
            trade.client_order_id = id.into();
            trade.timestamp = timestamp;
            trade.side = side;
            trade.symbol = symbol.into();
            trade.mode = mode;
            assert!(db.insert_trade(&trade).unwrap());
        }
        let stats = db
            .get_hourly_trade_stats("SOLUSDC", TradingMode::Paper, now)
            .unwrap();
        assert_eq!(stats.window_start, start);
        assert_eq!(stats.window_end, now);
        assert_eq!(stats.buckets[0].buy_count, 250);
        assert_eq!(stats.buckets[14].sell_count, 1);
        assert_eq!(stats.buckets[24].sell_count, 1);
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
        assert_eq!(later.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 0);
        assert_eq!(later.buckets.iter().map(|b| b.sell_count).sum::<u64>(), 3);
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
            db.last_fill_for("SOLUSDC", TradingMode::Testnet).unwrap(),
            None
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
            ("testnet", TradingMode::Testnet, dec!(3)),
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
        assert_eq!(
            db.load_scoped_stats("SOLUSDC", TradingMode::Testnet)
                .unwrap()
                .unwrap()
                .0,
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
    fn test_db_admin_auth_and_sessions() {
        let db = Database::open(":memory:").unwrap();
        assert!(!db.is_admin_password_set().unwrap());

        // Set password
        db.set_admin_password("Secret123").unwrap();
        assert!(db.is_admin_password_set().unwrap());

        // Verify password
        assert!(db.verify_admin_password("Secret123").unwrap());
        assert!(!db.verify_admin_password("WrongPassword").unwrap());

        // Session tokens
        let token = "test_token_123456";
        assert!(!db.is_session_valid(token).unwrap());

        db.create_session(token).unwrap();
        assert!(db.is_session_valid(token).unwrap());

        db.delete_session(token).unwrap();
        assert!(!db.is_session_valid(token).unwrap());
    }
}
