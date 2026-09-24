use crate::auth;
use crate::config::AppConfig;
use crate::types::{BotStatus, GridOrder, GridStats, OrderSide, TradeRecord};
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
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let inserted = insert_trade_on(&tx, trade)?;
        if inserted {
            save_stats_on(&tx, stats)?;
        }
        tx.commit()?;
        Ok(inserted)
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

    pub fn get_trade_by_client_id(&self, client_order_id: &str) -> Result<Option<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note FROM trades WHERE client_order_id = ?1 ORDER BY timestamp DESC LIMIT 1;",
        )?;
        let mut rows = stmt.query(params![client_order_id])?;
        rows.next()?
            .map(trade_from_row)
            .transpose()
            .map_err(Into::into)
    }

    /// Return true only for the first successful PnL verification of a saved trade.
    pub fn verify_trade_pnl(&self, trade: &TradeRecord) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let updated = conn.execute(
            "UPDATE trades SET realized_pnl = ?1, commission = ?2, is_maker = ?3, pnl_verified = 1 WHERE trade_id = ?4 AND pnl_verified = 0;",
            params![
                trade.realized_pnl.to_string(),
                trade.commission.to_string(),
                if trade.is_maker { 1 } else { 0 },
                trade.trade_id,
            ],
        )?;
        Ok(updated == 1)
    }

    pub fn get_recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT trade_id, client_order_id, symbol, side, price, quantity,
                   amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note
            FROM trades
            ORDER BY timestamp DESC
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

    pub fn get_unverified_trades(
        &self,
        symbol: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT trade_id, client_order_id, symbol, side, price, quantity, amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note FROM trades WHERE symbol = ?1 AND pnl_verified = 0 ORDER BY timestamp DESC LIMIT ?2 OFFSET ?3;",
        )?;
        let rows = stmt.query_map(params![symbol, limit as i64, offset as i64], trade_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn get_pnl_totals(&self, symbol: &str) -> Result<(Decimal, Decimal, usize)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT realized_pnl, commission, pnl_verified FROM trades WHERE symbol = ?1;",
        )?;
        let rows = stmt.query_map(params![symbol], |row| {
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
                trade_id, client_order_id, symbol, side, price, quantity,
                amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13);
            "#,
            params![
                trade.trade_id,
                trade.client_order_id,
                trade.symbol,
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
    fn test_db_trade_lifecycle() {
        let db = Database::open(":memory:").unwrap();
        let trade = TradeRecord {
            trade_id: "trade_1".to_string(),
            client_order_id: "order_1".to_string(),
            symbol: "SOLUSDC".to_string(),
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
            "CREATE TRIGGER reject_stats BEFORE INSERT ON grid_stats BEGIN SELECT RAISE(ABORT, 'reject stats'); END;",
        ).unwrap();
        assert!(db.insert_trade_with_stats(&trade, &stats).is_err());
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        assert!(db.load_stats().unwrap().is_none());

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
        assert_eq!(db.load_stats().unwrap().unwrap().0, 1);
        assert_eq!(db.load_stats().unwrap().unwrap().3, dec!(100));
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

        let mut pending = db.get_unverified_trades("SOLUSDC", 10, 0).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(db.get_pnl_totals("SOLUSDC").unwrap(), (dec!(0), dec!(0), 1));

        let mut trade = pending.pop().unwrap();
        trade.realized_pnl = dec!(-3.25);
        trade.commission = dec!(0.80);
        trade.pnl_verified = true;
        assert!(db.verify_trade_pnl(&trade).unwrap());
        assert!(!db.verify_trade_pnl(&trade).unwrap());
        assert!(db
            .get_unverified_trades("SOLUSDC", 10, 0)
            .unwrap()
            .is_empty());
        assert_eq!(
            db.get_pnl_totals("SOLUSDC").unwrap(),
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
