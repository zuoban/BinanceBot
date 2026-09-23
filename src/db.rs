use crate::config::AppConfig;
use crate::types::{GridStats, OrderSide, TradeRecord};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
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
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("Failed to create database directory: {:?}", parent))?;
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
                is_maker INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                note TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades (timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol);

            CREATE TABLE IF NOT EXISTS grid_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_trades INTEGER NOT NULL,
                completed_cycles INTEGER NOT NULL,
                total_realized_profit TEXT NOT NULL,
                total_volume_usdc TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            "#,
        )
        .context("Failed to initialize SQLite tables")?;

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
            params![
                config.exchange.symbol,
                json_str,
                Utc::now().to_rfc3339(),
            ],
        )
        .context("Failed to save config to SQLite database")?;

        Ok(())
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<()> {
        let side_str = match trade.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT OR REPLACE INTO trades (
                trade_id, client_order_id, symbol, side, price, quantity,
                amount_usdc, realized_pnl, commission, is_maker, timestamp, note
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12);
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
                if trade.is_maker { 1 } else { 0 },
                trade.timestamp,
                trade.note,
            ],
        )
        .context("Failed to insert trade into SQLite database")?;

        Ok(())
    }

    pub fn get_recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT trade_id, client_order_id, symbol, side, price, quantity,
                   amount_usdc, realized_pnl, commission, is_maker, timestamp, note
            FROM trades
            ORDER BY timestamp DESC
            LIMIT ?1;
            "#,
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            let trade_id: String = row.get(0)?;
            let client_order_id: String = row.get(1)?;
            let symbol: String = row.get(2)?;
            let side_str: String = row.get(3)?;
            let price_str: String = row.get(4)?;
            let qty_str: String = row.get(5)?;
            let amount_str: String = row.get(6)?;
            let pnl_str: String = row.get(7)?;
            let comm_str: String = row.get(8)?;
            let is_maker_int: i32 = row.get(9)?;
            let timestamp: DateTime<Utc> = row.get(10)?;
            let note: String = row.get(11)?;

            let side = match side_str.as_str() {
                "BUY" => OrderSide::Buy,
                _ => OrderSide::Sell,
            };
            let price = Decimal::from_str(&price_str).unwrap_or(Decimal::ZERO);
            let quantity = Decimal::from_str(&qty_str).unwrap_or(Decimal::ZERO);
            let amount_usdc = Decimal::from_str(&amount_str).unwrap_or(Decimal::ZERO);
            let realized_pnl = Decimal::from_str(&pnl_str).unwrap_or(Decimal::ZERO);
            let commission = Decimal::from_str(&comm_str).unwrap_or(Decimal::ZERO);

            Ok(TradeRecord {
                trade_id,
                client_order_id,
                symbol,
                side,
                price,
                quantity,
                amount_usdc,
                realized_pnl,
                commission,
                is_maker: is_maker_int != 0,
                timestamp,
                note,
            })
        })?;

        let mut list = Vec::new();
        for item in rows {
            list.push(item?);
        }
        Ok(list)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

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
    fn test_db_stats_lifecycle() {
        let db = Database::open(":memory:").unwrap();
        assert!(db.load_stats().unwrap().is_none());

        let mut stats = GridStats::default();
        stats.total_trades = 42;
        stats.completed_cycles = 21;
        stats.total_realized_profit = dec!(15.75);
        stats.total_volume_usdc = dec!(4200.0);

        db.save_stats(&stats).unwrap();
        let (trades, cycles, profit, volume) = db.load_stats().unwrap().expect("stats should exist");
        assert_eq!(trades, 42);
        assert_eq!(cycles, 21);
        assert_eq!(profit, dec!(15.75));
        assert_eq!(volume, dec!(4200.0));
    }
}
