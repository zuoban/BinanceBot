use super::Database;
use crate::config::AppConfig;
use crate::types::{BotStatus, TradingMode};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use tracing::warn;

impl Database {
    pub fn load_bot_status(&self) -> Result<Option<BotStatus>> {
        let conn = self.lock_connection("load_bot_status");
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
        save_bot_status_on(&self.lock_connection("save_bot_status"), status)
    }

    pub fn load_pair_status(&self, symbol: &str, mode: TradingMode) -> Result<Option<BotStatus>> {
        let value: Option<String> = {
            let conn = self.lock_connection("load_pair_status");
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
        save_pair_status_on(
            &self.lock_connection("save_pair_status"),
            symbol,
            mode,
            status,
        )
    }

    pub fn load_config(&self) -> Result<Option<AppConfig>> {
        load_config_on(&self.lock_connection("load_config"))
    }

    pub(crate) async fn load_saved_config(self: &std::sync::Arc<Self>) -> Result<AppConfig> {
        self.run_blocking(|db| db.load_config())
            .await?
            .ok_or_else(|| anyhow::anyhow!("无法读取全局配置"))
    }

    pub fn save_config(&self, config: &AppConfig) -> Result<()> {
        save_config_on(&self.lock_connection("save_config"), config)
    }

    /// Merge while holding the database mutex so simultaneous pair updates cannot lose changes.
    pub fn save_pair_config(&self, symbol: &str, config: &AppConfig) -> Result<()> {
        save_pair_config_on(&self.lock_connection("save_pair_config"), symbol, config)
    }

    pub fn save_global_config(&self, global: &crate::config::GlobalConfig) -> Result<()> {
        let conn = self.lock_connection("save_global_config");
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
        let mut conn = self.lock_connection("add_pair_config");
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
        let conn = self.lock_connection("remove_pair_config");
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
        let conn = self.lock_connection("save_server_config_checked");
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
        let mut conn = self.lock_connection("save_runtime_config");
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
        let mut conn = self.lock_connection("save_runtime_status");
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
        let mut conn = self.lock_connection("arm_execution_guard");
        let tx = conn.transaction()?;
        let dirty = arm_execution_guard_on(&tx, symbol, mode)?;
        tx.commit()?;
        Ok(dirty)
    }

    pub fn clear_execution_guard(&self, symbol: &str, mode: TradingMode) -> Result<()> {
        let conn = self.lock_connection("clear_execution_guard");
        conn.prepare_cached(
            "UPDATE pair_execution_guard SET dirty = 0 WHERE symbol = ?1 AND mode = ?2",
        )?
        .execute(params![symbol, mode.as_str()])?;
        Ok(())
    }
}

pub(super) fn load_config_on(conn: &Connection) -> Result<Option<AppConfig>> {
    conn.query_row(
        "SELECT config_json FROM app_config WHERE id = 1",
        [],
        |row| row.get::<_, String>(0),
    )
    .optional()?
    .map(|json| {
        serde_json::from_str(&json).context("Failed to deserialize AppConfig from SQLite database")
    })
    .transpose()
}

fn load_required_config_on(conn: &Connection) -> Result<AppConfig> {
    load_config_on(conn)?.context("Missing AppConfig in SQLite database")
}

pub(super) fn load_raw_config_value_on(conn: &Connection) -> Result<Option<serde_json::Value>> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT config_json FROM app_config WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(stored.and_then(|json| serde_json::from_str(&json).ok()))
}

pub(super) fn save_raw_config_value_on(
    conn: &Connection,
    config: &serde_json::Value,
) -> Result<()> {
    conn.execute(
        "UPDATE app_config SET config_json = ?1, updated_at = ?2 WHERE id = 1",
        params![
            serde_json::to_string_pretty(config)?,
            Utc::now().to_rfc3339()
        ],
    )?;
    Ok(())
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
    static KEY: std::sync::LazyLock<[u8; 32]> = std::sync::LazyLock::new(|| {
        let mut key = [0; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);
        key
    });
    struct MacWriter(hmac::Hmac<sha2::Sha256>);
    impl std::io::Write for MacWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.update(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = MacWriter(Hmac::<Sha256>::new_from_slice(&*KEY).expect("valid HMAC key"));
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        warn!(%error, "Failed to serialize configuration for version digest");
    }
    hex::encode(writer.0.finalize().into_bytes())
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

pub(super) fn save_pair_status_on(
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
