use super::Database;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{params, OptionalExtension};

impl Database {
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
            if let Err(error) = conn.execute(
                "DELETE FROM auth_sessions WHERE token = ?1;",
                params![token],
            ) {
                tracing::warn!(%error, "Failed to delete expired session");
            }
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
}
