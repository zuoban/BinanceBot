use super::Database;
use crate::sync_ext::MutexUnpoisonedExt;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{params, OptionalExtension};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Positive-only cache of recently validated session tokens. Every in-process
/// mutation of `auth_sessions` invalidates it while holding the connection lock,
/// so logout and expiry cleanup take effect immediately; the TTL only bounds how
/// long an out-of-process edit of the database can go unnoticed.
#[derive(Default)]
pub(super) struct SessionCache(std::sync::Mutex<HashMap<String, Instant>>);

const SESSION_CACHE_TTL: Duration = Duration::from_secs(5);
const SESSION_CACHE_PRUNE_AT: usize = 64;

impl SessionCache {
    fn hit(&self, token: &str) -> bool {
        self.0
            .lock_unpoisoned()
            .get(token)
            .is_some_and(|validated| validated.elapsed() < SESSION_CACHE_TTL)
    }

    fn insert(&self, token: &str) {
        let mut entries = self.0.lock_unpoisoned();
        if !entries.contains_key(token) && entries.len() >= SESSION_CACHE_PRUNE_AT {
            entries.retain(|_, validated| validated.elapsed() < SESSION_CACHE_TTL);
            if entries.len() >= SESSION_CACHE_PRUNE_AT {
                if let Some(oldest_key) = entries
                    .iter()
                    .min_by_key(|(_, validated)| **validated)
                    .map(|(k, _)| k.clone())
                {
                    entries.remove(&oldest_key);
                }
            }
        }
        entries.insert(token.to_owned(), Instant::now());
    }

    fn remove(&self, token: &str) {
        self.0.lock_unpoisoned().remove(token);
    }

    fn clear(&self) {
        self.0.lock_unpoisoned().clear();
    }
}

impl Database {
    /// Lock-free of SQLite: answers only from recent in-process validations.
    pub(crate) fn session_recently_validated(&self, token: &str) -> bool {
        self.session_cache.hit(token)
    }

    pub fn create_session(&self, token: &str) -> Result<()> {
        let conn = self.lock_connection("create_session");
        self.session_cache.remove(token);
        let now = Utc::now();
        conn.prepare_cached(
            "INSERT OR REPLACE INTO auth_sessions (token, created_at) VALUES (?1, ?2);",
        )?
        .execute(params![token, now.to_rfc3339()])
        .context("Failed to insert session token into SQLite database")?;
        let cutoff = (now - ChronoDuration::hours(24)).to_rfc3339();
        if let Ok(mut stmt) =
            conn.prepare_cached("DELETE FROM auth_sessions WHERE created_at <= ?1 RETURNING token;")
        {
            if let Ok(rows) = stmt.query_map(params![cutoff], |row| row.get::<_, String>(0)) {
                for evicted in rows.flatten() {
                    self.session_cache.remove(&evicted);
                }
            }
        }
        if let Ok(mut stmt) = conn.prepare_cached(
            "DELETE FROM auth_sessions WHERE token NOT IN (SELECT token FROM auth_sessions ORDER BY created_at DESC, rowid DESC LIMIT 128) RETURNING token;",
        ) {
            if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                for evicted in rows.flatten() {
                    self.session_cache.remove(&evicted);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn is_session_valid(&self, token: &str) -> Result<bool> {
        let conn = self.lock_connection("is_session_valid");
        let created: Option<String> = conn
            .prepare_cached("SELECT created_at FROM auth_sessions WHERE token = ?1;")?
            .query_row(params![token], |row| row.get(0))
            .optional()?;
        let Some(created) = created else {
            self.session_cache.remove(token);
            return Ok(false);
        };
        let valid = DateTime::parse_from_rfc3339(&created).is_ok_and(|time| {
            Utc::now().signed_duration_since(time.with_timezone(&Utc)) < ChronoDuration::hours(24)
        });
        if valid {
            self.session_cache.insert(token);
        } else {
            self.session_cache.remove(token);
            if let Err(error) = conn
                .prepare_cached("DELETE FROM auth_sessions WHERE token = ?1;")
                .and_then(|mut stmt| stmt.execute(params![token]))
            {
                tracing::warn!(%error, "Failed to delete expired session");
            }
        }
        Ok(valid)
    }

    pub fn delete_session(&self, token: &str) -> Result<()> {
        let conn = self.lock_connection("delete_session");
        self.session_cache.remove(token);
        let exists: bool = conn
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM auth_sessions WHERE token = ?1);")?
            .query_row(params![token], |row| row.get(0))
            .context("Failed to check session token in SQLite database")?;
        if !exists {
            return Ok(());
        }
        conn.prepare_cached("DELETE FROM auth_sessions WHERE token = ?1;")?
            .execute(params![token])
            .context("Failed to delete session token from SQLite database")?;
        Ok(())
    }

    pub fn clear_all_sessions(&self) -> Result<()> {
        let conn = self.lock_connection("clear_all_sessions");
        self.session_cache.clear();
        conn.execute("DELETE FROM auth_sessions;", [])
            .context("Failed to clear sessions from SQLite database")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
