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
/// long an out-of-process edit of the database can go unnoticed. Cache hits also
/// recheck the session's UTC validity window so caching never extends its lifetime.
#[derive(Default)]
pub(super) struct SessionCache(std::sync::Mutex<HashMap<String, CachedSession>>);

struct CachedSession {
    validated_at: Instant,
    created_at: DateTime<Utc>,
}

const SESSION_CACHE_TTL: Duration = Duration::from_secs(5);
const SESSION_CACHE_PRUNE_AT: usize = 64;
const SESSION_MAX_AGE: ChronoDuration = ChronoDuration::hours(24);

fn session_time_valid(created_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    let age = now.signed_duration_since(created_at);
    age >= ChronoDuration::zero() && age < SESSION_MAX_AGE
}

impl CachedSession {
    fn is_valid(&self, now: DateTime<Utc>) -> bool {
        self.validated_at.elapsed() < SESSION_CACHE_TTL && session_time_valid(self.created_at, now)
    }
}

impl SessionCache {
    fn hit(&self, token: &str, now: DateTime<Utc>) -> bool {
        self.0
            .lock_unpoisoned()
            .get(token)
            .is_some_and(|cached| cached.is_valid(now))
    }

    fn insert(&self, token: &str, created_at: DateTime<Utc>) {
        let mut entries = self.0.lock_unpoisoned();
        if !entries.contains_key(token) && entries.len() >= SESSION_CACHE_PRUNE_AT {
            let now = Utc::now();
            entries.retain(|_, cached| cached.is_valid(now));
            if entries.len() >= SESSION_CACHE_PRUNE_AT {
                if let Some(oldest_key) = entries
                    .iter()
                    .min_by_key(|(_, cached)| cached.validated_at)
                    .map(|(k, _)| k.clone())
                {
                    entries.remove(&oldest_key);
                }
            }
        }
        entries.insert(
            token.to_owned(),
            CachedSession {
                validated_at: Instant::now(),
                created_at,
            },
        );
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
        self.session_cache.hit(token, Utc::now())
    }

    pub fn create_session(&self, token: &str) -> Result<()> {
        let mut conn = self.lock_connection("create_session");
        self.session_cache.remove(token);
        let now = Utc::now();
        let tx = super::immediate_tx(&mut conn)?;
        tx.prepare_cached(
            "INSERT OR REPLACE INTO auth_sessions (token, created_at) VALUES (?1, ?2);",
        )?
        .execute(params![token, super::format_sqlite_timestamp(now)])
        .context("Failed to insert session token into SQLite database")?;
        let cutoff = super::format_sqlite_timestamp(now - SESSION_MAX_AGE);
        if let Ok(mut stmt) =
            tx.prepare_cached("DELETE FROM auth_sessions WHERE created_at <= ?1 RETURNING token;")
        {
            if let Ok(rows) = stmt.query_map(params![cutoff], |row| row.get::<_, String>(0)) {
                for evicted in rows.flatten() {
                    self.session_cache.remove(&evicted);
                }
            }
        }
        if let Ok(mut stmt) = tx.prepare_cached(
            "DELETE FROM auth_sessions WHERE token NOT IN (SELECT token FROM auth_sessions ORDER BY created_at DESC, rowid DESC LIMIT 128) RETURNING token;",
        ) {
            if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                for evicted in rows.flatten() {
                    self.session_cache.remove(&evicted);
                }
            }
        }
        tx.commit()
            .context("Failed to commit session token to SQLite database")?;
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
        let created_at = DateTime::parse_from_rfc3339(&created)
            .ok()
            .map(|time| time.with_timezone(&Utc))
            .filter(|time| session_time_valid(*time, Utc::now()));
        if let Some(created_at) = created_at {
            self.session_cache.insert(token, created_at);
        } else {
            self.session_cache.remove(token);
            if let Err(error) = conn
                .prepare_cached("DELETE FROM auth_sessions WHERE token = ?1;")
                .and_then(|mut stmt| stmt.execute(params![token]))
            {
                tracing::warn!(%error, "Failed to delete invalid or expired session");
            }
        }
        Ok(created_at.is_some())
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
