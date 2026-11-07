use super::Database;
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
            .lock()
            .unwrap()
            .get(token)
            .is_some_and(|validated| validated.elapsed() < SESSION_CACHE_TTL)
    }

    fn insert(&self, token: &str) {
        let mut entries = self.0.lock().unwrap();
        if entries.len() >= SESSION_CACHE_PRUNE_AT {
            entries.retain(|_, validated| validated.elapsed() < SESSION_CACHE_TTL);
        }
        entries.insert(token.to_owned(), Instant::now());
    }

    fn remove(&self, token: &str) {
        self.0.lock().unwrap().remove(token);
    }

    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
}

impl Database {
    /// Lock-free of SQLite: answers only from recent in-process validations.
    pub fn session_recently_validated(&self, token: &str) -> bool {
        self.session_cache.hit(token)
    }

    pub fn create_session(&self, token: &str) -> Result<()> {
        let conn = self.lock_connection("create_session");
        self.session_cache.remove(token);
        conn.execute(
            "INSERT OR REPLACE INTO auth_sessions (token, created_at) VALUES (?1, ?2);",
            params![token, Utc::now().to_rfc3339()],
        )
        .context("Failed to insert session token into SQLite database")?;
        Ok(())
    }

    pub fn is_session_valid(&self, token: &str) -> Result<bool> {
        let conn = self.lock_connection("is_session_valid");
        let created: Option<String> = conn
            .query_row(
                "SELECT created_at FROM auth_sessions WHERE token = ?1;",
                params![token],
                |row| row.get(0),
            )
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
        let conn = self.lock_connection("delete_session");
        self.session_cache.remove(token);
        conn.execute(
            "DELETE FROM auth_sessions WHERE token = ?1;",
            params![token],
        )
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
mod tests {
    use super::*;

    #[test]
    fn logout_and_clear_invalidate_the_validation_cache_immediately() {
        let db = Database::open(":memory:").unwrap();
        for token in ["a", "b"] {
            db.create_session(token).unwrap();
            assert!(db.is_session_valid(token).unwrap());
            assert!(db.session_recently_validated(token));
        }
        db.delete_session("a").unwrap();
        assert!(!db.session_recently_validated("a"));
        assert!(db.session_recently_validated("b"));
        db.clear_all_sessions().unwrap();
        assert!(!db.session_recently_validated("b"));
    }

    #[test]
    fn replacing_a_session_does_not_keep_a_stale_cache_entry() {
        let db = Database::open(":memory:").unwrap();
        db.create_session("a").unwrap();
        assert!(db.is_session_valid("a").unwrap());
        db.create_session("a").unwrap();
        assert!(!db.session_recently_validated("a"));
    }
}
