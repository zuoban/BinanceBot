use super::Database;
use crate::auth;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{params, OptionalExtension};

impl Database {
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

    pub fn initialize_admin_session(&self, password: &str, token: &str) -> Result<bool> {
        let hash = auth::hash_password_argon2(password)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO admin_auth (id, password_hash, salt, updated_at) VALUES (1, ?1, '', ?2)",
            params![hash, Utc::now().to_rfc3339()],
        )?;
        if inserted == 0 {
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO auth_sessions (token, created_at) VALUES (?1, ?2)",
            params![token, Utc::now().to_rfc3339()],
        )
        .context("Failed to create initial session")?;
        tx.commit()?;
        Ok(true)
    }

    fn password_record(&self) -> Result<Option<(String, String)>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT password_hash, salt FROM admin_auth WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?)
    }

    fn password_matches(password: &str, hash: &str, salt: &str) -> bool {
        if hash.starts_with("$argon2") {
            auth::verify_password_argon2(password, hash)
        } else {
            auth::hash_password(password, salt) == hash
        }
    }

    pub fn authenticate_and_create_session(&self, password: &str, token: &str) -> Result<bool> {
        let Some((hash, salt)) = self.password_record()? else {
            return Ok(false);
        };
        if !Self::password_matches(password, &hash, &salt) {
            return Ok(false);
        }
        let upgraded = if hash.starts_with("$argon2") {
            None
        } else {
            Some(auth::hash_password_argon2(password)?)
        };
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let unchanged: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM admin_auth WHERE id = 1 AND password_hash = ?1 AND salt = ?2)",
            params![hash, salt], |row| row.get(0),
        )?;
        if !unchanged {
            return Ok(false);
        }
        if let Some(upgraded) = upgraded {
            tx.execute(
                "UPDATE admin_auth SET password_hash = ?1, salt = '', updated_at = ?2 WHERE id = 1",
                params![upgraded, Utc::now().to_rfc3339()],
            )?;
        }
        tx.execute(
            "INSERT INTO auth_sessions (token, created_at) VALUES (?1, ?2)",
            params![token, Utc::now().to_rfc3339()],
        )
        .context("Failed to create session")?;
        tx.commit()?;
        Ok(true)
    }

    pub fn change_admin_password(&self, old_password: &str, new_password: &str) -> Result<bool> {
        let Some((hash, salt)) = self.password_record()? else {
            return Ok(false);
        };
        if !Self::password_matches(old_password, &hash, &salt) {
            return Ok(false);
        }
        let new_hash = auth::hash_password_argon2(new_password)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let updated = tx.execute(
            "UPDATE admin_auth SET password_hash = ?1, salt = '', updated_at = ?2 WHERE id = 1 AND password_hash = ?3 AND salt = ?4",
            params![new_hash, Utc::now().to_rfc3339(), hash, salt],
        )?;
        if updated == 0 {
            return Ok(false);
        }
        tx.execute("DELETE FROM auth_sessions", [])
            .context("Failed to clear sessions")?;
        tx.commit()?;
        Ok(true)
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
            let upgraded = auth::hash_password_argon2(password)?;
            let conn = self.conn.lock().unwrap();
            let updated = conn.execute(
                "UPDATE admin_auth SET password_hash = ?1, salt = '', updated_at = ?2 WHERE id = 1 AND password_hash = ?3 AND salt = ?4",
                params![upgraded, Utc::now().to_rfc3339(), stored_hash, salt],
            )?;
            return Ok(updated == 1);
        }
        Ok(false)
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
}
