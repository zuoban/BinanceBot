use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::Argon2;
use rand::RngCore;

/// Administrator credentials supplied at process startup.
///
/// Only the Argon2 hash is held in memory; the plaintext password is never
/// written to the database or configuration files.
pub struct AdminAuth {
    password_hash: String,
}

const MAX_ADMIN_PASSWORD_BYTES: usize = 1024;

impl AdminAuth {
    pub fn from_password(password: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            password.len() >= 6,
            "管理员密码长度至少为 6 个字符（--admin-password 或 ADMIN_PASSWORD）"
        );
        anyhow::ensure!(
            password.len() <= MAX_ADMIN_PASSWORD_BYTES,
            "管理员密码长度不能超过 {} 字节",
            MAX_ADMIN_PASSWORD_BYTES
        );
        Ok(Self {
            password_hash: hash_password_argon2(password)?,
        })
    }

    pub fn verify(&self, password: &str) -> bool {
        if password.len() > MAX_ADMIN_PASSWORD_BYTES {
            return false;
        }
        verify_password_argon2(password, &self.password_hash)
    }
}

pub fn hash_password_argon2(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| anyhow::anyhow!("Could not hash admin password: {}", error))
}

pub fn verify_password_argon2(password: &str, stored_hash: &str) -> bool {
    if password.len() > MAX_ADMIN_PASSWORD_BYTES {
        return false;
    }
    PasswordHash::new(stored_hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}

/// Generates a secure random 32-byte hex session token (64 hex characters).
pub fn generate_session_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_password_hash_verifies_only_the_configured_password() {
        let auth = AdminAuth::from_password("correct horse battery staple").unwrap();
        assert!(auth.verify("correct horse battery staple"));
        assert!(!auth.verify("Correct horse battery staple"));
        assert!(!auth.verify(""));
    }

    #[test]
    fn admin_password_requires_six_characters() {
        assert!(AdminAuth::from_password("short").is_err());
        assert!(AdminAuth::from_password("sixchr").is_ok());
        let max = "a".repeat(MAX_ADMIN_PASSWORD_BYTES);
        let over = "a".repeat(MAX_ADMIN_PASSWORD_BYTES + 1);
        let auth = AdminAuth::from_password(&max).unwrap();
        assert!(auth.verify(&max));
        assert!(!auth.verify(&over));
        assert!(AdminAuth::from_password(&over).is_err());
    }

    #[test]
    fn session_token_uniqueness() {
        let token1 = generate_session_token();
        let token2 = generate_session_token();
        assert_eq!(token1.len(), 64);
        assert_ne!(token1, token2);
    }
}
