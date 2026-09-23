use sha2::{Digest, Sha256};

const HASH_ROUNDS: usize = 10_000;

/// Hashes a password with salt using 10,000 iterative SHA-256 rounds.
pub fn hash_password(password: &str, salt: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update(password.as_bytes());
    let mut current_hash = hasher.finalize();

    for i in 0..HASH_ROUNDS {
        let mut next_hasher = Sha256::new();
        next_hasher.update(&current_hash);
        next_hasher.update(salt.as_bytes());
        next_hasher.update(i.to_le_bytes());
        current_hash = next_hasher.finalize();
    }

    hex::encode(current_hash)
}

/// Generates a cryptographically random 16-byte hex salt (32 hex characters).
pub fn generate_salt() -> String {
    let mut bytes = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    hex::encode(bytes)
}

/// Generates a secure random 32-byte hex session token (64 hex characters).
pub fn generate_session_token() -> String {
    let mut bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hashing_and_verification() {
        let salt = generate_salt();
        assert_eq!(salt.len(), 32);

        let pass1 = "Admin@123456";
        let hash1 = hash_password(pass1, &salt);
        let hash2 = hash_password(pass1, &salt);

        assert_eq!(hash1, hash2);

        let pass_wrong = "Admin@654321";
        let hash_wrong = hash_password(pass_wrong, &salt);
        assert_ne!(hash1, hash_wrong);
    }

    #[test]
    fn test_session_token_uniqueness() {
        let token1 = generate_session_token();
        let token2 = generate_session_token();
        assert_eq!(token1.len(), 64);
        assert_ne!(token1, token2);
    }
}
