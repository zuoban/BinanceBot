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
