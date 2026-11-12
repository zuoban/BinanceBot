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

#[test]
fn metrics_token_validates_bounds_charset_and_exact_digest_match() {
    assert!(MetricsAuth::from_token("too-short-15chr").is_err());
    assert!(MetricsAuth::from_token("valid_length_with space").is_err());
    assert!(MetricsAuth::from_token(&"x".repeat(MAX_METRICS_TOKEN_BYTES + 1)).is_err());

    let exact_min = "0123456789abcdef";
    let auth = MetricsAuth::from_token(exact_min).unwrap();
    assert!(auth.verify(exact_min));
    assert!(!auth.verify("0123456789abcdee"));
    assert!(!auth.verify("0123456789abcdef "));
    assert!(!auth.verify(""));

    let max_tok = "m".repeat(MAX_METRICS_TOKEN_BYTES);
    let max_auth = MetricsAuth::from_token(&max_tok).unwrap();
    assert!(max_auth.verify(&max_tok));
    assert!(!max_auth.verify(&"m".repeat(MAX_METRICS_TOKEN_BYTES + 1)));
}

#[test]
fn trusted_proxies_parse_ips_cidrs_and_ipv4_mapped_ipv6() {
    let empty = TrustedProxies::parse(" ,  ").unwrap();
    assert!(empty.is_empty());
    assert!(!empty.contains("127.0.0.1".parse().unwrap()));

    let proxies =
        TrustedProxies::parse("127.0.0.1, 172.16.0.0/12, 2001:db8::/32, ::ffff:10.0.0.5").unwrap();
    assert_eq!(proxies.len(), 4);
    assert!(proxies.contains("127.0.0.1".parse().unwrap()));
    assert!(proxies.contains("::ffff:127.0.0.1".parse().unwrap()));
    assert!(proxies.contains("172.24.19.8".parse().unwrap()));
    assert!(!proxies.contains("172.32.0.1".parse().unwrap()));
    assert!(proxies.contains("2001:db8::1".parse().unwrap()));
    assert!(!proxies.contains("2001:db9::1".parse().unwrap()));
    assert!(proxies.contains("10.0.0.5".parse().unwrap()));

    for bad in [
        "not-an-ip",
        "10.0.0.1/33",
        "2001:db8::/129",
        "10.0.0.1/16/8",
    ] {
        assert!(TrustedProxies::parse(bad).is_err(), "{bad}");
    }
}
