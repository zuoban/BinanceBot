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

#[test]
fn create_session_prunes_expired_rows_best_effort() {
    let db = Database::open(":memory:").unwrap();
    db.create_session("expired").unwrap();
    db.create_session("active").unwrap();
    db.conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE auth_sessions SET created_at = ?1 WHERE token = 'expired';",
            params![(Utc::now() - ChronoDuration::hours(25)).to_rfc3339()],
        )
        .unwrap();
    db.create_session("fresh").unwrap();
    let remaining: i64 = db
        .conn
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM auth_sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(remaining, 2);
    assert!(db.is_session_valid("active").unwrap());
    assert!(db.is_session_valid("fresh").unwrap());

    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "UPDATE auth_sessions SET created_at = '2000-01-01T00:00:00Z' WHERE token = 'active';
             CREATE TRIGGER reject_session_delete BEFORE DELETE ON auth_sessions
             BEGIN SELECT RAISE(ABORT, 'cleanup blocked'); END;",
        )
        .unwrap();
    db.create_session("after-trigger").unwrap();
    assert!(db.is_session_valid("after-trigger").unwrap());
}

#[test]
fn delete_session_skips_write_when_token_is_absent() {
    let db = Database::open(":memory:").unwrap();
    db.create_session("present").unwrap();
    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_session_delete BEFORE DELETE ON auth_sessions
             BEGIN SELECT RAISE(ABORT, 'delete executed'); END;",
        )
        .unwrap();
    db.delete_session("absent").unwrap();
    assert!(db.delete_session("present").is_err());
}
