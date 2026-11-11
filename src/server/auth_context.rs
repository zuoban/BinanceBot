use super::AppState;
use crate::db::Database;
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const MAX_GLOBAL_LOGIN_ATTEMPTS: usize = 100;
const MAX_LOGIN_SOURCES: usize = 10_000;
const MAX_SOURCE_LOGIN_ATTEMPTS: usize = 10;

const MAX_COLD_SESSION_LOOKUPS: usize = 16;

pub(crate) fn sanitize_session_token(raw: &str) -> Option<&str> {
    let t = raw.trim();
    (!t.is_empty() && t.len() <= 128 && t.bytes().all(|b| b.is_ascii_graphic())).then_some(t)
}
#[derive(Debug)]
pub(crate) enum SessionCheckError {
    Busy,
    Database(anyhow::Error),
}

impl std::fmt::Display for SessionCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => f.write_str("session validation saturated"),
            Self::Database(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

#[derive(Default)]
pub(crate) struct LoginAttempts {
    pub(crate) by_source: HashMap<IpAddr, VecDeque<Instant>>,
    pub(crate) global: VecDeque<Instant>,
}

/// Process-wide authentication, session validation, and login rate-limiting state.
/// Decoupled from individual `AppState` lifecycles so removing the first trading
/// pair never strands global HTTP routes or closes runtime-wide WebSocket streams.
pub(crate) struct AuthContext {
    pub(crate) db: Arc<Database>,
    pub(crate) login_failures: Mutex<LoginAttempts>,
    auth_work: Arc<Semaphore>,
    session_lookup_permits: Arc<Semaphore>,
    admin_auth: Option<Arc<crate::auth::AdminAuth>>,
}

impl AuthContext {
    pub(crate) fn new(
        db: Arc<Database>,
        admin_auth: Option<Arc<crate::auth::AdminAuth>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            login_failures: Mutex::new(LoginAttempts::default()),
            auth_work: Arc::new(Semaphore::new(4)),
            session_lookup_permits: Arc::new(Semaphore::new(MAX_COLD_SESSION_LOOKUPS)),
            admin_auth,
        })
    }

    pub(crate) fn admin_auth(&self) -> Option<Arc<crate::auth::AdminAuth>> {
        self.admin_auth.clone()
    }

    pub(crate) fn is_auth_configured(&self) -> bool {
        self.admin_auth.is_some()
    }

    pub(crate) async fn check_session_in_db(&self, token: &str) -> Result<bool, SessionCheckError> {
        let permit = self
            .session_lookup_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SessionCheckError::Busy)?;
        let token = token.to_string();
        self.db
            .run_blocking(move |db| {
                let _permit = permit;
                db.is_session_valid(&token)
            })
            .await
            .map_err(SessionCheckError::Database)
    }

    pub(crate) async fn check_token_valid(&self, token: &str) -> Result<bool, SessionCheckError> {
        if !self.is_auth_configured() {
            return Ok(true);
        }
        let Some(token) = sanitize_session_token(token).filter(|t| *t == token) else {
            return Ok(false);
        };
        if self.db.session_recently_validated(token) {
            return Ok(true);
        }
        self.check_session_in_db(token).await
    }

    pub(crate) async fn is_token_valid(&self, token: &str) -> bool {
        self.check_token_valid(token).await.unwrap_or(false)
    }

    pub(crate) async fn create_session(&self, token: &str) -> anyhow::Result<()> {
        let token = token.to_string();
        self.db
            .run_blocking(move |db| db.create_session(&token))
            .await
    }

    pub(crate) async fn delete_session(&self, token: &str) -> Result<(), SessionCheckError> {
        if !self.is_auth_configured() {
            return Ok(());
        }
        let permit = self
            .session_lookup_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SessionCheckError::Busy)?;
        let token = token.to_string();
        self.db
            .run_blocking(move |db| {
                let _permit = permit;
                db.delete_session(&token)
            })
            .await
            .map_err(SessionCheckError::Database)
    }

    pub(crate) async fn begin_login_attempt(&self, source: IpAddr) -> Option<Instant> {
        let mut failures = self.login_failures.lock().await;
        failures.by_source.retain(|_, attempts| {
            attempts.retain(|time| time.elapsed() < LOGIN_WINDOW);
            !attempts.is_empty()
        });
        failures.global.retain(|time| time.elapsed() < LOGIN_WINDOW);
        if failures.global.len() >= MAX_GLOBAL_LOGIN_ATTEMPTS {
            return None;
        }
        // Active windows must not be evicted by churn from other sources.
        if !failures.by_source.contains_key(&source)
            && failures.by_source.len() >= MAX_LOGIN_SOURCES
        {
            return None;
        }
        let attempts = failures.by_source.entry(source).or_default();
        if attempts.len() >= MAX_SOURCE_LOGIN_ATTEMPTS {
            return None;
        }
        let now = Instant::now();
        attempts.push_back(now);
        failures.global.push_back(now);
        Some(now)
    }

    pub(crate) async fn rollback_single_login_attempt(&self, source: IpAddr, attempt_at: Instant) {
        let mut failures = self.login_failures.lock().await;
        if let Some(attempts) = failures.by_source.get_mut(&source) {
            if let Some(pos) = attempts.iter().rposition(|t| *t == attempt_at) {
                attempts.remove(pos);
            }
            if attempts.is_empty() {
                failures.by_source.remove(&source);
            }
        }
        if let Some(pos) = failures.global.iter().rposition(|t| *t == attempt_at) {
            failures.global.remove(pos);
        }
    }

    #[cfg(test)]
    pub(crate) async fn clear_login_attempts(&self, source: IpAddr) {
        self.login_failures.lock().await.by_source.remove(&source);
    }

    pub(crate) fn try_begin_auth_work(&self) -> Option<OwnedSemaphorePermit> {
        self.auth_work.clone().try_acquire_owned().ok()
    }

    #[cfg(test)]
    pub(crate) fn try_acquire_session_lookup_permit(&self) -> Option<OwnedSemaphorePermit> {
        self.session_lookup_permits.clone().try_acquire_owned().ok()
    }
}

impl AppState {
    pub async fn is_token_valid(&self, token: &str) -> bool {
        self.auth.is_token_valid(token).await
    }

    pub async fn create_session(&self, token: &str) -> anyhow::Result<()> {
        self.auth.create_session(token).await
    }

    #[cfg(test)]
    pub(crate) async fn begin_login_attempt(&self, source: IpAddr) -> bool {
        self.auth.begin_login_attempt(source).await.is_some()
    }

    #[cfg(test)]
    pub(crate) async fn clear_login_attempts(&self, source: IpAddr) {
        self.auth.clear_login_attempts(source).await
    }

    pub fn try_begin_auth_work(&self) -> Option<OwnedSemaphorePermit> {
        self.auth.try_begin_auth_work()
    }
}
