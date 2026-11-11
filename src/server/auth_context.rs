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

#[derive(Default)]
pub(crate) struct LoginAttempts {
    pub(super) by_source: HashMap<IpAddr, VecDeque<Instant>>,
    pub(super) global: VecDeque<Instant>,
}

/// Process-wide authentication, session validation, and login rate-limiting state.
/// Decoupled from individual `AppState` lifecycles so removing the first trading
/// pair never strands global HTTP routes or closes runtime-wide WebSocket streams.
pub(crate) struct AuthContext {
    pub(crate) db: Arc<Database>,
    pub(crate) login_failures: Mutex<LoginAttempts>,
    auth_work: Arc<Semaphore>,
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
            admin_auth,
        })
    }

    pub(crate) fn admin_auth(&self) -> Option<Arc<crate::auth::AdminAuth>> {
        self.admin_auth.clone()
    }

    pub(crate) fn is_auth_configured(&self) -> bool {
        self.admin_auth.is_some()
    }

    pub(crate) async fn check_token_valid(&self, token: &str) -> anyhow::Result<bool> {
        if !self.is_auth_configured() {
            return Ok(true);
        }
        if token.is_empty() {
            return Ok(false);
        }
        if self.db.session_recently_validated(token) {
            return Ok(true);
        }
        let token = token.to_string();
        self.db
            .run_blocking(move |db| db.is_session_valid(&token))
            .await
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

    pub(crate) async fn delete_session(&self, token: &str) -> anyhow::Result<()> {
        let token = token.to_string();
        self.db
            .run_blocking(move |db| db.delete_session(&token))
            .await
    }

    pub(crate) async fn begin_login_attempt(&self, source: IpAddr) -> bool {
        let mut failures = self.login_failures.lock().await;
        failures.by_source.retain(|_, attempts| {
            attempts.retain(|time| time.elapsed() < LOGIN_WINDOW);
            !attempts.is_empty()
        });
        failures.global.retain(|time| time.elapsed() < LOGIN_WINDOW);
        if failures.global.len() >= MAX_GLOBAL_LOGIN_ATTEMPTS {
            return false;
        }
        // Active windows must not be evicted by churn from other sources.
        if !failures.by_source.contains_key(&source)
            && failures.by_source.len() >= MAX_LOGIN_SOURCES
        {
            return false;
        }
        let attempts = failures.by_source.entry(source).or_default();
        if attempts.len() >= MAX_SOURCE_LOGIN_ATTEMPTS {
            return false;
        }
        let now = Instant::now();
        attempts.push_back(now);
        failures.global.push_back(now);
        true
    }

    pub(crate) async fn clear_login_attempts(&self, source: IpAddr) {
        self.login_failures.lock().await.by_source.remove(&source);
    }

    pub(crate) fn try_begin_auth_work(&self) -> Option<OwnedSemaphorePermit> {
        self.auth_work.clone().try_acquire_owned().ok()
    }
}

impl AppState {
    pub async fn check_token_valid(&self, token: &str) -> anyhow::Result<bool> {
        self.auth.check_token_valid(token).await
    }

    pub async fn is_token_valid(&self, token: &str) -> bool {
        self.auth.is_token_valid(token).await
    }

    pub async fn create_session(&self, token: &str) -> anyhow::Result<()> {
        self.auth.create_session(token).await
    }

    pub async fn delete_session(&self, token: &str) -> anyhow::Result<()> {
        self.auth.delete_session(token).await
    }

    pub async fn begin_login_attempt(&self, source: IpAddr) -> bool {
        self.auth.begin_login_attempt(source).await
    }

    pub async fn clear_login_attempts(&self, source: IpAddr) {
        self.auth.clear_login_attempts(source).await
    }

    pub fn try_begin_auth_work(&self) -> Option<OwnedSemaphorePermit> {
        self.auth.try_begin_auth_work()
    }

    pub fn admin_auth(&self) -> Option<Arc<crate::auth::AdminAuth>> {
        self.auth.admin_auth()
    }

    pub fn is_auth_configured(&self) -> bool {
        self.auth.is_auth_configured()
    }
}
