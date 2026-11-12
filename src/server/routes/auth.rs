use super::{json_ok, json_ok_msg, status_err, ApiResponse};
use crate::server::state::{sanitize_session_token, AuthContext, SessionCheckError};
use crate::types::{AuthLoginPayload, AuthStatusResponse, AuthTokenResponse};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use axum::Json;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tracing::{error, info, warn};

fn extract_raw_token(headers: &HeaderMap) -> Option<&str> {
    if headers
        .get_all(axum::http::header::AUTHORIZATION)
        .iter()
        .count()
        > 1
        || headers.get_all("X-Auth-Token").iter().count() > 1
    {
        return None;
    }
    if let Some(token) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|val| val.to_str().ok())
        .and_then(|s| {
            s.strip_prefix("Bearer ")
                .or_else(|| s.strip_prefix("bearer "))
        })
        .map(str::trim)
        .filter(|t| {
            !t.is_empty()
                && t.len() <= crate::auth::MAX_METRICS_TOKEN_BYTES
                && t.bytes().all(|b| b.is_ascii_graphic())
        })
    {
        return Some(token);
    }
    headers
        .get("X-Auth-Token")
        .and_then(|val| val.to_str().ok())
        .map(str::trim)
        .filter(|t| {
            !t.is_empty()
                && t.len() <= crate::auth::MAX_METRICS_TOKEN_BYTES
                && t.bytes().all(|b| b.is_ascii_graphic())
        })
}

fn extract_token(headers: &HeaderMap) -> Option<&str> {
    extract_raw_token(headers).and_then(sanitize_session_token)
}

fn auth_service_error<T>() -> (StatusCode, Json<ApiResponse<T>>) {
    status_err(
        StatusCode::INTERNAL_SERVER_ERROR,
        "认证服务异常，请稍后重试",
    )
}

pub(super) async fn require_auth_middleware(
    State(auth): State<Arc<AuthContext>>,
    req: axum::extract::Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<ApiResponse<()>>)> {
    if !auth.is_auth_configured() {
        return Ok(next.run(req).await);
    }
    let is_valid = match extract_token(req.headers()) {
        Some(token) => match auth.check_token_valid(token).await {
            Ok(valid) => valid,
            Err(SessionCheckError::Busy) => return Err(auth_busy()),
            Err(SessionCheckError::Database(error)) => {
                error!("Failed to check session: {}", error);
                return Err(auth_service_error());
            }
        },
        None => false,
    };

    if !is_valid {
        return Err(status_err(
            StatusCode::UNAUTHORIZED,
            "未授权访问：请先输入管理员密码进行身份认证",
        ));
    }

    Ok(next.run(req).await)
}

pub(super) async fn require_metrics_auth_middleware(
    State(auth): State<Arc<AuthContext>>,
    req: axum::extract::Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<ApiResponse<()>>)> {
    let has_admin = auth.is_auth_configured();
    let metrics_auth = auth.metrics_auth();
    if !has_admin && metrics_auth.is_none() {
        return Ok(next.run(req).await);
    }
    let raw_token = extract_raw_token(req.headers());
    if let (Some(metrics_auth), Some(token)) = (metrics_auth, raw_token) {
        if metrics_auth.verify(token) {
            return Ok(next.run(req).await);
        }
    }
    if has_admin {
        let is_valid = match raw_token.and_then(sanitize_session_token) {
            Some(token) => match auth.check_token_valid(token).await {
                Ok(valid) => valid,
                Err(SessionCheckError::Busy) => return Err(auth_busy()),
                Err(SessionCheckError::Database(error)) => {
                    error!("Failed to check session: {}", error);
                    return Err(auth_service_error());
                }
            },
            None => false,
        };
        if is_valid {
            return Ok(next.run(req).await);
        }
    }
    Err(status_err(
        StatusCode::UNAUTHORIZED,
        "未授权访问：请提供有效的指标抓取令牌或管理员会话令牌",
    ))
}

pub(super) async fn get_auth_status_handler(
    State(auth): State<Arc<AuthContext>>,
    headers: HeaderMap,
) -> (StatusCode, Json<ApiResponse<AuthStatusResponse>>) {
    let initialized = auth.is_auth_configured();
    let result = if !initialized {
        Ok(true)
    } else if let Some(token) = extract_token(&headers) {
        auth.check_session_in_db(token).await
    } else {
        Ok(false)
    };
    match result {
        Ok(authenticated) => (
            StatusCode::OK,
            json_ok(AuthStatusResponse {
                initialized,
                authenticated,
            }),
        ),
        Err(SessionCheckError::Busy) => auth_busy(),
        Err(SessionCheckError::Database(error)) => {
            error!("Failed to check authentication status: {}", error);
            auth_service_error()
        }
    }
}

fn peer_ip(address: SocketAddr) -> IpAddr {
    crate::auth::canonical_ip(address.ip())
}

fn resolve_login_source_ip(
    address: SocketAddr,
    headers: &HeaderMap,
    trusted_proxies: Option<&crate::auth::TrustedProxies>,
) -> IpAddr {
    let peer = peer_ip(address);
    if !trusted_proxies.is_some_and(|trusted| trusted.contains(peer)) {
        return peer;
    }
    let mut real_ip_headers = headers.get_all("x-real-ip").iter();
    let Some(raw) = real_ip_headers.next() else {
        return peer;
    };
    if real_ip_headers.next().is_some() {
        return peer;
    }
    raw.to_str()
        .ok()
        .map(str::trim)
        .filter(|s| !s.is_empty() && s.len() <= 64)
        .and_then(|s| s.parse::<IpAddr>().ok())
        .map(crate::auth::canonical_ip)
        .unwrap_or(peer)
}

fn auth_busy<T>() -> (StatusCode, Json<ApiResponse<T>>) {
    status_err(StatusCode::SERVICE_UNAVAILABLE, "认证服务繁忙，请稍后重试")
}

pub(super) async fn post_auth_login_handler(
    State(auth): State<Arc<AuthContext>>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(payload): Json<AuthLoginPayload>,
) -> (StatusCode, Json<ApiResponse<AuthTokenResponse>>) {
    let Some(admin_auth) = auth.admin_auth() else {
        warn!("Admin login rejected: no password configured for this process");
        return status_err(StatusCode::BAD_REQUEST, "未启用管理员密码认证，无需登录");
    };
    let Some(permit) = auth.try_begin_auth_work() else {
        return auth_busy();
    };
    let source = resolve_login_source_ip(address, &headers, auth.trusted_proxies());
    let Some(attempt_at) = auth.begin_login_attempt(source).await else {
        return status_err(
            StatusCode::TOO_MANY_REQUESTS,
            "登录尝试过于频繁，请一分钟后重试",
        );
    };

    let password = payload.password;
    let verified = match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        admin_auth.verify(&password)
    })
    .await
    {
        Ok(verified) => verified,
        Err(error) => {
            error!("Admin password verification task failed: {}", error);
            return auth_service_error();
        }
    };
    if !verified {
        warn!("Failed admin authentication attempt: incorrect password");
        return status_err(StatusCode::UNAUTHORIZED, "管理员密码错误，请重新输入");
    }
    auth.rollback_single_login_attempt(source, attempt_at).await;

    let token = crate::auth::generate_session_token();
    match auth.create_session(&token).await {
        Ok(()) => {
            info!("Admin authentication successful");
            (
                StatusCode::OK,
                json_ok_msg(AuthTokenResponse { token }, "管理员身份认证成功！"),
            )
        }
        Err(error) => {
            error!("Failed to create admin session: {}", error);
            auth_service_error()
        }
    }
}

pub(super) async fn post_auth_logout_handler(
    State(auth): State<Arc<AuthContext>>,
    headers: HeaderMap,
) -> (StatusCode, Json<ApiResponse<()>>) {
    if !auth.is_auth_configured() {
        return (StatusCode::OK, json_ok_msg((), "已安全退出登录"));
    }
    if let Some(token) = extract_token(&headers) {
        match auth.delete_session(token).await {
            Ok(()) => {}
            Err(SessionCheckError::Busy) => return auth_busy(),
            Err(SessionCheckError::Database(error)) => {
                error!("Failed to delete session during logout: {}", error);
                return status_err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "退出登录失败，请重试或查看服务器日志",
                );
            }
        }
    }
    (StatusCode::OK, json_ok_msg((), "已安全退出登录"))
}
