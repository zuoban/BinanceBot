use super::{json_ok, json_ok_msg, status_err, ApiResponse};
use crate::server::state::AuthContext;
use crate::types::{AuthLoginPayload, AuthStatusResponse, AuthTokenResponse};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use axum::Json;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tracing::{error, info, warn};

fn extract_token(headers: &HeaderMap) -> Option<&str> {
    if let Some(auth_val) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_val.to_str() {
            if let Some(token) = auth_str
                .strip_prefix("Bearer ")
                .or_else(|| auth_str.strip_prefix("bearer "))
            {
                let t = token.trim();
                if !t.is_empty() {
                    return Some(t);
                }
            }
        }
    }

    if let Some(token_val) = headers.get("X-Auth-Token") {
        if let Ok(token_str) = token_val.to_str() {
            let t = token_str.trim();
            if !t.is_empty() {
                return Some(t);
            }
        }
    }

    None
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
            Err(error) => {
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

pub(super) async fn get_auth_status_handler(
    State(auth): State<Arc<AuthContext>>,
    headers: HeaderMap,
) -> (StatusCode, Json<ApiResponse<AuthStatusResponse>>) {
    let initialized = auth.is_auth_configured();
    let result = if !initialized {
        Ok(true)
    } else if let Some(token) = extract_token(&headers) {
        let token = token.to_owned();
        auth.db
            .run_blocking(move |db| db.is_session_valid(&token))
            .await
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
        Err(error) => {
            error!("Failed to check authentication status: {}", error);
            auth_service_error()
        }
    }
}

fn peer_ip(address: SocketAddr) -> IpAddr {
    match address.ip() {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    }
}

fn auth_busy<T>() -> (StatusCode, Json<ApiResponse<T>>) {
    status_err(StatusCode::SERVICE_UNAVAILABLE, "认证服务繁忙，请稍后重试")
}

pub(super) async fn post_auth_login_handler(
    State(auth): State<Arc<AuthContext>>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(payload): Json<AuthLoginPayload>,
) -> (StatusCode, Json<ApiResponse<AuthTokenResponse>>) {
    let Some(permit) = auth.try_begin_auth_work() else {
        return auth_busy();
    };
    if !auth.begin_login_attempt(peer_ip(address)).await {
        return status_err(
            StatusCode::TOO_MANY_REQUESTS,
            "登录尝试过于频繁，请一分钟后重试",
        );
    }
    let Some(admin_auth) = auth.admin_auth() else {
        warn!("Admin login rejected: no password configured for this process");
        return status_err(StatusCode::BAD_REQUEST, "未启用管理员密码认证，无需登录");
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

    let token = crate::auth::generate_session_token();
    match auth.create_session(&token).await {
        Ok(()) => {
            auth.clear_login_attempts(peer_ip(address)).await;
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
    if let Some(token) = extract_token(&headers) {
        if let Err(error) = auth.delete_session(token).await {
            error!("Failed to delete session during logout: {}", error);
            return status_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "退出登录失败，请重试或查看服务器日志",
            );
        }
    }
    (StatusCode::OK, json_ok_msg((), "已安全退出登录"))
}
