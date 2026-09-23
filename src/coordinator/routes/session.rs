//! Admin login, logout, and the one unauthenticated read that tells the
//! frontend whether to offer the admin UI at all.
//!
//! **A session is a signed token, not a row.** `crate::auth::Auth` validates
//! it by recomputing the signature and checking expiry — there is nothing in
//! storage to look up and nothing to garbage-collect. That is also why
//! `logout_handler` is best-effort: it tells the in-memory `Auth` to forget
//! the token if it still can, but a token already past its expiry, or
//! presented to a different node in a cluster that never saw the login, logs
//! the caller out just as well by simply expiring. Expiry is the real
//! control; the logout call is a courtesy that shortens the wait.
//!
//! **`require_admin`, `session_token`, and `bearer` live here because this is
//! the module that owns what a session *is*.** Every other route module
//! calls at least one of them to gate a write or an admin-only read, which is
//! why all three are `pub(in crate::coordinator)` rather than staying
//! file-private the way a helper used once would.

use super::super::{error_response, Coordinator};
use super::public::is_admin;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub(in crate::coordinator) struct LoginRequest {
    password: String,
}

#[derive(Serialize)]
struct SessionResponse {
    authenticated: bool,
}

/// The session token presented by a request, if any.
pub(in crate::coordinator) fn session_token(headers: &HeaderMap) -> Option<&str> {
    let cookies = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    crate::auth::cookie_value(cookies, crate::auth::SESSION_COOKIE)
}

/// Gate for every `/admin` route. Returns the 401 body the frontend expects.
pub(in crate::coordinator) fn require_admin(
    coord: &Coordinator,
    headers: &HeaderMap,
) -> Option<Response> {
    (!is_admin(coord, headers))
        .then(|| error_response(StatusCode::UNAUTHORIZED, "authentication required"))
}

/// The bearer credential on a request, if it carries one.
pub(in crate::coordinator) fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

pub(in crate::coordinator) async fn login_handler(
    State(coord): State<Coordinator>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<LoginRequest>,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let client_ip = crate::security::client_ip(peer.ip(), &headers, &coord.inner.trusted_proxies);
    match coord.inner.auth.login(&body.password, client_ip, now) {
        Ok(token) => {
            // The Secure attribute is an explicit deployment setting, never
            // inferred from Forwarded/X-Forwarded-Proto headers supplied by an
            // untrusted client.
            let secure = if coord.inner.secure_cookies {
                "; Secure"
            } else {
                ""
            };
            let cookie = format!(
                "{}={}; HttpOnly; SameSite=Strict; Path=/; Max-Age={}{}",
                crate::auth::SESSION_COOKIE,
                token,
                12 * 60 * 60,
                secure,
            );
            (
                StatusCode::OK,
                [(axum::http::header::SET_COOKIE, cookie)],
                Json(SessionResponse {
                    authenticated: true,
                }),
            )
                .into_response()
        }
        Err(crate::auth::LoginError::Disabled) => error_response(
            StatusCode::FORBIDDEN,
            "no admin password is configured on this server",
        ),
        Err(crate::auth::LoginError::RateLimited) => {
            error_response(StatusCode::TOO_MANY_REQUESTS, "too many attempts")
        }
        Err(crate::auth::LoginError::Invalid) => {
            error_response(StatusCode::UNAUTHORIZED, "invalid password")
        }
    }
}

pub(in crate::coordinator) async fn logout_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    if let Some(token) = session_token(&headers) {
        coord.inner.auth.logout(token);
    }
    let secure = if coord.inner.secure_cookies {
        "; Secure"
    } else {
        ""
    };
    let cleared = format!(
        "{}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0{}",
        crate::auth::SESSION_COOKIE,
        secure,
    );
    (
        StatusCode::OK,
        [(axum::http::header::SET_COOKIE, cleared)],
        Json(SessionResponse {
            authenticated: false,
        }),
    )
        .into_response()
}

pub(in crate::coordinator) async fn session_handler(
    State(coord): State<Coordinator>,
    headers: HeaderMap,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    let authenticated = session_token(&headers).is_some_and(|t| coord.inner.auth.validate(t, now));
    Json(SessionResponse { authenticated }).into_response()
}
