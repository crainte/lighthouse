//! HTTP plumbing shared by the coordinator and the global node: error
//! responses, the embedded dashboard, and the admin-session extractor. Both
//! tiers serve the same SPA and speak the same error shape, so this lives at
//! the crate root rather than under either.

use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::request::Parts;
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::Json;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

pub fn error_response(code: StatusCode, message: &str) -> Response {
    (
        code,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
        .into_response()
}

pub fn internal_error(e: anyhow::Error) -> Response {
    tracing::error!(error = %e, "request failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

#[derive(Serialize)]
pub struct DeletedResponse {
    pub deleted: String,
}

// ---- Embedded dashboard (built Svelte SPA under frontend/dist) ----

#[derive(rust_embed::RustEmbed)]
#[folder = "frontend/dist"]
struct Assets;

/// Serve the embedded SPA at `uri`'s path. Unknown paths fall back to
/// `index.html` so client-side routing works. Callers that also serve an API
/// under the same fallback are responsible for keeping unmatched API paths
/// out of this function (they should 404 instead of returning the SPA).
pub fn serve_asset(uri: &Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let lookup = if path.is_empty() { "index.html" } else { path };
    if let Some(content) = Assets::get(lookup) {
        let mime = mime_guess::from_path(lookup).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response();
    }

    // SPA fallback / unbuilt-frontend placeholder.
    match Assets::get("index.html") {
        Some(content) => Html(content.data).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "dashboard not built — run `bun run build` in frontend/",
        )
            .into_response(),
    }
}

// ---- Admin-session extractor ----

/// Implemented by each tier's shared state so [`AdminSession`] can gate a
/// handler without either tier reimplementing the extractor. Distinct from
/// "is this caller allowed a detailed answer" checks some public handlers
/// also make — those stay direct calls to each tier's own `is_admin`.
pub trait AdminGate {
    fn is_admin_request(&self, headers: &HeaderMap) -> bool;
}

/// Extractor for `/admin` routes. Rejects with the same 401 body a manual
/// `require_admin`-style check used to return, before the handler body runs.
pub struct AdminSession;

#[axum::async_trait]
impl<S> FromRequestParts<S> for AdminSession
where
    S: AdminGate + Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        admin_gate(state, &parts.headers).map_err(|e| *e)?;
        Ok(AdminSession)
    }
}

// Boxed: `Response` is >128 bytes, and clippy flags a bare `Result<(), Response>`
// on a function called from every admin-gated extractor.
fn admin_gate<S: AdminGate>(state: &S, headers: &HeaderMap) -> Result<(), Box<Response>> {
    if state.is_admin_request(headers) {
        Ok(())
    } else {
        Err(Box::new(error_response(
            StatusCode::UNAUTHORIZED,
            "authentication required",
        )))
    }
}

/// `Json<T>`, gated the same way as [`AdminSession`], but body-consuming: the
/// content-type check, the shared body-size limit, and parsing all happen
/// *before* the admin check, exactly as they did when `Json<T>` was a plain
/// handler parameter and the admin check ran as the first statement in the
/// body. An oversized or malformed admin payload is rejected the same way
/// whether or not the caller is signed in — [`AdminSession`] alone cannot
/// preserve that, since it would run, and reject, before `Json` ever reads
/// the body.
pub struct AdminJson<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequest<S> for AdminJson<T>
where
    S: AdminGate + Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let headers = req.headers().clone();
        let Json(value) = Json::<T>::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        admin_gate(state, &headers).map_err(|e| *e)?;
        Ok(AdminJson(value))
    }
}

/// `axum::body::Bytes`, gated the same way as [`AdminJson`]: the body-size
/// limit is enforced by reading the body before the admin check, but parsing
/// is left to the caller — for the couple of handlers that parse manually so
/// a malformed body never tells an anonymous caller more than "authentication
/// required" does.
pub struct AdminBytes(pub axum::body::Bytes);

#[axum::async_trait]
impl<S> FromRequest<S> for AdminBytes
where
    S: AdminGate + Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let headers = req.headers().clone();
        let bytes = axum::body::Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        admin_gate(state, &headers).map_err(|e| *e)?;
        Ok(AdminBytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Auth;
    use axum::routing::get;
    use axum::Router;
    use std::net::SocketAddr;
    use std::sync::Arc;

    #[derive(Clone)]
    struct FakeState(Arc<Auth>);

    impl AdminGate for FakeState {
        fn is_admin_request(&self, headers: &HeaderMap) -> bool {
            let now = chrono::Utc::now().timestamp_millis();
            let cookies = match headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) {
                Some(c) => c,
                None => return false,
            };
            crate::auth::cookie_value(cookies, "session").is_some_and(|t| self.0.validate(t, now))
        }
    }

    async fn probe(_admin: AdminSession) -> &'static str {
        "ok"
    }

    async fn spawn(state: FakeState) -> SocketAddr {
        let app = Router::new().route("/probe", get(probe)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    }

    #[tokio::test]
    async fn valid_session_is_accepted() {
        let auth = Arc::new(Auth::new(Some("secret".into())));
        let now = chrono::Utc::now().timestamp_millis();
        let token = auth
            .login("secret", "127.0.0.1".parse().unwrap(), now)
            .unwrap();
        let addr = spawn(FakeState(auth)).await;
        let res = reqwest::Client::new()
            .get(format!("http://{addr}/probe"))
            .header(header::COOKIE, format!("session={token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_session_is_rejected() {
        let auth = Arc::new(Auth::new(Some("secret".into())));
        let addr = spawn(FakeState(auth)).await;
        let res = reqwest::Client::new()
            .get(format!("http://{addr}/probe"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::UNAUTHORIZED);
        let body: serde_json::Value = res.json().await.unwrap();
        assert_eq!(body["error"], "authentication required");
    }

    #[tokio::test]
    async fn tampered_session_is_rejected() {
        let auth = Arc::new(Auth::new(Some("secret".into())));
        let now = chrono::Utc::now().timestamp_millis();
        let mut token = auth
            .login("secret", "127.0.0.1".parse().unwrap(), now)
            .unwrap();
        token.push('x');
        let addr = spawn(FakeState(auth)).await;
        let res = reqwest::Client::new()
            .get(format!("http://{addr}/probe"))
            .header(header::COOKIE, format!("session={token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::UNAUTHORIZED);
        let body: serde_json::Value = res.json().await.unwrap();
        assert_eq!(body["error"], "authentication required");
    }
}
