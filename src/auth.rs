//! Admin sessions and the agent shared secret.
//!
//! Sessions live in memory rather than in a signed cookie: there is nothing to
//! persist across a restart that is worth the key management, and "restarting
//! the coordinator logs admins out" is an acceptable, even desirable, property.
//! A restart therefore invalidates every session.

use parking_lot::Mutex;
use rand::RngCore;
use std::collections::HashMap;
use std::net::IpAddr;
use subtle::ConstantTimeEq;

/// How long a session stays valid after login.
const SESSION_TTL_MS: i64 = 12 * 60 * 60 * 1000;
/// Failed logins allowed from one address before it is refused for a while.
const MAX_ATTEMPTS: u32 = 5;
const ATTEMPT_WINDOW_MS: i64 = 60_000;

pub const SESSION_COOKIE: &str = "warden_session";

#[derive(Debug, PartialEq, Eq)]
pub enum LoginError {
    /// No admin password is configured, so there is no way to authenticate.
    Disabled,
    RateLimited,
    Invalid,
}

pub struct Auth {
    admin_password: Option<String>,
    sessions: Mutex<HashMap<String, i64>>,
    attempts: Mutex<HashMap<IpAddr, (u32, i64)>>,
}

impl Auth {
    pub fn new(admin_password: Option<String>) -> Self {
        Self {
            admin_password,
            sessions: Mutex::new(HashMap::new()),
            attempts: Mutex::new(HashMap::new()),
        }
    }

    /// Whether an admin password exists at all. Without one the admin API is
    /// closed rather than open: an uptime monitor that accepts anonymous edits
    /// is worse than one nobody can edit.
    pub fn admin_enabled(&self) -> bool {
        self.admin_password.is_some()
    }

    /// Exchange a password for a session token.
    pub fn login(
        &self,
        password: &str,
        from: IpAddr,
        now_millis: i64,
    ) -> Result<String, LoginError> {
        let Some(expected) = self.admin_password.as_deref() else {
            return Err(LoginError::Disabled);
        };
        if self.is_rate_limited(from, now_millis) {
            return Err(LoginError::RateLimited);
        }
        // Constant-time: a byte-by-byte early return would leak the password
        // prefix to anyone who can measure response times.
        if !bool::from(password.as_bytes().ct_eq(expected.as_bytes())) {
            self.record_failure(from, now_millis);
            return Err(LoginError::Invalid);
        }
        self.clear_failures(from);

        let mut raw = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut raw);
        let token = hex(&raw);
        let mut sessions = self.sessions.lock();
        sessions.retain(|_, expiry| *expiry > now_millis);
        sessions.insert(token.clone(), now_millis + SESSION_TTL_MS);
        Ok(token)
    }

    pub fn validate(&self, token: &str, now_millis: i64) -> bool {
        let mut sessions = self.sessions.lock();
        // Sweep here as well as on login: a long-lived coordinator that is never
        // logged into again should not hold expired entries forever.
        sessions.retain(|_, expiry| *expiry > now_millis);
        sessions.contains_key(token)
    }

    pub fn logout(&self, token: &str) {
        self.sessions.lock().remove(token);
    }

    fn is_rate_limited(&self, from: IpAddr, now_millis: i64) -> bool {
        let attempts = self.attempts.lock();
        match attempts.get(&from) {
            Some((count, started)) => {
                *count >= MAX_ATTEMPTS && now_millis - started < ATTEMPT_WINDOW_MS
            }
            None => false,
        }
    }

    fn record_failure(&self, from: IpAddr, now_millis: i64) {
        let mut attempts = self.attempts.lock();
        attempts.retain(|_, (_, started)| now_millis - *started < ATTEMPT_WINDOW_MS);
        let entry = attempts.entry(from).or_insert((0, now_millis));
        // A fresh window starts once the previous one lapses, so a slow drip of
        // wrong guesses never accumulates into a permanent lockout.
        if now_millis - entry.1 >= ATTEMPT_WINDOW_MS {
            *entry = (0, now_millis);
        }
        entry.0 += 1;
    }

    fn clear_failures(&self, from: IpAddr) {
        self.attempts.lock().remove(&from);
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Pull one cookie value out of a `Cookie:` header.
pub fn cookie_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header.split(';').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k.trim() == name).then_some(v.trim())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

    fn auth() -> Auth {
        Auth::new(Some("hunter2".into()))
    }

    #[test]
    fn a_correct_password_yields_a_session_a_wrong_one_does_not() {
        let a = auth();
        let now = 1_000_000i64;
        assert_eq!(a.login("wrong", IP, now), Err(LoginError::Invalid));
        let token = a.login("hunter2", IP, now).expect("correct password");
        assert!(a.validate(&token, now));
        // Tokens are unguessable, so a near-miss must not authenticate.
        assert!(!a.validate("", now));
        assert!(!a.validate(&token[..token.len() - 1], now));
    }

    #[test]
    fn sessions_expire_and_logout_is_immediate() {
        let a = auth();
        let now = 1_000_000i64;
        let token = a.login("hunter2", IP, now).unwrap();
        assert!(a.validate(&token, now + SESSION_TTL_MS - 1));
        assert!(
            !a.validate(&token, now + SESSION_TTL_MS + 1),
            "a session must not outlive its TTL"
        );

        let token = a.login("hunter2", IP, now).unwrap();
        a.logout(&token);
        assert!(!a.validate(&token, now), "logout revokes immediately");
    }

    #[test]
    fn repeated_failures_are_rate_limited_then_forgiven() {
        let a = auth();
        let now = 1_000_000i64;
        for _ in 0..MAX_ATTEMPTS {
            assert_eq!(a.login("wrong", IP, now), Err(LoginError::Invalid));
        }
        assert_eq!(
            a.login("wrong", IP, now),
            Err(LoginError::RateLimited),
            "guessing must not be unlimited"
        );
        // Even the right password is refused while the window is open, otherwise
        // the limit is trivially bypassed by interleaving a guess.
        assert_eq!(a.login("hunter2", IP, now), Err(LoginError::RateLimited));
        // ...but the lockout lifts, so a fat-fingered admin isn't locked out for good.
        assert!(a.login("hunter2", IP, now + ATTEMPT_WINDOW_MS + 1).is_ok());
    }

    #[test]
    fn a_successful_login_resets_the_failure_count() {
        let a = auth();
        let now = 1_000_000i64;
        for _ in 0..MAX_ATTEMPTS - 1 {
            let _ = a.login("wrong", IP, now);
        }
        assert!(a.login("hunter2", IP, now).is_ok());
        // The earlier near-miss streak must not carry over.
        for _ in 0..MAX_ATTEMPTS - 1 {
            assert_eq!(a.login("wrong", IP, now), Err(LoginError::Invalid));
        }
    }

    #[test]
    fn without_a_password_the_admin_api_is_closed_not_open() {
        let a = Auth::new(None);
        assert!(!a.admin_enabled());
        assert_eq!(a.login("", IP, 0), Err(LoginError::Disabled));
        assert_eq!(a.login("anything", IP, 0), Err(LoginError::Disabled));
    }

    #[test]
    fn cookie_value_finds_the_named_cookie_among_others() {
        assert_eq!(
            cookie_value("theme=dark; warden_session=abc123; other=1", SESSION_COOKIE),
            Some("abc123")
        );
        assert_eq!(
            cookie_value("warden_session=solo", SESSION_COOKIE),
            Some("solo")
        );
        assert_eq!(cookie_value("theme=dark", SESSION_COOKIE), None);
        // A cookie whose name merely ends with ours must not match.
        assert_eq!(cookie_value("not_warden_session=abc", SESSION_COOKIE), None);
    }
}
