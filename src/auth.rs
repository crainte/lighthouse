//! Admin sessions and the agent shared secret.
//!
//! A session token is a small signed, stateless blob: `base64url(payload) +
//! "." + base64url(HMAC-SHA256(key, payload))`, where `payload` is
//! `{ issued_at, expires_at, jti }` as JSON. Validating a token needs only the
//! signing key, not a lookup in a map that only the node which minted the
//! token has populated. That is what makes sessions work behind a load
//! balancer in front of a coordinator cluster: any node can validate a token
//! any other node issued, because the key is shared rather than the state.
//!
//! Two constructors, two threat models:
//!
//! - [`Auth::new`] generates a random per-process key, exactly as the old
//!   in-memory session map behaved: a restart invalidates every session,
//!   because the key that signed them is gone. This is the standalone
//!   default and it is unchanged behaviour.
//! - [`Auth::with_signing_key`] derives the key from caller-supplied material
//!   — in practice `peer_token`, which is stable across restarts and
//!   identical on every node by construction, or a random key persisted in
//!   the `settings` table. **This means sessions now survive a coordinator
//!   restart**, where they did not before. That is the point of the change —
//!   a rolling restart of a clustered coordinator must not force every admin
//!   to log back in — but it is a real behaviour change and worth stating
//!   plainly rather than leaving it to be discovered: whoever holds that key
//!   material can mint valid sessions offline, so it must be handled with the
//!   same care as `peer_token` itself.
//!
//! Logout does not need to reach every node: it revokes the token's `jti` in
//! a small local set, and [`Auth::revocations`] / [`Auth::revoke`] let the
//! integrator push that revocation to peers over the existing cluster
//! channel (best-effort). If a push is dropped, the logged-out token stays
//! valid on the node(s) that never heard about it until it expires on its
//! own — bounded by `SESSION_TTL_MS`, never longer, but not immediate
//! everywhere the way single-node logout was.

use hmac::{Hmac, KeyInit, Mac};
use parking_lot::Mutex;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::IpAddr;
use subtle::ConstantTimeEq;

/// How long a session stays valid after login.
const SESSION_TTL_MS: i64 = 12 * 60 * 60 * 1000;
/// Failed logins allowed from one address before it is refused for a while.
const MAX_ATTEMPTS: u32 = 5;
const ATTEMPT_WINDOW_MS: i64 = 60_000;
/// Separates the signing key derived for sessions from any other use of the
/// same key material (e.g. `peer_token` also gates the cluster HTTP API), so
/// a session token can never be replayed as proof of anything else.
const KEY_DOMAIN: &[u8] = b"warden-session-v1";

pub const SESSION_COOKIE: &str = "warden_session";

/// The global node names its session cookie distinctly so an operator can be
/// signed into both a regional coordinator and the global dashboard at once.
/// Cookies are not port-scoped, so on a shared host (localhost demo, or a
/// reverse proxy fronting both) a single `warden_session` name would let each
/// login clobber the other's cookie.
pub const GLOBAL_SESSION_COOKIE: &str = "warden_global_session";

#[derive(Debug, PartialEq, Eq)]
pub enum LoginError {
    /// No admin password is configured, so there is no way to authenticate.
    Disabled,
    RateLimited,
    Invalid,
}

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    issued_at: i64,
    expires_at: i64,
    /// 128+ bits of randomness: two logins in the same millisecond must still
    /// be distinguishable tokens, and logout must be able to revoke one
    /// session without guessing or invalidating every other one issued at
    /// the same time.
    jti: String,
}

pub struct Auth {
    admin_password: Option<String>,
    signing_key: Vec<u8>,
    /// jti -> expires_at, for tokens logged out before their natural expiry.
    /// Bounded by `SESSION_TTL_MS`: an entry can never usefully outlive the
    /// token it revokes, so once it is swept there is nothing left to prune.
    revoked: Mutex<HashMap<String, i64>>,
    attempts: Mutex<HashMap<IpAddr, (u32, i64)>>,
}

impl Auth {
    /// Standalone constructor: a random key that lives only as long as this
    /// process. Unchanged signature and unchanged behaviour — a restart still
    /// logs every admin out, which is fine when there is exactly one node and
    /// nothing else to keep the key in sync with.
    pub fn new(admin_password: Option<String>) -> Self {
        let mut key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        Self::from_signing_key(admin_password, key)
    }

    /// Clustered constructor: every node must derive the same key from the
    /// same material to validate each other's tokens, so the material itself
    /// (typically `peer_token`) has to be supplied rather than generated.
    /// The key actually used to sign is a domain-separated hash of it, not
    /// the material itself — see [`KEY_DOMAIN`].
    pub fn with_signing_key(admin_password: Option<String>, key: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(KEY_DOMAIN);
        hasher.update(key);
        Self::from_signing_key(admin_password, hasher.finalize().to_vec())
    }

    fn from_signing_key(admin_password: Option<String>, signing_key: Vec<u8>) -> Self {
        Self {
            admin_password,
            signing_key,
            revoked: Mutex::new(HashMap::new()),
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

        let mut jti_raw = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut jti_raw);
        let payload = Payload {
            issued_at: now_millis,
            expires_at: now_millis + SESSION_TTL_MS,
            jti: hex(&jti_raw),
        };
        Ok(self.sign(&payload))
    }

    /// Order matters: verify the signature before looking at anything the
    /// payload claims. An attacker who can get an unsigned or mis-signed
    /// token past this point controls `expires_at` and `jti` outright.
    pub fn validate(&self, token: &str, now_millis: i64) -> bool {
        let Some(payload) = self.open(token) else {
            return false;
        };
        if payload.expires_at <= now_millis {
            return false;
        }
        let mut revoked = self.revoked.lock();
        // Sweep here, as on login in the old map-based implementation: a
        // long-lived coordinator that nobody logs out of again should not
        // hold expired revocation entries forever.
        revoked.retain(|_, expiry| *expiry > now_millis);
        !revoked.contains_key(&payload.jti)
    }

    /// Revoke this token's session on this node immediately. Other nodes
    /// learn of it only if the integrator forwards it via [`Auth::revoke`]
    /// over the cluster channel — see the module docs for the dropped-push
    /// case.
    pub fn logout(&self, token: &str) {
        if let Some(payload) = self.open(token) {
            self.revoked.lock().insert(payload.jti, payload.expires_at);
        }
    }

    /// Snapshot of locally-known revocations, for the integrator to push to
    /// peers. Not swept on its own: sweeping needs a notion of "now", and
    /// `validate` already provides that on every read, which is where a
    /// revocation actually needs to be gone by.
    pub fn revocations(&self) -> Vec<(String, i64)> {
        self.revoked
            .lock()
            .iter()
            .map(|(jti, expires_at)| (jti.clone(), *expires_at))
            .collect()
    }

    /// Apply a revocation received from a peer. Idempotent: applying the same
    /// `(jti, expires_at)` twice, from a retry or from two peers relaying the
    /// same logout, is harmless.
    pub fn revoke(&self, jti: &str, expires_at: i64) {
        self.revoked.lock().insert(jti.to_string(), expires_at);
    }

    fn sign(&self, payload: &Payload) -> String {
        let payload_json =
            serde_json::to_vec(payload).expect("Payload has no non-serializable fields");
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.signing_key)
            .expect("HMAC accepts a key of any length");
        mac.update(&payload_json);
        let tag = mac.finalize().into_bytes();
        format!("{}.{}", b64_encode(&payload_json), b64_encode(&tag))
    }

    /// Verify the signature and, only if it matches, parse and return the
    /// payload. Never returns a payload for a token whose signature did not
    /// check out, no matter how well-formed the JSON inside it looks —
    /// base64-valid-but-unsigned input is attacker-supplied by definition,
    /// since this parses cookies.
    fn open(&self, token: &str) -> Option<Payload> {
        let (payload_part, sig_part) = token.split_once('.')?;
        let payload_json = b64_decode(payload_part)?;
        let sig = b64_decode(sig_part)?;

        let mut mac = Hmac::<Sha256>::new_from_slice(&self.signing_key)
            .expect("HMAC accepts a key of any length");
        mac.update(&payload_json);
        let expected = mac.finalize().into_bytes();
        // Constant-time: `ConstantTimeEq` for slices short-circuits only on
        // length (public, fixed by SHA-256's output size), never on content,
        // so a forged signature cannot be brute-forced byte by byte via
        // response timing.
        if !bool::from(expected.as_slice().ct_eq(&sig)) {
            return None;
        }

        serde_json::from_slice(&payload_json).ok()
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

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Unpadded base64url, hand-rolled: the only other option was pulling in a
/// whole crate for ~20 lines used in exactly one place.
fn b64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_ALPHABET[(n >> 18 & 0x3f) as usize] as char);
        out.push(B64_ALPHABET[(n >> 12 & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_ALPHABET[(n >> 6 & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(B64_ALPHABET[(n & 0x3f) as usize] as char);
        }
    }
    out
}

/// The inverse of [`b64_encode`]. This parses attacker-supplied cookies, so
/// every rejection returns `None` rather than indexing, unwrapping, or
/// otherwise trusting the input's shape.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    fn char_value(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a') as u32 + 26),
            b'0'..=b'9' => Some((c - b'0') as u32 + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    // A single leftover character carries only 6 bits, never enough to form
    // a byte, so it cannot be the tail of a valid encoding.
    if bytes.len() % 4 == 1 {
        return None;
    }

    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for &c in bytes {
        let v = char_value(c)?;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
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

    #[test]
    fn a_token_from_one_node_validates_on_another_sharing_its_key_material() {
        let now = 1_000_000i64;
        let a = Auth::with_signing_key(Some("hunter2".into()), b"shared-peer-token");
        let b = Auth::with_signing_key(Some("hunter2".into()), b"shared-peer-token");
        let token = a.login("hunter2", IP, now).unwrap();
        assert!(
            b.validate(&token, now),
            "same key material must validate across nodes"
        );

        // Different material must not cross-validate: that would make the
        // "derive from peer_token" scheme worthless as an access boundary.
        let c = Auth::with_signing_key(Some("hunter2".into()), b"different-peer-token");
        assert!(!c.validate(&token, now));
    }

    #[test]
    fn tampered_and_malformed_tokens_are_rejected_not_panicked_on() {
        let a = auth();
        let now = 1_000_000i64;
        let token = a.login("hunter2", IP, now).unwrap();
        assert!(a.validate(&token, now), "sanity: the real token is valid");

        let (payload_part, sig_part) = token.split_once('.').unwrap();

        // Flip a byte in the payload half.
        let mut bad_payload = payload_part.as_bytes().to_vec();
        bad_payload[0] ^= 0x01;
        let tampered_payload = format!("{}.{}", String::from_utf8_lossy(&bad_payload), sig_part);
        assert!(!a.validate(&tampered_payload, now));

        // Flip a byte in the signature half.
        let mut bad_sig = sig_part.as_bytes().to_vec();
        bad_sig[0] ^= 0x01;
        let tampered_sig = format!("{}.{}", payload_part, String::from_utf8_lossy(&bad_sig));
        assert!(!a.validate(&tampered_sig, now));

        // No separator at all.
        assert!(!a.validate("notoken", now));
        // Empty token.
        assert!(!a.validate("", now));
        // Valid base64 but not valid JSON underneath.
        let not_json = b64_encode(b"not json at all");
        let bogus = format!("{}.{}", not_json, sig_part);
        assert!(!a.validate(&bogus, now));
    }

    #[test]
    fn an_expired_token_is_rejected_even_with_a_valid_signature() {
        let a = auth();
        let now = 1_000_000i64;
        let token = a.login("hunter2", IP, now).unwrap();
        assert!(a.validate(&token, now + SESSION_TTL_MS - 1));
        assert!(!a.validate(&token, now + SESSION_TTL_MS + 1));
    }

    #[test]
    fn logout_revokes_locally_and_revoke_applies_a_peers_logout() {
        let now = 1_000_000i64;
        let a = Auth::with_signing_key(Some("hunter2".into()), b"shared-peer-token");
        let b = Auth::with_signing_key(Some("hunter2".into()), b"shared-peer-token");
        let token = a.login("hunter2", IP, now).unwrap();
        assert!(b.validate(&token, now), "sanity: token is valid on b too");

        a.logout(&token);
        assert!(
            !a.validate(&token, now),
            "logout revokes on the node that saw it"
        );
        // b never saw the logout, so the token is still valid there until the
        // integrator forwards the revocation.
        assert!(b.validate(&token, now));

        for (jti, expires_at) in a.revocations() {
            b.revoke(&jti, expires_at);
        }
        assert!(
            !b.validate(&token, now),
            "forwarding the revocation revokes it on the other node too"
        );
    }
}
