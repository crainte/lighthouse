//! Per-zone agent credentials.
//!
//! An agent authenticates with `Authorization: Bearer <id>.<secret>`. Only a
//! hash of the secret is stored, so a leaked database yields no working
//! credentials; the plaintext exists once, in the response to the mint call.
//!
//! **The credential carries the agent's location, and that is the point.**
//! Zone and region used to be whatever the caller said they were, and
//! placement believes them: anyone holding the fleet-wide token could register
//! agents into a zone, have real checks assigned to them, and never probe.
//! Nothing alerts on that, because "no results" reads the same as "not
//! scheduled yet" — the monitoring just goes quiet. Binding location to a
//! minted credential is what makes that impossible.
//!
//! ponytail: one SHA-256, not a password KDF. These are 256-bit random secrets,
//! so there is no low-entropy guess to slow down; argon2 here would cost every
//! ingest request for nothing. Revisit only if credentials ever become
//! human-chosen.

use crate::auth::hex;
use rand::RngCore;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Marks the public half in logs and secret scanners.
const ID_PREFIX: &str = "wac_";
/// Separates the lookup handle from the secret. The handle has to travel in
/// clear so the row can be found before anything is compared.
const SEPARATOR: char = '.';

/// A minted credential as persisted. The secret is not a field: it is returned
/// once by [`mint`] and never reconstructible from this.
#[derive(Debug, Clone)]
pub struct AgentCredential {
    /// Public lookup handle.
    pub id: String,
    pub zone: String,
    pub region: String,
    pub secret_hash: String,
    pub salt: String,
    pub created_at: i64,
    /// Revocation is permanent; the row is kept so an operator can still see
    /// that the credential existed and when it stopped working.
    pub revoked_at: Option<i64>,
}

impl AgentCredential {
    pub fn revoked(&self) -> bool {
        self.revoked_at.is_some()
    }
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    hex(&buf)
}

fn digest(salt: &str, secret: &str) -> String {
    let mut h = Sha256::new();
    h.update(salt.as_bytes());
    h.update(secret.as_bytes());
    hex(&h.finalize())
}

/// Mint a credential for one zone. Returns the record to persist and the
/// single plaintext token to hand the operator.
pub fn mint(zone: &str, region: &str, now: i64) -> (AgentCredential, String) {
    let id = format!("{ID_PREFIX}{}", random_hex(6));
    let secret = random_hex(32);
    let salt = random_hex(16);
    let token = format!("{id}{SEPARATOR}{secret}");
    let record = AgentCredential {
        secret_hash: digest(&salt, &secret),
        salt,
        id,
        zone: zone.to_owned(),
        region: region.to_owned(),
        created_at: now,
        revoked_at: None,
    };
    (record, token)
}

/// Build a credential from a token that already exists.
///
/// Minting invents the secret, which is right when an operator is standing at
/// the UI. It is wrong when a zone's agents must be configured before the
/// coordinator has ever run: then the value has to be one chosen in advance.
/// `None` if the token is not shaped like one of ours.
pub fn provision(token: &str, zone: &str, region: &str, now: i64) -> Option<AgentCredential> {
    let (id, secret) = split(token)?;
    let salt = random_hex(16);
    Some(AgentCredential {
        secret_hash: digest(&salt, secret),
        salt,
        id: id.to_owned(),
        zone: zone.to_owned(),
        region: region.to_owned(),
        created_at: now,
        revoked_at: None,
    })
}

/// Split a presented token into its lookup handle and secret. `None` for
/// anything that is not shaped like one of ours, so a legacy shared token is
/// never mistaken for a malformed credential.
pub fn split(presented: &str) -> Option<(&str, &str)> {
    let (id, secret) = presented.split_once(SEPARATOR)?;
    if !id.starts_with(ID_PREFIX) || secret.is_empty() {
        return None;
    }
    Some((id, secret))
}

/// Whether `secret` is the one this credential was minted with. Constant time:
/// a byte-by-byte early exit would leak the hash prefix to a caller willing to
/// measure, and the handle is public so guesses can be aimed.
pub fn verify(record: &AgentCredential, secret: &str) -> bool {
    if record.revoked() {
        return false;
    }
    let got = digest(&record.salt, secret);
    got.as_bytes().ct_eq(record.secret_hash.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_token_verifies_and_a_wrong_one_does_not() {
        let (rec, token) = mint("eu-west-1a", "eu-west", 1_000);
        let (id, secret) = split(&token).expect("minted token must parse");
        assert_eq!(id, rec.id);
        assert!(verify(&rec, secret));
        assert!(!verify(&rec, "not-the-secret"));
    }

    #[test]
    fn the_secret_is_not_recoverable_from_the_record() {
        let (rec, token) = mint("z", "r", 1);
        let (_, secret) = split(&token).unwrap();
        assert_ne!(rec.secret_hash, secret, "the hash must not be the secret");
        assert!(
            !format!("{rec:?}").contains(secret),
            "Debug must not leak it"
        );
    }

    #[test]
    fn revoking_stops_verification_even_with_the_right_secret() {
        let (mut rec, token) = mint("z", "r", 1);
        let (_, secret) = split(&token).unwrap();
        assert!(verify(&rec, secret));
        rec.revoked_at = Some(2);
        assert!(
            !verify(&rec, secret),
            "a revoked credential must stop working immediately"
        );
    }

    #[test]
    fn two_credentials_never_collide() {
        let (a, ta) = mint("z", "r", 1);
        let (b, tb) = mint("z", "r", 1);
        assert_ne!(a.id, b.id, "ids must be unique");
        assert_ne!(
            a.salt, b.salt,
            "salts must differ so hashes cannot be reused"
        );
        assert_ne!(ta, tb);
        // Same zone, same instant, different secret: a leaked credential must
        // not let anyone derive its siblings.
        let (_, secret_a) = split(&ta).unwrap();
        assert!(!verify(&b, secret_a));
    }

    #[test]
    fn a_provisioned_token_is_the_one_the_operator_chose() {
        let rec = provision("wac_seed.chosen-in-advance", "us-east-1a", "us-east", 5)
            .expect("a well-formed token provisions");
        assert_eq!(rec.id, "wac_seed");
        assert_eq!(rec.zone, "us-east-1a");
        assert!(verify(&rec, "chosen-in-advance"));
        assert!(!verify(&rec, "something-else"));
        assert!(
            provision("not-a-warden-token", "z", "r", 5).is_none(),
            "a malformed token must be refused, not silently reshaped"
        );
    }

    #[test]
    fn a_malformed_token_is_not_mistaken_for_a_credential() {
        assert!(split("agent-shared-secret").is_none());
        assert!(split("no-prefix.abc").is_none());
        assert!(split("wac_abc").is_none(), "missing separator");
        assert!(split("wac_abc.").is_none(), "empty secret");
    }
}
