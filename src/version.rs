//! What this binary is, stamped at compile time by `build.rs`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

/// The identity of a running lighthouse.
///
/// Deserialisable as well as serialisable: this crosses the wire in both
/// directions. An agent reports its build so the coordinator can explain why
/// an assigned check is not running, and a coordinator reports its build so a
/// peer can tell whether a rolling upgrade is half finished.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Build {
    /// Crate version, e.g. `0.1.0`.
    pub version: String,
    /// Short commit, or `unknown` when built outside a repository and without
    /// a `GIT_SHA` build-arg.
    pub git_sha: String,
    /// When it was compiled. The field that actually identifies a stale
    /// deployment: two builds of the same uncommitted tree share a version and
    /// a sha but never a timestamp.
    pub built_at: DateTime<Utc>,
}

impl std::fmt::Display for Build {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}, built {})",
            self.version,
            self.git_sha,
            self.built_at.format("%Y-%m-%d %H:%M:%S UTC")
        )
    }
}

/// This binary's build. Computed once; the inputs are compile-time constants.
pub fn current() -> &'static Build {
    static BUILD: OnceLock<Build> = OnceLock::new();
    BUILD.get_or_init(|| Build {
        version: env!("CARGO_PKG_VERSION").to_string(),
        git_sha: env!("LIGHTHOUSE_GIT_SHA").to_string(),
        // A build stamp that cannot be read is worse than one that is
        // obviously wrong, so an unparseable value becomes the epoch: visibly
        // absurd in the UI rather than absent from it.
        built_at: env!("LIGHTHOUSE_BUILT_AT")
            .parse::<i64>()
            .ok()
            .and_then(|secs| DateTime::from_timestamp(secs, 0))
            .unwrap_or_else(|| DateTime::from_timestamp(0, 0).expect("epoch is a valid timestamp")),
    })
}

#[cfg(test)]
mod tests {
    use super::current;

    #[test]
    fn the_binary_knows_what_it_is() {
        let build = current();
        assert_eq!(build.version, env!("CARGO_PKG_VERSION"));
        assert!(!build.git_sha.is_empty());
        // Not the epoch fallback: build.rs must have stamped a real clock
        // reading, otherwise every deployment looks equally stale.
        assert!(
            build.built_at.timestamp() > 1_700_000_000,
            "build.rs did not stamp a plausible build time: {}",
            build.built_at
        );
    }
}
