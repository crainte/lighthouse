//! Stamps the binary with what it is.
//!
//! A coordinator is upgraded before its fleet, so "which build is this?" is an
//! operational question rather than trivia. An agent that quietly refuses a
//! newly added check kind and a coordinator whose peer speaks an older wire
//! format are both diagnosed by comparing builds — but only if each binary can
//! say what it is without being asked to look at a file on disk.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // Seconds, not a formatted string. Formatting is chrono's job and the
    // crate already depends on it; doing it here would buy a build-dependency
    // to produce a value the runtime has to parse back anyway.
    let built_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=WARDEN_BUILT_AT={built_at}");

    println!("cargo:rustc-env=WARDEN_GIT_SHA={}", git_sha());
    println!("cargo:rerun-if-env-changed=GIT_SHA");
}

/// The commit this binary came from.
///
/// `.dockerignore` excludes `.git`, so the image build has no repository to
/// interrogate and passes the sha in as a build-arg instead. A local `cargo
/// build` is the reverse: no build-arg, but a repository right there. Neither
/// is guaranteed — a source tarball has neither — so this degrades to
/// "unknown" rather than failing a build over provenance.
fn git_sha() -> String {
    if let Ok(sha) = std::env::var("GIT_SHA") {
        let sha = sha.trim().to_string();
        if !sha.is_empty() {
            return sha;
        }
    }

    Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|sha| sha.trim().to_string())
        .filter(|sha| !sha.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}
