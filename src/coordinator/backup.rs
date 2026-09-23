//! Offline backup and restore for the coordinator's DuckDB file. See
//! SCALING.md's "Checkpointed DuckDB backup, restore, and peer recovery"
//! section for the operational contract this implements.
//!
//! Both directions run as a plain CLI invocation against the database file,
//! not as an admin HTTP route: DuckDB allows exactly one process to hold a
//! database open, so a backup taken here either gets an exclusive,
//! checkpointable view of the file or fails immediately because the
//! coordinator still holds it — there is no in-process "pause writes" toggle
//! to build or bypass, because DuckDB's own file lock already is one. A
//! multi-gigabyte database also has no business streaming through an admin
//! session; a local file copy on the machine that holds the data is the
//! right shape for an operator action this size.

use super::storage::Storage;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// What SCALING.md's backup section asks a generation to record: enough to
/// choose the right snapshot without opening it, and to prove the bytes that
/// arrived at the destination are the bytes that were written here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub warden_version: String,
    pub git_sha: String,
    pub created_at_millis: i64,
    pub source_node: String,
    pub source_path: String,
    pub database_bytes: u64,
    pub sha256: String,
}

fn manifest_path_for(destination: &Path) -> PathBuf {
    let mut name = destination.as_os_str().to_owned();
    name.push(".manifest.json");
    PathBuf::from(name)
}

fn sha256_file(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file =
        std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 1 << 20];
    loop {
        let n = file
            .read(&mut buf)
            .with_context(|| format!("reading {} to checksum it", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(crate::auth::hex(&hasher.finalize()))
}

/// Copy the WAL sidecar alongside a database file, if one exists. A clean
/// checkpoint leaves nothing meaningful in it, but copying it when present
/// costs little and means a copy taken a moment too early — a write landed
/// between the checkpoint and the copy — still carries whatever DuckDB
/// considers necessary to reopen cleanly, rather than silently losing it.
fn copy_wal_sidecar(source: &Path, destination: &Path) -> Result<()> {
    let wal_source = PathBuf::from(format!("{}.wal", source.display()));
    if wal_source.exists() {
        let wal_destination = format!("{}.wal", destination.display());
        std::fs::copy(&wal_source, &wal_destination)
            .with_context(|| format!("copying {} to {wal_destination}", wal_source.display()))?;
    }
    Ok(())
}

/// Checkpoint the source database, copy it to `destination`, and record a
/// manifest beside it.
///
/// Refuses to overwrite an existing destination: a backup generation is
/// immutable once written, and silently clobbering yesterday's verified copy
/// defeats the retention tiers (7 daily/4 weekly/12 monthly) this exists to
/// serve. Opens the source path directly with a fresh `Storage` handle
/// rather than going through a running coordinator's database worker,
/// because the whole point is a snapshot taken while nothing else is
/// concurrently writing to it.
pub fn create(source_path: &str, destination: &Path) -> Result<BackupManifest> {
    if destination.exists() {
        bail!(
            "backup destination {} already exists; choose a new path per generation",
            destination.display()
        );
    }
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating backup directory {}", parent.display()))?;
    }

    let store =
        Storage::open(source_path).with_context(|| format!("opening {source_path} for backup"))?;
    // The node id is generated and persisted on first boot when not
    // configured explicitly, so read whatever is actually stored rather than
    // assuming an operator set one.
    let source_node = store
        .get_setting(super::NODE_ID_KEY)
        .ok()
        .flatten()
        .unwrap_or_else(|| "unknown".to_string());
    store.checkpoint().context("checkpointing before backup")?;
    drop(store);

    let source = Path::new(source_path);
    std::fs::copy(source, destination)
        .with_context(|| format!("copying {source_path} to {}", destination.display()))?;
    copy_wal_sidecar(source, destination)?;

    // Prove the round trip before calling it a backup: open the copy and
    // read from it, not merely stat the file that came out the other end.
    let restored = Storage::open(
        destination
            .to_str()
            .context("backup destination path is not valid UTF-8")?,
    )
    .with_context(|| format!("verifying {} opens cleanly", destination.display()))?;
    restored
        .count_results()
        .context("verifying the backup is queryable")?;
    drop(restored);

    let database_bytes = std::fs::metadata(destination)
        .with_context(|| format!("statting {}", destination.display()))?
        .len();
    let sha256 = sha256_file(destination)?;
    let build = crate::version::current();
    let manifest = BackupManifest {
        warden_version: build.version.clone(),
        git_sha: build.git_sha.clone(),
        created_at_millis: chrono::Utc::now().timestamp_millis(),
        source_node,
        source_path: source_path.to_string(),
        database_bytes,
        sha256,
    };
    let manifest_path = manifest_path_for(destination);
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).context("serializing backup manifest")?,
    )
    .with_context(|| format!("writing manifest {}", manifest_path.display()))?;
    Ok(manifest)
}

/// Everything SCALING.md's restore checklist asks for that a standalone
/// process can verify without cluster context: integrity/openability, backup
/// checksum, and enough of a data read to say the copy is usable rather than
/// merely present. Replication cursor/manifest position and current-state
/// convergence against peers are cluster-level checks the operator makes
/// after this restored database rejoins — this report is the local half.
#[derive(Debug, Clone)]
pub struct RestoreReport {
    pub checksum_verified: bool,
    pub raw_results: u64,
    pub current_states: u64,
    pub oldest_result_ts: Option<i64>,
}

/// Copy a backup into a fresh destination and open it there — never over a
/// live database, per SCALING.md — then read enough of it to prove the
/// restore is real rather than a successful `cp`.
pub fn restore(backup_path: &Path, destination: &Path) -> Result<RestoreReport> {
    if destination.exists() {
        bail!(
            "restore destination {} already exists; restore into a fresh path or \
             replacement node, never over a live database",
            destination.display()
        );
    }
    if !backup_path.exists() {
        bail!("backup {} does not exist", backup_path.display());
    }

    let manifest_path = manifest_path_for(backup_path);
    let checksum_verified = if manifest_path.exists() {
        let manifest: BackupManifest = serde_json::from_str(
            &std::fs::read_to_string(&manifest_path)
                .with_context(|| format!("reading {}", manifest_path.display()))?,
        )
        .with_context(|| format!("parsing {}", manifest_path.display()))?;
        let actual = sha256_file(backup_path)?;
        if actual != manifest.sha256 {
            bail!(
                "backup {} does not match its manifest checksum \
                 (expected {}, got {actual}); do not trust this copy",
                backup_path.display(),
                manifest.sha256
            );
        }
        true
    } else {
        tracing::warn!(
            path = %manifest_path.display(),
            "no manifest found beside backup; restoring without checksum verification"
        );
        false
    };

    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating restore directory {}", parent.display()))?;
    }
    std::fs::copy(backup_path, destination).with_context(|| {
        format!(
            "copying {} to {}",
            backup_path.display(),
            destination.display()
        )
    })?;
    copy_wal_sidecar(backup_path, destination)?;

    let restored = Storage::open(
        destination
            .to_str()
            .context("restore destination path is not valid UTF-8")?,
    )
    .with_context(|| format!("opening restored database at {}", destination.display()))?;
    let raw_results = restored
        .count_results()
        .context("counting raw results in restored database")?;
    let current_states = restored
        .list_current_state()
        .context("reading current state in restored database")?
        .len() as u64;
    let oldest_result_ts = restored
        .oldest_result_ts()
        .context("reading oldest retained result in restored database")?;
    drop(restored);

    Ok(RestoreReport {
        checksum_verified,
        raw_results,
        current_states,
        oldest_result_ts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::NODE_ID_KEY;
    use crate::models::{CheckResult, Status};
    use chrono::DateTime;

    fn temp_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "warden-backup-{name}-{}.duckdb",
            uuid::Uuid::now_v7()
        ))
    }

    fn test_result() -> CheckResult {
        CheckResult {
            zone: "test-zone".into(),
            region: "test-region".into(),
            body: None,
            headers: None,
            check_id: crate::models::new_check_id(),
            agent_id: "agent".into(),
            kind: crate::models::CheckKind::Http,
            timestamp: DateTime::from_timestamp_millis(1_000).unwrap(),
            latency_ms: 42,
            status_code: Some(200),
            body_matched: None,
            error: None,
            failure: None,
            metric: None,
            metrics: None,
        }
    }

    #[test]
    fn backup_then_restore_preserves_rows_and_verifies_checksum() {
        let source_path = temp_db_path("source");
        let backup_path = temp_db_path("backup");
        let restore_path = temp_db_path("restore");

        {
            let store = Storage::open(source_path.to_str().unwrap()).unwrap();
            store.put_setting(NODE_ID_KEY, "node-a").unwrap();
            let row = test_result();
            let check_id = row.check_id;
            store.insert_results(&[(row, Status::Up)]).unwrap();
            store
                .update_current_state(check_id, crate::models::NodeState::Up, None, 1_000)
                .unwrap();
        }

        let manifest = create(source_path.to_str().unwrap(), &backup_path).unwrap();
        assert_eq!(manifest.source_node, "node-a");
        assert_eq!(
            manifest.database_bytes,
            std::fs::metadata(&backup_path).unwrap().len()
        );

        let report = restore(&backup_path, &restore_path).unwrap();
        assert!(report.checksum_verified);
        assert_eq!(report.raw_results, 1);
        assert_eq!(report.current_states, 1);
        assert_eq!(report.oldest_result_ts, Some(1_000));

        // The restored copy is independently queryable, not merely counted
        // through the helper above.
        let restored = Storage::open(restore_path.to_str().unwrap()).unwrap();
        assert_eq!(restored.count_results().unwrap(), 1);
        drop(restored);

        for path in [&source_path, &backup_path, &restore_path] {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_file(manifest_path_for(&backup_path));
    }

    #[test]
    fn restore_rejects_a_tampered_backup() {
        let source_path = temp_db_path("tamper-source");
        let backup_path = temp_db_path("tamper-backup");
        let restore_path = temp_db_path("tamper-restore");
        {
            Storage::open(source_path.to_str().unwrap()).unwrap();
        }
        create(source_path.to_str().unwrap(), &backup_path).unwrap();

        // Corrupt the backup after the manifest was written for it.
        let mut bytes = std::fs::read(&backup_path).unwrap();
        bytes.push(0);
        std::fs::write(&backup_path, bytes).unwrap();

        let error = restore(&backup_path, &restore_path).unwrap_err();
        assert!(error.to_string().contains("checksum"));
        assert!(!restore_path.exists());

        for path in [&source_path, &backup_path] {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_file(manifest_path_for(&backup_path));
    }

    #[test]
    fn restore_refuses_to_overwrite_an_existing_destination() {
        let source_path = temp_db_path("live-source");
        let backup_path = temp_db_path("live-backup");
        {
            Storage::open(source_path.to_str().unwrap()).unwrap();
        }
        create(source_path.to_str().unwrap(), &backup_path).unwrap();

        // Stand in for "a live database already occupies this path".
        let live_path = temp_db_path("live-destination");
        {
            Storage::open(live_path.to_str().unwrap()).unwrap();
        }

        let error = restore(&backup_path, &live_path).unwrap_err();
        assert!(error.to_string().contains("already exists"));

        for path in [&source_path, &backup_path, &live_path] {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_file(manifest_path_for(&backup_path));
    }
}
