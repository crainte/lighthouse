//! A process that exits while the database worker is still running a
//! statement must wait for it, not tear DuckDB's C++ statics down under it.
//!
//! DuckDB 2.0 keeps lazily built singletons (its PEG keyword helper among
//! them). `exit()` destroys those; a worker still executing a statement then
//! calls a virtual method on a half-destroyed object: libstdc++'s `pure
//! virtual method called` (SIGABRT), or a segfault. See `coordinator::database`.
//!
//! The test re-runs this binary as a child so it can be the one that exits.
//! The child registers a slow `atexit` handler *first*, which therefore runs
//! *last*, after every static destructor, to hold the process open long enough
//! for the unguarded race to be certain instead of a one-in-a-hundred event.
#![cfg(unix)]

use lighthouse::coordinator::database::Database;
use lighthouse::coordinator::db_limits::DatabaseLimits;
use std::time::{Duration, Instant};

const CHILD: &str = "LIGHTHOUSE_EXIT_WAIT_CHILD";

extern "C" {
    fn atexit(callback: extern "C" fn()) -> std::os::raw::c_int;
}

extern "C" fn linger_after_static_destructors() {
    std::thread::sleep(Duration::from_millis(1500));
}

/// Not a test of its own: the body the parent runs in a child process. A
/// normal run, without `CHILD` set, does nothing.
#[tokio::test]
async fn exit_wait_child() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    // SAFETY: a valid `extern "C"` function that does not unwind.
    unsafe { atexit(linger_after_static_destructors) };
    let database = Database::open(
        ":memory:".to_string(),
        DatabaseLimits::resolve(&Default::default()).unwrap(),
    )
    .await
    .unwrap();
    let busy = database.clone();
    tokio::spawn(async move {
        let _ = busy
            .call(|storage| {
                let end = Instant::now() + Duration::from_secs(2);
                while Instant::now() < end {
                    let _ = storage.count_results();
                    let _ = storage.list_checks();
                    let _ = storage.recent_alerts(10);
                }
                Ok(())
            })
            .await;
    });
    // Let the job start, then return: the runtime drops and the process begins
    // to exit with the worker mid-statement.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[test]
fn exiting_mid_statement_waits_for_the_database_worker() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "exit_wait_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "the child died at exit ({:?}); stdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert!(
        stderr.contains("waiting for 1 database worker(s) to finish closing before exit"),
        "the exit wait never ran, so this passed by luck; stderr:\n{stderr}"
    );
}
