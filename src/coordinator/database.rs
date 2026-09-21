use super::storage::Storage;
use anyhow::{anyhow, Result};
use std::future::Future;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

const QUEUE_CAPACITY: usize = 256;
const TIMING_REPORT_EVERY: u64 = 128;
/// A job holding the single worker this long starves every other reader and
/// writer on this node, so it is worth naming individually rather than only
/// folding into an average.
const SLOW_CALL: std::time::Duration = std::time::Duration::from_millis(250);

type Job = Box<dyn FnOnce(&Storage) + Send + 'static>;

#[derive(Default)]
struct TimingCounter {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl TimingCounter {
    fn observe(&self, operation: &'static str, elapsed: std::time::Duration) {
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.total_us.fetch_add(micros, Ordering::Relaxed);
        self.max_us.fetch_max(micros, Ordering::Relaxed);
        let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(TIMING_REPORT_EVERY) {
            tracing::info!(
                target: "warden_bench",
                component = "database_worker",
                operation,
                samples = count,
                total_us = self.total_us.load(Ordering::Relaxed),
                max_us = self.max_us.load(Ordering::Relaxed),
                "aggregated timing"
            );
        }
    }
}

struct Timings {
    enabled: bool,
    queue: TimingCounter,
    operation: TimingCounter,
}

impl Timings {
    fn enabled() -> bool {
        std::env::var_os("WARDEN_BENCH_TIMINGS").as_deref() == Some(std::ffi::OsStr::new("1"))
            || std::env::var("RUST_LOG").is_ok_and(|filter| filter.contains("warden_bench"))
    }
}

#[derive(Clone)]
pub struct Database {
    tx: mpsc::Sender<Job>,
    timings: Arc<Timings>,
}

impl Database {
    pub async fn open(path: String) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<Job>(QUEUE_CAPACITY);
        let (ready_tx, ready_rx) = oneshot::channel();
        let timings = Arc::new(Timings {
            enabled: Timings::enabled(),
            queue: TimingCounter::default(),
            operation: TimingCounter::default(),
        });
        std::thread::Builder::new()
            .name("warden-database".to_string())
            .spawn(move || {
                let storage = match Storage::open(&path) {
                    Ok(storage) => {
                        let _ = ready_tx.send(Ok(()));
                        storage
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                while let Some(job) = rx.blocking_recv() {
                    job(&storage);
                }
            })?;
        ready_rx
            .await
            .map_err(|_| anyhow!("database worker stopped during startup"))??;
        Ok(Self { tx, timings })
    }

    /// `#[track_caller]` so a slow job names the call site that submitted it:
    /// every job here is an opaque closure on one shared worker thread, so
    /// without the caller's location an aggregate only says "the database was
    /// busy", never which query made it so. Written as a synchronous function
    /// returning a future because `#[track_caller]` does not propagate through
    /// an `async fn` (rust#110011).
    #[track_caller]
    pub fn call<T, F>(&self, operation: F) -> impl Future<Output = Result<T>> + Send + '_
    where
        T: Send + 'static,
        F: FnOnce(&Storage) -> Result<T> + Send + 'static,
    {
        self.call_at(operation, std::panic::Location::caller())
    }

    /// `call` for wrappers: a helper that forwarded to `call` would otherwise
    /// name itself as the caller of every job in the process.
    pub fn call_at<T, F>(
        &self,
        operation: F,
        caller: &'static std::panic::Location<'static>,
    ) -> impl Future<Output = Result<T>> + Send + '_
    where
        T: Send + 'static,
        F: FnOnce(&Storage) -> Result<T> + Send + 'static,
    {
        let timings = Arc::clone(&self.timings);
        async move { self.dispatch(operation, caller, timings).await }
    }

    async fn dispatch<T, F>(
        &self,
        operation: F,
        caller: &'static std::panic::Location<'static>,
        timings: Arc<Timings>,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Storage) -> Result<T> + Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        let queued_at = Instant::now();
        self.tx
            .send(Box::new(move |storage| {
                if timings.enabled {
                    timings.queue.observe("call", queued_at.elapsed());
                }
                let operation_started = Instant::now();
                let result = operation(storage);
                if timings.enabled {
                    let elapsed = operation_started.elapsed();
                    timings.operation.observe("call", elapsed);
                    if elapsed >= SLOW_CALL {
                        tracing::info!(
                            target: "warden_bench",
                            component = "database_worker",
                            operation = "slow_call",
                            caller = %caller,
                            elapsed_us = elapsed.as_micros() as u64,
                            "slow database job"
                        );
                    }
                }
                let _ = result_tx.send(result);
            }))
            .await
            .map_err(|_| anyhow!("database worker is unavailable"))?;
        result_rx
            .await
            .map_err(|_| anyhow!("database worker stopped before completing operation"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn slow_database_work_does_not_block_tokio() {
        let database = Database::open(":memory:".to_string()).await.unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let slow = tokio::spawn({
            let database = database.clone();
            async move {
                database
                    .call(move |_| {
                        started_tx.send(()).unwrap();
                        std::thread::sleep(Duration::from_millis(150));
                        Ok(())
                    })
                    .await
                    .unwrap();
            }
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(
            Duration::from_millis(50),
            tokio::time::sleep(Duration::from_millis(1)),
        )
        .await
        .expect("Tokio timer stalled behind database work");
        slow.await.unwrap();
    }
}
