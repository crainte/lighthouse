use super::storage::Storage;
use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, oneshot};

const QUEUE_CAPACITY: usize = 256;

type Job = Box<dyn FnOnce(&Storage) + Send + 'static>;

#[derive(Clone)]
pub struct Database {
    tx: mpsc::Sender<Job>,
}

impl Database {
    pub async fn open(path: String) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<Job>(QUEUE_CAPACITY);
        let (ready_tx, ready_rx) = oneshot::channel();
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
        Ok(Self { tx })
    }

    pub async fn call<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Storage) -> Result<T> + Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        self.tx
            .send(Box::new(move |storage| {
                let _ = result_tx.send(operation(storage));
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
