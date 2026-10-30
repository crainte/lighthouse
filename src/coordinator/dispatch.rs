use super::*;
use futures_util::{stream, StreamExt};

/// Test-only control over notification dispatch. Holding this guard prevents a
/// queued wake-up from scanning the durable notification outbox.
#[doc(hidden)]
pub struct NotificationDispatchGuard<'a> {
    coordinator: &'a Coordinator,
    _guard: tokio::sync::MutexGuard<'a, ()>,
}

impl NotificationDispatchGuard<'_> {
    /// Scan the outbox while its normal dispatcher is paused.
    #[doc(hidden)]
    pub async fn retry_due(&self, now_millis: i64) -> usize {
        self.coordinator
            .retry_due_unlocked(now_millis, &reqwest::Client::new())
            .await
    }
}

/// How often the retry job looks for due notifications. Also the finest
/// backoff granularity there is, which is why the first retry waits exactly
/// this long.
const RETRY_TICK: std::time::Duration = std::time::Duration::from_secs(30);
/// Attempts per notification, the first one included: 30s, 1m, 2m, 4m, 8m,
/// 16m — a bit over half an hour of patience. Long enough to ride out a
/// deploy or a rate limit, short enough that the queue does not become a
/// place where notifications go to be forgotten.
const MAX_DELIVERY_ATTEMPTS: i32 = 6;
/// Retried per tick. A cap so one wedged notifier cannot hold the job for
/// minutes; the rest are simply still due on the next tick.
const RETRY_BATCH: usize = 100;

/// Exponential backoff on the number of failures so far. ponytail: no jitter —
/// one coordinator retrying its own queue has nothing to thunder against, and
/// jitter is the fix when many senders share a target.
fn backoff_ms(attempts: i32) -> i64 {
    let steps = attempts.clamp(1, MAX_DELIVERY_ATTEMPTS) - 1;
    RETRY_TICK.as_millis() as i64 * (1 << steps)
}

impl Coordinator {
    pub(in crate::coordinator) fn spawn_dispatcher(&self, mut rx: tokio::sync::mpsc::Receiver<()>) {
        // The dispatcher must not own the sender it is waiting on: Inner owns
        // notify_tx, so retaining a Coordinator here would keep the channel
        // open forever after the last external handle is dropped.
        let inner = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            // A wedged peer must not pin this sweep (or the periodic task that
            // owns it) indefinitely. Every digest and backfill request shares the
            // bound, while independent later sweeps can proceed normally.
            let client = match reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
            {
                Ok(client) => client,
                Err(error) => {
                    tracing::warn!(%error, "anti-entropy: failed to build HTTP client");
                    return;
                }
            };
            while rx.recv().await.is_some() {
                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let coord = Coordinator { inner };
                // `retry_due` scans the durable outbox and bounds concurrent
                // sends, so one slow backend cannot head-of-line block others.
                coord
                    .retry_due_with_client(chrono::Utc::now().timestamp_millis(), &client)
                    .await;
                drop(coord);
            }
        });
    }

    #[doc(hidden)]
    pub async fn pause_notification_dispatch_for_test(&self) -> NotificationDispatchGuard<'_> {
        NotificationDispatchGuard {
            coordinator: self,
            _guard: self.inner.notification_dispatch.lock().await,
        }
    }

    /// Fill the lossy wake-up hint. This deliberately does not touch the
    /// durable database outbox, letting integration tests exercise saturation.
    #[doc(hidden)]
    pub fn saturate_notification_wake_for_test(&self) {
        for _ in 0..=1024 {
            let _ = self.inner.notify_tx.try_send(());
        }
    }

    /// Retry every queued notification that is due, returning how many were
    /// delivered.
    ///
    /// `now_millis` is a parameter rather than read inside, exactly as `prune`
    /// takes one: a test must be able to reach the next attempt without waiting
    /// out the backoff.
    pub async fn retry_due(&self, now_millis: i64) -> usize {
        self.retry_due_with_client(now_millis, &reqwest::Client::new())
            .await
    }

    async fn retry_due_with_client(&self, now_millis: i64, client: &reqwest::Client) -> usize {
        let _dispatch = self.inner.notification_dispatch.lock().await;
        self.retry_due_unlocked(now_millis, client).await
    }

    async fn retry_due_unlocked(&self, now_millis: i64, client: &reqwest::Client) -> usize {
        let due = match self
            .db(move |storage| storage.due_notifications(now_millis, RETRY_BATCH))
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(error = %e, "failed to read the notification queue");
                return 0;
            }
        };
        if due.is_empty() {
            return 0;
        }
        let delivered = stream::iter(due)
            .map(|row| async move { self.deliver_queued(client, row, now_millis).await })
            .buffer_unordered(16)
            .filter_map(|delivered| async move { delivered.then_some(()) })
            .count()
            .await;
        delivered
    }

    async fn deliver_queued(
        &self,
        client: &reqwest::Client,
        row: storage::QueuedRow,
        now_millis: i64,
    ) -> bool {
        // Resolved by name against the live list, so fixing a wrong URL in
        // the admin page fixes the queued notifications too. A notifier that
        // has been deleted drops them: there is nobody to page, and a row
        // that can never succeed is not a retry, it is a leak.
        let notifier = self
            .inner
            .notifiers
            .read()
            .iter()
            .find(|n| n.name == row.notifier)
            .cloned();
        let Some(notifier) = notifier else {
            warn!(
                check = %row.check_id,
                notifier = %row.notifier,
                "dropping a queued notification: its notifier no longer exists"
            );
            self.drop_queued(&row).await;
            return false;
        };
        let t = alert::Transition {
            check_id: row.check_id,
            from: row.previous_state.unwrap_or(NodeState::Unknown),
            to: row.current_state,
        };
        let name = self.check_name(t.check_id);
        match crate::notify::send(client, &notifier, &t, &name).await {
            Ok(()) => {
                info!(
                    check = %row.check_id,
                    notifier = %row.notifier,
                    attempts = row.attempts + 1,
                    "queued notification delivered"
                );
                self.drop_queued(&row).await;
                return true;
            }
            Err(e) => {
                let attempts = row.attempts + 1;
                // Out of attempts: keep the row with no next attempt, which
                // is what the admin panel reads as "nobody was paged".
                let next =
                    (attempts < MAX_DELIVERY_ATTEMPTS).then(|| now_millis + backoff_ms(attempts));
                if next.is_none() {
                    self.inner
                        .notification_giveups
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        check = %row.check_id,
                        notifier = %row.notifier,
                        attempts,
                        error = %e,
                        "giving up on a notification; nobody was paged for this transition"
                    );
                } else {
                    self.inner
                        .notification_retries
                        .fetch_add(1, Ordering::Relaxed);
                }
                let next_row = storage::QueuedRow {
                    attempts,
                    next_attempt: next,
                    last_error: e,
                    ..row
                };
                if let Err(e) = self
                    .db(move |storage| storage.enqueue_notification(&next_row))
                    .await
                {
                    tracing::error!(error = %e, "failed to reschedule a notification");
                }
            }
        }
        false
    }

    async fn drop_queued(&self, row: &storage::QueuedRow) {
        let id = row.id.clone();
        if let Err(e) = self
            .db(move |storage| storage.delete_notification(&id))
            .await
        {
            tracing::error!(error = %e, "failed to clear a delivered notification");
        }
    }

    /// Start the retry job: every `RETRY_TICK`, attempt whatever is due.
    ///
    /// Spawned from `serve`, like retention, because it is a background job of
    /// the running server rather than part of building a coordinator — a test
    /// drives `retry_due` directly instead of waiting on a clock.
    pub fn spawn_retries(&self) {
        let coord = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(RETRY_TICK);
            loop {
                tick.tick().await;
                coord.retry_due(chrono::Utc::now().timestamp_millis()).await;
            }
        });
    }
}
