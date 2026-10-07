// SPDX-License-Identifier: MPL-2.0

//! The outbox tailer: one task per group, from applied events to the broker.
//!
//! [`crate::outbox`] holds the mechanism (identity, cursor, resume-by-flush);
//! this holds the loop a host runs. It wakes on the state machine's applied
//! notification rather than polling for work, and every successful publish is
//! followed by persisting the cursor, so a restart resumes instead of
//! republishing history the broker may no longer deduplicate.
//!
//! Failure policy:
//!
//! * a **broker or disk** failure is retryable — the cursor does not move, the
//!   worker backs off and retries, and the host sees a [`WorkerDetail::Failed`]
//!   report;
//! * a **serialization** failure is fatal — retrying cannot help, so the worker
//!   returns and the host decides (stopping is better than silently skipping a
//!   committed event).

use std::sync::Arc;
use std::time::Duration;

use loomery_core::id::Id;
use tokio::sync::watch;

use super::Cursor;
use super::Outbox;
use super::PublishError;
use super::Publisher;
use super::cursor::CursorStore;
use crate::raft::RaftGroup;

/// How long to wait for the next applied event before looking again.
///
/// The applied watch is the fast path; this interval is the safety net that
/// keeps a missed notification (or a snapshot install) from stalling the tailer.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The first retry delay after a failed publish.
pub const MIN_BACKOFF: Duration = Duration::from_millis(100);

/// The longest retry delay after repeated failures.
pub const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Why one flush attempt did not complete.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The broker or the cursor file failed; retrying from the same cursor is
    /// safe.
    #[error("the outbox could not be flushed")]
    Retryable(#[source] anyhow::Error),
    /// An applied event could not be encoded into a message; retrying cannot
    /// help.
    #[error("an applied event could not be encoded")]
    Fatal(#[source] anyhow::Error),
}

/// What one worker iteration observed.
#[derive(Debug)]
pub struct WorkerEvent {
    /// The group the worker tails.
    pub group_id: String,
    /// What happened.
    pub detail: WorkerDetail,
}

/// Worker progress a host can log.
#[derive(Debug)]
pub enum WorkerDetail {
    /// Events reached the broker and the cursor is now persisted there.
    Published {
        /// How many messages this iteration published.
        messages: usize,
        /// The cursor the outbox is now at.
        cursor: Cursor,
    },
    /// A publish failed; the worker backs off and retries from the same cursor.
    Failed {
        /// The failure, as text (retryable by construction).
        reason: String,
        /// How long the worker waits before trying again.
        backoff: Duration,
    },
}

/// How the host learns what a worker is doing.
///
/// A callback rather than logging: the shell has no logging dependency yet
/// (tracing is Phase 7), and tests assert on the events directly.
pub type Reporter = Arc<dyn Fn(WorkerEvent) + Send + Sync>;

/// Publishes one group's applied events to the broker, resuming from a cursor.
pub struct OutboxWorker<P: Publisher> {
    group_id: String,
    organization_id: Id,
    group: RaftGroup,
    outbox: Outbox<P>,
    cursors: CursorStore,
}

impl<P: Publisher> OutboxWorker<P> {
    /// Builds a worker that starts at the beginning of the group's log.
    #[must_use]
    pub fn new(
        group_id: String,
        organization_id: Id,
        group: RaftGroup,
        publisher: P,
        cursors: CursorStore,
    ) -> Self {
        Self {
            group_id,
            organization_id,
            group,
            outbox: Outbox::new(publisher),
            cursors,
        }
    }

    /// Builds a worker that resumes from the cursor persisted in `cursors`.
    ///
    /// # Errors
    ///
    /// The cursor file exists but is unreadable or malformed.
    pub async fn resuming(
        group_id: String,
        organization_id: Id,
        group: RaftGroup,
        publisher: P,
        cursors: CursorStore,
    ) -> anyhow::Result<Self> {
        let cursor = cursors.load().await?;
        Ok(Self {
            group_id,
            organization_id,
            group,
            outbox: Outbox::resuming(publisher, cursor),
            cursors,
        })
    }

    /// The group this worker tails.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The last position published.
    #[must_use]
    pub fn cursor(&self) -> Cursor {
        self.outbox.cursor()
    }

    /// Publishes everything applied after the cursor, then persists the cursor.
    ///
    /// Returns how many messages reached the broker. A publish failure leaves the
    /// cursor untouched, so calling this again resumes exactly there.
    ///
    /// # Errors
    ///
    /// See [`WorkerError`].
    pub async fn flush_once(&mut self) -> Result<usize, WorkerError> {
        let cursor = self.outbox.cursor();
        let applied = self
            .group
            .state_machine()
            .applied_events_since(&self.organization_id, cursor.log_index)
            .await;

        if applied.is_empty() {
            return Ok(0);
        }

        let published = self
            .outbox
            .flush(&self.group_id, &applied)
            .await
            .map_err(|error| match error {
                PublishError::Unavailable(source) => WorkerError::Retryable(source),
                PublishError::Serialize(source) => WorkerError::Fatal(anyhow::Error::new(source)),
            })?;

        if published > 0 {
            self.cursors
                .store(self.outbox.cursor())
                .await
                .map_err(WorkerError::Retryable)?;
        }

        Ok(published)
    }

    /// Runs until `shutdown` is set, a fatal error occurs, or the applied watch
    /// closes.
    ///
    /// Retryable failures are reported and retried with exponential backoff; the
    /// cursor does not move, so nothing is lost or duplicated.
    ///
    /// # Errors
    ///
    /// The fatal [`WorkerError::Fatal`] cause, which the host must decide on.
    pub async fn run(
        &mut self,
        mut shutdown: watch::Receiver<bool>,
        report: &Reporter,
    ) -> anyhow::Result<()> {
        let mut applied = self.group.state_machine().applied_watch();
        let mut backoff = MIN_BACKOFF;

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            match self.flush_once().await {
                Ok(0) => backoff = MIN_BACKOFF,
                Ok(messages) => {
                    backoff = MIN_BACKOFF;
                    report(WorkerEvent {
                        group_id: self.group_id.clone(),
                        detail: WorkerDetail::Published {
                            messages,
                            cursor: self.cursor(),
                        },
                    });
                }
                Err(WorkerError::Retryable(error)) => {
                    report(WorkerEvent {
                        group_id: self.group_id.clone(),
                        detail: WorkerDetail::Failed {
                            reason: error.to_string(),
                            backoff,
                        },
                    });
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        result = shutdown.changed() => {
                            if result.is_ok() && *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                    }
                    backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                    continue;
                }
                Err(WorkerError::Fatal(error)) => return Err(error),
            }

            // Wait for the next apply, a shutdown, or the safety-net interval.
            tokio::select! {
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                changed = applied.changed() => {
                    if changed.is_err() {
                        // The state machine is gone: nothing left to publish.
                        return Ok(());
                    }
                }
                () = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::GroupOps;
    use std::future::Future;
    use std::sync::Mutex;

    /// Records what it was asked to publish; can fail on demand.
    #[derive(Default)]
    struct FakePublisher {
        messages: Mutex<Vec<super::super::OutboxMessage>>,
        fail: Mutex<bool>,
    }

    impl FakePublisher {
        fn failing() -> Self {
            Self {
                fail: Mutex::new(true),
                ..Self::default()
            }
        }

        fn published(&self) -> usize {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        }
    }

    impl Publisher for Arc<FakePublisher> {
        fn publish(
            &self,
            message: super::super::OutboxMessage,
        ) -> impl Future<Output = Result<(), PublishError>> + Send {
            let record = Arc::clone(self);
            async move {
                if *record
                    .fail
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                {
                    return Err(PublishError::Unavailable(anyhow::anyhow!("broker down")));
                }
                record
                    .messages
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(message);
                Ok(())
            }
        }
    }

    async fn group() -> RaftGroup {
        RaftGroup::boot_single_node(1).await.unwrap()
    }

    fn organization() -> Id {
        Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9")
    }

    fn command(index: usize) -> loomery_core::envelope::Command {
        let label = format!("worker-{index}");
        loomery_core::envelope::Command {
            envelope_version: 1,
            id: Id::new(),
            aggregate_id: Id::from(loomery_core::key::Key::new(
                &loomery_core::Uuid::from_u128(7),
                &format!("task-{label}"),
            )),
            organization_id: organization(),
            workspace_id: None,
            occurred_at: loomery_core::timestamp::Timestamp::now(),
            causation_key: loomery_core::key::Key::new(
                &loomery_core::Uuid::from_u128(7),
                &format!("cause-{label}"),
            ),
            correlation_key: loomery_core::key::Key::new(
                &loomery_core::Uuid::from_u128(7),
                "worker",
            ),
            actor: loomery_core::actor::Actor::System,
            command_type: "task.create".to_owned(),
            payload: loomery_core::envelope::Payload {
                version: 1,
                data: format!(r#"{{"title":"{label}"}}"#),
            },
        }
    }

    #[tokio::test]
    async fn a_worker_publishes_applied_events_and_persists_its_cursor() {
        let mut raft = group().await;
        for index in 0..3 {
            raft.propose(command(index)).await.unwrap();
        }
        let root = tempfile::tempdir().unwrap();
        let cursors = CursorStore::in_group_dir(root.path());
        let publisher = Arc::new(FakePublisher::default());
        let mut worker = OutboxWorker::new(
            "tenant-1".to_owned(),
            organization(),
            raft.clone(),
            Arc::clone(&publisher),
            cursors,
        );

        let delivered = worker.flush_once().await.unwrap();

        assert_eq!(delivered, 3);
        assert_eq!(publisher.published(), 3);
        assert!(worker.cursor().log_index > 0);
        let persisted = CursorStore::in_group_dir(root.path()).load().await.unwrap();
        assert_eq!(persisted, worker.cursor(), "the cursor survives a restart");
    }

    #[tokio::test]
    async fn a_second_flush_publishes_nothing() {
        let mut raft = group().await;
        raft.propose(command(0)).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let publisher = Arc::new(FakePublisher::default());
        let mut worker = OutboxWorker::new(
            "tenant-1".to_owned(),
            organization(),
            raft,
            Arc::clone(&publisher),
            CursorStore::in_group_dir(root.path()),
        );

        assert_eq!(worker.flush_once().await.unwrap(), 1);
        assert_eq!(worker.flush_once().await.unwrap(), 0);
        assert_eq!(publisher.published(), 1);
    }

    #[tokio::test]
    async fn a_restarted_worker_resumes_from_the_persisted_cursor() {
        let mut raft = group().await;
        raft.propose(command(0)).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let publisher = Arc::new(FakePublisher::default());

        let mut first = OutboxWorker::new(
            "tenant-1".to_owned(),
            organization(),
            raft.clone(),
            Arc::clone(&publisher),
            CursorStore::in_group_dir(root.path()),
        );
        assert_eq!(first.flush_once().await.unwrap(), 1);

        // A second event arrives, then the host restarts the worker.
        let mut raft_for_propose = raft.clone();
        raft_for_propose.propose(command(1)).await.unwrap();
        let mut restarted = OutboxWorker::resuming(
            "tenant-1".to_owned(),
            organization(),
            raft,
            Arc::clone(&publisher),
            CursorStore::in_group_dir(root.path()),
        )
        .await
        .unwrap();

        assert_eq!(
            restarted.flush_once().await.unwrap(),
            1,
            "only the event after the cursor is published"
        );
        assert_eq!(publisher.published(), 2);
    }

    #[tokio::test]
    async fn a_retryable_failure_leaves_the_cursor_for_the_next_attempt() {
        let mut raft = group().await;
        raft.propose(command(0)).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let cursors = CursorStore::in_group_dir(root.path());
        let publisher = Arc::new(FakePublisher::failing());
        let mut worker = OutboxWorker::new(
            "tenant-1".to_owned(),
            organization(),
            raft,
            Arc::clone(&publisher),
            cursors,
        );

        let error = worker.flush_once().await.unwrap_err();
        assert!(matches!(error, WorkerError::Retryable(_)), "{error:?}");
        assert_eq!(worker.cursor(), Cursor::start());
        assert_eq!(
            CursorStore::in_group_dir(root.path()).load().await.unwrap(),
            Cursor::start()
        );

        // The broker recovers.
        *publisher
            .fail
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
        assert_eq!(worker.flush_once().await.unwrap(), 1);
        assert_eq!(publisher.published(), 1);
    }

    #[tokio::test]
    async fn run_publishes_events_applied_after_it_started() {
        let raft = group().await;
        let root = tempfile::tempdir().unwrap();
        let publisher = Arc::new(FakePublisher::default());
        let mut worker = OutboxWorker::new(
            "tenant-1".to_owned(),
            organization(),
            raft.clone(),
            Arc::clone(&publisher),
            CursorStore::in_group_dir(root.path()),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink: Reporter = {
            let seen = Arc::clone(&seen);
            Arc::new(move |event: WorkerEvent| {
                seen.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(event.group_id.clone());
            })
        };

        let running = tokio::spawn(async move { worker.run(shutdown_rx, &sink).await });

        // The worker starts with nothing applied; an event now wakes it.
        let mut raft_for_propose = raft.clone();
        raft_for_propose.propose(command(0)).await.unwrap();

        let mut delivered = 0;
        for _ in 0..100 {
            delivered = seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len();
            if delivered > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            delivered, 1,
            "the applied notification woke the tailer and the host was told"
        );
        assert_eq!(publisher.published(), 1);

        shutdown_tx.send(true).unwrap();
        running.await.unwrap().unwrap();
    }
}
