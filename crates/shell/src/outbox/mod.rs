// SPDX-License-Identifier: MPL-2.0

//! The outbox: publish committed events exactly once.
//!
//! Nothing in the consensus loop makes network calls (`design.md` principle 6).
//! Instead a tailer reads the state machine's **applied** events and hands them
//! to a [`Publisher`]. Two pieces of identity make at-least-once delivery safe
//! (D8/D11):
//!
//! * the subject is `loomery.events.<group_id>.<event_type>`;
//! * the message id is `<group_id>:<log_index>:e<pos>`, so a re-published
//!   message is absorbed by the broker's dedup window instead of delivered
//!   twice.
//!
//! A [`Cursor`] (`log_index`, `position`) is advanced only after a publish
//! succeeds, so a crash or a broker outage resumes from the last acknowledged
//! message rather than skipping it.

use std::sync::Arc;

pub mod cursor;
#[cfg(feature = "nats")]
pub mod nats;
pub mod worker;
#[cfg(feature = "nats")]
pub use nats::NatsConsumer;
#[cfg(feature = "nats")]
pub use nats::NatsPublisher;
use std::sync::Mutex;

use crate::raft::AppliedEvent;

/// A message ready for the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxMessage {
    /// The publishing subject.
    pub subject: String,
    /// The broker-side dedup id (`<group_id>:<log_index>:e<pos>`).
    pub message_id: String,
    /// The serialized event (the JSON envelope).
    pub payload: Vec<u8>,
}

/// Why a publish failed.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// The broker was unreachable or rejected the message; the caller should
    /// retry from the same cursor.
    #[error("the message could not be published")]
    Unavailable(#[source] anyhow::Error),
    /// The applied event could not be serialized; a retry will not help.
    #[error("the event could not be serialized")]
    Serialize(#[source] serde_json::Error),
}

/// Delivers outbox messages.
///
/// The production implementation is a `NATS` `JetStream` publisher behind an
/// explicit `LOOMERY_NATS_URL`; tests use an in-process fake. The trait is the
/// seam that keeps the default suite self-contained.
pub trait Publisher: Send + Sync {
    /// Publishes one message.
    ///
    /// # Errors
    ///
    /// [`PublishError::Unavailable`] when the message was not accepted; the
    /// outbox keeps its cursor and retries.
    fn publish(
        &self,
        message: OutboxMessage,
    ) -> impl std::future::Future<Output = Result<(), PublishError>> + Send;
}

/// The last position published for a group.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    /// The Raft log index.
    pub log_index: u64,
    /// The event's position within that log entry.
    pub position: usize,
}

impl Cursor {
    /// The cursor before anything has been published.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            log_index: 0,
            position: 0,
        }
    }
}

impl Default for Cursor {
    fn default() -> Self {
        Self::start()
    }
}

/// The prefix of every published subject (D11).
pub const SUBJECT_PREFIX: &str = "loomery.events.";

/// The subject a domain event publishes to (D11).
#[must_use]
pub fn subject_for(group_id: &str, event_type: &str) -> String {
    format!("{SUBJECT_PREFIX}{group_id}.{event_type}")
}

/// The inverse of [`subject_for`]: the `(group_id, event_type)` a subject carries.
///
/// The group id is the first segment after the prefix, because event types
/// contain dots (`task.created`) while group ids must not — [`subject_for`] cannot
/// enforce that, so the rule is stated here and checked where group ids are
/// configured. Returns `None` for anything that is not a Loomery event subject.
#[must_use]
pub fn split_subject(subject: &str) -> Option<(String, String)> {
    let rest = subject.strip_prefix(SUBJECT_PREFIX)?;
    let (group_id, event_type) = rest.split_once('.')?;
    (!group_id.is_empty() && !event_type.is_empty())
        .then(|| (group_id.to_owned(), event_type.to_owned()))
}

/// The log index inside `message_id` (`<group_id>:<log_index>:e<pos>`).
///
/// The group id is required to disambiguate the parse: it is known from the
/// subject, and a message id that does not carry it was not produced by
/// [`message_id`] for that group — so the answer is `None` rather than a guess.
#[must_use]
pub fn log_index_for(group_id: &str, message_id: &str) -> Option<u64> {
    let rest = message_id.strip_prefix(group_id)?.strip_prefix(':')?;
    rest.split_once(':')?.0.parse().ok()
}

/// The broker-side dedup id of one event (D11).
#[must_use]
pub fn message_id(group_id: &str, log_index: u64, position: usize) -> String {
    format!("{group_id}:{log_index}:e{position}")
}

/// Publishes a group's applied events exactly once, resuming from a cursor.
///
/// One tailer per group is expected: [`Outbox::flush`] reads the cursor, awaits
/// the publisher, then advances it, so concurrent flushes of the same outbox are
/// not meaningful.
pub struct Outbox<P: Publisher> {
    publisher: P,
    cursor: Mutex<Cursor>,
}

/// An `Arc` of a publisher is itself a publisher, so a host can share one
/// broker connection between groups.
impl<T: Publisher + ?Sized> Publisher for Arc<T> {
    fn publish(
        &self,
        message: OutboxMessage,
    ) -> impl std::future::Future<Output = Result<(), PublishError>> + Send {
        (**self).publish(message)
    }
}

impl<P: Publisher> Outbox<P> {
    /// Creates an outbox that starts from the beginning of the log.
    #[must_use]
    pub fn new(publisher: P) -> Self {
        Self {
            publisher,
            cursor: Mutex::new(Cursor::start()),
        }
    }

    /// Creates an outbox that resumes from a previously published `cursor`.
    ///
    /// A host loads the cursor from its [`cursor::CursorStore`] on boot, so a
    /// restart republishes nothing that already reached the broker — even if the
    /// broker's dedup window (D11) has since expired.
    #[must_use]
    pub fn resuming(publisher: P, cursor: Cursor) -> Self {
        Self {
            publisher,
            cursor: Mutex::new(cursor),
        }
    }

    /// The last position published.
    #[must_use]
    pub fn cursor(&self) -> Cursor {
        *self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Publishes every applied event after the cursor, advancing it as each
    /// publish succeeds.
    ///
    /// Returns the number of messages published. On the first failure it stops
    /// and returns the error: the cursor already covers everything published, so
    /// calling `flush` again resumes exactly there.
    ///
    /// # Errors
    ///
    /// The publisher's [`PublishError`].
    pub async fn flush(
        &self,
        group_id: &str,
        applied: &[AppliedEvent],
    ) -> Result<usize, PublishError> {
        let mut published: usize = 0;

        for (position, entry) in positions(applied) {
            let next = Cursor {
                log_index: entry.log_index,
                position,
            };

            if next <= self.cursor() {
                continue;
            }

            let message = OutboxMessage {
                subject: subject_for(group_id, &entry.event.event_type),
                message_id: message_id(group_id, entry.log_index, position),
                payload: serde_json::to_vec(&entry.event).map_err(PublishError::Serialize)?,
            };

            self.publisher.publish(message).await?;

            *self
                .cursor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
            published = published.saturating_add(1);
        }

        Ok(published)
    }
}

/// Pairs each applied event with its position within its log entry.
///
/// One command shares its log index with every event it produced; the position
/// distinguishes them, which is what keeps `Nats-Msg-Id` unique per message.
fn positions(applied: &[AppliedEvent]) -> impl Iterator<Item = (usize, &AppliedEvent)> {
    let mut previous_index: Option<u64> = None;
    let mut position: usize = 0;

    applied.iter().map(move |entry| {
        if previous_index == Some(entry.log_index) {
            position = position.saturating_add(1);
        } else {
            previous_index = Some(entry.log_index);
            position = 0;
        }
        (position, entry)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use loomery_core::Uuid;
    use loomery_core::actor::Actor;
    use loomery_core::envelope::Event;
    use loomery_core::envelope::Payload;
    use loomery_core::id::Id;
    use loomery_core::key::Key;
    use loomery_core::timestamp::Timestamp;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    const NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    fn event(id: &str, event_type: &str) -> Event {
        Event {
            envelope_version: 1,
            id: Id::from(id),
            aggregate_id: Id::from("agg-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&NS, id),
            correlation_key: Key::new(&NS, "corr"),
            actor: Actor::System,
            event_type: event_type.to_owned(),
            payload: Payload {
                version: 1,
                data: "{}".to_owned(),
            },
        }
    }

    fn applied() -> Vec<AppliedEvent> {
        vec![
            AppliedEvent {
                log_index: 1,
                event: event("e1", "task.created"),
            },
            AppliedEvent {
                log_index: 2,
                event: event("e2", "task.completed"),
            },
        ]
    }

    /// Records what it was asked to publish; can fail once.
    #[derive(Default)]
    struct RecordingPublisher {
        messages: Mutex<Vec<OutboxMessage>>,
        fail_after: AtomicUsize,
        attempts: AtomicUsize,
        failing: AtomicBool,
    }

    impl RecordingPublisher {
        fn failing_after(n: usize) -> Self {
            Self {
                fail_after: AtomicUsize::new(n),
                failing: AtomicBool::new(true),
                ..Self::default()
            }
        }

        fn published(&self) -> Vec<OutboxMessage> {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl Publisher for RecordingPublisher {
        fn publish(
            &self,
            message: OutboxMessage,
        ) -> impl std::future::Future<Output = Result<(), PublishError>> + Send {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.failing.load(Ordering::SeqCst)
                && attempt >= self.fail_after.load(Ordering::SeqCst)
            {
                return std::future::ready(Err(PublishError::Unavailable(anyhow::anyhow!(
                    "broker down"
                ))));
            }
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(message);
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn each_event_is_published_once_with_its_dedup_identity() {
        let publisher = Arc::new(RecordingPublisher::default());
        let outbox = Outbox::new(publisher.clone());

        let count = outbox.flush("tenant-1", &applied()).await.unwrap();

        assert_eq!(count, 2);
        let delivered = publisher.published();
        assert_eq!(delivered.len(), 2);
        assert_eq!(delivered[0].subject, "loomery.events.tenant-1.task.created");
        assert_eq!(delivered[0].message_id, "tenant-1:1:e0");
        assert_eq!(delivered[1].message_id, "tenant-1:2:e0");
    }

    #[tokio::test]
    async fn a_second_flush_republishes_nothing() {
        let publisher = Arc::new(RecordingPublisher::default());
        let outbox = Outbox::new(publisher.clone());

        outbox.flush("tenant-1", &applied()).await.unwrap();
        let again = outbox.flush("tenant-1", &applied()).await.unwrap();

        assert_eq!(again, 0);
        assert_eq!(publisher.published().len(), 2);
    }

    #[tokio::test]
    async fn a_failed_publish_resumes_from_the_cursor() {
        // Fail the second publish, then recover.
        let publisher = Arc::new(RecordingPublisher::failing_after(1));
        let outbox = Outbox::new(publisher.clone());

        let error = outbox.flush("tenant-1", &applied()).await;
        assert!(error.is_err());
        assert_eq!(publisher.published().len(), 1);
        assert_eq!(
            outbox.cursor(),
            Cursor {
                log_index: 1,
                position: 0
            }
        );

        // The broker recovers: only the unpublished event goes out.
        publisher.failing.store(false, Ordering::SeqCst);
        let resumed = outbox.flush("tenant-1", &applied()).await.unwrap();

        assert_eq!(resumed, 1);
        assert_eq!(publisher.published()[1].message_id, "tenant-1:2:e0");
    }

    #[test]
    fn a_subject_round_trips_through_its_parser() {
        let subject = subject_for("tenant-1", "task.created");
        assert_eq!(subject, "loomery.events.tenant-1.task.created");
        assert_eq!(
            split_subject(&subject),
            Some(("tenant-1".to_owned(), "task.created".to_owned()))
        );
    }

    #[test]
    fn an_event_type_with_dots_is_kept_whole() {
        let subject = subject_for("control", "task.created");
        assert_eq!(
            split_subject(&subject),
            Some(("control".to_owned(), "task.created".to_owned()))
        );
    }

    #[test]
    fn subjects_that_are_not_loomery_events_are_refused() {
        assert_eq!(split_subject("other.tenant-1.task.created"), None);
        assert_eq!(split_subject("loomery.events.tenant-1"), None);
        assert_eq!(split_subject("loomery.events..task.created"), None);
        assert_eq!(split_subject("loomery.events.tenant-1."), None);
    }

    #[test]
    fn the_log_index_is_read_from_the_message_id() {
        assert_eq!(log_index_for("tenant-1", "tenant-1:7:e0"), Some(7));
        assert_eq!(log_index_for("tenant-1", "tenant-1:9:e12"), Some(9));
        assert_eq!(log_index_for("tenant-1", "other:9:e12"), None);
        assert_eq!(log_index_for("tenant-1", "tenant-1"), None);
        assert_eq!(log_index_for("tenant-1", "tenant-1:not-a-number:e0"), None);
        assert_eq!(log_index_for("", ""), None);
    }

    #[test]
    fn events_sharing_a_log_index_get_distinct_positions() {
        let shared = vec![
            AppliedEvent {
                log_index: 7,
                event: event("a", "task.created"),
            },
            AppliedEvent {
                log_index: 7,
                event: event("b", "task.renamed"),
            },
        ];

        let positions: Vec<usize> = positions(&shared).map(|(position, _)| position).collect();
        assert_eq!(positions, [0, 1]);
        assert_ne!(message_id("g", 7, 0), message_id("g", 7, 1));
    }
}
