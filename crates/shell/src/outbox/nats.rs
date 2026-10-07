// SPDX-License-Identifier: MPL-2.0

//! NATS `JetStream`: the runtime [`Publisher`] and saga [`Consumer`].
//!
//! Both halves speak the same design D11 identity:
//!
//! * the publisher sets `Nats-Msg-Id` from [`OutboxMessage::message_id`], so a
//!   re-published message is absorbed by the broker's dedup window;
//! * the consumer rebuilds a [`SagaMessage`] from the subject, that header and
//!   the payload, and **peeks** (a message stays pending until it is acked, so a
//!   handler failure redelivers instead of dropping).
//!
//! The consumer uses explicit acks with a durable pull consumer. A message it
//! cannot decode is acknowledged and counted rather than retried forever: a
//! poison message must not block the cursor for every other saga (the runner's
//! own `Retry::Fatal` path covers messages that decode but cannot be handled).

use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_nats::header::HeaderMap;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::DeliverPolicy;
use loomery_core::envelope::Event;
use tokio_stream::StreamExt;

use super::OutboxMessage;
use super::PublishError;
use super::Publisher;
use super::log_index_for;
use super::split_subject;
use crate::config::NatsConfig;
use crate::saga::ConsumeError;
use crate::saga::Consumer;
use crate::saga::SagaMessage;

/// The `JetStream` stream every Loomery outbox publishes into (D11).
pub const STREAM: &str = "LOOMERY_OUTBOX";

/// The subjects the stream captures.
pub const SUBJECTS: &str = "loomery.>";

/// How long one pull request waits for a message before returning empty.
///
/// The saga runner calls `next` in a loop, so a short wait keeps shutdown
/// responsive without spinning on the broker.
pub const FETCH_EXPIRY: Duration = Duration::from_millis(500);

/// The stream configuration a [`NatsConfig`] asks for.
fn stream_config(config: &NatsConfig) -> jetstream::stream::Config {
    jetstream::stream::Config {
        name: config.stream.clone(),
        subjects: vec![config.subjects.clone()],
        duplicate_window: Duration::from_secs(config.duplicate_window_secs),
        ..Default::default()
    }
}

/// The consumer configuration the runtime uses for the saga stream.
fn consumer_config(config: &NatsConfig) -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        durable_name: Some(config.durable.clone()),
        deliver_policy: if config.deliver_all {
            // Replay what the stream already holds: a host that was down while an
            // event was published must still run its saga (handlers are
            // replay-safe by derived identity, D12).
            DeliverPolicy::All
        } else {
            DeliverPolicy::New
        },
        ack_policy: AckPolicy::Explicit,
        ack_wait: Duration::from_millis(config.ack_wait_ms),
        filter_subject: config
            .filter_subject
            .clone()
            .unwrap_or_else(|| config.subjects.clone()),
        ..Default::default()
    }
}

/// Publishes outbox messages to a `JetStream` stream.
pub struct NatsPublisher {
    context: jetstream::Context,
    stream: String,
}

impl NatsPublisher {
    /// Connects to `url` and ensures the default stream exists.
    ///
    /// # Errors
    ///
    /// Connection or stream-creation failures.
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        Self::connect_config(&NatsConfig {
            url: url.to_owned(),
            ..NatsConfig::default()
        })
        .await
    }

    /// Connects and ensures the stream the configuration names.
    ///
    /// # Errors
    ///
    /// Connection or stream-creation failures.
    pub async fn connect_config(config: &NatsConfig) -> anyhow::Result<Self> {
        let client = async_nats::connect(&config.url).await?;
        let context = jetstream::new(client);

        context.get_or_create_stream(stream_config(config)).await?;

        Ok(Self {
            context,
            stream: config.stream.clone(),
        })
    }

    /// How many messages the stream currently stores.
    ///
    /// Used by the integration suite and the stress harness to observe the dedup
    /// window.
    ///
    /// # Errors
    ///
    /// The stream could not be read.
    pub async fn stored_messages(&self) -> anyhow::Result<u64> {
        let mut stream = self.context.get_stream(&self.stream).await?;
        Ok(stream.info().await?.state.messages)
    }

    /// How many messages the broker holds for a subject filter.
    ///
    /// Every group publishes into the *same* stream (D11), so a stream-wide count
    /// also moves when another tailer publishes at the same moment. A caller
    /// asserting a delta around its own subject asks for that subject instead.
    ///
    /// # Errors
    ///
    /// The stream could not be read.
    pub async fn stored_messages_for(&self, subject_filter: &str) -> anyhow::Result<u64> {
        use tokio_stream::StreamExt;

        let stream = self.context.get_stream(&self.stream).await?;
        let mut subjects = stream.info_with_subjects(subject_filter).await?;

        let mut total: u64 = 0;
        while let Some(page) = subjects.next().await {
            let (_subject, count) = page?;
            total = total.saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        }
        Ok(total)
    }

    /// The broker's duplicate window for the stream.
    ///
    /// The outbox relies on it (D11): a re-published message is only absorbed
    /// while it is inside this window. The outbox's persisted cursor is what
    /// makes a restart correct beyond it.
    ///
    /// # Errors
    ///
    /// The stream could not be read.
    pub async fn duplicate_window(&self) -> anyhow::Result<Duration> {
        let mut stream = self.context.get_stream(&self.stream).await?;
        Ok(stream.info().await?.config.duplicate_window)
    }
}

impl Publisher for NatsPublisher {
    fn publish(
        &self,
        message: OutboxMessage,
    ) -> impl std::future::Future<Output = Result<(), PublishError>> + Send {
        let context = self.context.clone();

        async move {
            let mut headers = HeaderMap::new();
            headers.insert(NATS_MESSAGE_ID, message.message_id.as_str());

            context
                .publish_with_headers(message.subject, headers, message.payload.into())
                .await
                .map_err(|error| PublishError::Unavailable(anyhow::Error::new(error)))?
                .await
                .map_err(|error| PublishError::Unavailable(anyhow::Error::new(error)))?;

            Ok(())
        }
    }
}

/// A message that was fetched but not yet acknowledged.
struct Pending {
    message: jetstream::Message,
    saga: SagaMessage,
}

/// Consumes published events for the saga runner.
///
/// `next` returns the same message until it is acked (a peek, matching the saga
/// runner's contract), so a handler that fails with [`Retry::Retryable`] sees the
/// message again instead of losing it.
///
/// [`Retry::Retryable`]: crate::saga::Retry::Retryable
pub struct NatsConsumer {
    consumer: jetstream::consumer::PullConsumer,
    pending: Mutex<Option<Pending>>,
    dropped: AtomicU64,
}

impl NatsConsumer {
    /// Connects, ensures the stream, and creates or adopts the durable consumer.
    ///
    /// # Errors
    ///
    /// Connection, stream or consumer creation failures.
    pub async fn connect(config: &NatsConfig) -> anyhow::Result<Self> {
        let client = async_nats::connect(&config.url).await?;
        let context = jetstream::new(client);
        let stream = context.get_or_create_stream(stream_config(config)).await?;
        let consumer = stream
            .get_or_create_consumer(&config.durable, consumer_config(config))
            .await?;

        Ok(Self {
            consumer,
            pending: Mutex::new(None),
            dropped: AtomicU64::new(0),
        })
    }

    /// How many undecodable messages were acknowledged and skipped.
    ///
    /// A non-zero value means something other than the outbox published to the
    /// event stream; the host reports it so the cause can be found.
    #[must_use]
    pub fn dropped_messages(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The message waiting for an ack, if any.
    fn pending(&self) -> Option<SagaMessage> {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|pending| pending.saga.clone())
    }

    /// Puts a message that failed to ack back, so a later attempt can retry it.
    fn restore(&self, pending: Pending) {
        *self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(pending);
    }

    /// Rebuilds a saga message from a broker message.
    fn decode(message: &jetstream::Message) -> Option<SagaMessage> {
        let subject = message.subject.as_str().to_owned();
        let (group_id, _event_type) = split_subject(&subject)?;
        let message_id = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get(NATS_MESSAGE_ID))
            .map_or_else(|| subject.clone(), |value| value.as_str().to_owned());
        let event: Event = serde_json::from_slice(&message.payload).ok()?;
        let log_index = log_index_for(&group_id, &message_id).unwrap_or(0);

        Some(SagaMessage {
            group_id,
            subject,
            message_id,
            log_index,
            event,
        })
    }

    /// The next unacked message, fetching one when nothing is pending.
    async fn fetch_next(&self) -> Result<Option<SagaMessage>, ConsumeError> {
        if let Some(pending) = self.pending() {
            return Ok(Some(pending));
        }

        let mut messages = self
            .consumer
            .fetch()
            .max_messages(1)
            .expires(FETCH_EXPIRY)
            .messages()
            .await
            .map_err(|error| ConsumeError::Unavailable(anyhow::Error::new(error)))?;

        let message = match messages.next().await {
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                return Err(ConsumeError::Unavailable(anyhow::Error::from_boxed(error)));
            }
            None => return Ok(None),
        };

        let Some(saga) = Self::decode(&message) else {
            // A poison message is acked and counted: blocking the cursor would
            // stop every other saga behind it.
            let _ = message.ack().await;
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };

        if let Ok(mut pending) = self.pending.lock() {
            *pending = Some(Pending {
                message,
                saga: saga.clone(),
            });
        }

        Ok(Some(saga))
    }
}

impl Consumer for NatsConsumer {
    fn next(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<SagaMessage>, ConsumeError>> + Send {
        self.fetch_next()
    }

    fn ack(
        &self,
        message: &SagaMessage,
    ) -> impl std::future::Future<Output = Result<(), ConsumeError>> + Send {
        let expected = message.message_id.clone();

        async move {
            let pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();

            let Some(pending) = pending else {
                // Nothing is pending: acking again is a no-op, which keeps the
                // runner's retry idempotent.
                return Ok(());
            };

            if pending.saga.message_id != expected {
                let held = pending.saga.message_id.clone();
                self.restore(pending);
                return Err(ConsumeError::Unavailable(anyhow::anyhow!(
                    "acked {expected} while {held} is pending"
                )));
            }

            if let Err(error) = pending.message.ack().await {
                self.restore(pending);
                return Err(ConsumeError::Unavailable(anyhow::Error::from_boxed(error)));
            }

            Ok(())
        }
    }
}
