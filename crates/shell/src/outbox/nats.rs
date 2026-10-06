// SPDX-License-Identifier: MPL-2.0

//! A `NATS` `JetStream` publisher — the `test-services` implementation.
//!
//! Publishes outbox messages into the [`STREAM`] with `Nats-Msg-Id` set from
//! [`OutboxMessage::message_id`], so a re-published message is absorbed by the
//! broker's dedup window (D11) instead of delivered twice.

use std::future::Future;

use async_nats::header::HeaderMap;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream;

use super::OutboxMessage;
use super::PublishError;
use super::Publisher;

/// The `JetStream` stream every Loomery outbox publishes into (D11).
pub const STREAM: &str = "LOOMERY_OUTBOX";

/// The subjects the stream captures.
pub const SUBJECTS: &str = "loomery.>";

/// Publishes outbox messages to a `JetStream` stream.
pub struct NatsPublisher {
    context: jetstream::Context,
}

impl NatsPublisher {
    /// Connects to `url` and ensures [`STREAM`] exists.
    ///
    /// # Errors
    ///
    /// Connection or stream-creation failures.
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let client = async_nats::connect(url).await?;
        let context = jetstream::new(client);

        context
            .get_or_create_stream(jetstream::stream::Config {
                name: STREAM.to_owned(),
                subjects: vec![SUBJECTS.to_owned()],
                ..Default::default()
            })
            .await?;

        Ok(Self { context })
    }

    /// How many messages [`STREAM`] currently stores.
    ///
    /// Used by the integration suite to observe the dedup window.
    ///
    /// # Errors
    ///
    /// The stream could not be read.
    pub async fn stored_messages(&self) -> anyhow::Result<u64> {
        let mut stream = self.context.get_stream(STREAM).await?;
        Ok(stream.info().await?.state.messages)
    }

    /// The broker's duplicate window for [`STREAM`].
    ///
    /// The outbox relies on it (D11): a re-published message is only absorbed
    /// while it is inside this window. The stress harness reads it to check that
    /// its crash replay really did fall inside it.
    ///
    /// # Errors
    ///
    /// The stream could not be read.
    pub async fn duplicate_window(&self) -> anyhow::Result<std::time::Duration> {
        let mut stream = self.context.get_stream(STREAM).await?;
        Ok(stream.info().await?.config.duplicate_window)
    }
}

impl Publisher for NatsPublisher {
    fn publish(
        &self,
        message: OutboxMessage,
    ) -> impl Future<Output = Result<(), PublishError>> + Send {
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
