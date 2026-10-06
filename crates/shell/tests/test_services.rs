// SPDX-License-Identifier: MPL-2.0

//! Integration tests against the local Keycloak and NATS `JetStream` services.
//!
//! Feature-gated and env-driven, so the default suite never needs a broker or an
//! `IdP`. Start the services and run these with:
//!
//! ```sh
//! mise run test-services
//! ```
#![cfg(feature = "test-services")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // a test crate

use std::env;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use loomery_shell::gateway::AuthError;
use loomery_shell::gateway::Authenticator;
use loomery_shell::gateway::keycloak::KeycloakAuthenticator;
use loomery_shell::outbox::OutboxMessage;
use loomery_shell::outbox::Publisher;
use loomery_shell::outbox::nats::NatsPublisher;

/// A required service URL; fails loudly if the stack is not running.
fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is not set; run `mise run svc-up` first"))
}

/// An optional setting with a default.
fn optional(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// A per-run suffix so repeated runs do not collide in the stream.
fn run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("{}-{nanos}", std::process::id())
}

#[tokio::test]
async fn jetstream_stores_each_outbox_message_once_per_dedup_id() {
    let publisher = NatsPublisher::connect(&required("LOOMERY_TEST_NATS_URL"))
        .await
        .expect("connect to NATS");

    let run = run_id();
    let subject = format!("loomery.events.test.{run}");
    let message = |id: &str, payload: &str| OutboxMessage {
        subject: subject.clone(),
        message_id: format!("{run}:{id}"),
        payload: payload.as_bytes().to_vec(),
    };

    let before = publisher.stored_messages().await.expect("stream info");

    publisher
        .publish(message("1", "first"))
        .await
        .expect("publish first");
    publisher
        .publish(message("1", "a retry with the same id"))
        .await
        .expect("publish duplicate id");
    publisher
        .publish(message("2", "second"))
        .await
        .expect("publish second");

    let after = publisher.stored_messages().await.expect("stream info");
    assert_eq!(
        after - before,
        2,
        "two distinct Nats-Msg-Id values; the duplicate is absorbed"
    );
}

#[tokio::test]
async fn keycloak_tokens_become_identities_with_the_admin_claim() {
    let base = required("LOOMERY_TEST_KEYCLOAK_URL");
    let realm = optional("LOOMERY_TEST_KEYCLOAK_REALM", "loomery");
    let client = optional("LOOMERY_TEST_KEYCLOAK_CLIENT", "loomery-gateway");
    let authenticator = KeycloakAuthenticator::new(&base, &realm);

    let ada = KeycloakAuthenticator::password_token(&base, &realm, &client, "ada", "ada")
        .await
        .expect("ada's token");
    let identity = authenticator
        .authenticate(Some(&ada))
        .await
        .expect("ada authenticates");
    assert!(!identity.is_admin, "ada is not in the admins group");

    let admin = KeycloakAuthenticator::password_token(&base, &realm, &client, "admin", "admin")
        .await
        .expect("admin's token");
    let identity = authenticator
        .authenticate(Some(&admin))
        .await
        .expect("admin authenticates");
    assert!(identity.is_admin, "admin is in the admins group");

    assert_eq!(
        authenticator.authenticate(None).await,
        Err(AuthError::Missing)
    );
    assert_eq!(
        authenticator.authenticate(Some("not-a-token")).await,
        Err(AuthError::Unknown)
    );
}
