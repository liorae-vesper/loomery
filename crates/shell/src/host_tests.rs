// SPDX-License-Identifier: MPL-2.0

//! The host's end-to-end tests: boot, provision, serve, write, read, publish.
//!
//! These run entirely in-process with fakes for the two external services, so
//! the wiring — control group, tenant groups, routing, the command plane, the
//! HTTP gateway and the outbox worker — is exercised in the default suite.
//! `tests/test_services.rs` covers the same wiring against the real Keycloak and
//! NATS.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use loomery_core::id::Id;
use loomery_core::tenant::Replica;
use loomery_core::timestamp::Timestamp;
use loomery_genesis::Bootstrap;
use serde_json::json;
use tower::ServiceExt;

use crate::config::HostConfig;
use crate::config::HttpConfig;
use crate::gateway::Identity;
use crate::gateway::StaticAuthenticator;
use crate::host::Host;
use crate::outbox::OutboxMessage;
use crate::outbox::PublishError;
use crate::outbox::Publisher;
use crate::saga::ConsumeError;
use crate::saga::Consumer;
use crate::saga::SagaMessage;

const ORGANIZATION: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9";
const MEMBER: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0";

/// A broker that records what it was asked to publish.
#[derive(Default)]
struct RecordingPublisher {
    messages: Mutex<Vec<OutboxMessage>>,
}

impl RecordingPublisher {
    fn published(&self) -> usize {
        self.messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

impl Publisher for RecordingPublisher {
    fn publish(
        &self,
        message: OutboxMessage,
    ) -> impl Future<Output = Result<(), PublishError>> + Send {
        self.messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(message);
        std::future::ready(Ok(()))
    }
}

/// A consumer that never has anything: the saga runner idles on it.
#[derive(Default)]
struct EmptyConsumer;

impl Consumer for EmptyConsumer {
    fn next(&self) -> impl Future<Output = Result<Option<SagaMessage>, ConsumeError>> + Send {
        std::future::ready(Ok(None))
    }

    fn ack(&self, _message: &SagaMessage) -> impl Future<Output = Result<(), ConsumeError>> + Send {
        std::future::ready(Ok(()))
    }
}

/// A host configuration in a temporary directory, bound to an ephemeral port.
fn configuration(root: &std::path::Path) -> HostConfig {
    HostConfig {
        data_dir: root.to_path_buf(),
        http: HttpConfig {
            bind: "127.0.0.1:0".to_owned(),
            ryw_hold_ms: 50,
        },
        ..HostConfig::default()
    }
}

/// The identity provider the tests fake: one member token.
fn authenticator() -> Arc<StaticAuthenticator> {
    Arc::new(StaticAuthenticator::new().with_token(
        "member",
        Identity {
            user_id: Id::from(MEMBER),
            is_admin: false,
        },
    ))
}

/// The genesis bootstrap for the test organization.
fn bootstrap() -> Bootstrap {
    Bootstrap {
        organization_id: Id::from(ORGANIZATION),
        leader_user_id: Id::from(MEMBER),
        occurred_at: Timestamp::from(1_700_000_000_000),
    }
}

/// One replica record, as a placement carries.
fn replicas() -> Vec<Replica> {
    vec![Replica {
        node_id: 1,
        address: "http://127.0.0.1:7001".to_owned(),
    }]
}

/// A `POST /organizations/{id}/commands` request for a `task.create`.
fn command_request(aggregate_id: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/organizations/{ORGANIZATION}/commands"))
        .header("authorization", "Bearer member")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "aggregate_id": aggregate_id,
                "command_type": "task.create",
                "payload": { "title": "a host test task" },
            }))
            .unwrap(),
        ))
        .unwrap()
}

/// A `GET /organizations/{id}/events` request.
fn events_request() -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/organizations/{ORGANIZATION}/events"))
        .header("authorization", "Bearer member")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn a_host_provisions_serves_and_reads_its_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut host = Host::boot(
        configuration(root.path()),
        authenticator(),
        None::<Arc<RecordingPublisher>>,
        None::<Arc<EmptyConsumer>>,
    )
    .await
    .unwrap();

    assert!(host.groups().is_empty(), "no tenant is provisioned yet");

    host.provision("tenant-1", &replicas(), &bootstrap())
        .await
        .unwrap();

    assert_eq!(host.groups().len(), 1, "the tenant group is hosted");
    assert_eq!(host.groups().ids(), vec!["tenant-1".to_owned()]);
    assert!(
        host.incomplete_tenants().await.is_empty(),
        "provisioning finished, so nothing is left to reconcile"
    );
    assert!(host.routes().route(&Id::from(ORGANIZATION)).is_some());

    // The gateway refuses an anonymous caller...
    let anonymous = Request::builder()
        .method("GET")
        .uri(format!("/organizations/{ORGANIZATION}/events"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        host.router().oneshot(anonymous).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );

    // ...accepts a write, and answers it from applied state (read-your-writes).
    let task_id = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8fb";
    assert_eq!(
        host.router()
            .oneshot(command_request(task_id))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let response = host.router().oneshot(events_request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let events = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        events.contains("task.created"),
        "the event is readable straight after the write: {events}"
    );

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_host_serves_over_http_and_shuts_down_cleanly() {
    let root = tempfile::tempdir().unwrap();
    let mut host = Host::boot(
        configuration(root.path()),
        authenticator(),
        None::<Arc<RecordingPublisher>>,
        None::<Arc<EmptyConsumer>>,
    )
    .await
    .unwrap();
    // Workers start before the host is shared: they only need `&mut` here.
    host.start_workers().await.unwrap();
    let host = Arc::new(host);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let serving = tokio::spawn({
        let host = Arc::clone(&host);
        async move { host.serve_on(listener).await }
    });

    // A real TCP request: the socket is listening and the router answers.
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    {
        use tokio::io::AsyncWriteExt;

        stream
            .write_all(
                format!(
                    "GET /organizations/{ORGANIZATION}/events HTTP/1.1\r\n\
                     Host: {address}\r\n\
                     Authorization: Bearer member\r\n\
                     Connection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }
    let mut buffer = Vec::new();
    {
        use tokio::io::AsyncReadExt;

        stream.read_to_end(&mut buffer).await.unwrap();
    }
    let response = String::from_utf8_lossy(&buffer);
    assert!(
        response.starts_with("HTTP/1.1"),
        "an HTTP response arrives: {response:?}"
    );

    host.signal_shutdown();
    serving.await.unwrap().unwrap();

    let host = Arc::try_unwrap(host)
        .ok()
        .expect("the serving task released its handle");
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn the_outbox_worker_publishes_what_the_gateway_wrote() {
    let root = tempfile::tempdir().unwrap();
    let publisher = Arc::new(RecordingPublisher::default());
    let mut host = Host::boot(
        configuration(root.path()),
        authenticator(),
        Some(Arc::clone(&publisher)),
        None::<Arc<EmptyConsumer>>,
    )
    .await
    .unwrap();
    host.provision("tenant-1", &replicas(), &bootstrap())
        .await
        .unwrap();
    host.start_workers().await.unwrap();

    let task_id = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8fc";
    assert_eq!(
        host.router()
            .oneshot(command_request(task_id))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // The applied-event notification wakes the tailer; the cursor is persisted
    // next to the group's database.
    let mut delivered = 0;
    for _ in 0..200 {
        delivered = publisher.published();
        if delivered > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        delivered > 0,
        "the outbox worker published the committed event"
    );

    let cursor = crate::outbox::cursor::CursorStore::in_group_dir(&root.path().join("tenant-1"))
        .load()
        .await
        .unwrap();
    assert!(cursor.log_index > 0, "the cursor was persisted: {cursor:?}");

    host.shutdown().await.unwrap();
}
