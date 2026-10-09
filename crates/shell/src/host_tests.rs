// SPDX-License-Identifier: MPL-2.0

//! The host's end-to-end tests: boot, provision, serve, write, read, publish,
//! reconcile.
//!
//! These run entirely in-process with fakes for the two external services, so
//! the wiring — control group, tenant groups, routing, the command plane, the
//! HTTP gateway, the outbox worker and the provisioning route — is exercised in
//! the default suite. `tests/test_services.rs` covers the same wiring against the
//! real Keycloak and NATS.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use loomery_core::Uuid;
use loomery_core::envelope::Command;
use loomery_core::envelope::Payload;
use loomery_core::id::Id;
use loomery_core::key::Key;
use loomery_core::tenant;
use loomery_core::timestamp::Timestamp;
use loomery_genesis::Bootstrap;
use openraft::BasicNode;
use serde_json::json;
use tower::ServiceExt;

use crate::config::HostConfig;
use crate::config::HttpConfig;
use crate::gateway::GroupRegistry;
use crate::gateway::Identity;
use crate::gateway::ProvisionRequest;
use crate::gateway::StaticAuthenticator;
use crate::group::GroupOps;
use crate::host::Host;
use crate::outbox::OutboxMessage;
use crate::outbox::PublishError;
use crate::outbox::Publisher;
use crate::raft::RaftGroup;
use crate::saga::ConsumeError;
use crate::saga::Consumer;
use crate::saga::SagaMessage;

const ORGANIZATION: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9";
const MEMBER: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0";
const ADMIN: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f1";
const STRANGER: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f2";
const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

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

/// The identity provider the tests fake: one member and one admin token.
fn authenticator() -> Arc<StaticAuthenticator> {
    Arc::new(
        StaticAuthenticator::new()
            .with_token(
                "member",
                Identity {
                    user_id: Id::from(MEMBER),
                    is_admin: false,
                    email: None,
                },
            )
            .with_token(
                "admin",
                Identity {
                    user_id: Id::from(ADMIN),
                    is_admin: true,
                    email: None,
                },
            )
            .with_token(
                "stranger",
                Identity {
                    user_id: Id::from(STRANGER),
                    is_admin: false,
                    // The address the invitation is issued to: the acceptance
                    // binds to the verified email, not to the request body.
                    email: Some("stranger@example.com".to_owned()),
                },
            ),
    )
}

/// The genesis bootstrap for the test organization.
fn bootstrap() -> Bootstrap {
    Bootstrap {
        organization_id: Id::from(ORGANIZATION),
        leader_user_id: Id::from(MEMBER),
        occurred_at: Timestamp::from(1_700_000_000_000),
    }
}

/// A `tenant.register` command, as the controller writes it.
fn register_command(bootstrap: &Bootstrap) -> Command {
    Command {
        envelope_version: 1,
        id: Id::new(),
        aggregate_id: bootstrap.organization_id.clone(),
        organization_id: bootstrap.organization_id.clone(),
        workspace_id: None,
        occurred_at: Timestamp::from(1_700_000_000_000),
        causation_key: Key::new(&KEY_NS, "tenant-register"),
        correlation_key: Key::new(&KEY_NS, "provisioning"),
        actor: loomery_core::actor::Actor::Saga {
            user_id: None,
            name: "control-plane".to_owned(),
        },
        command_type: tenant::REGISTER.to_owned(),
        payload: Payload {
            version: 1,
            data: json!({
                "group_id": "tenant-1",
                "replicas": [{ "node_id": 1, "address": "http://127.0.0.1:7001" }],
                "leader_user_id": bootstrap.leader_user_id.to_string(),
            })
            .to_string(),
        },
    }
}

/// A `POST /organizations` request, as an operator would send it.
fn provision_request(token: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/organizations")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "organization_id": ORGANIZATION,
                "leader_user_id": MEMBER,
                "group_id": "tenant-1",
            })
            .to_string(),
        ))
        .unwrap()
}

/// A `POST /organizations/{id}/commands` request for a `task.create` in
/// `workspace_id` — the scope is required, because the command is role-checked
/// inside the workspace it names.
fn command_request(workspace_id: &str, aggregate_id: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/organizations/{ORGANIZATION}/commands"))
        .header("authorization", "Bearer member")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "workspace_id": workspace_id,
                "aggregate_id": aggregate_id,
                "command_type": "task.create",
                "payload": { "title": "a host test task" },
            }))
            .unwrap(),
        ))
        .unwrap()
}

/// The workspace genesis created for the organization.
///
/// A client discovers it by reading the board; the tests read it from the events.
async fn genesis_workspace(
    host: &Host<StaticAuthenticator, RecordingPublisher, EmptyConsumer>,
) -> String {
    let group = host
        .groups()
        .group("tenant-1")
        .expect("the tenant is hosted");
    group
        .committed_events(&Id::from(ORGANIZATION))
        .await
        .expect("the board")
        .into_iter()
        .find(|event| event.event_type == "workspace.created")
        .map(|event| event.aggregate_id.to_string())
        .expect("genesis creates the workspace")
}

/// A `GET /organizations/{id}/workspaces/{ws}/events` request.
fn workspace_events_request(token: Option<&str>, workspace_id: &str) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri(format!(
        "/organizations/{ORGANIZATION}/workspaces/{workspace_id}/events"
    ));
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

/// A `GET /organizations/{id}/events` request.
fn events_request(token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("/organizations/{ORGANIZATION}/events"));
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

/// Waits for a group to have a leader.
async fn await_leader(group: &RaftGroup) {
    group
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .metrics(|metrics| metrics.current_leader.is_some(), "a leader")
        .await
        .unwrap();
}

#[tokio::test]
async fn a_host_provisions_serves_and_reads_its_writes() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(
        Host::boot(
            configuration(root.path()),
            authenticator(),
            None::<Arc<RecordingPublisher>>,
            None::<Arc<EmptyConsumer>>,
        )
        .await
        .unwrap(),
    );

    assert!(host.groups().is_empty(), "no tenant is provisioned yet");

    let provisioned = host
        .provision_with(ProvisionRequest {
            organization_id: Id::from(ORGANIZATION),
            leader_user_id: Id::from(MEMBER),
            group_id: Some("tenant-1".to_owned()),
        })
        .await
        .unwrap();
    assert_eq!(provisioned.group_id, "tenant-1");

    assert_eq!(host.groups().len(), 1, "the tenant group is hosted");
    assert_eq!(host.groups().ids(), vec!["tenant-1".to_owned()]);
    assert!(
        host.incomplete_tenants().await.is_empty(),
        "provisioning finished, so nothing is left to reconcile"
    );
    assert!(host.routes().route(&Id::from(ORGANIZATION)).is_some());

    // The gateway refuses an anonymous caller...
    assert_eq!(
        host.router()
            .oneshot(events_request(None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    // ...accepts a write, and answers it from applied state (read-your-writes).
    let workspace = genesis_workspace(&host).await;
    let task_id = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8fb";
    assert_eq!(
        host.router()
            .oneshot(command_request(&workspace, task_id))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let response = host
        .router()
        .oneshot(events_request(Some("member")))
        .await
        .unwrap();
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
async fn provisioning_over_http_requires_the_admin_claim() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(
        Host::boot(
            configuration(root.path()),
            authenticator(),
            None::<Arc<RecordingPublisher>>,
            None::<Arc<EmptyConsumer>>,
        )
        .await
        .unwrap(),
    );
    let router = host.router_with_provisioning();

    // Anonymous, and authenticated-but-not-admin, are both refused.
    let anonymous = Request::builder()
        .method("POST")
        .uri("/organizations")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "organization_id": ORGANIZATION, "leader_user_id": MEMBER }).to_string(),
        ))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(anonymous).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        router
            .clone()
            .oneshot(provision_request("member"))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN,
        "a member cannot create organizations"
    );

    // The admin can, and the tenant is hosted and routable afterwards.
    let response = router
        .clone()
        .oneshot(provision_request("admin"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("tenant-1"),
        "the answer names the group that hosts it"
    );

    assert_eq!(host.groups().len(), 1);
    assert!(host.routes().route(&Id::from(ORGANIZATION)).is_some());

    // Replaying the same request is idempotent, not a second organization.
    assert_eq!(
        router
            .oneshot(provision_request("admin"))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    assert_eq!(host.groups().len(), 1, "the same tenant, still one group");

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_boot_reconciles_a_provisioning_a_crash_interrupted() {
    let root = tempfile::tempdir().unwrap();
    let config = configuration(root.path());
    let bootstrap = bootstrap();

    // A previous process recorded the placement and died before genesis — the
    // exact state the control plane is designed to be recoverable from.
    {
        let directory = root.path().join(&config.control_group);
        let mut control = RaftGroup::boot_persistent(
            config.node_id,
            config.control_group.clone(),
            &directory,
            config.group.clone(),
        )
        .await
        .unwrap();
        control
            .raft()
            .initialize(BTreeMap::from([(1u64, BasicNode::default())]))
            .await
            .unwrap();
        await_leader(&control).await;
        control.propose(register_command(&bootstrap)).await.unwrap();
        control.shutdown().await.unwrap();
    }

    // The control group is shut down but its store may not be released yet, and this
    // boot is product code that a test cannot route through `test_disk::boot`. A fresh
    // process never races the release, so wait for it.
    crate::raft::test_disk::wait_for_release(
        &root.path().join(&config.control_group),
        &config.group.storage,
    )
    .await
    .unwrap();

    // The boot resumes it from state alone: no operator, no caller memory.
    let host = Arc::new(
        Host::boot(
            config,
            authenticator(),
            None::<Arc<RecordingPublisher>>,
            None::<Arc<EmptyConsumer>>,
        )
        .await
        .unwrap(),
    );

    assert!(
        host.incomplete_tenants().await.is_empty(),
        "the boot finished the interrupted progression"
    );
    let record = host
        .tenants()
        .await
        .into_iter()
        .find(|(organization_id, _)| organization_id == &Id::from(ORGANIZATION))
        .map(|(_, record)| record)
        .expect("a tenant record");
    assert!(record.is_active(), "genesis ran: the tenant is routable");
    assert_eq!(host.groups().ids(), vec!["tenant-1".to_owned()]);

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_host_serves_over_http_and_shuts_down_cleanly() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(
        Host::boot(
            configuration(root.path()),
            authenticator(),
            None::<Arc<RecordingPublisher>>,
            None::<Arc<EmptyConsumer>>,
        )
        .await
        .unwrap(),
    );
    host.start_workers().await.unwrap();

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

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn the_outbox_worker_publishes_what_the_gateway_wrote() {
    let root = tempfile::tempdir().unwrap();
    let publisher = Arc::new(RecordingPublisher::default());
    let host = Arc::new(
        Host::boot(
            configuration(root.path()),
            authenticator(),
            Some(Arc::clone(&publisher)),
            None::<Arc<EmptyConsumer>>,
        )
        .await
        .unwrap(),
    );
    host.provision_with(ProvisionRequest {
        organization_id: Id::from(ORGANIZATION),
        leader_user_id: Id::from(MEMBER),
        group_id: Some("tenant-1".to_owned()),
    })
    .await
    .unwrap();
    host.start_workers().await.unwrap();

    let workspace = genesis_workspace(&host).await;
    let task_id = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8fc";
    assert_eq!(
        host.router()
            .oneshot(command_request(&workspace, task_id))
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

#[tokio::test]
async fn a_stranger_token_cannot_read_or_write_a_tenant() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(
        Host::boot(
            configuration(root.path()),
            authenticator(),
            None::<Arc<RecordingPublisher>>,
            None::<Arc<EmptyConsumer>>,
        )
        .await
        .unwrap(),
    );
    host.provision_with(ProvisionRequest {
        organization_id: Id::from(ORGANIZATION),
        leader_user_id: Id::from(MEMBER),
        group_id: Some("tenant-1".to_owned()),
    })
    .await
    .unwrap();

    // A valid token is not enough: the caller must belong to the organization.
    let read = Request::builder()
        .method("GET")
        .uri(format!("/organizations/{ORGANIZATION}/events"))
        .header("authorization", "Bearer stranger")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        host.router().oneshot(read).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    let write = Request::builder()
        .method("POST")
        .uri(format!("/organizations/{ORGANIZATION}/commands"))
        .header("authorization", "Bearer stranger")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "aggregate_id": "018f2c3d-4e5f-7071-8293-a4b5c6d7e8fd",
                "command_type": "task.create",
                "payload": { "title": "not mine" },
            })
            .to_string(),
        ))
        .unwrap();
    assert_eq!(
        host.router().oneshot(write).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    // The member still can, so the gate is authorization, not a broken plane.
    let workspace = genesis_workspace(&host).await;
    assert_eq!(
        host.router()
            .oneshot(command_request(
                &workspace,
                "018f2c3d-4e5f-7071-8293-a4b5c6d7e8fe"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_invitee_may_accept_before_they_are_a_member() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(
        Host::boot(
            configuration(root.path()),
            authenticator(),
            None::<Arc<RecordingPublisher>>,
            None::<Arc<EmptyConsumer>>,
        )
        .await
        .unwrap(),
    );
    host.provision_with(ProvisionRequest {
        organization_id: Id::from(ORGANIZATION),
        leader_user_id: Id::from(MEMBER),
        group_id: Some("tenant-1".to_owned()),
    })
    .await
    .unwrap();

    // The member (its owner) invites the stranger.
    let invitation_id = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8ff";
    let invite = Request::builder()
        .method("POST")
        .uri(format!("/organizations/{ORGANIZATION}/commands"))
        .header("authorization", "Bearer member")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "aggregate_id": invitation_id,
                "command_type": "invitation.create",
                "payload": { "email": "stranger@example.com", "role": "Member" },
            })
            .to_string(),
        ))
        .unwrap();
    assert_eq!(
        host.router().oneshot(invite).await.unwrap().status(),
        StatusCode::OK
    );

    // Onboarding is the one flow that starts before membership: the stranger may
    // accept (the acceptance is what makes them a member, via the saga).
    let accept = Request::builder()
        .method("POST")
        .uri(format!("/organizations/{ORGANIZATION}/commands"))
        .header("authorization", "Bearer stranger")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "aggregate_id": invitation_id,
                "command_type": "invitation.accept",
                "payload": { "user_id": STRANGER },
            })
            .to_string(),
        ))
        .unwrap();
    let response = host.router().oneshot(accept).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the acceptance is exempt from the membership gate"
    );

    host.shutdown().await.unwrap();
}

/// A `POST /organizations/{id}/commands` request with an explicit body.
fn submit(token: &str, body: &serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/organizations/{ORGANIZATION}/commands"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // The policy has four paths; the sequence is the test.
async fn workspace_roles_decide_what_a_member_may_do() {
    let root = tempfile::tempdir().unwrap();
    let host = Arc::new(
        Host::boot(
            configuration(root.path()),
            authenticator(),
            None::<Arc<RecordingPublisher>>,
            None::<Arc<EmptyConsumer>>,
        )
        .await
        .unwrap(),
    );
    host.provision_with(ProvisionRequest {
        organization_id: Id::from(ORGANIZATION),
        leader_user_id: Id::from(MEMBER),
        group_id: Some("tenant-1".to_owned()),
    })
    .await
    .unwrap();
    let workspace = genesis_workspace(&host).await;

    // The genesis owner may invite and may manage the workspace's membership —
    // that is what being an Owner means.
    assert_eq!(
        host.router()
            .oneshot(submit(
                "member",
                &json!({
                    "workspace_id": workspace,
                    "aggregate_id": "018f2c3d-4e5f-7071-8293-a4b5c6d7e8ff",
                    "command_type": "invitation.create",
                    "payload": { "email": "stranger@example.com", "role": "Member" },
                }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        host.router()
            .oneshot(submit(
                "member",
                &json!({
                    "workspace_id": workspace,
                    "aggregate_id": loomery_core::membership::workspace_membership_id(
                        &Id::from(ORGANIZATION),
                        &Id::from(workspace.as_str()),
                        &Id::from(STRANGER),
                    )
                    .to_string(),
                    "command_type": "membership.add_member",
                    "payload": { "user_id": STRANGER, "role": "Viewer" },
                }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "an Owner may add a Viewer"
    );

    // A Viewer reads their workspace...
    assert_eq!(
        host.router()
            .oneshot(workspace_events_request(Some("stranger"), &workspace))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    // ...but not the organization's whole log, which is the owner's view...
    assert_eq!(
        host.router()
            .oneshot(events_request(Some("stranger")))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    // ...and does not write.
    assert_eq!(
        host.router()
            .oneshot(submit(
                "stranger",
                &json!({
                    "workspace_id": workspace,
                    "aggregate_id": "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0",
                    "command_type": "task.create",
                    "payload": { "title": "a viewer's task" },
                }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN,
        "a Viewer may not write"
    );

    // ...nor manage the workspace, and nor may a plain Member.
    assert_eq!(
        host.router()
            .oneshot(submit(
                "stranger",
                &json!({
                    "workspace_id": workspace,
                    "aggregate_id": "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f1",
                    "command_type": "membership.change_role",
                    "payload": { "user_id": STRANGER, "role": "Member" },
                }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );

    // ...but the Owner may promote them, and the new role takes effect.
    let membership_id = loomery_core::membership::workspace_membership_id(
        &Id::from(ORGANIZATION),
        &Id::from(workspace.as_str()),
        &Id::from(STRANGER),
    )
    .to_string();
    assert_eq!(
        host.router()
            .oneshot(submit(
                "member",
                &json!({
                    "workspace_id": workspace,
                    "aggregate_id": membership_id,
                    "command_type": "membership.change_role",
                    "payload": { "user_id": STRANGER, "role": "Member" },
                }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "an Owner may change a role"
    );
    assert_eq!(
        host.router()
            .oneshot(submit(
                "stranger",
                &json!({
                    "workspace_id": workspace,
                    "aggregate_id": "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f3",
                    "command_type": "task.create",
                    "payload": { "title": "now allowed" },
                }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "the promoted Member may write"
    );

    // A scoped command that names no workspace cannot be checked, so it is
    // refused rather than treated as unscoped.
    assert_eq!(
        host.router()
            .oneshot(submit(
                "member",
                &json!({
                    "aggregate_id": "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f2",
                    "command_type": "task.create",
                    "payload": { "title": "unscoped" },
                }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );

    host.shutdown().await.unwrap();
}
