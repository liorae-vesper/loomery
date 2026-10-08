// SPDX-License-Identifier: MPL-2.0

//! End-to-end invitation acceptance (`design.md` §5, Phase 1 gate).
//!
//! **invite by email → accept → provisioned → can log in and read the board.**
//! The test wires the real pieces together: the control plane provisions a
//! tenant, the gateway accepts an invitation, the outbox publishes the committed
//! events, the invitation saga consumes `invitation.accepted` and provisions the
//! user, and the new user then authenticates and works on the board.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use loomery_core::envelope::Event;
use loomery_core::id::Id;
use loomery_core::invitation;
use loomery_core::tenant::Replica;
use serde_json::json;

use crate::control::Router;
use crate::control::provision;
use crate::gateway::Authenticator;
use crate::gateway::CommandPlane;
use crate::gateway::CommandRequest;
use crate::gateway::GroupRegistry;
use crate::gateway::Identity;
use crate::gateway::StaticAuthenticator;
use crate::group::GroupOps;
use crate::outbox::Outbox;
use crate::outbox::OutboxMessage;
use crate::outbox::PublishError;
use crate::outbox::Publisher;
use crate::raft::RaftGroup;
use crate::saga::ConsumeError;
use crate::saga::Consumer;
use crate::saga::InvitationAcceptance;
use crate::saga::SagaMessage;
use crate::saga::SagaRunner;
use crate::test_support::bootstrap_value;
use openraft::type_config::async_runtime::WatchReceiver;

const GROUP: &str = "tenant-1";

fn replicas() -> Vec<Replica> {
    vec![Replica {
        node_id: 1,
        address: "http://127.0.0.1:7001".to_owned(),
    }]
}

/// Hosts one group.
struct OneGroup {
    group: RaftGroup,
}

impl GroupRegistry for OneGroup {
    fn group(&self, group_id: &str) -> Option<RaftGroup> {
        (group_id == GROUP).then(|| self.group.clone())
    }
}

/// Records everything the outbox publishes.
#[derive(Default)]
struct CapturingPublisher {
    messages: Mutex<Vec<OutboxMessage>>,
}

impl Publisher for CapturingPublisher {
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

/// A peek-and-ack saga consumer over a fixed queue.
#[derive(Default)]
struct QueueConsumer {
    queue: Mutex<VecDeque<SagaMessage>>,
}

impl QueueConsumer {
    fn with(messages: Vec<SagaMessage>) -> Self {
        Self {
            queue: Mutex::new(messages.into()),
        }
    }
}

impl Consumer for QueueConsumer {
    fn next(&self) -> impl Future<Output = Result<Option<SagaMessage>, ConsumeError>> + Send {
        let front = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .front()
            .cloned();
        std::future::ready(Ok(front))
    }

    fn ack(&self, message: &SagaMessage) -> impl Future<Output = Result<(), ConsumeError>> + Send {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if queue
            .front()
            .is_some_and(|front| front.message_id == message.message_id)
        {
            queue.pop_front();
        }
        std::future::ready(Ok(()))
    }
}

/// Rebuilds a saga message from a published outbox message.
fn saga_message(message: &OutboxMessage) -> SagaMessage {
    let event: Event = serde_json::from_slice(&message.payload).expect("an event payload");
    let log_index = message
        .message_id
        .split(':')
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);

    SagaMessage {
        group_id: GROUP.to_owned(),
        subject: message.subject.clone(),
        message_id: message.message_id.clone(),
        log_index,
        event,
    }
}

#[allow(clippy::too_many_lines)] // The flow *is* the test: control plane → gateway → outbox → saga → board.
#[tokio::test]
async fn an_invited_user_is_provisioned_and_can_work_on_the_board() {
    let organization_id = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9");
    let workspace_id = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8fa");
    let invitation_id = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8fb");
    let task_id = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8fc");
    let invitee = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8fd");

    // --- the control plane births the tenant --------------------------------
    let mut control = RaftGroup::boot_single_node(1).await.unwrap();
    let mut tenant = RaftGroup::boot_single_node(1).await.unwrap();
    let router = Arc::new(Router::new());
    let bootstrap = bootstrap_value();
    provision(
        &mut control,
        &mut tenant,
        &router,
        GROUP,
        &replicas(),
        &bootstrap,
    )
    .await
    .unwrap();

    let groups = Arc::new(OneGroup {
        group: tenant.clone(),
    });
    let authenticator = Arc::new(
        StaticAuthenticator::new()
            .with_token(
                "admin",
                Identity {
                    user_id: Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0"),
                    is_admin: true,
                    email: None,
                },
            )
            .with_token(
                "invitee",
                Identity {
                    user_id: invitee.clone(),
                    is_admin: false,
                    // The address the invitation was issued to: an acceptance
                    // binds to it (the gateway carries it into the payload).
                    email: Some("new@example.com".to_owned()),
                },
            ),
    );
    let plane = CommandPlane::new(
        router.clone(),
        groups.clone(),
        authenticator.clone(),
        Duration::from_millis(50),
    );

    // --- invite, then accept ------------------------------------------------
    plane
        .submit(CommandRequest {
            organization_id: organization_id.clone(),
            aggregate_id: invitation_id.clone(),
            workspace_id: Some(workspace_id.clone()),
            command_type: invitation::CREATE.to_owned(),
            payload: json!({ "email": "new@example.com", "role": "Member" }),
            causation_id: None,
            correlation_id: None,
            token: Some("admin".to_owned()),
        })
        .await
        .unwrap();

    plane
        .submit(CommandRequest {
            organization_id: organization_id.clone(),
            aggregate_id: invitation_id.clone(),
            workspace_id: Some(workspace_id.clone()),
            command_type: invitation::ACCEPT.to_owned(),
            payload: json!({ "user_id": invitee }),
            causation_id: None,
            correlation_id: None,
            token: Some("invitee".to_owned()),
        })
        .await
        .unwrap();

    // --- the outbox publishes, the saga consumes and provisions -------------
    let applied = tenant
        .state_machine()
        .applied_events(&organization_id)
        .await;
    let publisher = Arc::new(CapturingPublisher::default());
    Outbox::new(publisher.clone())
        .flush(GROUP, &applied)
        .await
        .unwrap();

    let queue: Vec<SagaMessage> = {
        let messages = publisher
            .messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        messages.iter().map(saga_message).collect()
    };
    let runner = SagaRunner::new(
        QueueConsumer::with(queue),
        InvitationAcceptance,
        groups.clone(),
    );
    while runner.run_once().await.unwrap() {}

    // --- provisioned: organization assignment + workspace membership --------
    let events = tenant.committed_events(&organization_id).await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "organization.member_assigned"),
        "the invitee joins the organization"
    );
    let membership = events
        .iter()
        .find(|event| event.event_type == "membership.member_added")
        .expect("the invitee joins the workspace");
    assert_eq!(membership.workspace_id.as_deref(), Some(&*workspace_id));

    // --- the invitee authenticates and works on the board -------------------
    let identity = authenticator.authenticate(Some("invitee")).await.unwrap();
    assert_eq!(identity.user_id, invitee);

    plane
        .submit(CommandRequest {
            organization_id: organization_id.clone(),
            aggregate_id: task_id,
            workspace_id: Some(workspace_id),
            command_type: "task.create".to_owned(),
            payload: json!({ "title": "first task" }),
            causation_id: None,
            correlation_id: None,
            token: Some("invitee".to_owned()),
        })
        .await
        .unwrap();

    let board = tenant.committed_events(&organization_id).await.unwrap();
    assert!(
        board.iter().any(|event| event.event_type == "task.created"),
        "the provisioned user can write and read the board"
    );
}

#[allow(clippy::too_many_lines)] // The flow *is* the test: dedup, conflict, RYW and the admin claim.
#[tokio::test]
async fn a_reused_causation_key_is_a_conflict_and_reads_are_your_writes() {
    let organization_id = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9");
    let workspace_id = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8fa");
    let task_id = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8fb");

    let mut control = RaftGroup::boot_single_node(1).await.unwrap();
    let mut tenant = RaftGroup::boot_single_node(1).await.unwrap();
    let router = Arc::new(Router::new());
    provision(
        &mut control,
        &mut tenant,
        &router,
        GROUP,
        &replicas(),
        &bootstrap_value(),
    )
    .await
    .unwrap();

    // The caller works in the workspace it addresses (the invitation flow gives
    // this role in a deployment).
    crate::test_support::join_workspace(
        &mut tenant,
        &organization_id,
        &workspace_id,
        &Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0"),
        loomery_core::membership::Role::Member,
    )
    .await;
    let groups = Arc::new(OneGroup {
        group: tenant.clone(),
    });
    let authenticator = Arc::new(StaticAuthenticator::new().with_token(
        "member",
        Identity {
            user_id: Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0"),
            is_admin: false,
            email: None,
        },
    ));
    let plane = CommandPlane::new(
        router.clone(),
        groups.clone(),
        authenticator,
        Duration::from_millis(50),
    );

    // A client-supplied idempotency key (a canonical UUIDv5).
    let causation_id =
        loomery_core::key::Key::new(&loomery_core::Uuid::from_u128(1), "create-task").to_string();
    let request = |payload: serde_json::Value| CommandRequest {
        organization_id: organization_id.clone(),
        aggregate_id: task_id.clone(),
        workspace_id: Some(workspace_id.clone()),
        command_type: "task.create".to_owned(),
        payload,
        causation_id: Some(causation_id.clone()),
        correlation_id: None,
        token: Some("member".to_owned()),
    };

    // 1. the first write appends.
    let first = plane
        .submit(request(json!({ "title": "first" })))
        .await
        .unwrap();
    assert!(matches!(
        first.outcome,
        crate::group::ProposeOutcome::Appended { .. }
    ));

    // 2. an exact retry replays and does not duplicate.
    let retry = plane
        .submit(request(json!({ "title": "first" })))
        .await
        .unwrap();
    assert!(matches!(
        retry.outcome,
        crate::group::ProposeOutcome::Replayed { .. }
    ));
    assert_eq!(
        tenant
            .committed_events(&organization_id)
            .await
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "task.created")
            .count(),
        1
    );

    // 3. the same key with a different intent is a conflict, not a replay.
    let conflict = plane.submit(request(json!({ "title": "different" }))).await;
    assert!(matches!(
        conflict,
        Err(crate::gateway::CommandError::KeyReused)
    ));

    // 4. read-your-writes: the caller's own write is already visible locally.
    let applied = tenant
        .raft()
        .metrics()
        .borrow_watched()
        .last_applied
        .map_or(0, |log_id| log_id.index);
    assert_eq!(
        crate::gateway::ensure_min_index(&tenant, applied, Duration::from_millis(50)).await,
        crate::gateway::RywOutcome::Recent
    );
    assert!(
        tenant
            .committed_events(&organization_id)
            .await
            .unwrap()
            .iter()
            .any(|event| event.event_type == "task.created")
    );

    // 5. an admin-only command is rejected for a member.
    let forbidden = plane
        .submit(CommandRequest {
            command_type: loomery_core::org::ARCHIVE.to_owned(),
            ..request(json!({}))
        })
        .await;
    assert!(matches!(
        forbidden,
        Err(crate::gateway::CommandError::Auth(
            crate::gateway::AuthError::Forbidden
        ))
    ));
}
