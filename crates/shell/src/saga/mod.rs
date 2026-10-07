// SPDX-License-Identifier: MPL-2.0

//! The saga layer: consume committed events, do the follow-up work.
//!
//! Cross-group coordination is **choreography, never 2PC** (`design.md`
//! principle 10): a saga consumes published events and proposes the follow-up
//! commands itself. Two properties make that safe to retry:
//!
//! * the consumer has a **durable cursor** and only acks a message after the
//!   handler succeeded, so a crash or a retryable failure redelivers rather
//!   than drops;
//! * the commands a handler proposes carry **derived** causation keys and ids
//!   (D12), so a redelivered message replays instead of duplicating.
//!
//! [`InvitationAcceptance`] is the first saga: accepting an invitation
//! provisions the user by assigning them to the organization and adding them to
//! the workspace with the invited role.

use std::future::Future;
use std::sync::Arc;

use loomery_core::Uuid;
use loomery_core::actor::Actor;
use loomery_core::envelope::Command;
use loomery_core::envelope::Event;
use loomery_core::envelope::Payload;
use loomery_core::id::Id;
use loomery_core::invitation;
use loomery_core::key::Key;
use loomery_core::membership;
use loomery_core::membership::Role;
use loomery_core::timestamp::Timestamp;

use crate::gateway::GroupRegistry;
use crate::group::GroupOps;
use crate::raft::RaftGroup;

/// The namespace saga causation keys derive from (D12).
///
/// The *entity* ids the saga writes to derive from the same namespace, but live
/// in the core ([`membership::organization_assignment_id`] and
/// [`membership::workspace_membership_id`]) because the state machine needs them
/// too.
const SAGA_NAMESPACE: Uuid = membership::NAMESPACE;

/// A published event plus the routing metadata the bus carried.
#[derive(Debug, Clone)]
pub struct SagaMessage {
    /// The group the event came from (the transport's routing key).
    pub group_id: String,
    /// The subject it was published to.
    pub subject: String,
    /// The broker message id (`<group>:<log_index>:e<pos>`).
    pub message_id: String,
    /// The log index the event was applied at.
    pub log_index: u64,
    /// The published event.
    pub event: Event,
}

/// Why consuming or acking failed.
#[derive(Debug, thiserror::Error)]
pub enum ConsumeError {
    /// The bus is unavailable; retry later.
    #[error("the saga bus is unavailable")]
    Unavailable(#[source] anyhow::Error),
}

/// A durable-cursor consumer of published events.
///
/// `next` **peeks** the next unacked message (a real broker redelivers it), and
/// `ack` removes it. That is what makes an unacked retryable failure safe.
pub trait Consumer: Send + Sync {
    /// The next unacked message, if any.
    ///
    /// # Errors
    ///
    /// [`ConsumeError::Unavailable`].
    fn next(&self) -> impl Future<Output = Result<Option<SagaMessage>, ConsumeError>> + Send;

    /// Acknowledges a message, advancing the cursor past it.
    ///
    /// # Errors
    ///
    /// [`ConsumeError::Unavailable`].
    fn ack(&self, message: &SagaMessage) -> impl Future<Output = Result<(), ConsumeError>> + Send;
}

/// An `Arc` of a consumer is itself a consumer, so a host can share one broker
/// subscription between the runner and its own bookkeeping.
impl<T: Consumer + ?Sized> Consumer for Arc<T> {
    fn next(&self) -> impl Future<Output = Result<Option<SagaMessage>, ConsumeError>> + Send {
        (**self).next()
    }

    fn ack(&self, message: &SagaMessage) -> impl Future<Output = Result<(), ConsumeError>> + Send {
        (**self).ack(message)
    }
}

/// How a handler failure should be treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// Try again later: the message stays unacked and is redelivered.
    Retryable,
    /// Never retry: the message is acked and dropped (a poison message).
    Fatal,
}

/// Handles one saga's messages.
pub trait SagaHandler: Send + Sync {
    /// Whether this handler consumes `subject`.
    fn handles(&self, subject: &str) -> bool;

    /// Handles the message, proposing through `group`.
    fn handle(
        &self,
        group: &mut RaftGroup,
        message: &SagaMessage,
    ) -> impl Future<Output = Result<(), Retry>> + Send;
}

/// Runs a saga: peek a message, resolve its group, handle, ack or retry.
pub struct SagaRunner<C: Consumer, H: SagaHandler> {
    consumer: C,
    handler: H,
    groups: Arc<dyn GroupRegistry>,
}

impl<C: Consumer, H: SagaHandler> SagaRunner<C, H> {
    /// Builds a runner over a consumer, a handler and the host's group registry.
    #[must_use]
    pub fn new(consumer: C, handler: H, groups: Arc<dyn GroupRegistry>) -> Self {
        Self {
            consumer,
            handler,
            groups,
        }
    }

    /// Processes at most one message.
    ///
    /// Returns whether a message was consumed. An unknown subject is acked and
    /// skipped; a message whose group this host does not run is left for the
    /// host that does; a retryable handler failure leaves the message unacked
    /// so the next run redelivers it.
    ///
    /// # Errors
    ///
    /// [`ConsumeError`] when the bus itself fails.
    pub async fn run_once(&self) -> Result<bool, ConsumeError> {
        let Some(message) = self.consumer.next().await? else {
            return Ok(false);
        };

        if !self.handler.handles(&message.subject) {
            self.consumer.ack(&message).await?;
            return Ok(true);
        }

        let Some(group) = self.groups.group(&message.group_id) else {
            // Not this host's tenant; leave it for the host that runs it.
            return Ok(false);
        };

        let mut group = group;
        match self.handler.handle(&mut group, &message).await {
            Ok(()) => {
                self.consumer.ack(&message).await?;
            }
            Err(Retry::Retryable) => return Ok(false),
            Err(Retry::Fatal) => {
                // A poison message must not block the cursor forever.
                self.consumer.ack(&message).await?;
            }
        }

        Ok(true)
    }
}

/// The invitation acceptance saga.
///
/// On `invitation.accepted` it assigns the user to the organization and adds
/// them to the workspace with the invited role. Both commands derive their
/// identity from the invitation's business tuple, so a redelivery replays.
pub struct InvitationAcceptance;

impl InvitationAcceptance {
    /// Whether `subject` is an invitation acceptance.
    #[must_use]
    pub fn is_acceptance(subject: &str) -> bool {
        subject.ends_with(&format!(".{}", invitation::ACCEPTED))
    }
}

impl SagaHandler for InvitationAcceptance {
    fn handles(&self, subject: &str) -> bool {
        Self::is_acceptance(subject)
    }

    async fn handle(&self, group: &mut RaftGroup, message: &SagaMessage) -> Result<(), Retry> {
        let event = &message.event;
        let accepted: invitation::Accepted =
            serde_json::from_str(&event.payload.data).map_err(|_| Retry::Fatal)?;

        let organization_id = event.organization_id.clone();
        let workspace_id = event.workspace_id.clone().ok_or(Retry::Fatal)?;

        // 1. the organization assignment
        let assignment =
            assignment_command(&organization_id, &accepted.user_id).map_err(|_| Retry::Fatal)?;
        group
            .propose(assignment)
            .await
            .map_err(|_| Retry::Retryable)?;

        // 2. the workspace membership with the invited role
        let membership = membership_command(
            &organization_id,
            &workspace_id,
            &accepted.user_id,
            accepted.role,
        )
        .map_err(|_| Retry::Fatal)?;
        group
            .propose(membership)
            .await
            .map_err(|_| Retry::Retryable)?;

        Ok(())
    }
}

/// Builds the organization-assignment command.
fn assignment_command(organization_id: &Id, user_id: &Id) -> Result<Command, serde_json::Error> {
    let data = serde_json::to_string(&membership::AssignMember {
        user_id: user_id.clone(),
    })?;

    Ok(command(
        organization_id,
        None,
        membership::organization_assignment_id(organization_id, user_id),
        &format!("{organization_id}:{user_id}:assign-member"),
        membership::ASSIGN_MEMBER,
        data,
    ))
}

/// Builds the workspace-membership command.
fn membership_command(
    organization_id: &Id,
    workspace_id: &Id,
    user_id: &Id,
    role: Role,
) -> Result<Command, serde_json::Error> {
    let data = serde_json::to_string(&membership::AddMember {
        user_id: user_id.clone(),
        role,
    })?;

    Ok(command(
        organization_id,
        Some(workspace_id.clone()),
        membership::workspace_membership_id(organization_id, workspace_id, user_id),
        &format!("{organization_id}:{workspace_id}:{user_id}:add-member"),
        membership::ADD_MEMBER,
        data,
    ))
}

/// Builds a saga command with a derived causation key and a saga actor.
fn command(
    organization_id: &Id,
    workspace_id: Option<Id>,
    aggregate_id: Id,
    causation_data: &str,
    command_type: &str,
    data: String,
) -> Command {
    Command {
        envelope_version: 1,
        id: Id::new(),
        aggregate_id,
        organization_id: organization_id.clone(),
        workspace_id,
        occurred_at: Timestamp::now(),
        causation_key: Key::new(&SAGA_NAMESPACE, causation_data),
        correlation_key: Key::new(
            &SAGA_NAMESPACE,
            &format!("{organization_id}:invitation-acceptance"),
        ),
        actor: Actor::Saga {
            user_id: None,
            name: "InvitationAcceptanceSaga".to_owned(),
        },
        command_type: command_type.to_owned(),
        payload: Payload { version: 1, data },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::GroupRegistry;
    use loomery_core::Uuid;
    use loomery_core::aggregate::AggregatePlan;
    use loomery_core::invitation::Invitation;
    use loomery_core::invitation::InvitationState;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    const NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    /// A peek-and-ack consumer: `next` does not remove the message, `ack` does.
    #[derive(Default)]
    struct InMemoryConsumer {
        queue: Mutex<VecDeque<SagaMessage>>,
        acked: Mutex<Vec<String>>,
    }

    impl InMemoryConsumer {
        fn with(messages: Vec<SagaMessage>) -> Self {
            let consumer = Self::default();
            *consumer.queue.lock().unwrap() = messages.into();
            consumer
        }

        fn acked(&self) -> Vec<String> {
            self.acked.lock().unwrap().clone()
        }
    }

    impl Consumer for InMemoryConsumer {
        fn next(&self) -> impl Future<Output = Result<Option<SagaMessage>, ConsumeError>> + Send {
            let front = self.queue.lock().unwrap().front().cloned();
            std::future::ready(Ok(front))
        }

        fn ack(
            &self,
            message: &SagaMessage,
        ) -> impl Future<Output = Result<(), ConsumeError>> + Send {
            let mut queue = self.queue.lock().unwrap();
            if queue.front().map(|front| front.message_id.clone())
                == Some(message.message_id.clone())
            {
                queue.pop_front();
            }
            self.acked.lock().unwrap().push(message.message_id.clone());
            std::future::ready(Ok(()))
        }
    }

    /// A handler that fails with a configured classification.
    struct TestHandler {
        outcome: Retry,
        calls: AtomicUsize,
    }

    impl TestHandler {
        fn failing(outcome: Retry) -> Self {
            Self {
                outcome,
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl SagaHandler for TestHandler {
        fn handles(&self, _subject: &str) -> bool {
            true
        }

        fn handle(
            &self,
            _group: &mut RaftGroup,
            _message: &SagaMessage,
        ) -> impl Future<Output = Result<(), Retry>> + Send {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Err(self.outcome))
        }
    }

    /// A registry that hosts one group under one id.
    struct OneGroup {
        group_id: String,
        group: RaftGroup,
    }

    impl GroupRegistry for OneGroup {
        fn group(&self, group_id: &str) -> Option<RaftGroup> {
            (group_id == self.group_id).then(|| self.group.clone())
        }
    }

    fn event(id: &str, event_type: &str) -> Event {
        Event {
            envelope_version: 1,
            id: Id::from(id),
            aggregate_id: Id::from("agg-1"),
            organization_id: Id::from("org-1"),
            workspace_id: Some(Id::from("ws-1")),
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

    fn message(id: &str, subject: &str) -> SagaMessage {
        SagaMessage {
            group_id: "tenant-1".to_owned(),
            subject: subject.to_owned(),
            message_id: format!("tenant-1:{id}:e0"),
            log_index: 1,
            event: event(id, "task.created"),
        }
    }

    async fn registry() -> Arc<OneGroup> {
        Arc::new(OneGroup {
            group_id: "tenant-1".to_owned(),
            group: RaftGroup::boot_single_node(1).await.unwrap(),
        })
    }

    #[tokio::test]
    async fn a_handled_message_is_acked_and_removed() {
        let consumer =
            InMemoryConsumer::with(vec![message("1", "loomery.events.tenant-1.task.created")]);
        let groups = registry().await;
        let runner = SagaRunner::new(consumer, TestHandler::failing(Retry::Fatal), groups);

        assert!(runner.run_once().await.unwrap());
        assert_eq!(runner.consumer.acked().len(), 1);
        assert!(runner.consumer.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_retryable_failure_keeps_the_message_for_redelivery() {
        let consumer =
            InMemoryConsumer::with(vec![message("1", "loomery.events.tenant-1.task.created")]);
        let groups = registry().await;
        let runner = SagaRunner::new(consumer, TestHandler::failing(Retry::Retryable), groups);

        assert!(!runner.run_once().await.unwrap());
        assert!(runner.consumer.acked().is_empty());
        // The same message is still there for the next attempt.
        assert!(runner.consumer.next().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_fatal_failure_drops_the_poison_message() {
        let consumer =
            InMemoryConsumer::with(vec![message("1", "loomery.events.tenant-1.task.created")]);
        let groups = registry().await;
        let runner = SagaRunner::new(consumer, TestHandler::failing(Retry::Fatal), groups);

        runner.run_once().await.unwrap();
        assert_eq!(runner.consumer.acked().len(), 1);
        assert!(runner.consumer.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_unhosted_group_is_left_for_its_host() {
        let consumer =
            InMemoryConsumer::with(vec![message("1", "loomery.events.tenant-1.task.created")]);
        let groups = Arc::new(OneGroup {
            group_id: "other".to_owned(),
            group: RaftGroup::boot_single_node(1).await.unwrap(),
        });
        let runner = SagaRunner::new(consumer, TestHandler::failing(Retry::Fatal), groups);

        assert!(!runner.run_once().await.unwrap());
        assert!(runner.consumer.acked().is_empty());
    }

    /// An `invitation.accepted` event, built through the real aggregate.
    fn acceptance_event() -> Event {
        let create = Command {
            envelope_version: 1,
            id: Id::from("cmd-create"),
            aggregate_id: Id::from("inv-1"),
            organization_id: Id::from("org-1"),
            workspace_id: Some(Id::from("ws-1")),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&NS, "create"),
            correlation_key: Key::new(&NS, "corr"),
            actor: Actor::System,
            command_type: invitation::CREATE.to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"email":"ada@example.com","role":"Member"}"#.to_owned(),
            },
        };
        let pending = Invitation::apply(
            InvitationState::default(),
            Invitation::prepare(InvitationState::default(), create)
                .unwrap()
                .events
                .remove(0),
        );

        let accept = Command {
            envelope_version: 1,
            id: Id::from("cmd-accept"),
            aggregate_id: Id::from("inv-1"),
            organization_id: Id::from("org-1"),
            workspace_id: Some(Id::from("ws-1")),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&NS, "accept"),
            correlation_key: Key::new(&NS, "corr"),
            actor: Actor::System,
            command_type: invitation::ACCEPT.to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"user_id":"user-1"}"#.to_owned(),
            },
        };

        Invitation::prepare(pending, accept)
            .unwrap()
            .events
            .remove(0)
    }

    #[tokio::test]
    async fn accepting_an_invitation_provisions_the_user_replay_safely() {
        let event = acceptance_event();
        let consumer = InMemoryConsumer::with(vec![SagaMessage {
            group_id: "tenant-1".to_owned(),
            subject: format!("loomery.events.tenant-1.{}", invitation::ACCEPTED),
            message_id: "tenant-1:1:e0".to_owned(),
            log_index: 1,
            event: event.clone(),
        }]);
        let groups = registry().await;
        let runner = SagaRunner::new(consumer, InvitationAcceptance, groups.clone());

        assert!(runner.run_once().await.unwrap());

        let organization_id = Id::from("org-1");
        let events = groups
            .group
            .committed_events(&organization_id)
            .await
            .unwrap();
        let types: Vec<&str> = events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(
            types,
            ["organization.member_assigned", "membership.member_added"]
        );
        assert_eq!(
            events[1].actor,
            Actor::Saga {
                user_id: None,
                name: "InvitationAcceptanceSaga".to_owned()
            }
        );

        // A redelivery replays: derived identity means nothing is duplicated.
        let redelivery = InMemoryConsumer::with(vec![SagaMessage {
            group_id: "tenant-1".to_owned(),
            subject: format!("loomery.events.tenant-1.{}", invitation::ACCEPTED),
            message_id: "tenant-1:1:e0".to_owned(),
            log_index: 1,
            event,
        }]);
        let rerunner = SagaRunner::new(redelivery, InvitationAcceptance, groups.clone());
        assert!(rerunner.run_once().await.unwrap());

        let events = groups
            .group
            .committed_events(&organization_id)
            .await
            .unwrap();
        assert_eq!(events.len(), 2, "a replayed acceptance adds no new events");
    }
}
