// SPDX-License-Identifier: MPL-2.0

//! Test doubles shared by the shell's tests.

use std::future::Future;

use crate::group::{GroupOps, ProposeOutcome};
use loomery_core::envelope::{Command, Event, Payload};
use loomery_core::id::Id;
use loomery_core::timestamp::Timestamp;
use loomery_genesis::{Bootstrap, Step, step_key};

/// A well-formed organization id for the tests below.
pub(crate) fn organization() -> Id {
    Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9")
}

/// The worker's inputs, with the injected timestamp pinned.
pub(crate) fn bootstrap_value() -> Bootstrap {
    Bootstrap {
        organization_id: organization(),
        leader_user_id: Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0"),
        occurred_at: Timestamp::from(1_700_000_000_000),
    }
}

/// The namespace the test helpers' derived keys use.
const KEY_NS: loomery_core::Uuid =
    loomery_core::Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

/// Assigns `user_id` to `organization_id` in `group`, exactly as the invitation
/// saga does: the stream id is derived from the business tuple (D12), so the
/// gateway's membership check finds it.
pub(crate) async fn assign_member(
    group: &mut crate::raft::RaftGroup,
    organization_id: &loomery_core::id::Id,
    user_id: &loomery_core::id::Id,
) {
    use crate::group::GroupOps;
    use loomery_core::actor::Actor;
    use loomery_core::envelope::Command;
    use loomery_core::envelope::Payload;
    use loomery_core::id::Id;
    use loomery_core::key::Key;
    use loomery_core::membership;
    use loomery_core::timestamp::Timestamp;

    let command = Command {
        envelope_version: 1,
        id: Id::new(),
        aggregate_id: membership::organization_assignment_id(organization_id, user_id),
        organization_id: organization_id.clone(),
        workspace_id: None,
        occurred_at: Timestamp::from(1_700_000_000_000),
        causation_key: Key::new(&KEY_NS, &format!("assign:{organization_id}:{user_id}")),
        correlation_key: Key::new(&KEY_NS, "test-support"),
        actor: Actor::System,
        command_type: membership::ASSIGN_MEMBER.to_owned(),
        payload: Payload {
            version: 1,
            data: format!(r#"{{"user_id":"{user_id}"}}"#),
        },
    };

    group.propose(command).await.unwrap();
}

/// The group, faked: it appends commands, "applies" the event an aggregate
/// would produce, and can lose a response so the tests can crash the worker at
/// the worst possible moment.
#[derive(Default)]
pub(crate) struct FakeGroup {
    pub(crate) events: Vec<Event>,
    pub(crate) appended: Vec<Command>,
    /// When set to `n`, the `n`th append is committed and *then* reported as a
    /// failure — the classic "the write succeeded, the client never heard
    /// back".
    pub(crate) lose_response_after: Option<usize>,
}

impl GroupOps for FakeGroup {
    fn committed_events(
        &self,
        _organization_id: &Id,
    ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send {
        std::future::ready(Ok(self.events.clone()))
    }

    fn propose(
        &mut self,
        command: Command,
    ) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send {
        std::future::ready(self.apply(&command))
    }
}

impl FakeGroup {
    /// The append path, sync so tests can pre-seed a group and inspect the
    /// result without a runtime.
    pub(crate) fn test_propose(&mut self, command: &Command) -> anyhow::Result<ProposeOutcome> {
        self.apply(command)
    }

    fn apply(&mut self, command: &Command) -> anyhow::Result<ProposeOutcome> {
        // The state machine's dedup window: same intent, no new events.
        if let Some(previous) = self
            .appended
            .iter()
            .find(|previous| previous.causation_key == command.causation_key)
        {
            return Ok(ProposeOutcome::Replayed {
                first_log_index: 0,
                fingerprint: previous.fingerprint(),
            });
        }

        self.appended.push(command.clone());
        self.events.push(event_for(command));

        if self.lose_response_after == Some(self.appended.len()) {
            anyhow::bail!("the response was lost");
        }

        Ok(ProposeOutcome::Appended { first_log_index: 0 })
    }
}

/// What the aggregate's `apply` would put in the log. The script reads only the
/// causation key back out, so the fake copies the command's identity.
pub(crate) fn event_for(command: &Command) -> Event {
    let mycommand = command.clone();
    Event {
        envelope_version: mycommand.envelope_version,
        id: mycommand.event_id(0),
        aggregate_id: mycommand.aggregate_id,
        organization_id: mycommand.organization_id,
        workspace_id: mycommand.workspace_id,
        occurred_at: mycommand.occurred_at,
        causation_key: mycommand.causation_key,
        correlation_key: mycommand.correlation_key,
        actor: mycommand.actor,
        event_type: "genesis.test".to_owned(),
        payload: Payload {
            version: 1,
            data: "{}".to_owned(),
        },
    }
}

/// How many events carry `step`'s causation key — the "no duplicate genesis"
/// assertion.
pub(crate) fn committed(group: &FakeGroup, step: Step) -> usize {
    let key = step_key(&organization(), step);
    group
        .events
        .iter()
        .filter(|event| event.causation_key == key)
        .count()
}
