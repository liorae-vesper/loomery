// SPDX-License-Identifier: MPL-2.0

//! The organization aggregate — the **bootstrap slice**.
//!
//! Genesis ① (`organization.assign_leader`) grants the organization's creator
//! leadership. That command and its event (`organization.leader_assigned`) are
//! the frozen wire contract of the genesis script
//! ([`loomery_genesis::Step`](https://docs.rs/loomery-genesis)); this module
//! implements exactly that slice of the aggregate, so the shell's state machine
//! can run the pure core instead of hand-building events.
//!
//! The remaining organization commands (registration, renaming, membership
//! rules) land here as the full Phase-0 aggregate does.

use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use crate::id::Id;
use serde::{Deserialize, Serialize};

/// The `command_type` of genesis ① — part of the control-plane wire contract.
pub const ASSIGN_LEADER: &str = "organization.assign_leader";

/// The `event_type` genesis ① must produce — part of the wire contract.
pub const LEADER_ASSIGNED: &str = "organization.leader_assigned";

/// The payload of `organization.assign_leader`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignLeader {
    /// The user who becomes the organization's leader.
    pub user_id: Id,
}

/// The payload of `organization.leader_assigned`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderAssigned {
    /// The user who became the organization's leader.
    pub user_id: Id,
}

/// Everything the organization aggregate remembers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationState {
    /// The user holding leadership, once ① has been applied.
    pub leader_user_id: Option<Id>,
}

/// Why the organization plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrganizationCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed `AssignLeader`.
    InvalidPayload,
    /// The event payload could not be serialized (cannot happen for ids).
    UnserializableEvent,
}

/// The organization aggregate's plan — a zero-sized marker; the type is the plan.
pub struct Organization;

impl AggregatePlan<OrganizationState, OrganizationCode> for Organization {
    fn prepare(
        _state: OrganizationState,
        command: Command,
    ) -> Result<Execution, DomainError<OrganizationCode>> {
        if command.command_type != ASSIGN_LEADER {
            return Err(DomainError::new(
                OrganizationCode::UnknownCommand,
                "the organization plan handles organization.assign_leader only",
            ));
        }

        let payload: AssignLeader =
            serde_json::from_str(&command.payload.data).map_err(|cause| {
                DomainError::with_cause(
                    OrganizationCode::InvalidPayload,
                    "organization.assign_leader payload is malformed",
                    Some(anyhow::Error::new(cause)),
                )
            })?;

        let data = serde_json::to_string(&LeaderAssigned {
            user_id: payload.user_id,
        })
        .map_err(|cause| {
            DomainError::with_cause(
                OrganizationCode::UnserializableEvent,
                "could not serialize organization.leader_assigned",
                Some(anyhow::Error::new(cause)),
            )
        })?;

        Ok(Execution {
            events: vec![event_from_command(
                &command,
                0,
                LEADER_ASSIGNED,
                Payload {
                    version: command.payload.version,
                    data,
                },
            )],
            outbound_events: Vec::new(),
        })
    }

    fn apply(state: OrganizationState, event: Event) -> OrganizationState {
        if event.event_type != LEADER_ASSIGNED {
            return state;
        }

        match serde_json::from_str::<LeaderAssigned>(&event.payload.data) {
            Ok(assigned) => OrganizationState {
                leader_user_id: Some(assigned.user_id),
            },
            // An event this plan cannot read is not ours to fold; leave the
            // state untouched rather than guessing.
            Err(_) => state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Actor;
    use crate::envelope::Payload;
    use crate::id::Id;
    use crate::key::Key;
    use crate::timestamp::Timestamp;
    use uuid::Uuid;

    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    fn command() -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-1"),
            aggregate_id: Id::from("org-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, "assign-leader"),
            correlation_key: Key::new(&KEY_NS, "bootstrap"),
            actor: Actor::System,
            command_type: ASSIGN_LEADER.to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"user_id":"user-1"}"#.to_owned(),
            },
        }
    }

    #[test]
    fn prepare_builds_the_leader_assigned_event() {
        let execution = Organization::prepare(OrganizationState::default(), command()).unwrap();

        assert_eq!(execution.events.len(), 1);
        let event = &execution.events[0];
        assert_eq!(event.event_type, LEADER_ASSIGNED);
        assert_eq!(event.id, command().event_id(0));
        assert_eq!(event.causation_key, command().causation_key);
        assert_eq!(
            serde_json::from_str::<LeaderAssigned>(&event.payload.data).unwrap(),
            LeaderAssigned {
                user_id: Id::from("user-1")
            }
        );
    }

    #[test]
    fn applying_the_event_records_the_leader() {
        let event = Organization::prepare(OrganizationState::default(), command())
            .unwrap()
            .events
            .remove(0);

        let state = Organization::apply(OrganizationState::default(), event);
        assert_eq!(state.leader_user_id, Some(Id::from("user-1")));
    }

    #[test]
    fn prepare_rejects_an_unknown_command() {
        let mut cmd = command();
        cmd.command_type = "task.create".to_owned();

        let error = Organization::prepare(OrganizationState::default(), cmd).unwrap_err();
        assert_eq!(error.code, OrganizationCode::UnknownCommand);
    }

    #[test]
    fn prepare_rejects_a_malformed_payload() {
        let mut cmd = command();
        cmd.payload.data = "not json".to_owned();

        let error = Organization::prepare(OrganizationState::default(), cmd).unwrap_err();
        assert_eq!(error.code, OrganizationCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_events() {
        let mut event = Organization::prepare(OrganizationState::default(), command())
            .unwrap()
            .events
            .remove(0);
        event.event_type = "task.created".to_owned();

        let state = Organization::apply(OrganizationState::default(), event);
        assert_eq!(state, OrganizationState::default());
    }
}
