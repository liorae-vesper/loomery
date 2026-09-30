// SPDX-License-Identifier: MPL-2.0

//! The workspace-membership aggregate — the **bootstrap slice**.
//!
//! Genesis ③ (`membership.add_owner`) adds the organization's creator to the
//! default workspace as its Owner. The command and its event
//! (`membership.owner_added`) are the frozen wire contract of the genesis
//! script; this module implements exactly that slice of the aggregate.
//!
//! The membership entity is identified by a **derived** id (D12): the
//! membership's `aggregate_id` is `owner_membership_id(organization, user)`,
//! so a resumed ③ addresses the same entity the crashed attempt did.

use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use crate::id::Id;
use serde::{Deserialize, Serialize};

/// The `command_type` of genesis ③ — part of the control-plane wire contract.
pub const ADD_OWNER: &str = "membership.add_owner";

/// The `event_type` genesis ③ must produce — part of the wire contract.
pub const OWNER_ADDED: &str = "membership.owner_added";

/// The payload of `membership.add_owner`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddOwner {
    /// The user who joins the workspace as Owner.
    pub user_id: Id,
}

/// The payload of `membership.owner_added`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerAdded {
    /// The user who became the workspace's Owner.
    pub user_id: Id,
}

/// Everything the membership aggregate remembers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMembershipState {
    /// The user holding the Owner role, once ③ has been applied.
    pub owner_user_id: Option<Id>,
}

/// Why the membership plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed `AddOwner`.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
}

/// The membership aggregate's plan — a zero-sized marker; the type is the plan.
pub struct WorkspaceMembership;

impl AggregatePlan<WorkspaceMembershipState, MembershipCode> for WorkspaceMembership {
    fn prepare(
        _state: WorkspaceMembershipState,
        command: Command,
    ) -> Result<Execution, DomainError<MembershipCode>> {
        if command.command_type != ADD_OWNER {
            return Err(DomainError::new(
                MembershipCode::UnknownCommand,
                "the membership plan handles membership.add_owner only",
            ));
        }

        let payload: AddOwner = serde_json::from_str(&command.payload.data).map_err(|cause| {
            DomainError::with_cause(
                MembershipCode::InvalidPayload,
                "membership.add_owner payload is malformed",
                Some(anyhow::Error::new(cause)),
            )
        })?;

        let data = serde_json::to_string(&OwnerAdded {
            user_id: payload.user_id,
        })
        .map_err(|cause| {
            DomainError::with_cause(
                MembershipCode::UnserializableEvent,
                "could not serialize membership.owner_added",
                Some(anyhow::Error::new(cause)),
            )
        })?;

        Ok(Execution {
            events: vec![event_from_command(
                &command,
                0,
                OWNER_ADDED,
                Payload {
                    version: command.payload.version,
                    data,
                },
            )],
            outbound_events: Vec::new(),
        })
    }

    fn apply(state: WorkspaceMembershipState, event: Event) -> WorkspaceMembershipState {
        if event.event_type != OWNER_ADDED {
            return state;
        }

        match serde_json::from_str::<OwnerAdded>(&event.payload.data) {
            Ok(added) => WorkspaceMembershipState {
                owner_user_id: Some(added.user_id),
            },
            Err(_) => state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Actor;
    use crate::key::Key;
    use crate::timestamp::Timestamp;
    use uuid::Uuid;

    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    fn command() -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-3"),
            aggregate_id: Id::from("membership-1"),
            organization_id: Id::from("org-1"),
            workspace_id: Some(Id::from("ws-1")),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, "add-owner"),
            correlation_key: Key::new(&KEY_NS, "bootstrap"),
            actor: Actor::System,
            command_type: ADD_OWNER.to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"user_id":"user-1"}"#.to_owned(),
            },
        }
    }

    #[test]
    fn prepare_builds_the_owner_added_event() {
        let execution =
            WorkspaceMembership::prepare(WorkspaceMembershipState::default(), command()).unwrap();

        let event = &execution.events[0];
        assert_eq!(event.event_type, OWNER_ADDED);
        assert_eq!(event.workspace_id, Some(Id::from("ws-1")));
        assert_eq!(event.actor, command().actor);
        assert_eq!(
            serde_json::from_str::<OwnerAdded>(&event.payload.data).unwrap(),
            OwnerAdded {
                user_id: Id::from("user-1")
            }
        );
    }

    #[test]
    fn applying_the_event_records_the_owner() {
        let event = WorkspaceMembership::prepare(WorkspaceMembershipState::default(), command())
            .unwrap()
            .events
            .remove(0);

        let state = WorkspaceMembership::apply(WorkspaceMembershipState::default(), event);
        assert_eq!(state.owner_user_id, Some(Id::from("user-1")));
    }

    #[test]
    fn prepare_rejects_an_unknown_command() {
        let mut cmd = command();
        cmd.command_type = "task.create".to_owned();

        let error =
            WorkspaceMembership::prepare(WorkspaceMembershipState::default(), cmd).unwrap_err();
        assert_eq!(error.code, MembershipCode::UnknownCommand);
    }

    #[test]
    fn prepare_rejects_a_malformed_payload() {
        let mut cmd = command();
        cmd.payload.data = "null".to_owned();

        let error =
            WorkspaceMembership::prepare(WorkspaceMembershipState::default(), cmd).unwrap_err();
        assert_eq!(error.code, MembershipCode::InvalidPayload);
    }
}
