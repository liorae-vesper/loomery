// SPDX-License-Identifier: MPL-2.0

//! The workspace aggregate — the **bootstrap slice**.
//!
//! Genesis ② (`workspace.create`) creates the organization's default
//! workspace. The command and its event (`workspace.created`) are the frozen
//! wire contract of the genesis script; this module implements exactly that
//! slice of the aggregate.
//!
//! The workspace the script creates is identified by a **derived** id (D12):
//! `workspace_id` arrives inside the command payload, because "the one default
//! workspace of organization X" is a function of the workflow, not a minted
//! value.

use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use crate::id::Id;
use serde::{Deserialize, Serialize};

/// The `command_type` of genesis ② — part of the control-plane wire contract.
pub const CREATE: &str = "workspace.create";

/// The `event_type` genesis ② must produce — part of the wire contract.
pub const CREATED: &str = "workspace.created";

/// The payload of `workspace.create`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkspace {
    /// The id the workspace must be created with — derived, not minted, so a
    /// resumed attempt asks for the same entity (D12).
    pub workspace_id: Id,
    /// The workspace's name.
    pub name: String,
}

/// The payload of `workspace.created`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCreated {
    /// The workspace's id.
    pub workspace_id: Id,
    /// The workspace's name.
    pub name: String,
}

/// Everything the workspace aggregate remembers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceState {
    /// The workspace's id, once ② has been applied.
    pub workspace_id: Option<Id>,
    /// The workspace's name, once ② has been applied.
    pub name: Option<String>,
}

/// Why the workspace plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed `CreateWorkspace`.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
}

/// The workspace aggregate's plan — a zero-sized marker; the type is the plan.
pub struct Workspace;

impl AggregatePlan<WorkspaceState, WorkspaceCode> for Workspace {
    fn prepare(
        _state: WorkspaceState,
        command: Command,
    ) -> Result<Execution, DomainError<WorkspaceCode>> {
        if command.command_type != CREATE {
            return Err(DomainError::new(
                WorkspaceCode::UnknownCommand,
                "the workspace plan handles workspace.create only",
            ));
        }

        let payload: CreateWorkspace =
            serde_json::from_str(&command.payload.data).map_err(|cause| {
                DomainError::with_cause(
                    WorkspaceCode::InvalidPayload,
                    "workspace.create payload is malformed",
                    Some(anyhow::Error::new(cause)),
                )
            })?;

        // The event is scoped to the workspace it creates — never to the
        // command's (optional) `workspace_id`, which for ② is the same id but
        // is not what identifies the event.
        let data = serde_json::to_string(&WorkspaceCreated {
            workspace_id: payload.workspace_id.clone(),
            name: payload.name.clone(),
        })
        .map_err(|cause| {
            DomainError::with_cause(
                WorkspaceCode::UnserializableEvent,
                "could not serialize workspace.created",
                Some(anyhow::Error::new(cause)),
            )
        })?;

        let mut event = event_from_command(
            &command,
            0,
            CREATED,
            Payload {
                version: command.payload.version,
                data,
            },
        );
        event.workspace_id = Some(payload.workspace_id);

        Ok(Execution {
            events: vec![event],
            outbound_events: Vec::new(),
        })
    }

    fn apply(state: WorkspaceState, event: Event) -> WorkspaceState {
        if event.event_type != CREATED {
            return state;
        }

        match serde_json::from_str::<WorkspaceCreated>(&event.payload.data) {
            Ok(created) => WorkspaceState {
                workspace_id: Some(created.workspace_id),
                name: Some(created.name),
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
            id: Id::from("cmd-2"),
            aggregate_id: Id::from("ws-1"),
            organization_id: Id::from("org-1"),
            workspace_id: Some(Id::from("ws-1")),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, "create-workspace"),
            correlation_key: Key::new(&KEY_NS, "bootstrap"),
            actor: Actor::System,
            command_type: CREATE.to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"workspace_id":"ws-1","name":"General"}"#.to_owned(),
            },
        }
    }

    #[test]
    fn prepare_builds_the_created_event_scoped_to_the_workspace() {
        let execution = Workspace::prepare(WorkspaceState::default(), command()).unwrap();

        let event = &execution.events[0];
        assert_eq!(event.event_type, CREATED);
        assert_eq!(event.workspace_id, Some(Id::from("ws-1")));
        assert_eq!(event.aggregate_id, Id::from("ws-1"));
        assert_eq!(
            serde_json::from_str::<WorkspaceCreated>(&event.payload.data).unwrap(),
            WorkspaceCreated {
                workspace_id: Id::from("ws-1"),
                name: "General".to_owned()
            }
        );
    }

    #[test]
    fn applying_the_event_records_the_workspace() {
        let event = Workspace::prepare(WorkspaceState::default(), command())
            .unwrap()
            .events
            .remove(0);

        let state = Workspace::apply(WorkspaceState::default(), event);
        assert_eq!(state.workspace_id, Some(Id::from("ws-1")));
        assert_eq!(state.name.as_deref(), Some("General"));
    }

    #[test]
    fn prepare_rejects_an_unknown_command() {
        let mut cmd = command();
        cmd.command_type = "task.create".to_owned();

        let error = Workspace::prepare(WorkspaceState::default(), cmd).unwrap_err();
        assert_eq!(error.code, WorkspaceCode::UnknownCommand);
    }

    #[test]
    fn prepare_rejects_a_malformed_payload() {
        let mut cmd = command();
        cmd.payload.data = "{}".to_owned();

        let error = Workspace::prepare(WorkspaceState::default(), cmd).unwrap_err();
        assert_eq!(error.code, WorkspaceCode::InvalidPayload);
    }
}
