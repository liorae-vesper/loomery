// SPDX-License-Identifier: MPL-2.0

//! The workspace aggregate.
//!
//! A collaboration space inside an organization. Genesis 2 creates the default
//! one with a **derived** id (`default_workspace_id(org)`), so a resumed
//! bootstrap cannot create a second; later workspaces are created with a minted
//! id carried in the command payload.
//!
//! The wire contract — `workspace.create` → `workspace.created` — is frozen by
//! the genesis script. Archiving is a compensating, monotonic event: there is
//! no workspace deletion.
//!
//! See `docs/domain-model.md` for the command/event table and transition matrix.

use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use crate::id::Id;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The `command_type` of genesis 2 — part of the control-plane wire contract.
pub const CREATE: &str = "workspace.create";

/// The `event_type` genesis 2 must produce — part of the wire contract.
pub const CREATED: &str = "workspace.created";

/// `workspace.rename` — replace the workspace's display name.
pub const RENAME: &str = "workspace.rename";

/// `workspace.renamed` — the name was replaced.
pub const RENAMED: &str = "workspace.renamed";

/// `workspace.archive` — retire the workspace (append-only; no delete).
pub const ARCHIVE: &str = "workspace.archive";

/// `workspace.archived` — the workspace was retired.
pub const ARCHIVED: &str = "workspace.archived";

/// The maximum workspace-name length, in bytes (D10).
pub const MAX_NAME_BYTES: usize = 200;

/// The payload of `workspace.create`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkspace {
    /// The id the workspace must be created with — derived for the default
    /// workspace, minted otherwise (D12), so a resumed attempt asks for the
    /// same entity.
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

/// The payload of `workspace.rename`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rename {
    /// The workspace's new display name.
    pub name: String,
}

/// The payload of `workspace.renamed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Renamed {
    /// The workspace's new display name.
    pub name: String,
}

/// The (empty) payload of `workspace.archive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Archive {}

/// The (empty) payload of `workspace.archived`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Archived {}

/// Everything the workspace aggregate remembers.
///
/// `#[serde(default)]` keeps older snapshots decodable as fields are added.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkspaceState {
    /// The workspace's id, once created.
    pub workspace_id: Option<Id>,
    /// The workspace's name, once created.
    pub name: Option<String>,
    /// Whether the workspace has been archived. Monotonic: never unset.
    pub archived: bool,
}

impl WorkspaceState {
    /// Whether the workspace exists. Creation sets both id and name.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.workspace_id.is_some() || self.name.is_some()
    }
}

/// Why the workspace plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed shape for its type.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
    /// The workspace has already been created.
    AlreadyCreated,
    /// The workspace has not been created yet.
    NotCreated,
    /// The workspace is archived and accepts no further changes.
    Archived,
    /// The workspace has already been archived.
    AlreadyArchived,
    /// The requested name is empty or longer than [`MAX_NAME_BYTES`].
    InvalidName,
}

/// The workspace aggregate's plan — a zero-sized marker; the type is the plan.
pub struct Workspace;

impl AggregatePlan<WorkspaceState, WorkspaceCode> for Workspace {
    fn prepare(
        state: WorkspaceState,
        command: Command,
    ) -> Result<Execution, DomainError<WorkspaceCode>> {
        match command.command_type.as_str() {
            CREATE => create(&state, &command),
            RENAME => rename(&state, &command),
            ARCHIVE => archive(&state, &command),
            _ => Err(reject(
                WorkspaceCode::UnknownCommand,
                "the workspace plan handles workspace.create, workspace.rename and workspace.archive only",
            )),
        }
    }

    fn apply(state: WorkspaceState, event: Event) -> WorkspaceState {
        match event.event_type.as_str() {
            CREATED => match decode::<WorkspaceCreated>(&event.payload.data) {
                Ok(created) => WorkspaceState {
                    workspace_id: Some(created.workspace_id),
                    name: Some(created.name),
                    archived: state.archived,
                },
                Err(_) => state,
            },
            RENAMED => match decode::<Renamed>(&event.payload.data) {
                Ok(renamed) => WorkspaceState {
                    name: Some(renamed.name),
                    ..state
                },
                Err(_) => state,
            },
            ARCHIVED => WorkspaceState {
                archived: true,
                ..state
            },
            // An event this plan cannot read is not ours to fold.
            _ => state,
        }
    }
}

/// Genesis ② (and later workspace creation): create the workspace. Re-creating
/// is a different intent and is rejected; the dedup window handles the retry.
fn create(
    state: &WorkspaceState,
    command: &Command,
) -> Result<Execution, DomainError<WorkspaceCode>> {
    if state.is_created() {
        return Err(reject(
            WorkspaceCode::AlreadyCreated,
            "the workspace has already been created",
        ));
    }

    let payload: CreateWorkspace = decode_command(command)?;
    if !valid_name(&payload.name) {
        return Err(reject(
            WorkspaceCode::InvalidName,
            "the workspace name must be non-empty and within the length bound",
        ));
    }

    let data = encode(
        &WorkspaceCreated {
            workspace_id: payload.workspace_id.clone(),
            name: payload.name,
        },
        CREATED,
    )?;

    // The event is scoped to the workspace it creates.
    let mut event = event_from_command(
        command,
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

/// Replace the name. Requires a created, unarchived workspace.
fn rename(
    state: &WorkspaceState,
    command: &Command,
) -> Result<Execution, DomainError<WorkspaceCode>> {
    if state.archived {
        return Err(reject(
            WorkspaceCode::Archived,
            "an archived workspace cannot be renamed",
        ));
    }
    if !state.is_created() {
        return Err(reject(
            WorkspaceCode::NotCreated,
            "the workspace has not been created yet",
        ));
    }

    let payload: Rename = decode_command(command)?;
    if !valid_name(&payload.name) {
        return Err(reject(
            WorkspaceCode::InvalidName,
            "the workspace name must be non-empty and within the length bound",
        ));
    }

    let data = encode(&Renamed { name: payload.name }, RENAMED)?;
    Ok(execution(command, RENAMED, data))
}

/// Retire the workspace. Requires a created, unarchived workspace.
fn archive(
    state: &WorkspaceState,
    command: &Command,
) -> Result<Execution, DomainError<WorkspaceCode>> {
    if state.archived {
        return Err(reject(
            WorkspaceCode::AlreadyArchived,
            "the workspace has already been archived",
        ));
    }
    if !state.is_created() {
        return Err(reject(
            WorkspaceCode::NotCreated,
            "the workspace has not been created yet",
        ));
    }

    let _: Archive = decode_command(command)?;
    let data = encode(&Archived {}, ARCHIVED)?;
    Ok(execution(command, ARCHIVED, data))
}

/// Whether `name` is a legal workspace name: non-blank and within
/// [`MAX_NAME_BYTES`].
fn valid_name(name: &str) -> bool {
    !name.trim().is_empty() && name.len() <= MAX_NAME_BYTES
}

/// Builds the single event a command produces.
fn execution(command: &Command, event_type: &str, data: String) -> Execution {
    let version = command.payload.version;
    Execution {
        events: vec![event_from_command(
            command,
            0,
            event_type,
            Payload { version, data },
        )],
        outbound_events: Vec::new(),
    }
}

/// Decodes a command's payload, mapping failures to
/// [`WorkspaceCode::InvalidPayload`].
fn decode_command<T: DeserializeOwned>(command: &Command) -> Result<T, DomainError<WorkspaceCode>> {
    decode(&command.payload.data)
}

/// Decodes a payload string.
fn decode<T: DeserializeOwned>(data: &str) -> Result<T, DomainError<WorkspaceCode>> {
    serde_json::from_str(data).map_err(|cause| {
        DomainError::with_cause(
            WorkspaceCode::InvalidPayload,
            "the workspace payload is malformed",
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Serializes an event payload, mapping failures to
/// [`WorkspaceCode::UnserializableEvent`].
fn encode<T: Serialize>(value: &T, event_type: &str) -> Result<String, DomainError<WorkspaceCode>> {
    serde_json::to_string(value).map_err(|cause| {
        DomainError::with_cause(
            WorkspaceCode::UnserializableEvent,
            &format!("could not serialize {event_type}"),
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Builds a domain error for `code`.
fn reject(code: WorkspaceCode, message: &str) -> DomainError<WorkspaceCode> {
    DomainError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Actor;
    use crate::key::Key;
    use crate::timestamp::Timestamp;
    use proptest::prelude::*;
    use uuid::Uuid;

    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    fn command(command_type: &str, payload: &str) -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-2"),
            aggregate_id: Id::from("ws-1"),
            organization_id: Id::from("org-1"),
            workspace_id: Some(Id::from("ws-1")),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, &format!("{command_type}:{payload}")),
            correlation_key: Key::new(&KEY_NS, "bootstrap"),
            actor: Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload.to_owned(),
            },
        }
    }

    fn create_ws(name: &str) -> Command {
        command(
            CREATE,
            &format!(r#"{{"workspace_id":"ws-1","name":"{name}"}}"#),
        )
    }

    fn rename_ws(name: &str) -> Command {
        command(RENAME, &format!(r#"{{"name":"{name}"}}"#))
    }

    fn archive_ws() -> Command {
        command(ARCHIVE, "{}")
    }

    fn code_of<T>(result: Result<T, DomainError<WorkspaceCode>>) -> Option<WorkspaceCode> {
        result.err().map(|error| error.code)
    }

    fn advance(state: WorkspaceState, command: Command) -> WorkspaceState {
        let execution = Workspace::prepare(state.clone(), command).unwrap();
        execution.events.into_iter().fold(state, Workspace::apply)
    }

    fn created() -> WorkspaceState {
        WorkspaceState {
            workspace_id: Some(Id::from("ws-1")),
            name: Some("General".to_owned()),
            archived: false,
        }
    }

    /// A fixture event from a command envelope with a different type.
    fn typed(command: Command, event_type: &str) -> Event {
        Event {
            envelope_version: command.envelope_version,
            id: command.event_id(0),
            aggregate_id: command.aggregate_id,
            organization_id: command.organization_id,
            workspace_id: command.workspace_id,
            occurred_at: command.occurred_at,
            causation_key: command.causation_key,
            correlation_key: command.correlation_key,
            actor: command.actor,
            event_type: event_type.to_owned(),
            payload: command.payload,
        }
    }

    // --- frozen genesis behavior -----------------------------------------

    #[test]
    fn create_builds_the_frozen_event_scoped_to_the_workspace() {
        let command = create_ws("General");
        let execution = Workspace::prepare(WorkspaceState::default(), command.clone()).unwrap();
        let event = &execution.events[0];

        assert_eq!(event.event_type, CREATED);
        assert_eq!(event.workspace_id, Some(Id::from("ws-1")));
        assert_eq!(event.aggregate_id, Id::from("ws-1"));
        assert_eq!(event.id, command.event_id(0));
        assert_eq!(
            decode::<WorkspaceCreated>(&event.payload.data).unwrap(),
            WorkspaceCreated {
                workspace_id: Id::from("ws-1"),
                name: "General".to_owned()
            }
        );
    }

    #[test]
    fn applying_created_records_the_workspace() {
        let state = advance(WorkspaceState::default(), create_ws("General"));
        assert_eq!(state.workspace_id, Some(Id::from("ws-1")));
        assert_eq!(state.name.as_deref(), Some("General"));
        assert!(!state.archived);
    }

    #[test]
    fn create_rejects_an_existing_workspace_and_a_blank_name() {
        assert_eq!(
            code_of(Workspace::prepare(created(), create_ws("Other"))),
            Some(WorkspaceCode::AlreadyCreated)
        );
        assert_eq!(
            code_of(Workspace::prepare(
                WorkspaceState::default(),
                create_ws("   ")
            )),
            Some(WorkspaceCode::InvalidName)
        );
    }

    // --- rename / archive ------------------------------------------------

    #[test]
    fn rename_replaces_the_name() {
        let state = advance(created(), rename_ws("Engineering"));
        assert_eq!(state.name.as_deref(), Some("Engineering"));
        assert_eq!(state.workspace_id, Some(Id::from("ws-1")));
    }

    #[test]
    fn rename_requires_a_created_unarchived_workspace() {
        assert_eq!(
            code_of(Workspace::prepare(
                WorkspaceState::default(),
                rename_ws("Engineering")
            )),
            Some(WorkspaceCode::NotCreated)
        );

        let archived = WorkspaceState {
            archived: true,
            ..created()
        };
        assert_eq!(
            code_of(Workspace::prepare(archived, rename_ws("Engineering"))),
            Some(WorkspaceCode::Archived)
        );
    }

    #[test]
    fn archive_is_monotonic_and_requires_creation() {
        assert_eq!(
            code_of(Workspace::prepare(WorkspaceState::default(), archive_ws())),
            Some(WorkspaceCode::NotCreated)
        );

        let state = advance(created(), archive_ws());
        assert!(state.archived);
        assert_eq!(state.name.as_deref(), Some("General"));

        assert_eq!(
            code_of(Workspace::prepare(state, archive_ws())),
            Some(WorkspaceCode::AlreadyArchived)
        );
    }

    // --- unknown / malformed ---------------------------------------------

    #[test]
    fn unknown_commands_and_malformed_payloads_are_rejected() {
        assert_eq!(
            code_of(Workspace::prepare(
                created(),
                command("workspace.move", "{}")
            )),
            Some(WorkspaceCode::UnknownCommand)
        );

        let mut malformed = create_ws("General");
        malformed.payload.data = "not json".to_owned();
        let error = Workspace::prepare(WorkspaceState::default(), malformed).unwrap_err();
        assert_eq!(error.code, WorkspaceCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_and_malformed_events() {
        let foreign = typed(create_ws("General"), "task.created");
        assert_eq!(
            Workspace::apply(WorkspaceState::default(), foreign),
            WorkspaceState::default()
        );

        let malformed = Event {
            payload: Payload {
                version: 1,
                data: "not json".to_owned(),
            },
            ..typed(create_ws("General"), CREATED)
        };
        assert_eq!(
            Workspace::apply(WorkspaceState::default(), malformed),
            WorkspaceState::default()
        );
    }

    // --- transition matrix -----------------------------------------------

    #[test]
    fn transition_matrix_matches_the_documented_table() {
        let fresh = WorkspaceState::default();
        let live = created();
        let archived = WorkspaceState {
            archived: true,
            ..created()
        };

        // create
        assert!(Workspace::prepare(fresh.clone(), create_ws("General")).is_ok());
        assert_eq!(
            code_of(Workspace::prepare(live.clone(), create_ws("General"))),
            Some(WorkspaceCode::AlreadyCreated)
        );

        // rename
        assert_eq!(
            code_of(Workspace::prepare(fresh.clone(), rename_ws("Engineering"))),
            Some(WorkspaceCode::NotCreated)
        );
        assert!(Workspace::prepare(live.clone(), rename_ws("Engineering")).is_ok());
        assert_eq!(
            code_of(Workspace::prepare(
                archived.clone(),
                rename_ws("Engineering")
            )),
            Some(WorkspaceCode::Archived)
        );

        // archive
        assert_eq!(
            code_of(Workspace::prepare(fresh, archive_ws())),
            Some(WorkspaceCode::NotCreated)
        );
        assert!(Workspace::prepare(live, archive_ws()).is_ok());
        assert_eq!(
            code_of(Workspace::prepare(archived, archive_ws())),
            Some(WorkspaceCode::AlreadyArchived)
        );
    }

    // --- property tests --------------------------------------------------

    fn operation() -> impl Strategy<Value = u8> {
        0u8..6
    }

    fn command_for(op: u8) -> Command {
        match op {
            0 => create_ws("General"),
            1 => create_ws(""),
            2 => rename_ws("Engineering"),
            3 => rename_ws("   "),
            4 => archive_ws(),
            _ => command("workspace.move", "{}"),
        }
    }

    proptest! {
        // prepare/apply never panic, creation is one-way and archive monotonic.
        #[test]
        fn arbitrary_command_sequences_respect_the_invariants(
            ops in prop::collection::vec(operation(), 0..64),
        ) {
            let mut state = WorkspaceState::default();
            let mut created_seen = false;
            let mut archive_seen = false;

            for op in ops {
                if let Ok(execution) = Workspace::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Workspace::apply(state, event);
                    }
                }

                if state.is_created() {
                    created_seen = true;
                }
                if state.archived {
                    archive_seen = true;
                }

                // Created and archived are monotonic — nothing can unset them.
                prop_assert!(!created_seen || state.is_created());
                prop_assert!(!archive_seen || state.archived);
                // The workspace id never changes once set.
                if let Some(workspace_id) = &state.workspace_id {
                    prop_assert_eq!(workspace_id, &Id::from("ws-1"));
                }
            }
        }

        // Replaying the same events always yields the same state.
        #[test]
        fn replay_is_deterministic(ops in prop::collection::vec(operation(), 0..64)) {
            let mut state = WorkspaceState::default();
            let mut events = Vec::new();

            for op in ops {
                if let Ok(execution) = Workspace::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Workspace::apply(state, event.clone());
                        events.push(event);
                    }
                }
            }

            let replayed = events
                .iter()
                .cloned()
                .fold(WorkspaceState::default(), Workspace::apply);
            prop_assert_eq!(replayed, state);
        }
    }
}
