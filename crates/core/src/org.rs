// SPDX-License-Identifier: MPL-2.0

//! The organization aggregate.
//!
//! Tenant-scoped facts about an organization: its name, its leader (genesis ①)
//! and whether it has been archived. The control plane's *registry* of
//! organizations is a separate concern; this plan is the domain state machine
//! that a tenant group folds.
//!
//! The wire contract — `organization.assign_leader` →
//! `organization.leader_assigned` — is frozen by the genesis script. The
//! commands added here (`organization.rename`, `organization.archive`) follow
//! the same append-only shape: a correction is a compensating event, never a
//! mutation, and no transition is deleted.
//!
//! See [`docs/domain-model.md`](../../../docs/domain-model.md) for the full
//! command/event table, payload shapes and transition matrix.

use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use crate::id::Id;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The `command_type` of genesis ① — part of the control-plane wire contract.
pub const ASSIGN_LEADER: &str = "organization.assign_leader";

/// The `event_type` genesis ① must produce — part of the wire contract.
pub const LEADER_ASSIGNED: &str = "organization.leader_assigned";

/// `organization.rename` — replace the organization's display name.
pub const RENAME: &str = "organization.rename";

/// `organization.renamed` — the name was replaced.
pub const RENAMED: &str = "organization.renamed";

/// `organization.archive` — retire the organization (append-only; no delete).
pub const ARCHIVE: &str = "organization.archive";

/// `organization.archived` — the organization was retired.
pub const ARCHIVED: &str = "organization.archived";

/// The maximum organization-name length, in bytes (D10).
pub const MAX_NAME_BYTES: usize = 200;

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

/// The payload of `organization.rename`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rename {
    /// The organization's new display name.
    pub name: String,
}

/// The payload of `organization.renamed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Renamed {
    /// The organization's new display name.
    pub name: String,
}

/// The (empty) payload of `organization.archive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Archive {}

/// The (empty) payload of `organization.archived`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Archived {}

/// Everything the organization aggregate remembers.
///
/// `#[serde(default)]` keeps snapshots written before a field was added
/// decodable — state is never rewritten, so old shapes must keep loading.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct OrganizationState {
    /// The organization's display name, once it has been renamed.
    pub name: Option<String>,
    /// The user holding leadership, once ① has been applied.
    pub leader_user_id: Option<Id>,
    /// Whether the organization has been archived. Monotonic: never unset.
    pub archived: bool,
}

impl OrganizationState {
    /// Whether the organization has been provisioned — a leader was assigned or
    /// a name was set. `rename` is only legal once this is true.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.leader_user_id.is_some() || self.name.is_some()
    }
}

/// Why the organization plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrganizationCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed shape for its type.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
    /// The organization has not been provisioned yet.
    NotCreated,
    /// The organization is archived and accepts no further changes.
    Archived,
    /// The organization has already been archived.
    AlreadyArchived,
    /// The requested name is empty or longer than [`MAX_NAME_BYTES`].
    InvalidName,
}

/// The organization aggregate's plan — a zero-sized marker; the type is the plan.
pub struct Organization;

impl AggregatePlan<OrganizationState, OrganizationCode> for Organization {
    fn prepare(
        state: OrganizationState,
        command: Command,
    ) -> Result<Execution, DomainError<OrganizationCode>> {
        match command.command_type.as_str() {
            ASSIGN_LEADER => assign_leader(&state, &command),
            RENAME => rename(&state, &command),
            ARCHIVE => archive(&state, &command),
            _ => Err(reject(
                OrganizationCode::UnknownCommand,
                "the organization plan handles organization.assign_leader, organization.rename and organization.archive only",
            )),
        }
    }

    fn apply(state: OrganizationState, event: Event) -> OrganizationState {
        match event.event_type.as_str() {
            LEADER_ASSIGNED => match decode::<LeaderAssigned>(&event.payload.data) {
                Ok(assigned) => OrganizationState {
                    leader_user_id: Some(assigned.user_id),
                    ..state
                },
                Err(_) => state,
            },
            RENAMED => match decode::<Renamed>(&event.payload.data) {
                Ok(renamed) => OrganizationState {
                    name: Some(renamed.name),
                    ..state
                },
                Err(_) => state,
            },
            ARCHIVED => OrganizationState {
                archived: true,
                ..state
            },
            // An event this plan cannot read is not ours to fold; leave the
            // state untouched rather than guessing.
            _ => state,
        }
    }
}

/// Genesis ①: grant leadership. Re-assignment is allowed (last write wins);
/// an archived organization refuses it.
fn assign_leader(
    state: &OrganizationState,
    command: &Command,
) -> Result<Execution, DomainError<OrganizationCode>> {
    if state.archived {
        return Err(reject(
            OrganizationCode::Archived,
            "an archived organization cannot assign a leader",
        ));
    }

    let payload: AssignLeader = decode_command(command)?;
    let data = encode(
        &LeaderAssigned {
            user_id: payload.user_id,
        },
        LEADER_ASSIGNED,
    )?;

    Ok(execution(command, LEADER_ASSIGNED, data))
}

/// Replace the name. Requires a provisioned, unarchived organization and a
/// non-empty, bounded name.
fn rename(
    state: &OrganizationState,
    command: &Command,
) -> Result<Execution, DomainError<OrganizationCode>> {
    if state.archived {
        return Err(reject(
            OrganizationCode::Archived,
            "an archived organization cannot be renamed",
        ));
    }
    if !state.is_created() {
        return Err(reject(
            OrganizationCode::NotCreated,
            "the organization has not been provisioned yet",
        ));
    }

    let payload: Rename = decode_command(command)?;
    if !valid_name(&payload.name) {
        return Err(reject(
            OrganizationCode::InvalidName,
            "the organization name must be non-empty and within the length bound",
        ));
    }

    let data = encode(&Renamed { name: payload.name }, RENAMED)?;

    Ok(execution(command, RENAMED, data))
}

/// Retire the organization. Idempotence is handled by the dedup window; a
/// second archive with a different intent is a domain error, not a no-op.
fn archive(
    state: &OrganizationState,
    command: &Command,
) -> Result<Execution, DomainError<OrganizationCode>> {
    if state.archived {
        return Err(reject(
            OrganizationCode::AlreadyArchived,
            "the organization has already been archived",
        ));
    }

    let _: Archive = decode_command(command)?;
    let data = encode(&Archived {}, ARCHIVED)?;

    Ok(execution(command, ARCHIVED, data))
}

/// Whether `name` is a legal organization name: non-blank and within
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

/// Decodes a command's payload, mapping failures to [`OrganizationCode::InvalidPayload`].
fn decode_command<T: DeserializeOwned>(
    command: &Command,
) -> Result<T, DomainError<OrganizationCode>> {
    decode(&command.payload.data)
}

/// Decodes a payload string.
fn decode<T: DeserializeOwned>(data: &str) -> Result<T, DomainError<OrganizationCode>> {
    serde_json::from_str(data).map_err(|cause| {
        DomainError::with_cause(
            OrganizationCode::InvalidPayload,
            "the organization payload is malformed",
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Serializes an event payload, mapping failures to
/// [`OrganizationCode::UnserializableEvent`].
fn encode<T: Serialize>(
    value: &T,
    event_type: &str,
) -> Result<String, DomainError<OrganizationCode>> {
    serde_json::to_string(value).map_err(|cause| {
        DomainError::with_cause(
            OrganizationCode::UnserializableEvent,
            &format!("could not serialize {event_type}"),
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Builds a domain error for `code`.
fn reject(code: OrganizationCode, message: &str) -> DomainError<OrganizationCode> {
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
            id: Id::from("cmd-1"),
            aggregate_id: Id::from("org-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
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

    fn assign(user: &str) -> Command {
        command(ASSIGN_LEADER, &format!(r#"{{"user_id":"{user}"}}"#))
    }

    fn rename_to(name: &str) -> Command {
        command(RENAME, &format!(r#"{{"name":"{name}"}}"#))
    }

    fn archive_it() -> Command {
        command(ARCHIVE, "{}")
    }

    fn load(payload: &str) -> Command {
        command("organization.load", payload)
    }

    /// The plan's error code for `result`, if it failed.
    fn code_of<T>(result: Result<T, DomainError<OrganizationCode>>) -> Option<OrganizationCode> {
        result.err().map(|error| error.code)
    }

    /// The state after applying the events a successful `prepare` produced.
    fn advance(state: OrganizationState, command: Command) -> OrganizationState {
        let execution = Organization::prepare(state.clone(), command).unwrap();
        execution
            .events
            .into_iter()
            .fold(state, Organization::apply)
    }

    fn active() -> OrganizationState {
        OrganizationState {
            name: Some("Acme".to_owned()),
            leader_user_id: Some(Id::from("user-1")),
            archived: false,
        }
    }

    // --- frozen genesis behavior -----------------------------------------

    #[test]
    fn assign_leader_builds_the_frozen_event() {
        let execution =
            Organization::prepare(OrganizationState::default(), assign("user-1")).unwrap();

        assert_eq!(execution.events.len(), 1);
        let event = &execution.events[0];
        assert_eq!(event.event_type, LEADER_ASSIGNED);
        assert_eq!(event.id, assign("user-1").event_id(0));
        assert_eq!(
            decode::<LeaderAssigned>(&event.payload.data).unwrap(),
            LeaderAssigned {
                user_id: Id::from("user-1")
            }
        );
    }

    #[test]
    fn applying_leader_assigned_records_the_leader() {
        let state = advance(OrganizationState::default(), assign("user-1"));
        assert_eq!(state.leader_user_id, Some(Id::from("user-1")));
        assert!(!state.archived);
    }

    // --- rename ----------------------------------------------------------

    #[test]
    fn rename_replaces_the_name() {
        let state = advance(active(), rename_to("Acme Two"));
        assert_eq!(state.name.as_deref(), Some("Acme Two"));
    }

    #[test]
    fn rename_requires_a_provisioned_organization() {
        assert_eq!(
            code_of(Organization::prepare(
                OrganizationState::default(),
                rename_to("Acme")
            )),
            Some(OrganizationCode::NotCreated)
        );
    }

    #[test]
    fn rename_rejects_a_blank_name() {
        assert_eq!(
            code_of(Organization::prepare(active(), rename_to("   "))),
            Some(OrganizationCode::InvalidName)
        );
    }

    #[test]
    fn rename_rejects_an_overlong_name() {
        let long = "a".repeat(MAX_NAME_BYTES + 1);
        assert_eq!(
            code_of(Organization::prepare(active(), rename_to(&long))),
            Some(OrganizationCode::InvalidName)
        );
    }

    // --- archive ---------------------------------------------------------

    #[test]
    fn archive_is_monotonic() {
        let state = advance(active(), archive_it());
        assert!(state.archived);
        assert_eq!(state.name.as_deref(), Some("Acme"));
        assert_eq!(state.leader_user_id, Some(Id::from("user-1")));
    }

    #[test]
    fn archived_organizations_refuse_further_changes() {
        let archived = OrganizationState {
            archived: true,
            ..active()
        };

        assert_eq!(
            code_of(Organization::prepare(archived.clone(), assign("user-2"))),
            Some(OrganizationCode::Archived)
        );
        assert_eq!(
            code_of(Organization::prepare(archived.clone(), rename_to("Other"))),
            Some(OrganizationCode::Archived)
        );
        assert_eq!(
            code_of(Organization::prepare(archived, archive_it())),
            Some(OrganizationCode::AlreadyArchived)
        );
    }

    // --- unknown / malformed ---------------------------------------------

    #[test]
    fn unknown_commands_are_rejected() {
        assert_eq!(
            code_of(Organization::prepare(active(), load("{}"))),
            Some(OrganizationCode::UnknownCommand)
        );
    }

    #[test]
    fn malformed_payloads_are_rejected_with_a_cause() {
        let mut malformed = assign("user-1");
        malformed.payload.data = "not json".to_owned();

        let error = Organization::prepare(active(), malformed).unwrap_err();

        assert_eq!(error.code, OrganizationCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_and_malformed_events() {
        let foreign = typed(assign("user-1"), "task.created");
        assert_eq!(Organization::apply(active(), foreign), active());

        let malformed = with_data(
            Organization::prepare(active(), assign("user-1"))
                .unwrap()
                .events
                .remove(0),
            "not json",
        );
        assert_eq!(Organization::apply(active(), malformed), active());
    }

    // --- the transition matrix, stated once ------------------------------

    #[test]
    fn transition_matrix_matches_the_documented_table() {
        let fresh = OrganizationState::default();
        let live = active();
        let archived = OrganizationState {
            archived: true,
            ..active()
        };

        // assign_leader
        assert!(Organization::prepare(fresh.clone(), assign("user-1")).is_ok());
        assert!(Organization::prepare(live.clone(), assign("user-2")).is_ok());
        assert_eq!(
            code_of(Organization::prepare(archived.clone(), assign("user-1"))),
            Some(OrganizationCode::Archived)
        );

        // rename
        assert_eq!(
            code_of(Organization::prepare(fresh.clone(), rename_to("Acme"))),
            Some(OrganizationCode::NotCreated)
        );
        assert!(Organization::prepare(live.clone(), rename_to("Acme")).is_ok());
        assert_eq!(
            code_of(Organization::prepare(archived.clone(), rename_to("Acme"))),
            Some(OrganizationCode::Archived)
        );

        // archive
        assert!(Organization::prepare(fresh, archive_it()).is_ok());
        assert!(Organization::prepare(live, archive_it()).is_ok());
        assert_eq!(
            code_of(Organization::prepare(archived, archive_it())),
            Some(OrganizationCode::AlreadyArchived)
        );
    }

    // --- property tests ---------------------------------------------------

    /// One random operation: which command to issue.
    fn operation() -> impl Strategy<Value = u8> {
        0u8..6
    }

    fn command_for(op: u8) -> Command {
        match op {
            0 => assign("user-1"),
            1 => assign("user-2"),
            2 => rename_to("Acme Renamed"),
            3 => rename_to("   "),
            4 => archive_it(),
            _ => load("{}"),
        }
    }

    proptest! {
        // prepare/apply never panic, a rejected command never mutates state,
        // and archive is monotonic.
        #[test]
        fn arbitrary_command_sequences_respect_the_invariants(
            ops in prop::collection::vec(operation(), 0..64),
        ) {
            let mut state = OrganizationState::default();
            let mut archive_seen = false;

            for op in ops {
                match Organization::prepare(state.clone(), command_for(op)) {
                    Ok(execution) => {
                        for event in execution.events {
                            state = Organization::apply(state, event);
                        }
                    }
                    Err(error) => {
                        // A rejection is always one of the documented codes.
                        prop_assert!(matches!(
                            error.code,
                            OrganizationCode::UnknownCommand
                                | OrganizationCode::InvalidPayload
                                | OrganizationCode::UnserializableEvent
                                | OrganizationCode::NotCreated
                                | OrganizationCode::Archived
                                | OrganizationCode::AlreadyArchived
                                | OrganizationCode::InvalidName
                        ));
                    }
                }

                if state.archived {
                    archive_seen = true;
                } else {
                    prop_assert!(!archive_seen, "archive must be monotonic");
                }
            }
        }

        // Replaying the same events always yields the same state.
        #[test]
        fn replay_is_deterministic(ops in prop::collection::vec(operation(), 0..64)) {
            let mut state = OrganizationState::default();
            let mut events = Vec::new();

            for op in ops {
                if let Ok(execution) = Organization::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Organization::apply(state, event.clone());
                        events.push(event);
                    }
                }
            }

            let replayed = events
                .iter()
                .cloned()
                .fold(OrganizationState::default(), Organization::apply);
            prop_assert_eq!(replayed, state);
        }
    }

    /// Builds an [`Event`] fixture from a command envelope, with a different
    /// event type — used to prove `apply` ignores foreign events.
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

    /// Replaces an event's payload data — used to prove `apply` ignores
    /// payloads it cannot decode.
    fn with_data(mut event: Event, data: &str) -> Event {
        event.payload.data = data.to_owned();
        event
    }
}
