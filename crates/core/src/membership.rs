// SPDX-License-Identifier: MPL-2.0

//! The membership aggregates: [`WorkspaceMembership`] and
//! [`OrganizationAssignment`].
//!
//! Two "who is in what" plans live here (`design.md` §3):
//!
//! - [`WorkspaceMembership`] — one instance per `(workspace, user)`: what a
//!   user may do in a workspace. Its stream id is **derived** from
//!   `(organization, user)` for the genesis Owner (D12), minted for later
//!   members.
//! - [`OrganizationAssignment`] — one instance per `(organization, user)`:
//!   whether the user belongs to the organization at all.
//!
//! They are deliberately separate streams: a workspace membership can be
//! removed while the organization assignment stays (and vice versa).
//!
//! Genesis 3 (`membership.add_owner` → `membership.owner_added`) is the frozen
//! wire contract and is kept exactly as it was; the additional commands
//! (`add_member`, `change_role`, `remove_member`) carry the role explicitly.
//! Removal is a compensating event (append-only): it clears the role and never
//! deletes the stream.
//!
//! See `docs/domain-model.md` for the command/event table and transition matrix.

use crate::Uuid;
use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use crate::id::Id;
use crate::key::Key;

/// The namespace every membership identity derives from (D12).
///
/// It lives here rather than in the saga because **two callers need it**: the
/// invitation saga derives the streams it writes to, and the state machine
/// derives the same ids to answer "is this user in this organization?".
pub const NAMESPACE: Uuid = Uuid::from_u128(0x3b7e_1d92_4c05_4f68_a9b0_c1d2_e3f4_5061);

/// The [`OrganizationAssignment`] stream's derived id: one per
/// `(organization, user)`.
#[must_use]
pub fn organization_assignment_id(organization_id: &Id, user_id: &Id) -> Id {
    Id::from(Key::new(
        &NAMESPACE,
        &format!("{organization_id}:{user_id}:org-assignment"),
    ))
}

/// The [`WorkspaceMembership`] stream's derived id: one per
/// `(organization, workspace, user)`, so a redelivered acceptance addresses the
/// same membership.
#[must_use]
pub fn workspace_membership_id(organization_id: &Id, workspace_id: &Id, user_id: &Id) -> Id {
    Id::from(Key::new(
        &NAMESPACE,
        &format!("{organization_id}:{workspace_id}:{user_id}:membership"),
    ))
}
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The `command_type` of genesis 3 — part of the control-plane wire contract.
pub const ADD_OWNER: &str = "membership.add_owner";

/// The `event_type` genesis 3 must produce — part of the wire contract.
pub const OWNER_ADDED: &str = "membership.owner_added";

/// `membership.add_member` — add a member with an explicit role.
pub const ADD_MEMBER: &str = "membership.add_member";

/// `membership.member_added` — a member was added.
pub const MEMBER_ADDED: &str = "membership.member_added";

/// `membership.change_role` — replace a member's role.
pub const CHANGE_ROLE: &str = "membership.change_role";

/// `membership.role_changed` — a member's role was replaced.
pub const ROLE_CHANGED: &str = "membership.role_changed";

/// `membership.remove_member` — clear a member's role (append-only removal).
pub const REMOVE_MEMBER: &str = "membership.remove_member";

/// `membership.member_removed` — a member's role was cleared.
pub const MEMBER_REMOVED: &str = "membership.member_removed";

/// `organization.assign_member` — add a user to the organization.
pub const ASSIGN_MEMBER: &str = "organization.assign_member";

/// `organization.member_assigned` — a user was added to the organization.
pub const MEMBER_ASSIGNED: &str = "organization.member_assigned";

/// `organization.remove_member` — remove a user from the organization.
pub const ORG_REMOVE_MEMBER: &str = "organization.remove_member";

/// `organization.member_removed` — a user was removed from the organization.
pub const ORG_MEMBER_REMOVED: &str = "organization.member_removed";

/// A workspace role, ordered by authority: `Owner > Member > Viewer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Role {
    /// Full control of the workspace, including its membership.
    Owner,
    /// Can work in the workspace.
    Member,
    /// Read-only access to the workspace.
    Viewer,
}

impl Role {
    /// Whether this role may do what `required` describes.
    ///
    /// The order is `Owner ≥ Member ≥ Viewer`: an Owner may do everything a
    /// Member may, and a Member everything a Viewer may (which is reading).
    #[must_use]
    pub const fn satisfies(self, required: Self) -> bool {
        self.rank() >= required.rank()
    }

    /// The role's position in that order.
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Owner => 2,
            Self::Member => 1,
            Self::Viewer => 0,
        }
    }
}

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

/// The payload of `membership.add_member`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddMember {
    /// The user joining the workspace.
    pub user_id: Id,
    /// The role the user joins with.
    pub role: Role,
}

/// The payload of `membership.member_added`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberAdded {
    /// The user who joined the workspace.
    pub user_id: Id,
    /// The role the user joined with.
    pub role: Role,
}

/// The payload of `membership.change_role`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeRole {
    /// The member whose role changes.
    pub user_id: Id,
    /// The member's new role.
    pub role: Role,
}

/// The payload of `membership.role_changed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleChanged {
    /// The member whose role changed.
    pub user_id: Id,
    /// The member's new role.
    pub role: Role,
}

/// The payload of `membership.remove_member`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveMember {
    /// The member whose role is cleared.
    pub user_id: Id,
}

/// The payload of `membership.member_removed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberRemoved {
    /// The member whose role was cleared.
    pub user_id: Id,
}

/// The payload of `organization.assign_member`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignMember {
    /// The user joining the organization.
    pub user_id: Id,
}

/// The payload of `organization.member_assigned`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberAssigned {
    /// The user who joined the organization.
    pub user_id: Id,
}

/// The payload of `organization.remove_member`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrgRemoveMember {
    /// The user leaving the organization.
    pub user_id: Id,
}

/// The payload of `organization.member_removed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrgMemberRemoved {
    /// The user who left the organization.
    pub user_id: Id,
}

/// Everything the membership aggregate remembers.
///
/// The `role` field is `None` when the user is not a member (or has been
/// removed). `owner_user_id` is accepted as a legacy alias for `user_id` so a
/// snapshot written before roles existed still decodes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkspaceMembershipState {
    /// The member this stream belongs to, once added.
    #[serde(alias = "owner_user_id")]
    pub user_id: Option<Id>,
    /// The member's current role; `None` when not a member.
    pub role: Option<Role>,
}

impl WorkspaceMembershipState {
    /// Whether the user is currently a member.
    #[must_use]
    pub const fn is_member(&self) -> bool {
        self.role.is_some()
    }
}

/// Why the membership plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed shape for its type.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
    /// The user is already a member.
    AlreadyMember,
    /// The user is not a member.
    NotMember,
    /// The requested role is the member's current role.
    NoChange,
    /// The command names a different user than this membership belongs to.
    UserMismatch,
}

/// The membership aggregate's plan — a zero-sized marker; the type is the plan.
pub struct WorkspaceMembership;

impl AggregatePlan<WorkspaceMembershipState, MembershipCode> for WorkspaceMembership {
    fn prepare(
        state: WorkspaceMembershipState,
        command: Command,
    ) -> Result<Execution, DomainError<MembershipCode>> {
        match command.command_type.as_str() {
            ADD_OWNER => add_owner(&state, &command),
            ADD_MEMBER => add_member(&state, &command),
            CHANGE_ROLE => change_role(&state, &command),
            REMOVE_MEMBER => remove_member(&state, &command),
            _ => Err(reject(
                MembershipCode::UnknownCommand,
                "the membership plan handles membership.add_owner, membership.add_member, membership.change_role and membership.remove_member only",
            )),
        }
    }

    fn apply(state: WorkspaceMembershipState, event: Event) -> WorkspaceMembershipState {
        match event.event_type.as_str() {
            OWNER_ADDED => match decode::<OwnerAdded>(&event.payload.data) {
                Ok(added) => WorkspaceMembershipState {
                    user_id: Some(added.user_id),
                    role: Some(Role::Owner),
                },
                Err(_) => state,
            },
            MEMBER_ADDED => match decode::<MemberAdded>(&event.payload.data) {
                Ok(added) => WorkspaceMembershipState {
                    user_id: Some(added.user_id),
                    role: Some(added.role),
                },
                Err(_) => state,
            },
            ROLE_CHANGED => match decode::<RoleChanged>(&event.payload.data) {
                Ok(changed) => WorkspaceMembershipState {
                    role: Some(changed.role),
                    ..state
                },
                Err(_) => state,
            },
            MEMBER_REMOVED => WorkspaceMembershipState {
                role: None,
                ..state
            },
            // An event this plan cannot read is not ours to fold.
            _ => state,
        }
    }
}

/// Everything the organization-assignment aggregate remembers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct OrganizationAssignmentState {
    /// The assigned user, once assigned.
    pub user_id: Option<Id>,
    /// Whether the user currently belongs to the organization.
    pub assigned: bool,
}

impl OrganizationAssignmentState {
    /// Whether the user is currently assigned.
    #[must_use]
    pub const fn is_assigned(&self) -> bool {
        self.assigned
    }
}

/// Why the organization-assignment plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed shape for its type.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
    /// The user is already assigned to the organization.
    AlreadyAssigned,
    /// The user is not assigned to the organization.
    NotAssigned,
    /// The command names a different user than this assignment belongs to.
    UserMismatch,
}

/// The organization-assignment aggregate's plan.
pub struct OrganizationAssignment;

impl AggregatePlan<OrganizationAssignmentState, AssignmentCode> for OrganizationAssignment {
    fn prepare(
        state: OrganizationAssignmentState,
        command: Command,
    ) -> Result<Execution, DomainError<AssignmentCode>> {
        match command.command_type.as_str() {
            ASSIGN_MEMBER => assign_member(&state, &command),
            ORG_REMOVE_MEMBER => unassign_member(&state, &command),
            _ => Err(reject(
                AssignmentCode::UnknownCommand,
                "the organization-assignment plan handles organization.assign_member and organization.remove_member only",
            )),
        }
    }

    fn apply(state: OrganizationAssignmentState, event: Event) -> OrganizationAssignmentState {
        match event.event_type.as_str() {
            MEMBER_ASSIGNED => match decode_assignment::<MemberAssigned>(&event.payload.data) {
                Ok(assigned) => OrganizationAssignmentState {
                    user_id: Some(assigned.user_id),
                    assigned: true,
                },
                Err(_) => state,
            },
            ORG_MEMBER_REMOVED => OrganizationAssignmentState {
                assigned: false,
                ..state
            },
            // An event this plan cannot read is not ours to fold.
            _ => state,
        }
    }
}

/// Assign the user to the organization.
fn assign_member(
    state: &OrganizationAssignmentState,
    command: &Command,
) -> Result<Execution, DomainError<AssignmentCode>> {
    if state.user_id.is_some() || state.assigned {
        return Err(reject(
            AssignmentCode::AlreadyAssigned,
            "this assignment stream already exists",
        ));
    }

    let payload: AssignMember = decode_assignment_command(command)?;
    let data = encode_assignment(
        &MemberAssigned {
            user_id: payload.user_id,
        },
        MEMBER_ASSIGNED,
    )?;

    Ok(execution(command, MEMBER_ASSIGNED, data))
}

/// Remove the user from the organization.
fn unassign_member(
    state: &OrganizationAssignmentState,
    command: &Command,
) -> Result<Execution, DomainError<AssignmentCode>> {
    if !state.assigned {
        return Err(reject(
            AssignmentCode::NotAssigned,
            "the user is not assigned to this organization",
        ));
    }

    let payload: OrgRemoveMember = decode_assignment_command(command)?;
    if state.user_id.as_ref() != Some(&payload.user_id) {
        return Err(reject(
            AssignmentCode::UserMismatch,
            "the command names a different user than this assignment",
        ));
    }

    let data = encode_assignment(
        &OrgMemberRemoved {
            user_id: payload.user_id,
        },
        ORG_MEMBER_REMOVED,
    )?;

    Ok(execution(command, ORG_MEMBER_REMOVED, data))
}

/// Genesis 3: add the workspace's Owner. Re-adding an existing member is a
/// different intent and is rejected.
fn add_owner(
    state: &WorkspaceMembershipState,
    command: &Command,
) -> Result<Execution, DomainError<MembershipCode>> {
    if state.user_id.is_some() {
        return Err(reject(
            MembershipCode::AlreadyMember,
            "this membership stream already exists",
        ));
    }

    let payload: AddOwner = decode_command(command)?;
    let data = encode(
        &OwnerAdded {
            user_id: payload.user_id,
        },
        OWNER_ADDED,
    )?;

    Ok(execution(command, OWNER_ADDED, data))
}

/// Add a member with an explicit role.
fn add_member(
    state: &WorkspaceMembershipState,
    command: &Command,
) -> Result<Execution, DomainError<MembershipCode>> {
    if state.user_id.is_some() {
        return Err(reject(
            MembershipCode::AlreadyMember,
            "this membership stream already exists; re-inviting mints a new one",
        ));
    }

    let payload: AddMember = decode_command(command)?;
    let data = encode(
        &MemberAdded {
            user_id: payload.user_id,
            role: payload.role,
        },
        MEMBER_ADDED,
    )?;

    Ok(execution(command, MEMBER_ADDED, data))
}

/// Replace a member's role.
fn change_role(
    state: &WorkspaceMembershipState,
    command: &Command,
) -> Result<Execution, DomainError<MembershipCode>> {
    let Some(current) = state.role else {
        return Err(reject(
            MembershipCode::NotMember,
            "the user is not a member of this workspace",
        ));
    };

    let payload: ChangeRole = decode_command(command)?;
    ensure_same_user(state, &payload.user_id)?;

    if payload.role == current {
        return Err(reject(
            MembershipCode::NoChange,
            "the user already holds this role",
        ));
    }

    let data = encode(
        &RoleChanged {
            user_id: payload.user_id,
            role: payload.role,
        },
        ROLE_CHANGED,
    )?;

    Ok(execution(command, ROLE_CHANGED, data))
}

/// Clear a member's role (append-only removal).
fn remove_member(
    state: &WorkspaceMembershipState,
    command: &Command,
) -> Result<Execution, DomainError<MembershipCode>> {
    if !state.is_member() {
        return Err(reject(
            MembershipCode::NotMember,
            "the user is not a member of this workspace",
        ));
    }

    let payload: RemoveMember = decode_command(command)?;
    ensure_same_user(state, &payload.user_id)?;

    let data = encode(
        &MemberRemoved {
            user_id: payload.user_id,
        },
        MEMBER_REMOVED,
    )?;

    Ok(execution(command, MEMBER_REMOVED, data))
}

/// Rejects a command that names a user other than the membership's owner.
fn ensure_same_user(
    state: &WorkspaceMembershipState,
    user_id: &Id,
) -> Result<(), DomainError<MembershipCode>> {
    if state.user_id.as_ref() == Some(user_id) {
        Ok(())
    } else {
        Err(reject(
            MembershipCode::UserMismatch,
            "the command names a different user than this membership",
        ))
    }
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
/// [`MembershipCode::InvalidPayload`].
fn decode_command<T: DeserializeOwned>(
    command: &Command,
) -> Result<T, DomainError<MembershipCode>> {
    decode(&command.payload.data)
}

/// Decodes a membership payload string.
fn decode<T: DeserializeOwned>(data: &str) -> Result<T, DomainError<MembershipCode>> {
    decode_payload(
        data,
        MembershipCode::InvalidPayload,
        "the membership payload is malformed",
    )
}

/// Serializes a membership event payload.
fn encode<T: Serialize>(
    value: &T,
    event_type: &str,
) -> Result<String, DomainError<MembershipCode>> {
    encode_payload(value, event_type, MembershipCode::UnserializableEvent)
}

/// Decodes an assignment command's payload.
fn decode_assignment_command<T: DeserializeOwned>(
    command: &Command,
) -> Result<T, DomainError<AssignmentCode>> {
    decode_assignment(&command.payload.data)
}

/// Decodes an assignment payload string.
fn decode_assignment<T: DeserializeOwned>(data: &str) -> Result<T, DomainError<AssignmentCode>> {
    decode_payload(
        data,
        AssignmentCode::InvalidPayload,
        "the assignment payload is malformed",
    )
}

/// Serializes an assignment event payload.
fn encode_assignment<T: Serialize>(
    value: &T,
    event_type: &str,
) -> Result<String, DomainError<AssignmentCode>> {
    encode_payload(value, event_type, AssignmentCode::UnserializableEvent)
}

/// Decodes a payload string, mapping failures to `code`.
fn decode_payload<T: DeserializeOwned, C: Copy>(
    data: &str,
    code: C,
    context: &str,
) -> Result<T, DomainError<C>> {
    serde_json::from_str(data)
        .map_err(|cause| DomainError::with_cause(code, context, Some(anyhow::Error::new(cause))))
}

/// Serializes an event payload, mapping failures to `code`.
fn encode_payload<T: Serialize, C: Copy>(
    value: &T,
    event_type: &str,
    code: C,
) -> Result<String, DomainError<C>> {
    serde_json::to_string(value).map_err(|cause| {
        DomainError::with_cause(
            code,
            &format!("could not serialize {event_type}"),
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Builds a domain error for `code`.
fn reject<C>(code: C, message: &str) -> DomainError<C> {
    DomainError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_are_ordered_by_authority() {
        assert!(Role::Owner.satisfies(Role::Owner));
        assert!(Role::Owner.satisfies(Role::Member));
        assert!(Role::Owner.satisfies(Role::Viewer));
        assert!(Role::Member.satisfies(Role::Member));
        assert!(Role::Member.satisfies(Role::Viewer));
        assert!(!Role::Member.satisfies(Role::Owner));
        assert!(Role::Viewer.satisfies(Role::Viewer));
        assert!(!Role::Viewer.satisfies(Role::Member));
        assert!(!Role::Viewer.satisfies(Role::Owner));
    }
    use crate::actor::Actor;
    use crate::key::Key;
    use crate::timestamp::Timestamp;
    use proptest::prelude::*;
    use uuid::Uuid;

    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    fn command(command_type: &str, payload: &str) -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-3"),
            aggregate_id: Id::from("membership-1"),
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

    fn add_owner(user: &str) -> Command {
        command(ADD_OWNER, &format!(r#"{{"user_id":"{user}"}}"#))
    }

    fn add_member(user: &str, role: &str) -> Command {
        command(
            ADD_MEMBER,
            &format!(r#"{{"user_id":"{user}","role":"{role}"}}"#),
        )
    }

    fn change(user: &str, role: &str) -> Command {
        command(
            CHANGE_ROLE,
            &format!(r#"{{"user_id":"{user}","role":"{role}"}}"#),
        )
    }

    fn remove(user: &str) -> Command {
        command(REMOVE_MEMBER, &format!(r#"{{"user_id":"{user}"}}"#))
    }

    fn code_of<T>(result: Result<T, DomainError<MembershipCode>>) -> Option<MembershipCode> {
        result.err().map(|error| error.code)
    }

    fn advance(state: WorkspaceMembershipState, command: Command) -> WorkspaceMembershipState {
        let execution = WorkspaceMembership::prepare(state.clone(), command).unwrap();
        execution
            .events
            .into_iter()
            .fold(state, WorkspaceMembership::apply)
    }

    fn member() -> WorkspaceMembershipState {
        WorkspaceMembershipState {
            user_id: Some(Id::from("user-1")),
            role: Some(Role::Member),
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
    fn add_owner_builds_the_frozen_event_and_grants_owner() {
        let command = add_owner("user-1");
        let execution =
            WorkspaceMembership::prepare(WorkspaceMembershipState::default(), command.clone())
                .unwrap();
        let event = &execution.events[0];

        assert_eq!(event.event_type, OWNER_ADDED);
        assert_eq!(event.workspace_id, Some(Id::from("ws-1")));
        assert_eq!(event.id, command.event_id(0));

        let state = WorkspaceMembership::apply(WorkspaceMembershipState::default(), event.clone());
        assert_eq!(state.user_id, Some(Id::from("user-1")));
        assert_eq!(state.role, Some(Role::Owner));
    }

    #[test]
    fn add_owner_rejects_an_existing_member() {
        assert_eq!(
            code_of(WorkspaceMembership::prepare(member(), add_owner("user-1"))),
            Some(MembershipCode::AlreadyMember)
        );
    }

    // --- add_member / change_role / remove_member ------------------------

    #[test]
    fn add_member_sets_the_requested_role() {
        let state = advance(
            WorkspaceMembershipState::default(),
            add_member("user-2", "Viewer"),
        );
        assert_eq!(state.user_id, Some(Id::from("user-2")));
        assert_eq!(state.role, Some(Role::Viewer));
    }

    #[test]
    fn add_member_rejects_an_existing_member() {
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                member(),
                add_member("user-1", "Viewer")
            )),
            Some(MembershipCode::AlreadyMember)
        );
    }

    #[test]
    fn change_role_replaces_the_role() {
        let state = advance(member(), change("user-1", "Owner"));
        assert_eq!(state.role, Some(Role::Owner));
    }

    #[test]
    fn change_role_requires_a_member_and_a_different_role() {
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                WorkspaceMembershipState::default(),
                change("user-1", "Owner")
            )),
            Some(MembershipCode::NotMember)
        );
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                member(),
                change("user-1", "Member")
            )),
            Some(MembershipCode::NoChange)
        );
    }

    #[test]
    fn commands_naming_another_user_are_rejected() {
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                member(),
                change("user-9", "Owner")
            )),
            Some(MembershipCode::UserMismatch)
        );
        assert_eq!(
            code_of(WorkspaceMembership::prepare(member(), remove("user-9"))),
            Some(MembershipCode::UserMismatch)
        );
    }

    #[test]
    fn remove_member_clears_the_role_and_keeps_the_identity() {
        let state = advance(member(), remove("user-1"));
        assert_eq!(state.role, None);
        assert_eq!(state.user_id, Some(Id::from("user-1")));
        assert!(!state.is_member());
    }

    #[test]
    fn remove_member_requires_a_member() {
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                WorkspaceMembershipState::default(),
                remove("user-1")
            )),
            Some(MembershipCode::NotMember)
        );
    }

    // --- unknown / malformed / legacy ------------------------------------

    #[test]
    fn unknown_commands_and_malformed_payloads_are_rejected() {
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                member(),
                command("membership.promote", "{}")
            )),
            Some(MembershipCode::UnknownCommand)
        );

        let mut malformed = add_owner("user-1");
        malformed.payload.data = "not json".to_owned();
        let error = WorkspaceMembership::prepare(WorkspaceMembershipState::default(), malformed)
            .unwrap_err();
        assert_eq!(error.code, MembershipCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_and_malformed_events() {
        let foreign = typed(add_owner("user-1"), "task.created");
        assert_eq!(
            WorkspaceMembership::apply(WorkspaceMembershipState::default(), foreign),
            WorkspaceMembershipState::default()
        );

        let malformed = Event {
            payload: Payload {
                version: 1,
                data: "not json".to_owned(),
            },
            ..typed(add_owner("user-1"), OWNER_ADDED)
        };
        assert_eq!(
            WorkspaceMembership::apply(WorkspaceMembershipState::default(), malformed),
            WorkspaceMembershipState::default()
        );
    }

    #[test]
    fn a_legacy_snapshot_field_still_decodes() {
        let legacy = r#"{"owner_user_id":"user-1"}"#;
        let state: WorkspaceMembershipState = serde_json::from_str(legacy).unwrap();
        assert_eq!(state.user_id, Some(Id::from("user-1")));
        assert_eq!(state.role, None);
    }

    // --- transition matrix -----------------------------------------------

    #[test]
    fn transition_matrix_matches_the_documented_table() {
        let fresh = WorkspaceMembershipState::default();
        let live = member();
        let removed = WorkspaceMembershipState {
            role: None,
            ..member()
        };

        // add (owner or member)
        assert!(WorkspaceMembership::prepare(fresh.clone(), add_owner("user-1")).is_ok());
        assert!(WorkspaceMembership::prepare(fresh, add_member("user-1", "Member")).is_ok());
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                live.clone(),
                add_owner("user-1")
            )),
            Some(MembershipCode::AlreadyMember)
        );
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                removed.clone(),
                add_member("user-1", "Member")
            )),
            Some(MembershipCode::AlreadyMember)
        );

        // change_role
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                fresh_again(),
                change("user-1", "Owner")
            )),
            Some(MembershipCode::NotMember)
        );
        assert!(WorkspaceMembership::prepare(live.clone(), change("user-1", "Owner")).is_ok());
        assert_eq!(
            code_of(WorkspaceMembership::prepare(
                removed.clone(),
                change("user-1", "Owner")
            )),
            Some(MembershipCode::NotMember)
        );

        // remove_member
        assert!(WorkspaceMembership::prepare(live, remove("user-1")).is_ok());
        assert_eq!(
            code_of(WorkspaceMembership::prepare(removed, remove("user-1"))),
            Some(MembershipCode::NotMember)
        );
    }

    fn fresh_again() -> WorkspaceMembershipState {
        WorkspaceMembershipState::default()
    }

    // --- property tests --------------------------------------------------

    fn operation() -> impl Strategy<Value = u8> {
        0u8..6
    }

    fn command_for(op: u8) -> Command {
        match op {
            0 => add_owner("user-1"),
            1 => add_member("user-1", "Member"),
            2 => change("user-1", "Owner"),
            3 => change("user-1", "Member"),
            4 => remove("user-1"),
            _ => command("membership.promote", "{}"),
        }
    }

    proptest! {
        // A membership never changes hands, and a removed member stays removed
        // until a new add (which the state forbids).
        #[test]
        fn arbitrary_command_sequences_respect_the_invariants(
            ops in prop::collection::vec(operation(), 0..64),
        ) {
            let mut state = WorkspaceMembershipState::default();
            let mut removed_seen = false;

            for op in ops {
                if let Ok(execution) = WorkspaceMembership::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = WorkspaceMembership::apply(state, event);
                    }
                }

                // The user never changes once set.
                if let Some(user_id) = &state.user_id {
                    prop_assert_eq!(user_id, &Id::from("user-1"));
                }

                if state.user_id.is_some() && !state.is_member() {
                    removed_seen = true;
                }
                prop_assert!(!(removed_seen && state.is_member()), "a removed member must not rejoin without an add");
            }
        }

        // Replaying the same events always yields the same state.
        #[test]
        fn replay_is_deterministic(ops in prop::collection::vec(operation(), 0..64)) {
            let mut state = WorkspaceMembershipState::default();
            let mut events = Vec::new();

            for op in ops {
                if let Ok(execution) = WorkspaceMembership::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = WorkspaceMembership::apply(state, event.clone());
                        events.push(event);
                    }
                }
            }

            let replayed = events
                .iter()
                .cloned()
                .fold(WorkspaceMembershipState::default(), WorkspaceMembership::apply);
            prop_assert_eq!(replayed, state);
        }
    }
}

#[cfg(test)]
mod assignment_tests {
    use super::*;
    use crate::actor::Actor;
    use crate::id::Id;
    use crate::key::Key;
    use crate::timestamp::Timestamp;
    use proptest::prelude::*;
    use uuid::Uuid;

    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    fn command(command_type: &str, payload: &str) -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-4"),
            aggregate_id: Id::from("assignment-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, &format!("{command_type}:{payload}")),
            correlation_key: Key::new(&KEY_NS, "onboarding"),
            actor: Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload.to_owned(),
            },
        }
    }

    fn assign(user: &str) -> Command {
        command(ASSIGN_MEMBER, &format!(r#"{{"user_id":"{user}"}}"#))
    }

    fn unassign(user: &str) -> Command {
        command(ORG_REMOVE_MEMBER, &format!(r#"{{"user_id":"{user}"}}"#))
    }

    fn code_of<T>(result: Result<T, DomainError<AssignmentCode>>) -> Option<AssignmentCode> {
        result.err().map(|error| error.code)
    }

    fn advance(
        state: OrganizationAssignmentState,
        command: Command,
    ) -> OrganizationAssignmentState {
        let execution = OrganizationAssignment::prepare(state.clone(), command).unwrap();
        execution
            .events
            .into_iter()
            .fold(state, OrganizationAssignment::apply)
    }

    fn assigned() -> OrganizationAssignmentState {
        OrganizationAssignmentState {
            user_id: Some(Id::from("user-1")),
            assigned: true,
        }
    }

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

    #[test]
    fn assign_builds_the_event_and_assigns() {
        let command = assign("user-1");
        let execution = OrganizationAssignment::prepare(
            OrganizationAssignmentState::default(),
            command.clone(),
        )
        .unwrap();
        let event = &execution.events[0];

        assert_eq!(event.event_type, MEMBER_ASSIGNED);
        assert_eq!(event.id, command.event_id(0));

        let state =
            OrganizationAssignment::apply(OrganizationAssignmentState::default(), event.clone());
        assert_eq!(state.user_id, Some(Id::from("user-1")));
        assert!(state.assigned);
    }

    #[test]
    fn assign_rejects_an_existing_stream() {
        assert_eq!(
            code_of(OrganizationAssignment::prepare(
                assigned(),
                assign("user-1")
            )),
            Some(AssignmentCode::AlreadyAssigned)
        );
    }

    #[test]
    fn unassign_clears_the_flag_and_keeps_the_identity() {
        let state = advance(assigned(), unassign("user-1"));
        assert!(!state.assigned);
        assert_eq!(state.user_id, Some(Id::from("user-1")));
    }

    #[test]
    fn unassign_requires_an_assignment_and_the_same_user() {
        assert_eq!(
            code_of(OrganizationAssignment::prepare(
                OrganizationAssignmentState::default(),
                unassign("user-1")
            )),
            Some(AssignmentCode::NotAssigned)
        );
        assert_eq!(
            code_of(OrganizationAssignment::prepare(
                assigned(),
                unassign("user-9")
            )),
            Some(AssignmentCode::UserMismatch)
        );
    }

    #[test]
    fn unknown_and_malformed_commands_are_rejected() {
        assert_eq!(
            code_of(OrganizationAssignment::prepare(
                assigned(),
                command("organization.promote", "{}")
            )),
            Some(AssignmentCode::UnknownCommand)
        );

        let mut malformed = assign("user-1");
        malformed.payload.data = "not json".to_owned();
        let error =
            OrganizationAssignment::prepare(OrganizationAssignmentState::default(), malformed)
                .unwrap_err();
        assert_eq!(error.code, AssignmentCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_and_malformed_events() {
        let foreign = typed(assign("user-1"), "task.created");
        assert_eq!(
            OrganizationAssignment::apply(OrganizationAssignmentState::default(), foreign),
            OrganizationAssignmentState::default()
        );

        let malformed = Event {
            payload: Payload {
                version: 1,
                data: "not json".to_owned(),
            },
            ..typed(assign("user-1"), MEMBER_ASSIGNED)
        };
        assert_eq!(
            OrganizationAssignment::apply(OrganizationAssignmentState::default(), malformed),
            OrganizationAssignmentState::default()
        );
    }

    #[test]
    fn transition_matrix_matches_the_documented_table() {
        let fresh = OrganizationAssignmentState::default();
        let live = assigned();
        let removed = OrganizationAssignmentState {
            assigned: false,
            ..assigned()
        };

        // assign_member
        assert!(OrganizationAssignment::prepare(fresh.clone(), assign("user-1")).is_ok());
        assert_eq!(
            code_of(OrganizationAssignment::prepare(
                live.clone(),
                assign("user-1")
            )),
            Some(AssignmentCode::AlreadyAssigned)
        );
        assert_eq!(
            code_of(OrganizationAssignment::prepare(
                removed.clone(),
                assign("user-1")
            )),
            Some(AssignmentCode::AlreadyAssigned)
        );

        // remove_member
        assert_eq!(
            code_of(OrganizationAssignment::prepare(fresh, unassign("user-1"))),
            Some(AssignmentCode::NotAssigned)
        );
        assert!(OrganizationAssignment::prepare(live, unassign("user-1")).is_ok());
        assert_eq!(
            code_of(OrganizationAssignment::prepare(removed, unassign("user-1"))),
            Some(AssignmentCode::NotAssigned)
        );
    }

    fn operation() -> impl Strategy<Value = u8> {
        0u8..4
    }

    fn command_for(op: u8) -> Command {
        match op {
            0 => assign("user-1"),
            1 => unassign("user-1"),
            2 => unassign("user-9"),
            _ => command("organization.promote", "{}"),
        }
    }

    proptest! {
        #[test]
        fn arbitrary_command_sequences_respect_the_invariants(
            ops in prop::collection::vec(operation(), 0..64),
        ) {
            let mut state = OrganizationAssignmentState::default();
            let mut removed_seen = false;

            for op in ops {
                if let Ok(execution) =
                    OrganizationAssignment::prepare(state.clone(), command_for(op))
                {
                    for event in execution.events {
                        state = OrganizationAssignment::apply(state, event);
                    }
                }

                if state.user_id.is_some() && !state.assigned {
                    removed_seen = true;
                }
                prop_assert!(
                    !(removed_seen && state.assigned),
                    "an unassigned user must not become assigned again"
                );
            }
        }

        #[test]
        fn replay_is_deterministic(ops in prop::collection::vec(operation(), 0..64)) {
            let mut state = OrganizationAssignmentState::default();
            let mut events = Vec::new();

            for op in ops {
                if let Ok(execution) =
                    OrganizationAssignment::prepare(state.clone(), command_for(op))
                {
                    for event in execution.events {
                        state = OrganizationAssignment::apply(state, event.clone());
                        events.push(event);
                    }
                }
            }

            let replayed = events
                .iter()
                .cloned()
                .fold(OrganizationAssignmentState::default(), OrganizationAssignment::apply);
            prop_assert_eq!(replayed, state);
        }
    }
}
