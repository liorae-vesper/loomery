// SPDX-License-Identifier: MPL-2.0

//! The invitation aggregate.
//!
//! An invitation is the email-based onboarding path (`design.md` §5, Phase 1):
//! an administrator invites an address with a role, and accepting it is what
//! triggers provisioning (organization assignment + workspace membership) via
//! the acceptance saga.
//!
//! The aggregate is deliberately small — create, accept, expire — and pure: the
//! token and the invitee's identity arrive in the command envelope, and
//! acceptance names the user the invitation was accepted *by*. Status is
//! monotonic: a pending invitation becomes accepted or expired, never both and
//! never back.
//!
//! See `docs/domain-model.md` for the command/event table and transition matrix.

use crate::actor::Actor;
use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use crate::id::Id;
use crate::membership::Role;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// `invitation.create` — invite an address with a role.
pub const CREATE: &str = "invitation.create";

/// `invitation.created` — an invitation was issued.
pub const CREATED: &str = "invitation.created";

/// `invitation.accept` — the invitee accepts.
pub const ACCEPT: &str = "invitation.accept";

/// `invitation.accepted` — the invitation was accepted.
pub const ACCEPTED: &str = "invitation.accepted";

/// `invitation.expire` — the invitation lapses.
pub const EXPIRE: &str = "invitation.expire";

/// `invitation.expired` — the invitation lapsed.
pub const EXPIRED: &str = "invitation.expired";

/// The maximum email length, in bytes (D10).
pub const MAX_EMAIL_BYTES: usize = 320;

/// Where an invitation is in its lifecycle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvitationStatus {
    /// No invitation has been issued for this stream.
    #[default]
    Unspecified,
    /// Issued and awaiting acceptance.
    Pending,
    /// Accepted; the acceptance saga provisions the user.
    Accepted,
    /// Lapsed without acceptance.
    Expired,
}

/// The payload of `invitation.create`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Create {
    /// The invited email address.
    pub email: String,
    /// The role to grant on acceptance.
    pub role: Role,
}

/// The payload of `invitation.created`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Created {
    /// The invited email address.
    pub email: String,
    /// The role to grant on acceptance.
    pub role: Role,
}

/// The payload of `invitation.accept`.
///
/// Both fields are **attribution**, not client input: the gateway overwrites
/// them from the authenticated caller (the identity's user id and its verified
/// email claim) before the command reaches consensus. `email` is what binds the
/// acceptance to the invitation — an invitation is issued to an address, and only
/// the holder of that address may accept it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accept {
    /// The user who accepted the invitation.
    pub user_id: Id,
    /// The email the caller proved control of.
    pub email: String,
}

/// The payload of `invitation.accepted` — everything the acceptance saga needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Accepted {
    /// The user who accepted the invitation.
    pub user_id: Id,
    /// The invited email address.
    pub email: String,
    /// The role to grant.
    pub role: Role,
}

/// The (empty) payload of `invitation.expire`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expire {}

/// The (empty) payload of `invitation.expired`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expired {}

/// Everything the invitation aggregate remembers.
///
/// `#[serde(default)]` keeps older snapshots decodable as fields are added.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InvitationState {
    /// The invited email, once created.
    pub email: Option<String>,
    /// The role to grant, once created.
    pub role: Option<Role>,
    /// The user who accepted, once accepted.
    pub accepted_by: Option<Id>,
    /// Where the invitation is in its lifecycle.
    pub status: InvitationStatus,
}

impl InvitationState {
    /// Whether an invitation has been issued.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        matches!(self.status, InvitationStatus::Pending)
    }
}

/// Why the invitation plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvitationCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed shape for its type.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
    /// An invitation already exists for this stream.
    AlreadyCreated,
    /// No invitation has been issued yet.
    NotCreated,
    /// The invitation has already been accepted.
    AlreadyAccepted,
    /// The invitation has already expired.
    AlreadyExpired,
    /// The invitation expired and cannot be accepted.
    Expired,
    /// The invited email is empty or out of bounds.
    InvalidEmail,
    /// The caller's email does not match the invited address.
    EmailMismatch,
    /// The caller is not the user the acceptance names.
    NotTheInvitee,
}

/// The invitation aggregate's plan — a zero-sized marker; the type is the plan.
pub struct Invitation;

impl AggregatePlan<InvitationState, InvitationCode> for Invitation {
    fn prepare(
        state: InvitationState,
        command: Command,
    ) -> Result<Execution, DomainError<InvitationCode>> {
        match command.command_type.as_str() {
            CREATE => create(&state, &command),
            ACCEPT => accept(&state, &command),
            EXPIRE => expire(&state, &command),
            _ => Err(reject(
                InvitationCode::UnknownCommand,
                "the invitation plan handles invitation.create, invitation.accept and invitation.expire only",
            )),
        }
    }

    fn apply(state: InvitationState, event: Event) -> InvitationState {
        match event.event_type.as_str() {
            CREATED => match decode::<Created>(&event.payload.data) {
                Ok(created) => InvitationState {
                    email: Some(created.email),
                    role: Some(created.role),
                    accepted_by: None,
                    status: InvitationStatus::Pending,
                },
                Err(_) => state,
            },
            ACCEPTED => match decode::<Accepted>(&event.payload.data) {
                Ok(accepted) => InvitationState {
                    accepted_by: Some(accepted.user_id),
                    status: InvitationStatus::Accepted,
                    ..state
                },
                Err(_) => state,
            },
            EXPIRED => InvitationState {
                status: InvitationStatus::Expired,
                ..state
            },
            // An event this plan cannot read is not ours to fold.
            _ => state,
        }
    }
}

/// Issue an invitation.
fn create(
    state: &InvitationState,
    command: &Command,
) -> Result<Execution, DomainError<InvitationCode>> {
    if state.status != InvitationStatus::Unspecified {
        return Err(reject(
            InvitationCode::AlreadyCreated,
            "an invitation already exists for this stream",
        ));
    }

    let payload: Create = decode_command(command)?;
    if !valid_email(&payload.email) {
        return Err(reject(
            InvitationCode::InvalidEmail,
            "the invited email is empty or out of bounds",
        ));
    }

    let data = encode(
        &Created {
            email: payload.email,
            role: payload.role,
        },
        CREATED,
    )?;

    Ok(execution(command, CREATED, data))
}

/// Accept an invitation.
/// Whether two addresses are the same one, ignoring case and surrounding space.
///
/// Email is not case-sensitive in practice, and a provider may pad a claim.
#[must_use]
fn emails_match(invited: &str, claimed: &str) -> bool {
    let normalize = |address: &str| address.trim().to_lowercase();
    !normalize(invited).is_empty() && normalize(invited) == normalize(claimed)
}

fn accept(
    state: &InvitationState,
    command: &Command,
) -> Result<Execution, DomainError<InvitationCode>> {
    match state.status {
        InvitationStatus::Unspecified => {
            return Err(reject(
                InvitationCode::NotCreated,
                "no invitation has been issued for this stream",
            ));
        }
        InvitationStatus::Accepted => {
            return Err(reject(
                InvitationCode::AlreadyAccepted,
                "the invitation has already been accepted",
            ));
        }
        InvitationStatus::Expired => {
            return Err(reject(
                InvitationCode::Expired,
                "the invitation expired and cannot be accepted",
            ));
        }
        InvitationStatus::Pending => {}
    }

    let payload: Accept = decode_command(command)?;

    // The caller must be the invitee. The gateway derives both from the
    // authenticated identity, and this refuses a forged payload even if some
    // other path submits it.
    if let Actor::User { id } = &command.actor
        && id != &payload.user_id
    {
        return Err(reject(
            InvitationCode::NotTheInvitee,
            "the acceptance names a user other than the caller",
        ));
    }

    // ...and must prove the address the invitation was issued to.
    let invited = state.email.clone().unwrap_or_default();
    if !emails_match(&invited, &payload.email) {
        return Err(reject(
            InvitationCode::EmailMismatch,
            "the acceptance comes from a different email than the invitation was issued to",
        ));
    }

    let role = state.role.unwrap_or(Role::Member);

    let data = encode(
        &Accepted {
            user_id: payload.user_id,
            // The invitation's address is authoritative, not the payload's.
            email: invited,
            role,
        },
        ACCEPTED,
    )?;

    Ok(execution(command, ACCEPTED, data))
}

/// Expire the invitation.
fn expire(
    state: &InvitationState,
    command: &Command,
) -> Result<Execution, DomainError<InvitationCode>> {
    match state.status {
        InvitationStatus::Unspecified => {
            return Err(reject(
                InvitationCode::NotCreated,
                "no invitation has been issued for this stream",
            ));
        }
        InvitationStatus::Accepted => {
            return Err(reject(
                InvitationCode::AlreadyAccepted,
                "an accepted invitation cannot expire",
            ));
        }
        InvitationStatus::Expired => {
            return Err(reject(
                InvitationCode::AlreadyExpired,
                "the invitation has already expired",
            ));
        }
        InvitationStatus::Pending => {}
    }

    let _: Expire = decode_command(command)?;
    let data = encode(&Expired {}, EXPIRED)?;
    Ok(execution(command, EXPIRED, data))
}

/// Whether `email` is a plausible address: one `@`, non-empty on both sides,
/// within [`MAX_EMAIL_BYTES`].
fn valid_email(email: &str) -> bool {
    if email.trim().is_empty() || email.len() > MAX_EMAIL_BYTES {
        return false;
    }

    match email.trim().split_once('@') {
        Some((local, domain)) => !local.is_empty() && !domain.is_empty() && !domain.contains('@'),
        None => false,
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
/// [`InvitationCode::InvalidPayload`].
fn decode_command<T: DeserializeOwned>(
    command: &Command,
) -> Result<T, DomainError<InvitationCode>> {
    decode(&command.payload.data)
}

/// Decodes a payload string.
fn decode<T: DeserializeOwned>(data: &str) -> Result<T, DomainError<InvitationCode>> {
    serde_json::from_str(data).map_err(|cause| {
        DomainError::with_cause(
            InvitationCode::InvalidPayload,
            "the invitation payload is malformed",
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Serializes an event payload, mapping failures to
/// [`InvitationCode::UnserializableEvent`].
fn encode<T: Serialize>(
    value: &T,
    event_type: &str,
) -> Result<String, DomainError<InvitationCode>> {
    serde_json::to_string(value).map_err(|cause| {
        DomainError::with_cause(
            InvitationCode::UnserializableEvent,
            &format!("could not serialize {event_type}"),
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Builds a domain error for `code`.
fn reject(code: InvitationCode, message: &str) -> DomainError<InvitationCode> {
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
            id: Id::from("cmd-inv"),
            aggregate_id: Id::from("invitation-1"),
            organization_id: Id::from("org-1"),
            workspace_id: Some(Id::from("ws-1")),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, &format!("{command_type}:{payload}")),
            correlation_key: Key::new(&KEY_NS, "invitation"),
            actor: Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload.to_owned(),
            },
        }
    }

    fn create_invite(email: &str, role: &str) -> Command {
        command(CREATE, &format!(r#"{{"email":"{email}","role":"{role}"}}"#))
    }

    /// The invited address the tests issue invitations to.
    const INVITED: &str = "ada@example.com";

    fn accept_invite(user: &str) -> Command {
        command(
            ACCEPT,
            &format!(r#"{{"user_id":"{user}","email":"{INVITED}"}}"#),
        )
    }

    /// An acceptance whose payload claims a different address.
    fn accept_invite_from(email: &str, user: &str) -> Command {
        command(
            ACCEPT,
            &format!(r#"{{"user_id":"{user}","email":"{email}"}}"#),
        )
    }

    /// An acceptance submitted by `actor`.
    fn accept_invite_as(actor: Actor, user: &str) -> Command {
        Command {
            actor,
            ..accept_invite(user)
        }
    }

    fn expire_invite() -> Command {
        command(EXPIRE, "{}")
    }

    fn code_of<T>(result: Result<T, DomainError<InvitationCode>>) -> Option<InvitationCode> {
        result.err().map(|error| error.code)
    }

    fn advance(state: InvitationState, command: Command) -> InvitationState {
        let execution = Invitation::prepare(state.clone(), command).unwrap();
        execution.events.into_iter().fold(state, Invitation::apply)
    }

    fn pending() -> InvitationState {
        InvitationState {
            email: Some("ada@example.com".to_owned()),
            role: Some(Role::Member),
            accepted_by: None,
            status: InvitationStatus::Pending,
        }
    }

    fn accepted() -> InvitationState {
        InvitationState {
            accepted_by: Some(Id::from("user-1")),
            status: InvitationStatus::Accepted,
            ..pending()
        }
    }

    fn expired() -> InvitationState {
        InvitationState {
            status: InvitationStatus::Expired,
            ..pending()
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
    fn create_issues_a_pending_invitation() {
        let command = create_invite("ada@example.com", "Member");
        let execution = Invitation::prepare(InvitationState::default(), command.clone()).unwrap();
        let event = &execution.events[0];

        assert_eq!(event.event_type, CREATED);
        assert_eq!(event.id, command.event_id(0));

        let state = Invitation::apply(InvitationState::default(), event.clone());
        assert_eq!(state.email.as_deref(), Some("ada@example.com"));
        assert_eq!(state.role, Some(Role::Member));
        assert!(state.is_pending());
    }

    #[test]
    fn create_rejects_a_second_invitation_and_a_bad_email() {
        assert_eq!(
            code_of(Invitation::prepare(
                pending(),
                create_invite("b@x.com", "Member")
            )),
            Some(InvitationCode::AlreadyCreated)
        );
        assert_eq!(
            code_of(Invitation::prepare(
                InvitationState::default(),
                create_invite("not-an-email", "Member")
            )),
            Some(InvitationCode::InvalidEmail)
        );
    }

    #[test]
    fn accept_names_the_user_and_carries_what_the_saga_needs() {
        let execution = Invitation::prepare(pending(), accept_invite("user-1")).unwrap();
        let event = &execution.events[0];

        assert_eq!(event.event_type, ACCEPTED);
        let accepted: Accepted = decode(&event.payload.data).unwrap();
        assert_eq!(accepted.user_id, Id::from("user-1"));
        assert_eq!(accepted.email, "ada@example.com");
        assert_eq!(accepted.role, Role::Member);

        let state = Invitation::apply(pending(), event.clone());
        assert_eq!(state.accepted_by, Some(Id::from("user-1")));
        assert_eq!(state.status, InvitationStatus::Accepted);
    }

    #[test]
    fn accept_rejects_every_other_status() {
        assert_eq!(
            code_of(Invitation::prepare(
                InvitationState::default(),
                accept_invite("user-1")
            )),
            Some(InvitationCode::NotCreated)
        );
        assert_eq!(
            code_of(Invitation::prepare(accepted(), accept_invite("user-1"))),
            Some(InvitationCode::AlreadyAccepted)
        );
        assert_eq!(
            code_of(Invitation::prepare(expired(), accept_invite("user-1"))),
            Some(InvitationCode::Expired)
        );
    }

    #[test]
    fn an_acceptance_from_another_address_is_refused() {
        assert_eq!(
            code_of(Invitation::prepare(
                pending(),
                accept_invite_from("mallory@example.com", "user-1"),
            )),
            Some(InvitationCode::EmailMismatch),
            "the invitation belongs to the address it was issued to"
        );
    }

    #[test]
    fn the_invited_address_is_matched_case_insensitively() {
        let state = advance(
            pending(),
            accept_invite_from("  ADA@Example.COM ", "user-1"),
        );
        assert!(matches!(state.status, InvitationStatus::Accepted));
    }

    #[test]
    fn an_acceptance_naming_another_user_is_refused() {
        assert_eq!(
            code_of(Invitation::prepare(
                pending(),
                accept_invite_as(
                    Actor::User {
                        id: Id::from("user-2")
                    },
                    "user-1"
                ),
            )),
            Some(InvitationCode::NotTheInvitee),
            "the caller must be the user the acceptance names"
        );
    }

    #[test]
    fn a_system_actor_may_accept_on_behalf_of_the_invitee() {
        // The saga and the tests use a system actor; only a *user* actor is
        // checked against the payload, and the email still has to match.
        assert!(
            Invitation::prepare(pending(), accept_invite("user-1")).is_ok(),
            "a system actor is not the invitee and is not checked as one"
        );
    }

    #[test]
    fn expire_is_monotonic() {
        let state = advance(pending(), expire_invite());
        assert_eq!(state.status, InvitationStatus::Expired);

        assert_eq!(
            code_of(Invitation::prepare(expired(), expire_invite())),
            Some(InvitationCode::AlreadyExpired)
        );
        assert_eq!(
            code_of(Invitation::prepare(accepted(), expire_invite())),
            Some(InvitationCode::AlreadyAccepted)
        );
    }

    #[test]
    fn unknown_commands_and_malformed_payloads_are_rejected() {
        assert_eq!(
            code_of(Invitation::prepare(
                pending(),
                command("invitation.revoke", "{}")
            )),
            Some(InvitationCode::UnknownCommand)
        );

        let mut malformed = create_invite("ada@example.com", "Member");
        malformed.payload.data = "not json".to_owned();
        let error = Invitation::prepare(InvitationState::default(), malformed).unwrap_err();
        assert_eq!(error.code, InvitationCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_and_malformed_events() {
        let foreign = typed(create_invite("ada@example.com", "Member"), "task.created");
        assert_eq!(
            Invitation::apply(InvitationState::default(), foreign),
            InvitationState::default()
        );

        let malformed = Event {
            payload: Payload {
                version: 1,
                data: "not json".to_owned(),
            },
            ..typed(create_invite("ada@example.com", "Member"), CREATED)
        };
        assert_eq!(
            Invitation::apply(InvitationState::default(), malformed),
            InvitationState::default()
        );
    }

    #[test]
    fn transition_matrix_matches_the_documented_table() {
        let fresh = InvitationState::default();
        let pending = pending();
        let accepted = accepted();
        let expired = expired();

        // create
        assert!(Invitation::prepare(fresh.clone(), create_invite("a@b.com", "Member")).is_ok());
        assert_eq!(
            code_of(Invitation::prepare(
                pending.clone(),
                create_invite("a@b.com", "Member")
            )),
            Some(InvitationCode::AlreadyCreated)
        );

        // accept
        assert_eq!(
            code_of(Invitation::prepare(fresh.clone(), accept_invite("user-1"))),
            Some(InvitationCode::NotCreated)
        );
        assert!(Invitation::prepare(pending.clone(), accept_invite("user-1")).is_ok());
        assert_eq!(
            code_of(Invitation::prepare(
                accepted.clone(),
                accept_invite("user-1")
            )),
            Some(InvitationCode::AlreadyAccepted)
        );
        assert_eq!(
            code_of(Invitation::prepare(
                expired.clone(),
                accept_invite("user-1")
            )),
            Some(InvitationCode::Expired)
        );

        // expire
        assert_eq!(
            code_of(Invitation::prepare(fresh, expire_invite())),
            Some(InvitationCode::NotCreated)
        );
        assert!(Invitation::prepare(pending, expire_invite()).is_ok());
        assert_eq!(
            code_of(Invitation::prepare(accepted, expire_invite())),
            Some(InvitationCode::AlreadyAccepted)
        );
        assert_eq!(
            code_of(Invitation::prepare(expired, expire_invite())),
            Some(InvitationCode::AlreadyExpired)
        );
    }

    fn operation() -> impl Strategy<Value = u8> {
        0u8..5
    }

    fn command_for(op: u8) -> Command {
        match op {
            0 => create_invite("ada@example.com", "Member"),
            1 => create_invite("bad", "Member"),
            2 => accept_invite("user-1"),
            3 => expire_invite(),
            _ => command("invitation.revoke", "{}"),
        }
    }

    proptest! {
        // prepare/apply never panic; a decided invitation never changes status.
        #[test]
        fn arbitrary_command_sequences_respect_the_invariants(
            ops in prop::collection::vec(operation(), 0..64),
        ) {
            let mut state = InvitationState::default();
            let mut decided = false;

            for op in ops {
                if let Ok(execution) = Invitation::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Invitation::apply(state, event);
                    }
                }

                if decided {
                    prop_assert!(state.status == InvitationStatus::Accepted
                        || state.status == InvitationStatus::Expired);
                }
                if state.status == InvitationStatus::Accepted
                    || state.status == InvitationStatus::Expired
                {
                    decided = true;
                }
            }
        }

        // Replaying the same events always yields the same state.
        #[test]
        fn replay_is_deterministic(ops in prop::collection::vec(operation(), 0..64)) {
            let mut state = InvitationState::default();
            let mut events = Vec::new();

            for op in ops {
                if let Ok(execution) = Invitation::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Invitation::apply(state, event.clone());
                        events.push(event);
                    }
                }
            }

            let replayed = events
                .iter()
                .cloned()
                .fold(InvitationState::default(), Invitation::apply);
            prop_assert_eq!(replayed, state);
        }
    }
}
