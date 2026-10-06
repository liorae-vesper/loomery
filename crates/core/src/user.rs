// SPDX-License-Identifier: MPL-2.0

//! The user aggregate.
//!
//! A person known to Loomery. Users are provisioned by the OIDC path (Phase 1);
//! the aggregate itself is pure — the gateway injects the (edge-validated)
//! profile fields and the envelope metadata, `prepare` checks the shape against
//! the current state, and `apply` folds.
//!
//! `email` is the provisioning marker: it is `None` until `user.provision`, and
//! a provisioned user is never un-provisioned (only deactivated). Deactivation
//! is monotonic — there is no reactivation command; a returning user is a new
//! intent.
//!
//! See `docs/domain-model.md` for the command/event table and transition matrix.

use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// `user.provision` — create the user record from an `IdP` profile.
pub const PROVISION: &str = "user.provision";

/// `user.provisioned` — the user record was created.
pub const PROVISIONED: &str = "user.provisioned";

/// `user.update_profile` — replace the display name.
pub const UPDATE_PROFILE: &str = "user.update_profile";

/// `user.profile_updated` — the display name was replaced.
pub const PROFILE_UPDATED: &str = "user.profile_updated";

/// `user.deactivate` — disable the user (append-only; no delete).
pub const DEACTIVATE: &str = "user.deactivate";

/// `user.deactivated` — the user was disabled.
pub const DEACTIVATED: &str = "user.deactivated";

/// The maximum email length, in bytes (D10).
pub const MAX_EMAIL_BYTES: usize = 320;

/// The maximum display-name length, in bytes (D10).
pub const MAX_DISPLAY_NAME_BYTES: usize = 200;

/// The payload of `user.provision`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provision {
    /// The user's email address.
    pub email: String,
    /// The user's display name.
    pub display_name: String,
}

/// The payload of `user.provisioned`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provisioned {
    /// The user's email address.
    pub email: String,
    /// The user's display name.
    pub display_name: String,
}

/// The payload of `user.update_profile`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateProfile {
    /// The user's new display name.
    pub display_name: String,
}

/// The payload of `user.profile_updated`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileUpdated {
    /// The user's new display name.
    pub display_name: String,
}

/// The (empty) payload of `user.deactivate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deactivate {}

/// The (empty) payload of `user.deactivated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deactivated {}

/// Everything the user aggregate remembers.
///
/// `#[serde(default)]` keeps older snapshots decodable as fields are added.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UserState {
    /// The user's email, once provisioned.
    pub email: Option<String>,
    /// The user's display name, once provisioned.
    pub display_name: Option<String>,
    /// Whether the user may act. Only provisioning turns this on.
    pub active: bool,
}

impl UserState {
    /// Whether the user record exists. `email` is the provisioning marker.
    #[must_use]
    pub const fn is_provisioned(&self) -> bool {
        self.email.is_some()
    }
}

/// Why the user plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed shape for its type.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
    /// The user has not been provisioned yet.
    NotProvisioned,
    /// The user has already been provisioned.
    AlreadyProvisioned,
    /// The user is deactivated and accepts no further changes.
    Inactive,
    /// The user has already been deactivated.
    AlreadyInactive,
    /// The email or display name is empty or out of bounds.
    InvalidProfile,
}

/// The user aggregate's plan — a zero-sized marker; the type is the plan.
pub struct User;

impl AggregatePlan<UserState, UserCode> for User {
    fn prepare(state: UserState, command: Command) -> Result<Execution, DomainError<UserCode>> {
        match command.command_type.as_str() {
            PROVISION => provision(&state, &command),
            UPDATE_PROFILE => update_profile(&state, &command),
            DEACTIVATE => deactivate(&state, &command),
            _ => Err(reject(
                UserCode::UnknownCommand,
                "the user plan handles user.provision, user.update_profile and user.deactivate only",
            )),
        }
    }

    fn apply(state: UserState, event: Event) -> UserState {
        match event.event_type.as_str() {
            PROVISIONED => match decode::<Provisioned>(&event.payload.data) {
                Ok(provisioned) => UserState {
                    email: Some(provisioned.email),
                    display_name: Some(provisioned.display_name),
                    active: true,
                },
                Err(_) => state,
            },
            PROFILE_UPDATED => match decode::<ProfileUpdated>(&event.payload.data) {
                Ok(updated) => UserState {
                    display_name: Some(updated.display_name),
                    ..state
                },
                Err(_) => state,
            },
            DEACTIVATED => UserState {
                active: false,
                ..state
            },
            // An event this plan cannot read is not ours to fold.
            _ => state,
        }
    }
}

/// Provision the user record. A re-provision is a different intent and is
/// rejected; the dedup window handles the retry case.
fn provision(state: &UserState, command: &Command) -> Result<Execution, DomainError<UserCode>> {
    if state.is_provisioned() {
        return Err(reject(
            UserCode::AlreadyProvisioned,
            "the user has already been provisioned",
        ));
    }

    let payload: Provision = decode_command(command)?;
    if !valid_email(&payload.email) || !valid_display_name(&payload.display_name) {
        return Err(reject(
            UserCode::InvalidProfile,
            "the user profile is empty or out of bounds",
        ));
    }

    let data = encode(
        &Provisioned {
            email: payload.email,
            display_name: payload.display_name,
        },
        PROVISIONED,
    )?;

    Ok(execution(command, PROVISIONED, data))
}

/// Replace the display name. Requires a provisioned, active user.
fn update_profile(
    state: &UserState,
    command: &Command,
) -> Result<Execution, DomainError<UserCode>> {
    if !state.is_provisioned() {
        return Err(reject(
            UserCode::NotProvisioned,
            "the user has not been provisioned yet",
        ));
    }
    if !state.active {
        return Err(reject(
            UserCode::Inactive,
            "a deactivated user's profile cannot be changed",
        ));
    }

    let payload: UpdateProfile = decode_command(command)?;
    if !valid_display_name(&payload.display_name) {
        return Err(reject(
            UserCode::InvalidProfile,
            "the display name is empty or out of bounds",
        ));
    }

    let data = encode(
        &ProfileUpdated {
            display_name: payload.display_name,
        },
        PROFILE_UPDATED,
    )?;

    Ok(execution(command, PROFILE_UPDATED, data))
}

/// Deactivate the user. Deactivation is monotonic.
fn deactivate(state: &UserState, command: &Command) -> Result<Execution, DomainError<UserCode>> {
    if !state.is_provisioned() {
        return Err(reject(
            UserCode::NotProvisioned,
            "the user has not been provisioned yet",
        ));
    }
    if !state.active {
        return Err(reject(
            UserCode::AlreadyInactive,
            "the user has already been deactivated",
        ));
    }

    let _: Deactivate = decode_command(command)?;
    let data = encode(&Deactivated {}, DEACTIVATED)?;

    Ok(execution(command, DEACTIVATED, data))
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

/// Whether `name` is a legal display name: non-blank and within
/// [`MAX_DISPLAY_NAME_BYTES`].
fn valid_display_name(name: &str) -> bool {
    !name.trim().is_empty() && name.len() <= MAX_DISPLAY_NAME_BYTES
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

/// Decodes a command's payload, mapping failures to [`UserCode::InvalidPayload`].
fn decode_command<T: DeserializeOwned>(command: &Command) -> Result<T, DomainError<UserCode>> {
    decode(&command.payload.data)
}

/// Decodes a payload string.
fn decode<T: DeserializeOwned>(data: &str) -> Result<T, DomainError<UserCode>> {
    serde_json::from_str(data).map_err(|cause| {
        DomainError::with_cause(
            UserCode::InvalidPayload,
            "the user payload is malformed",
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Serializes an event payload, mapping failures to
/// [`UserCode::UnserializableEvent`].
fn encode<T: Serialize>(value: &T, event_type: &str) -> Result<String, DomainError<UserCode>> {
    serde_json::to_string(value).map_err(|cause| {
        DomainError::with_cause(
            UserCode::UnserializableEvent,
            &format!("could not serialize {event_type}"),
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Builds a domain error for `code`.
fn reject(code: UserCode, message: &str) -> DomainError<UserCode> {
    DomainError::new(code, message)
}

#[cfg(test)]
mod tests {
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
            id: Id::from("cmd-1"),
            aggregate_id: Id::from("user-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, &format!("{command_type}:{payload}")),
            correlation_key: Key::new(&KEY_NS, "oidc"),
            actor: Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload.to_owned(),
            },
        }
    }

    fn provision_user(email: &str, name: &str) -> Command {
        command(
            PROVISION,
            &format!(r#"{{"email":"{email}","display_name":"{name}"}}"#),
        )
    }

    fn rename_user(name: &str) -> Command {
        command(UPDATE_PROFILE, &format!(r#"{{"display_name":"{name}"}}"#))
    }

    fn deactivate_user() -> Command {
        command(DEACTIVATE, "{}")
    }

    fn code_of<T>(result: Result<T, DomainError<UserCode>>) -> Option<UserCode> {
        result.err().map(|error| error.code)
    }

    fn advance(state: UserState, command: Command) -> UserState {
        let execution = User::prepare(state.clone(), command).unwrap();
        execution.events.into_iter().fold(state, User::apply)
    }

    fn active() -> UserState {
        UserState {
            email: Some("a@example.com".to_owned()),
            display_name: Some("Ada".to_owned()),
            active: true,
        }
    }

    fn inactive() -> UserState {
        UserState {
            active: false,
            ..active()
        }
    }

    /// A fixture event built from a command envelope with a different type.
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

    /// Replaces an event's payload data.
    fn with_data(mut event: Event, data: &str) -> Event {
        event.payload.data = data.to_owned();
        event
    }

    // --- provisioning ----------------------------------------------------

    #[test]
    fn provision_builds_the_event_and_activates_on_apply() {
        let command = provision_user("a@example.com", "Ada");
        let execution = User::prepare(UserState::default(), command.clone()).unwrap();
        let event = &execution.events[0];

        assert_eq!(event.event_type, PROVISIONED);
        assert_eq!(event.id, command.event_id(0));

        let state = User::apply(UserState::default(), event.clone());
        assert_eq!(state.email.as_deref(), Some("a@example.com"));
        assert_eq!(state.display_name.as_deref(), Some("Ada"));
        assert!(state.active);
    }

    #[test]
    fn provision_rejects_an_existing_user() {
        assert_eq!(
            code_of(User::prepare(
                active(),
                provision_user("b@example.com", "Bob")
            )),
            Some(UserCode::AlreadyProvisioned)
        );
    }

    #[test]
    fn provision_rejects_a_bad_email() {
        for bad in ["", "   ", "no-at-sign", "@example.com", "a@", "a@b@c"] {
            assert_eq!(
                code_of(User::prepare(
                    UserState::default(),
                    provision_user(bad, "Ada")
                )),
                Some(UserCode::InvalidProfile),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn provision_rejects_a_blank_display_name() {
        assert_eq!(
            code_of(User::prepare(
                UserState::default(),
                provision_user("a@example.com", "   ")
            )),
            Some(UserCode::InvalidProfile)
        );
    }

    // --- profile ---------------------------------------------------------

    #[test]
    fn update_profile_replaces_the_name() {
        let state = advance(active(), rename_user("Ada Lovelace"));
        assert_eq!(state.display_name.as_deref(), Some("Ada Lovelace"));
        assert!(state.active);
    }

    #[test]
    fn update_profile_requires_a_provisioned_active_user() {
        assert_eq!(
            code_of(User::prepare(UserState::default(), rename_user("Ada"))),
            Some(UserCode::NotProvisioned)
        );
        assert_eq!(
            code_of(User::prepare(inactive(), rename_user("Ada"))),
            Some(UserCode::Inactive)
        );
    }

    // --- deactivation ----------------------------------------------------

    #[test]
    fn deactivate_is_monotonic() {
        let state = advance(active(), deactivate_user());
        assert!(!state.active);
        assert!(state.is_provisioned());
        assert_eq!(state.display_name.as_deref(), Some("Ada"));
    }

    #[test]
    fn deactivated_users_refuse_further_changes() {
        assert_eq!(
            code_of(User::prepare(
                inactive(),
                provision_user("b@example.com", "Bob")
            )),
            Some(UserCode::AlreadyProvisioned)
        );
        assert_eq!(
            code_of(User::prepare(inactive(), rename_user("Ada"))),
            Some(UserCode::Inactive)
        );
        assert_eq!(
            code_of(User::prepare(inactive(), deactivate_user())),
            Some(UserCode::AlreadyInactive)
        );
    }

    // --- unknown / malformed ---------------------------------------------

    #[test]
    fn unknown_commands_are_rejected() {
        assert_eq!(
            code_of(User::prepare(active(), command("user.promote", "{}"))),
            Some(UserCode::UnknownCommand)
        );
    }

    #[test]
    fn malformed_payloads_are_rejected_with_a_cause() {
        let mut malformed = provision_user("a@example.com", "Ada");
        malformed.payload.data = "not json".to_owned();

        let error = User::prepare(UserState::default(), malformed).unwrap_err();
        assert_eq!(error.code, UserCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_and_malformed_events() {
        let foreign = typed(provision_user("a@example.com", "Ada"), "task.created");
        assert_eq!(
            User::apply(UserState::default(), foreign),
            UserState::default()
        );

        let malformed = with_data(
            User::prepare(UserState::default(), provision_user("a@example.com", "Ada"))
                .unwrap()
                .events
                .remove(0),
            "not json",
        );
        assert_eq!(
            User::apply(UserState::default(), malformed),
            UserState::default()
        );
    }

    // --- transition matrix -----------------------------------------------

    #[test]
    fn transition_matrix_matches_the_documented_table() {
        let fresh = UserState::default();
        let live = active();
        let off = inactive();

        // provision
        assert!(User::prepare(fresh.clone(), provision_user("a@example.com", "Ada")).is_ok());
        assert_eq!(
            code_of(User::prepare(
                live.clone(),
                provision_user("a@example.com", "Ada")
            )),
            Some(UserCode::AlreadyProvisioned)
        );
        assert_eq!(
            code_of(User::prepare(
                off.clone(),
                provision_user("a@example.com", "Ada")
            )),
            Some(UserCode::AlreadyProvisioned)
        );

        // update_profile
        assert_eq!(
            code_of(User::prepare(fresh, rename_user("Ada"))),
            Some(UserCode::NotProvisioned)
        );
        assert!(User::prepare(live.clone(), rename_user("Ada")).is_ok());
        assert_eq!(
            code_of(User::prepare(off.clone(), rename_user("Ada"))),
            Some(UserCode::Inactive)
        );

        // deactivate
        assert!(User::prepare(live.clone(), deactivate_user()).is_ok());
        assert_eq!(
            code_of(User::prepare(off, deactivate_user())),
            Some(UserCode::AlreadyInactive)
        );
    }

    // --- property tests --------------------------------------------------

    fn operation() -> impl Strategy<Value = u8> {
        0u8..6
    }

    fn command_for(op: u8) -> Command {
        match op {
            0 => provision_user("a@example.com", "Ada"),
            1 => provision_user("bad", ""),
            2 => rename_user("Ada Lovelace"),
            3 => rename_user(""),
            4 => deactivate_user(),
            _ => command("user.promote", "{}"),
        }
    }

    proptest! {
        // prepare/apply never panic, and deactivation is monotonic once reached.
        #[test]
        fn arbitrary_command_sequences_respect_the_invariants(
            ops in prop::collection::vec(operation(), 0..64),
        ) {
            let mut state = UserState::default();
            let mut inactive_seen = false;

            for op in ops {
                if let Ok(execution) = User::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = User::apply(state, event);
                    }
                }

                // `active` implies `is_provisioned`.
                prop_assert!(!state.active || state.is_provisioned());

                if state.is_provisioned() && !state.active {
                    inactive_seen = true;
                }
                prop_assert!(!(inactive_seen && state.active), "deactivation must be monotonic");
            }
        }

        // Replaying the same events always yields the same state.
        #[test]
        fn replay_is_deterministic(ops in prop::collection::vec(operation(), 0..64)) {
            let mut state = UserState::default();
            let mut events = Vec::new();

            for op in ops {
                if let Ok(execution) = User::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = User::apply(state, event.clone());
                        events.push(event);
                    }
                }
            }

            let replayed = events
                .iter()
                .cloned()
                .fold(UserState::default(), User::apply);
            prop_assert_eq!(replayed, state);
        }
    }
}
