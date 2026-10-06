// SPDX-License-Identifier: MPL-2.0

//! The tenant aggregate — the control plane's placement record.
//!
//! Loomery gives every organization its own Raft group (design principle 3),
//! and the **control group** remembers where that group lives: its id, its
//! voters' addresses, and whether it is still being provisioned or already
//! carries traffic. The router projects this stream into
//! `organization_id → group`, and the tenant-creation controller drives the
//! transitions.
//!
//! This is a control-plane aggregate, not a tenant-group one: its stream lives
//! in the control group, and its `aggregate_id` is the organization id it
//! places. `tenant.tombstone` is the append-only deletion marker — a retired
//! tenant keeps its record so delayed workers cannot resurrect it.
//!
//! See `docs/domain-model.md` for the command/event table and transition matrix.

use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// `tenant.register` — record a new tenant's group and placement.
pub const REGISTER: &str = "tenant.register";

/// `tenant.registered` — the tenant was recorded, provisioning may start.
pub const REGISTERED: &str = "tenant.registered";

/// `tenant.activate` — genesis completed; the tenant may carry traffic.
pub const ACTIVATE: &str = "tenant.activate";

/// `tenant.activated` — the tenant became routable.
pub const ACTIVATED: &str = "tenant.activated";

/// `tenant.tombstone` — retire the tenant (append-only; no delete).
pub const TOMBSTONE: &str = "tenant.tombstone";

/// `tenant.tombstoned` — the tenant was retired.
pub const TOMBSTONED: &str = "tenant.tombstoned";

/// The maximum group-id length, in bytes (D10).
pub const MAX_GROUP_ID_BYTES: usize = 200;

/// The maximum peer-address length, in bytes (D10).
pub const MAX_ADDRESS_BYTES: usize = 300;

/// The maximum number of replicas in one placement (D10).
pub const MAX_REPLICAS: usize = 16;

/// One replica of a tenant group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replica {
    /// The Raft node id inside the tenant group.
    pub node_id: u64,
    /// The peer's address, as the transport understands it.
    pub address: String,
}

/// Where a tenant is in its lifecycle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantStatus {
    /// No record yet.
    #[default]
    Unregistered,
    /// Recorded; the controller is booting replicas and running genesis.
    Registering,
    /// Genesis completed; application traffic is allowed.
    Active,
    /// Retired; no new work may start for this tenant.
    Tombstoned,
}

/// The payload of `tenant.register`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Register {
    /// The tenant group's id (the transport routes by this).
    pub group_id: String,
    /// The group's intended replicas.
    pub replicas: Vec<Replica>,
}

/// The payload of `tenant.registered`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registered {
    /// The tenant group's id.
    pub group_id: String,
    /// The group's intended replicas.
    pub replicas: Vec<Replica>,
}

/// The (empty) payload of `tenant.activate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Activate {}

/// The (empty) payload of `tenant.activated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Activated {}

/// The (empty) payload of `tenant.tombstone`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {}

/// The (empty) payload of `tenant.tombstoned`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstoned {}

/// Everything the tenant aggregate remembers.
///
/// `#[serde(default)]` keeps older snapshots decodable as fields are added.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TenantState {
    /// The tenant group's id, once registered.
    pub group_id: Option<String>,
    /// The group's intended replicas, once registered.
    pub replicas: Vec<Replica>,
    /// Where the tenant is in its lifecycle.
    pub status: TenantStatus,
}

impl TenantState {
    /// Whether the tenant has a record (registered or beyond).
    #[must_use]
    pub const fn is_registered(&self) -> bool {
        !matches!(self.status, TenantStatus::Unregistered)
    }

    /// Whether the tenant may carry application traffic.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self.status, TenantStatus::Active)
    }
}

/// Why the tenant plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed shape for its type.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
    /// The tenant already has a record.
    AlreadyRegistered,
    /// The tenant has no record yet.
    NotRegistered,
    /// The tenant is already active.
    AlreadyActive,
    /// The tenant is tombstoned and accepts no further work.
    Tombstoned,
    /// The tenant has already been tombstoned.
    AlreadyTombstoned,
    /// The placement is empty or out of bounds (group id, addresses, replicas).
    InvalidPlacement,
}

/// The tenant aggregate's plan — a zero-sized marker; the type is the plan.
pub struct Tenant;

impl AggregatePlan<TenantState, TenantCode> for Tenant {
    fn prepare(state: TenantState, command: Command) -> Result<Execution, DomainError<TenantCode>> {
        match command.command_type.as_str() {
            REGISTER => register(&state, &command),
            ACTIVATE => activate(&state, &command),
            TOMBSTONE => tombstone(&state, &command),
            _ => Err(reject(
                TenantCode::UnknownCommand,
                "the tenant plan handles tenant.register, tenant.activate and tenant.tombstone only",
            )),
        }
    }

    fn apply(state: TenantState, event: Event) -> TenantState {
        match event.event_type.as_str() {
            REGISTERED => match decode::<Registered>(&event.payload.data) {
                Ok(registered) => TenantState {
                    group_id: Some(registered.group_id),
                    replicas: registered.replicas,
                    status: TenantStatus::Registering,
                },
                Err(_) => state,
            },
            ACTIVATED => TenantState {
                status: TenantStatus::Active,
                ..state
            },
            TOMBSTONED => TenantState {
                status: TenantStatus::Tombstoned,
                ..state
            },
            // An event this plan cannot read is not ours to fold.
            _ => state,
        }
    }
}

/// Record the tenant's group and placement.
fn register(state: &TenantState, command: &Command) -> Result<Execution, DomainError<TenantCode>> {
    if state.is_registered() {
        return Err(reject(
            TenantCode::AlreadyRegistered,
            "the tenant already has a record",
        ));
    }

    let payload: Register = decode_command(command)?;
    if !valid_placement(&payload.group_id, &payload.replicas) {
        return Err(reject(
            TenantCode::InvalidPlacement,
            "the tenant placement is empty or out of bounds",
        ));
    }

    let data = encode(
        &Registered {
            group_id: payload.group_id,
            replicas: payload.replicas,
        },
        REGISTERED,
    )?;

    Ok(execution(command, REGISTERED, data))
}

/// Mark genesis complete and allow traffic.
fn activate(state: &TenantState, command: &Command) -> Result<Execution, DomainError<TenantCode>> {
    match state.status {
        TenantStatus::Unregistered => {
            return Err(reject(
                TenantCode::NotRegistered,
                "the tenant has no record yet",
            ));
        }
        TenantStatus::Active => {
            return Err(reject(
                TenantCode::AlreadyActive,
                "the tenant is already active",
            ));
        }
        TenantStatus::Tombstoned => {
            return Err(reject(
                TenantCode::Tombstoned,
                "a tombstoned tenant cannot be activated",
            ));
        }
        TenantStatus::Registering => {}
    }

    let _: Activate = decode_command(command)?;
    let data = encode(&Activated {}, ACTIVATED)?;
    Ok(execution(command, ACTIVATED, data))
}

/// Retire the tenant.
fn tombstone(state: &TenantState, command: &Command) -> Result<Execution, DomainError<TenantCode>> {
    match state.status {
        TenantStatus::Unregistered => {
            return Err(reject(
                TenantCode::NotRegistered,
                "the tenant has no record yet",
            ));
        }
        TenantStatus::Tombstoned => {
            return Err(reject(
                TenantCode::AlreadyTombstoned,
                "the tenant has already been tombstoned",
            ));
        }
        TenantStatus::Registering | TenantStatus::Active => {}
    }

    let _: Tombstone = decode_command(command)?;
    let data = encode(&Tombstoned {}, TOMBSTONED)?;
    Ok(execution(command, TOMBSTONED, data))
}

/// Whether a placement is legal: a bounded, non-blank group id, at least one
/// replica, bounded addresses and distinct node ids.
fn valid_placement(group_id: &str, replicas: &[Replica]) -> bool {
    if group_id.trim().is_empty() || group_id.len() > MAX_GROUP_ID_BYTES {
        return false;
    }
    if replicas.is_empty() || replicas.len() > MAX_REPLICAS {
        return false;
    }

    let mut node_ids: Vec<u64> = replicas.iter().map(|replica| replica.node_id).collect();
    node_ids.sort_unstable();
    node_ids.dedup();

    node_ids.len() == replicas.len()
        && replicas.iter().all(|replica| {
            !replica.address.trim().is_empty() && replica.address.len() <= MAX_ADDRESS_BYTES
        })
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

/// Decodes a command's payload, mapping failures to [`TenantCode::InvalidPayload`].
fn decode_command<T: DeserializeOwned>(command: &Command) -> Result<T, DomainError<TenantCode>> {
    decode(&command.payload.data)
}

/// Decodes a payload string.
fn decode<T: DeserializeOwned>(data: &str) -> Result<T, DomainError<TenantCode>> {
    serde_json::from_str(data).map_err(|cause| {
        DomainError::with_cause(
            TenantCode::InvalidPayload,
            "the tenant payload is malformed",
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Serializes an event payload, mapping failures to
/// [`TenantCode::UnserializableEvent`].
fn encode<T: Serialize>(value: &T, event_type: &str) -> Result<String, DomainError<TenantCode>> {
    serde_json::to_string(value).map_err(|cause| {
        DomainError::with_cause(
            TenantCode::UnserializableEvent,
            &format!("could not serialize {event_type}"),
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Builds a domain error for `code`.
fn reject(code: TenantCode, message: &str) -> DomainError<TenantCode> {
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
            id: Id::from("cmd-t1"),
            aggregate_id: Id::from("org-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, &format!("{command_type}:{payload}")),
            correlation_key: Key::new(&KEY_NS, "control-plane"),
            actor: Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload.to_owned(),
            },
        }
    }

    fn register(group_id: &str, replicas: &str) -> Command {
        command(
            REGISTER,
            &format!(r#"{{"group_id":"{group_id}","replicas":{replicas}}}"#),
        )
    }

    fn one_replica() -> String {
        r#"[{"node_id":1,"address":"http://127.0.0.1:7001"}]"#.to_owned()
    }

    fn activate() -> Command {
        command(ACTIVATE, "{}")
    }

    fn tombstone_it() -> Command {
        command(TOMBSTONE, "{}")
    }

    fn code_of<T>(result: Result<T, DomainError<TenantCode>>) -> Option<TenantCode> {
        result.err().map(|error| error.code)
    }

    fn advance(state: TenantState, command: Command) -> TenantState {
        let execution = Tenant::prepare(state.clone(), command).unwrap();
        execution.events.into_iter().fold(state, Tenant::apply)
    }

    fn registering() -> TenantState {
        TenantState {
            group_id: Some("tenant-1".to_owned()),
            replicas: vec![Replica {
                node_id: 1,
                address: "http://127.0.0.1:7001".to_owned(),
            }],
            status: TenantStatus::Registering,
        }
    }

    fn active() -> TenantState {
        TenantState {
            status: TenantStatus::Active,
            ..registering()
        }
    }

    fn tombstoned() -> TenantState {
        TenantState {
            status: TenantStatus::Tombstoned,
            ..registering()
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
    fn register_records_the_placement() {
        let command = register("tenant-1", &one_replica());
        let execution = Tenant::prepare(TenantState::default(), command.clone()).unwrap();
        let event = &execution.events[0];

        assert_eq!(event.event_type, REGISTERED);
        assert_eq!(event.id, command.event_id(0));

        let state = Tenant::apply(TenantState::default(), event.clone());
        assert_eq!(state.group_id.as_deref(), Some("tenant-1"));
        assert_eq!(state.replicas.len(), 1);
        assert_eq!(state.status, TenantStatus::Registering);
    }

    #[test]
    fn register_rejects_an_existing_record_and_a_bad_placement() {
        assert_eq!(
            code_of(Tenant::prepare(
                registering(),
                register("tenant-2", &one_replica())
            )),
            Some(TenantCode::AlreadyRegistered)
        );
        assert_eq!(
            code_of(Tenant::prepare(
                TenantState::default(),
                register("", &one_replica())
            )),
            Some(TenantCode::InvalidPlacement)
        );
        assert_eq!(
            code_of(Tenant::prepare(
                TenantState::default(),
                register("tenant-1", "[]")
            )),
            Some(TenantCode::InvalidPlacement)
        );
        // Duplicate node ids are not a valid placement.
        let duplicate = r#"[{"node_id":1,"address":"a"},{"node_id":1,"address":"b"}]"#;
        assert_eq!(
            code_of(Tenant::prepare(
                TenantState::default(),
                register("tenant-1", duplicate)
            )),
            Some(TenantCode::InvalidPlacement)
        );
    }

    #[test]
    fn activate_moves_registering_to_active() {
        let state = advance(registering(), activate());
        assert_eq!(state.status, TenantStatus::Active);
        assert!(state.is_active());
    }

    #[test]
    fn activate_rejects_the_other_statuses() {
        assert_eq!(
            code_of(Tenant::prepare(TenantState::default(), activate())),
            Some(TenantCode::NotRegistered)
        );
        assert_eq!(
            code_of(Tenant::prepare(active(), activate())),
            Some(TenantCode::AlreadyActive)
        );
        assert_eq!(
            code_of(Tenant::prepare(tombstoned(), activate())),
            Some(TenantCode::Tombstoned)
        );
    }

    #[test]
    fn tombstone_is_monotonic() {
        let from_registering = advance(registering(), tombstone_it());
        assert_eq!(from_registering.status, TenantStatus::Tombstoned);

        let from_active = advance(active(), tombstone_it());
        assert_eq!(from_active.status, TenantStatus::Tombstoned);
    }

    #[test]
    fn tombstone_rejects_unregistered_and_repeat() {
        assert_eq!(
            code_of(Tenant::prepare(TenantState::default(), tombstone_it())),
            Some(TenantCode::NotRegistered)
        );
        assert_eq!(
            code_of(Tenant::prepare(tombstoned(), tombstone_it())),
            Some(TenantCode::AlreadyTombstoned)
        );
    }

    #[test]
    fn unknown_commands_and_malformed_payloads_are_rejected() {
        assert_eq!(
            code_of(Tenant::prepare(registering(), command("tenant.move", "{}"))),
            Some(TenantCode::UnknownCommand)
        );

        let mut malformed = register("tenant-1", &one_replica());
        malformed.payload.data = "not json".to_owned();
        let error = Tenant::prepare(TenantState::default(), malformed).unwrap_err();
        assert_eq!(error.code, TenantCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_and_malformed_events() {
        let foreign = typed(register("tenant-1", &one_replica()), "task.created");
        assert_eq!(
            Tenant::apply(TenantState::default(), foreign),
            TenantState::default()
        );

        let malformed = Event {
            payload: Payload {
                version: 1,
                data: "not json".to_owned(),
            },
            ..typed(register("tenant-1", &one_replica()), REGISTERED)
        };
        assert_eq!(
            Tenant::apply(TenantState::default(), malformed),
            TenantState::default()
        );
    }

    #[test]
    fn transition_matrix_matches_the_documented_table() {
        let fresh = TenantState::default();
        let registering = registering();
        let active = active();
        let dead = tombstoned();

        // register
        assert!(Tenant::prepare(fresh.clone(), register("tenant-1", &one_replica())).is_ok());
        assert_eq!(
            code_of(Tenant::prepare(
                registering.clone(),
                register("tenant-1", &one_replica())
            )),
            Some(TenantCode::AlreadyRegistered)
        );

        // activate
        assert_eq!(
            code_of(Tenant::prepare(fresh, activate())),
            Some(TenantCode::NotRegistered)
        );
        assert!(Tenant::prepare(registering, activate()).is_ok());
        assert_eq!(
            code_of(Tenant::prepare(active.clone(), activate())),
            Some(TenantCode::AlreadyActive)
        );
        assert_eq!(
            code_of(Tenant::prepare(dead.clone(), activate())),
            Some(TenantCode::Tombstoned)
        );

        // tombstone
        assert!(Tenant::prepare(active, tombstone_it()).is_ok());
        assert_eq!(
            code_of(Tenant::prepare(dead, tombstone_it())),
            Some(TenantCode::AlreadyTombstoned)
        );
    }

    fn operation() -> impl Strategy<Value = u8> {
        0u8..5
    }

    fn command_for(op: u8) -> Command {
        match op {
            0 => register("tenant-1", &one_replica()),
            1 => register("", "[]"),
            2 => activate(),
            3 => tombstone_it(),
            _ => command("tenant.move", "{}"),
        }
    }

    proptest! {
        // prepare/apply never panic; registration is one-way and tombstone is
        // monotonic.
        #[test]
        fn arbitrary_command_sequences_respect_the_invariants(
            ops in prop::collection::vec(operation(), 0..64),
        ) {
            let mut state = TenantState::default();
            let mut registered_seen = false;
            let mut tombstone_seen = false;

            for op in ops {
                if let Ok(execution) = Tenant::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Tenant::apply(state, event);
                    }
                }

                if state.is_registered() {
                    registered_seen = true;
                }
                if state.status == TenantStatus::Tombstoned {
                    tombstone_seen = true;
                }

                prop_assert!(!registered_seen || state.is_registered());
                prop_assert!(!tombstone_seen || state.status == TenantStatus::Tombstoned);
            }
        }

        // Replaying the same events always yields the same state.
        #[test]
        fn replay_is_deterministic(ops in prop::collection::vec(operation(), 0..64)) {
            let mut state = TenantState::default();
            let mut events = Vec::new();

            for op in ops {
                if let Ok(execution) = Tenant::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Tenant::apply(state, event.clone());
                        events.push(event);
                    }
                }
            }

            let replayed = events
                .iter()
                .cloned()
                .fold(TenantState::default(), Tenant::apply);
            prop_assert_eq!(replayed, state);
        }
    }
}
