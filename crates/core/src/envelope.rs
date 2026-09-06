// SPDX-License-Identifier: MPL-2.0

//! The envelope types — the published forms of domain commands and events.
//!
//! Every command that enters a Raft group is a [`Command`]; every committed
//! event that leaves it is an [`Event`]. Both carry identity, scoping
//! (organization/workspace), the [`Actor`], dedup keys, and a versioned
//! [`Payload`]. Version-guarded by `envelope_version` + `payload_version`;
//! see `docs/design.md` §6.

use crate::actor::Actor;
use crate::id::Id;
use crate::timestamp::Timestamp;
use serde::{Deserialize, Serialize};

/// Envelope and payload version numbers.
pub type Version = u16;

/// A dedup / correlation key. Same representation as [`Id`].
pub type Key = Id;

/// A frozen, versioned event payload.
///
/// `data` is the serialized event value (a `JSON` string, D3); `version` is the
/// payload schema version so `apply` can upcast old events (P2). Old payloads
/// must never be mutated — a schema change is a new version.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payload {
    /// Current version of the payload.
    pub version: Version,
    /// Serialized data.
    pub data: String,
}

/// The canonical wrapper for every committed domain event.
///
/// This is the published form used by the outbox and all saga/projector
/// consumers. Re-publishing the same committed event yields byte-identical
/// bytes (deterministic ids and timestamps), which is what makes outbox
/// dedup by `(group_id, log_index)` sound (D8/D11).
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// The version of this envelope.
    pub envelope_version: Version,
    /// Identity of the event.
    pub id: Id,
    /// The aggregate that emitted this event.
    pub aggregate_id: Id,
    /// The organization that this event is scoped for.
    pub organization_id: Id,
    /// The workspace which this event came from, optional.
    pub workspace_id: Option<Id>,
    /// A timestamp when this event occurred.
    pub occurred_at: Timestamp,
    /// The idempotency key, to avoid event duplication.
    pub causation_key: Key,
    /// Correlated id, from the outer shell plane.
    pub correlation_id: Key,
    /// The actor that emitted this event (user, system, or saga).
    pub actor: Actor,
    /// The type of event.
    pub event_type: String,
    /// The contents of this event.
    pub payload: Payload,
}

/// The canonical command form — the input to the pure core's `execute`.
///
/// This is what the shell builds with **injected** ids and timestamps.
/// The `causation_key` carries the client-supplied idempotency key and is what
/// `DedupIndex` keys on.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    /// The version of this envelope.
    pub envelope_version: Version,
    /// Identity of the command.
    pub id: Id,
    /// The aggregate this command targets.
    pub aggregate_id: Id,
    /// The organization this command is scoped for.
    pub organization_id: Id,
    /// The workspace this command is scoped for, optional.
    pub workspace_id: Option<Id>,
    /// The injected timestamp this command occurred at.
    pub occurred_at: Timestamp,
    /// The idempotency key, to avoid command duplication.
    pub causation_key: Key,
    /// Correlated id, from the outer shell plane.
    pub correlation_id: Key,
    /// The actor issuing this command (user, system, or saga).
    pub actor: Actor,
    /// The type of command.
    pub command_type: String,
    /// The contents of this command.
    pub payload: Payload,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fully populated envelope with deterministic values, so wire-format
    /// tests are byte-exact and replay-stable.
    fn sample() -> Event {
        Event {
            envelope_version: 1,
            id: Id::from("evt-123"),
            aggregate_id: Id::from("agg-123"),
            organization_id: Id::from("org-123"),
            workspace_id: Some(Id::from("ws-123")),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Id::from("cause-123"),
            correlation_id: Id::from("corr-123"),
            actor: Actor::System,
            event_type: "task.created".to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"title":"build the trellis"}"#.to_owned(),
            },
        }
    }

    #[test]
    fn json_round_trip_preserves_the_envelope() {
        let env = sample();
        let json = serde_json::to_string(&env).unwrap();
        let decoded: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, env);
    }

    /// The command form mirrors the event form — same injected fields, so it
    /// must round-trip the same way.
    #[test]
    fn command_round_trip_preserves_the_command() {
        let cmd = Command {
            envelope_version: 1,
            id: Id::from("cmd-123"),
            aggregate_id: Id::from("agg-123"),
            organization_id: Id::from("org-123"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Id::from("client-key-1"),
            correlation_id: Id::from("corr-123"),
            actor: Actor::User {
                id: Id::from("user-1"),
            },
            command_type: "task.create".to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"title":"a task"}"#.to_owned(),
            },
        };

        let json = serde_json::to_string(&cmd).unwrap();
        let decoded: Command = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, cmd);
    }

    /// Locks the exact wire format: frozen payloads must decode forever, so
    /// a change to this JSON is a breaking change (bump `envelope_version`).
    #[test]
    fn json_wire_format_snapshot() {
        let env = sample();
        let json = serde_json::to_string(&env).unwrap();
        assert_eq!(
            json,
            r#"{"envelope_version":1,"id":"evt-123","aggregate_id":"agg-123","organization_id":"org-123","workspace_id":"ws-123","occurred_at":1700000000000,"causation_key":"cause-123","correlation_id":"corr-123","actor":"System","event_type":"task.created","payload":{"version":1,"data":"{\"title\":\"build the trellis\"}"}}"#
        );
    }
}
