// SPDX-License-Identifier: MPL-2.0

//! The envelope types — the published forms of domain commands and events.
//!
//! Every command that enters a Raft group is a [`Command`]; every committed
//! event that leaves it is an [`Event`]. Both carry identity, scoping
//! (organization/workspace), the [`Actor`], dedup keys, and a versioned
//! [`Payload`]. Version-guarded by `envelope_version` + `payload_version`;
//! see `workpad/design.md` §6.

use crate::actor::Actor;
use crate::id::Id;
use crate::key::Key;
use crate::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Envelope and payload version numbers.
pub type Version = u16;

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
    ///
    /// Derived from the command ([`Command::event_id`]) — never minted in the
    /// core: the committed command is re-`prepare`d on *every* replica, so a
    /// minted id here would diverge between them (D12).
    pub id: Id,
    /// The aggregate that emitted this event.
    pub aggregate_id: Id,
    /// The organization that this event is scoped for.
    pub organization_id: Id,
    /// The workspace which this event came from, optional.
    pub workspace_id: Option<Id>,
    /// A timestamp when this event occurred.
    pub occurred_at: Timestamp,
    /// The idempotency key ([`Key`]), to avoid event duplication.
    pub causation_key: Key,
    /// The saga/workflow this event belongs to ([`Key`]).
    ///
    /// One value per saga instance, shared by every command and event it
    /// produces and derived from the workflow's business identity — an
    /// invitation, an organization — never from per-attempt state. A resumed
    /// saga runner re-derives it, and consumers group a workflow's events by
    /// it. The *intent* of one command is [`Event::causation_key`]; a
    /// per-attempt trace id is observability's business, not the envelope's.
    pub correlation_key: Key,
    /// The actor that emitted this event (user, system, or saga).
    pub actor: Actor,
    /// The type of event.
    pub event_type: String,
    /// The contents of this event.
    pub payload: Payload,
}

/// Namespace for ids derived from a command — see [`Command::event_id`].
const EVENT_NAMESPACE: Uuid = Uuid::from_u128(0x6d2f_9a4e_b1c7_4f3d_8e5a_2b6c_9d0e_1f3a);

/// Namespace for command intent fingerprints — see [`Command::fingerprint`].
const FINGERPRINT_NAMESPACE: Uuid = Uuid::from_u128(0x1c9b_7e35_2a48_4d16_9f0b_6e2c_8a71_5d4e);

/// The canonical command form — the input to the pure core's `execute`.
///
/// This is what the shell builds with **injected** ids and timestamps.
/// The `causation_key` carries the client-supplied idempotency key and is what
/// `DedupIndex` keys on.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    /// The version of this envelope.
    pub envelope_version: Version,
    /// Identity of this *attempt* — minted shell-side, once per envelope.
    ///
    /// Entity and event identity does **not** come from here: a retry that
    /// rebuilds the envelope gets a fresh `id`, while [`Command::event_id`]
    /// and [`Command::fingerprint`] stay stable because they derive from
    /// `causation_key` (D12).
    pub id: Id,
    /// The aggregate this command targets.
    pub aggregate_id: Id,
    /// The organization this command is scoped for.
    pub organization_id: Id,
    /// The workspace this command is scoped for, optional.
    pub workspace_id: Option<Id>,
    /// The injected timestamp this command occurred at.
    pub occurred_at: Timestamp,
    /// The idempotency key ([`Key`]), to avoid command duplication.
    pub causation_key: Key,
    /// The saga/workflow this command belongs to ([`Key`]) — see
    /// [`Event::correlation_key`].
    pub correlation_key: Key,
    /// The actor issuing this command (user, system, or saga).
    pub actor: Actor,
    /// The type of command.
    pub command_type: String,
    /// The contents of this command.
    pub payload: Payload,
}

impl Command {
    /// The id of the `index`th event this command produces.
    ///
    /// Derived from `causation_key` and `index`, deliberately **not** from
    /// [`Command::id`] — which changes whenever the shell rebuilds an attempt.
    /// Aggregates call this while building their events (see
    /// [`AggregatePlan::prepare`](crate::aggregate::AggregatePlan::prepare)),
    /// so every replica derives identical event ids for a committed command,
    /// and a resumed retry proposes the same ids as the attempt that crashed.
    #[must_use]
    pub fn event_id(&self, index: usize) -> Id {
        Id::from(Key::new(
            &EVENT_NAMESPACE,
            &format!("{}:{index}", self.causation_key),
        ))
    }

    /// Fingerprint of the command's *intent* — what the caller asked for.
    ///
    /// Per-attempt fields (`id`, `occurred_at`, `correlation_key`) are left
    /// out, so two attempts at the same intent fingerprint identically. The
    /// shell records this next to the dedup entry
    /// ([`Entry::fingerprint`](crate::dedup::Entry::fingerprint)) and answers
    /// a conflict — not a replay — when a `causation_key` comes back with a
    /// *different* fingerprint: that is a client reusing an idempotency key
    /// for a different request, and replaying the earlier result would be a
    /// lie (D12).
    #[must_use]
    pub fn fingerprint(&self) -> Key {
        let workspace = self.workspace_id.as_ref().map_or("-", |id| &**id);
        let payload = &self.payload;

        Key::new(
            &FINGERPRINT_NAMESPACE,
            // Length-prefixed framing: `payload.data` is caller-controlled and
            // may contain `:`, so its length is hashed alongside it.
            &format!(
                "{}:{}:{}:{}:{}:{}:{}",
                self.command_type,
                payload.version,
                self.aggregate_id,
                self.organization_id,
                workspace,
                payload.data.len(),
                payload.data,
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// A stable namespace for the deterministic test keys below. Real keys are
    /// minted shell-side; here it just has to never change, so the wire
    /// snapshot stays byte-exact.
    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    /// A deterministic dedup/correlation key for the tests.
    fn key(data: &str) -> Key {
        Key::new(&KEY_NS, data)
    }

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
            causation_key: key("cause-123"),
            correlation_key: key("corr-123"),
            actor: Actor::System,
            event_type: "task.created".to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"title":"build the trellis"}"#.to_owned(),
            },
        }
    }

    /// A command and a faithful *retry* of it: same intent, freshly minted
    /// per-attempt fields.
    fn command() -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-123"),
            aggregate_id: Id::from("agg-123"),
            organization_id: Id::from("org-123"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: key("client-key-1"),
            correlation_key: key("corr-123"),
            actor: Actor::User {
                id: Id::from("user-1"),
            },
            command_type: "task.create".to_owned(),
            payload: Payload {
                version: 1,
                data: r#"{"title":"a task"}"#.to_owned(),
            },
        }
    }

    /// The same intent, rebuilt after a crash: new minted id, new timestamp,
    /// new trace.
    fn retry() -> Command {
        Command {
            id: Id::from("cmd-999"),
            occurred_at: Timestamp::from(1_700_000_999_999),
            correlation_key: key("corr-999"),
            ..command()
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
        let cmd = command();

        let json = serde_json::to_string(&cmd).unwrap();
        let decoded: Command = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, cmd);
    }

    // ---------------------------------------------------------------------
    // Identity derived from a command: event ids and intent fingerprints.

    #[test]
    fn event_ids_ignore_per_attempt_fields() {
        let first = command();
        let retry = retry();

        // A rebuilt attempt proposes the same event ids — a replay, not a
        // second creation.
        assert_eq!(first.event_id(0), retry.event_id(0));
        assert_eq!(first.event_id(1), retry.event_id(1));
    }

    #[test]
    fn event_ids_separate_events_and_intents() {
        let base = command();

        // Two events from one command are two events.
        assert_ne!(base.event_id(0), base.event_id(1));

        // A different intent is a different event.
        let other = Command {
            causation_key: key("another-client-key"),
            ..command()
        };
        assert_ne!(base.event_id(0), other.event_id(0));
    }

    #[test]
    fn event_ids_are_canonical_uuid_v5s() {
        let id = command().event_id(0);

        assert_eq!(Uuid::parse_str(&id).unwrap().get_version_num(), 5);
        assert_eq!(Id::parse(&id).unwrap(), id);
    }

    #[test]
    fn fingerprint_ignores_per_attempt_fields() {
        assert_eq!(command().fingerprint(), retry().fingerprint());
    }

    #[test]
    fn fingerprint_moves_when_the_intent_does() {
        let base = command().fingerprint();

        let other_payload = Command {
            payload: Payload {
                version: 1,
                data: r#"{"title":"a different task"}"#.to_owned(),
            },
            ..command()
        };
        let other_type = Command {
            command_type: "task.delete".to_owned(),
            ..command()
        };
        let other_scope = Command {
            workspace_id: Some(Id::from("ws-1")),
            ..command()
        };

        assert_ne!(base, other_payload.fingerprint());
        assert_ne!(base, other_type.fingerprint());
        assert_ne!(base, other_scope.fingerprint());
    }

    /// The payload is caller-controlled: two different intents must not be
    /// able to collide by shifting the field boundaries.
    #[test]
    fn fingerprint_framing_resists_boundary_shifting() {
        let first = Command {
            payload: Payload {
                version: 1,
                data: "a:b".to_owned(),
            },
            ..command()
        };
        let second = Command {
            payload: Payload {
                version: 1,
                data: "a".to_owned(),
            },
            ..command()
        };

        assert_ne!(first.fingerprint(), second.fingerprint());
    }

    /// Locks the exact wire format: frozen payloads must decode forever, so
    /// a change to this JSON is a breaking change (bump `envelope_version`).
    #[test]
    fn json_wire_format_snapshot() {
        let env = sample();
        let json = serde_json::to_string(&env).unwrap();
        assert_eq!(
            json,
            r#"{"envelope_version":1,"id":"evt-123","aggregate_id":"agg-123","organization_id":"org-123","workspace_id":"ws-123","occurred_at":1700000000000,"causation_key":"55cd88a7-c96a-5a8c-8a40-76bcf3db16ef","correlation_key":"df4a50ac-a82d-59f6-b0fe-36c72641334a","actor":"System","event_type":"task.created","payload":{"version":1,"data":"{\"title\":\"build the trellis\"}"}}"#
        );
    }
}
