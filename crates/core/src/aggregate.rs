// SPDX-License-Identifier: MPL-2.0

//! The aggregate contract — the pure core's command/event behaviour.
//!
//! An [`AggregatePlan`] is pure and deterministic: `prepare` validates a
//! [`Command`] against the current state and produces an [`Execution`]
//! (nothing is executed — the plan *prepares*; the shell commits it), and
//! `apply` folds an [`Event`] into state. `process` composes the
//! [`Registry`] dedup check with `prepare`; `fold` is the replay primitive.
//!
//! The plan is its **type**, not an instance: trait methods are static
//! associated functions, dispatched through the type parameter `A` — the
//! Rust mirror of Elixir's module dispatch (`mod.execute/2`). `self` is
//! deliberately absent: a zero-data plan has no state worth borrowing.
//!
//! **Critical-section contract:** `process` does **not** record the dedup
//! entry — the shell records it *after* the events are durably appended and
//! applied, so a failed append needs no rollback.

use crate::aggregate::Processed::{Error, Executed, Replayed};
use crate::dedup::Registry;
use crate::envelope::{Command, Event};
use crate::error::DomainError;

/// The content type of an outbound (integration) event payload.
#[derive(Debug)]
pub enum ContentType {
    /// Plain text payload.
    Text,
    /// JSON payload.
    JSON,
}

/// An outbound integration event — emitted *beside* the domain events and
/// published by the shell after commit (the outbox payload, D8/D11).
#[derive(Debug)]
pub struct OutboundEvent {
    /// The NATS subject / integration channel (e.g. `trellis.email.invitation`).
    pub subject: &'static str,
    /// The payload's content type.
    pub content_type: ContentType,
    /// The serialized payload bytes.
    pub payload: Vec<u8>,
}

/// The result of a successful [`AggregatePlan::prepare`]: the events to
/// append through consensus, plus optional outbound integration events.
#[derive(Debug)]
pub struct Execution {
    /// The domain events to commit via the log — these get folded into state
    /// on every replica.
    pub events: Vec<Event>,
    /// Optional outbound integration events (emails, notifications…), kept
    /// off the consensus hot path — the outbox tailer publishes these after
    /// commit (no direct network calls from the consensus loop).
    pub outbound_events: Vec<OutboundEvent>,
}

/// The pure command/event behaviour of one aggregate.
///
/// Implementations are plain data transformers: no I/O, no wall clock, no
/// randomness (determinism is what makes replicated apply identical on
/// every `OpenRaft` replica — `docs/design.md` §2.1). Methods are **static**:
/// the type is the plan, so implementors hold no instance data.
pub trait AggregatePlan<State, ErrorCode> {
    /// Validates `command` against `state` and produces the events to append.
    ///
    /// `state` is the aggregate's current state; commands that mutate it are
    /// rejected with a [`DomainError`]. Does **not** execute anything.
    ///
    /// # Errors
    ///
    /// Returns `Err(DomainError)` when the command violates the aggregate's
    /// preconditions.
    fn prepare(state: State, command: Command) -> Result<Execution, DomainError<ErrorCode>>;

    /// Folds `event` into `state`, yielding the new state.
    ///
    /// Given the same state and event, always yields the same state — the
    /// replay primitive. Creation events build state regardless of prior
    /// state (re-add/re-assign safe).
    fn apply(state: State, event: Event) -> State;
}

/// The outcome of [`process`]: the command was executed, replayed from the
/// dedup window, or rejected.
pub enum Processed<ErrorCode> {
    /// The command passed `prepare`; the shell should commit the execution.
    Executed(Execution),
    /// Replayed from the dedup window — the command was already processed.
    /// `index` is the log index of its first committed event.
    Replayed {
        /// Log index of the first event the replayed command produced (the
        /// Read-Your-Writes anchor).
        index: usize,
    },
    /// The command was rejected by the aggregate's preconditions.
    Error(DomainError<ErrorCode>),
}

/// Composes the dedup check with command preparation.
///
/// A hit on the command's `causation_key` yields [`Replayed`]
/// (no re-execution); a miss runs `A::prepare` on the plan type `A`.
///
/// Does **not** record the dedup entry — see the module docs.
#[must_use]
pub fn process<State, ErrorCode, A: AggregatePlan<State, ErrorCode>>(
    state: State,
    registry: &Registry,
    command: Command,
) -> Processed<ErrorCode> {
    if let Some(hit) = registry.lookup(&command.causation_key) {
        return Replayed {
            index: hit.first_log_index,
        };
    }

    match A::prepare(state, command) {
        Ok(execution) => Executed(execution),
        Err(error) => Error(error),
    }
}

/// Folds events into state, in order — the replay primitive.
///
/// `fold::<A>(state, events)` must equal applying each event sequentially:
/// the algebraic guarantee every replica depends on.
#[must_use]
pub fn fold<State, ErrorCode, A: AggregatePlan<State, ErrorCode>>(
    state: State,
    events: &[Event],
) -> State {
    events
        .iter()
        .fold(state, |state, event| A::apply(state, event.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Actor;
    use crate::envelope::Payload;
    use crate::id::Id;
    use crate::timestamp::Timestamp;

    /// A minimal aggregate: one creation event, one mutation event. The
    /// type itself is the plan — no instance needed.
    struct Counter;
    #[derive(PartialEq, Debug)]
    struct CounterState(u32);

    impl AggregatePlan<CounterState, EmptyCode> for Counter {
        fn prepare(
            state: CounterState,
            _command: Command,
        ) -> Result<Execution, DomainError<EmptyCode>> {
            if state.0 > 0 {
                Err(DomainError::new(EmptyCode::OnlyOnce, "already created"))
            } else {
                Ok(Execution {
                    events: vec![event()],
                    outbound_events: vec![OutboundEvent {
                        subject: "trellis.counter.incremented",
                        content_type: ContentType::JSON,
                        payload: b"{}".to_vec(),
                    }],
                })
            }
        }

        fn apply(state: CounterState, _event: Event) -> CounterState {
            CounterState(state.0 + 1)
        }
    }

    #[derive(Debug, PartialEq)]
    enum EmptyCode {
        OnlyOnce,
    }

    fn command() -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-1"),
            aggregate_id: Id::from("agg-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Id::from("cause-1"),
            correlation_id: Id::from("corr-1"),
            actor: Actor::System,
            command_type: "counter.create".to_owned(),
            payload: Payload {
                version: 1,
                data: "{}".to_owned(),
            },
        }
    }

    fn event() -> Event {
        Event {
            envelope_version: 1,
            id: Id::from("evt-1"),
            aggregate_id: Id::from("agg-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Id::from("cause-1"),
            correlation_id: Id::from("corr-1"),
            actor: Actor::System,
            event_type: "counter.incremented".to_owned(),
            payload: Payload {
                version: 1,
                data: "{}".to_owned(),
            },
        }
    }

    #[test]
    fn miss_prepares_and_executes() {
        let registry = Registry::new(10);
        let result = process::<_, _, Counter>(CounterState(0), &registry, command());
        assert!(matches!(result, Executed(_)));
    }

    #[test]
    fn prepare_returns_planned_events_and_outbound() {
        let execution = Counter::prepare(CounterState(0), command()).unwrap();

        // one planned domain event, folded on every replica after commit
        assert_eq!(execution.events.len(), 1);
        assert_eq!(execution.events[0].event_type, "counter.incremented");

        // one outbound integration event for the shell's outbox (D8/D11)
        assert_eq!(execution.outbound_events.len(), 1);
        let outbound = &execution.outbound_events[0];
        assert_eq!(outbound.subject, "trellis.counter.incremented");
        assert!(matches!(outbound.content_type, ContentType::JSON));
        assert_eq!(outbound.payload, b"{}");
    }

    #[test]
    fn hit_replays_with_the_anchor_index() {
        let mut registry = Registry::new(10);
        registry.insert(Id::from("cause-1"), 42);

        let result = process::<_, _, Counter>(CounterState(0), &registry, command());
        assert!(matches!(result, Replayed { index: 42 }));
    }

    #[test]
    fn rejection_surfaces_the_error() {
        let registry = Registry::new(10);
        let result = process::<_, _, Counter>(CounterState(1), &registry, command());
        assert!(matches!(result, Error(_)));
    }

    #[test]
    fn fold_is_sequential_apply() {
        let events = [event(), event(), event()];
        let state = fold::<_, _, Counter>(CounterState(0), &events);
        assert_eq!(state, CounterState(3));
    }

    // ---------------------------------------------------------------------
    // Property tests: the aggregate algebra, held to its guarantees.

    use proptest::prelude::*;

    proptest! {
        // fold is associative: replaying events in any chunking yields the
        // same state — the identity replays and distributes over commits.
        #[test]
        fn fold_associativity_chunked(start in 0u32..128, a in 0usize..64, b in 0usize..64) {
            let events = vec![event(); a + b];
            let whole = fold::<_, _, Counter>(CounterState(start), &events);
            let (left, right) = events.split_at(a);
            let mid = fold::<_, _, Counter>(CounterState(start), left);
            let split = fold::<_, _, Counter>(mid, right);
            assert_eq!(whole, split);
        }

        // replay determinism: identical events produce identical state.
        #[test]
        fn fold_is_deterministic(start in 0u32..128, k in 0usize..128) {
            let events = vec![event(); k];
            let first = fold::<_, _, Counter>(CounterState(start), &events);
            let second = fold::<_, _, Counter>(CounterState(start), &events);
            assert_eq!(first, second);
        }

        // the counter's concrete algebra: k increments are exactly +k.
        #[test]
        fn fold_counts_events(start in 0u32..128, k in 0u32..128) {
            let events = vec![event(); k as usize];
            let state = fold::<_, _, Counter>(CounterState(start), &events);
            assert_eq!(state, CounterState(start + k));
        }

        // prepare is the creation gate: only a fresh (zero) state is accepted.
        #[test]
        fn prepare_accepts_only_zero_state(n in 0u32..1000) {
            let result = Counter::prepare(CounterState(n), command());
            if let Err(e) = result {
                assert_eq!(e.code, EmptyCode::OnlyOnce);
            } else {
                assert_eq!(n, 0);
            }
        }

        // process classifies every command: a dedup hit replays with the
        // recorded anchor index; a miss runs prepare (Executed or Error) —
        // never anything else, never panics.
        #[test]
        fn process_dedup_classification(already_seen in any::<bool>(), index in 0usize..1000) {
            let mut registry = Registry::new(16);
            if already_seen {
                registry.insert(Id::from("cause-1"), index);
            }

            let result = process::<_, _, Counter>(CounterState(0), &registry, command());
            match result {
                Replayed { index: got } => {
                    assert!(already_seen);
                    assert_eq!(got, index);
                }
                Executed(_) | Error(_) => assert!(!already_seen),
            }
        }
    }
}
