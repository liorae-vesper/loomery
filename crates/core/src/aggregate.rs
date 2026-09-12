// SPDX-License-Identifier: MPL-2.0

//! The aggregate contract — the pure core's command/event behaviour.
//!
//! # What an aggregate is
//!
//! An [`AggregatePlan`] is the **pure logic** for one domain shape (a task,
//! a workspace, …): `prepare` validates a [`Command`] against the current
//! state and produces an [`Execution`]; `apply` folds an [`Event`] into
//! state. Nothing executes — the plan *prepares*; the shell commits through
//! consensus and every replica folds.
//!
//! The plan is its **type**, not an instance: methods are static associated
//! functions dispatched through the type parameter `A` (the Rust mirror of
//! Elixir's `mod.execute/2`), with no borrowable `self`. One plan type
//! serves **many states** — every stream (per organization / per aggregate
//! instance) folds its own `State` through the same [`AggregatePlan`].
//!
//! # Where the data comes from
//!
//! `prepare` receives exactly two things, and everything else must be
//! derivable from them:
//!
//! - **`state`** — the *truth*: everything the aggregate already learned
//!   from its own events (folded on every replica). Never re-read from a
//!   store, never fetched — it arrives as a value.
//! - **`command`** — the *intent* plus **injected metadata**: what the user
//!   wants (payload fields), and the ids / `occurred_at` / `actor` /
//!   `causation_key` the shell mints at the boundary. The core never mints
//!   or reads these itself (D5).
//!
//! **External data** (blocking-but-pure work, cross-group facts) is
//! pre-computed at the **edge by the shell and delivered inside the command
//! payload** — e.g. an `argon2` password hash, a rate read once at the
//! gateway (#7 edge pre-computation). The core never *gathers*: it
//! *receives*. Cross-aggregate or control-group data must ride in the
//! command too (no core-side reads of other groups — the no-2PC rule,
//! D10-style).
//!
//! # How to use it (Shape A — full state, focused destructuring)
//!
//! 1. Define the plan as a zero-sized marker and its state as a data
//!    struct: `struct Task; struct TaskState { … }`.
//! 2. `impl AggregatePlan<TaskState, TaskCode> for Task`.
//! 3. In `prepare`, match on `command.command_type` and destructure **only**
//!    the state fields that command needs: `let TaskState { status, … } =
//!    &state;` — the “50-field state” shrinks to the fields in scope.
//!    Validate → derive the *result* data → build the events. The events
//!    **are** the resulted data: everything the system must remember about
//!    this command is frozen into their payloads, nothing is recomputed or
//!    fetched later.
//! 4. In `apply`, a pure fold: `State { field: new_value, ..state }`
//!    struct-update copies everything except the touched field.
//!
//! The **shell** owns everything else: loading state, edge pre-computation,
//! building the [`Command`], calling [`process`], committing via consensus,
//! and recording the dedup entry **after** durable append+apply.
//!
//! # What the core must never do
//!
//! - No I/O, no wall clock, no randomness, no message passing.
//! - No reads of other aggregates or the control group — cross-group data
//!   comes through the command.
//! - Nothing that could diverge between replicas: the committed command is
//!   re-`prepare`d on **every** replica, so purity is what keeps replicas
//!   agreeing (and what makes replays from a snapshot deterministic).
//!
//! **Critical-section contract:** [`process`] does **not** record the dedup
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
    /// # Data in
    ///
    /// - `state` — the aggregate's current truth (per stream, folded from
    ///   its own events).
    /// - `command` — user intent + injected metadata (ids, `occurred_at`,
    ///   `actor`, `causation_key`) + edge-precomputed externals in the
    ///   payload (e.g. `argon2` hashes — see module docs).
    ///
    /// # Discipline (Shape A)
    ///
    /// ```ignore
    /// match command.command_type.as_str() {
    ///     "task.rename" => {
    ///         // destructure only what THIS command touches
    ///         let TaskState { title, status, .. } = &state;
    ///         // validate … then derive the RESULT and freeze it into events
    ///     }
    ///     // …
    /// }
    /// ```
    ///
    /// The events carry the full resulted data — nothing is recomputed or
    /// re-fetched after commit.
    ///
    /// # Errors
    ///
    /// Returns `Err(DomainError)` when the command violates the aggregate's
    /// preconditions.
    fn prepare(state: State, command: Command) -> Result<Execution, DomainError<ErrorCode>>;

    /// Folds `event` into `state`, yielding the new state.
    ///
    /// Pure and total: given the same state and event it always yields the
    /// same state — the replay primitive. Creation events build state
    /// regardless of prior state (re-add/re-assign safe). Prefer struct
    /// update syntax so untouched fields flow through:
    ///
    /// ```ignore
    /// TaskState { status: EventStatus::Done, ..state }
    /// ```
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
    use crate::key::Key;
    use crate::timestamp::Timestamp;
    use uuid::Uuid;

    /// A stable namespace for the deterministic test keys below.
    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    /// A deterministic dedup/correlation key for the tests.
    fn key(data: &str) -> Key {
        Key::new(&KEY_NS, data)
    }

    /// The intent fingerprint a command's `causation_key` is recorded with.
    fn fingerprint(data: &str) -> Key {
        Key::new(&KEY_NS, &format!("intent-{data}"))
    }

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
            causation_key: key("cause-1"),
            correlation_key: key("corr-1"),
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
            causation_key: key("cause-1"),
            correlation_key: key("corr-1"),
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
        registry.insert(key("cause-1"), fingerprint("cause-1"), 42);

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
                registry.insert(key("cause-1"), fingerprint("cause-1"), index);
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
