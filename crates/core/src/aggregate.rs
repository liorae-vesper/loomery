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

/// The result of a successful [`AggregatePlan::prepare`]: the events to
/// append (and, later, optional outbox integration events).
///
/// Placeholder while [`trellis_core::execution`] lands — the shell commits
/// this result through consensus, then folds the produced events.
pub struct Execution {}

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
/// A hit on the command's `causation_key` yields [`Processed::Replayed`]
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

    impl AggregatePlan<u32, EmptyCode> for Counter {
        fn prepare(state: u32, _command: Command) -> Result<Execution, DomainError<EmptyCode>> {
            if state > 0 {
                Err(DomainError::new(EmptyCode::OnlyOnce, "already created"))
            } else {
                Ok(Execution {})
            }
        }

        fn apply(state: u32, _event: Event) -> u32 {
            state + 1
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
        let result = process::<_, _, Counter>(0, &registry, command());
        assert!(matches!(result, Processed::Executed(_)));
    }

    #[test]
    fn hit_replays_with_the_anchor_index() {
        let mut registry = Registry::new(10);
        registry.insert(Id::from("cause-1"), 42);

        let result = process::<_, _, Counter>(0, &registry, command());
        assert!(matches!(result, Processed::Replayed { index: 42 }));
    }

    #[test]
    fn rejection_surfaces_the_error() {
        let registry = Registry::new(10);
        let result = process::<_, _, Counter>(1, &registry, command());
        assert!(matches!(result, Processed::Error(_)));
    }

    #[test]
    fn fold_is_sequential_apply() {
        let events = [event(), event(), event()];
        let state = fold::<_, _, Counter>(0, &events);
        assert_eq!(state, 3);
    }
}
