// SPDX-License-Identifier: MPL-2.0

//! The task aggregate — the Phase-0 slice of the work core.
//!
//! Enough for a workspace board to exist: create, rename, complete, reopen.
//! Projects, comments, dependencies and follow-ups are Phase 2 (`design.md`
//! §5); this plan is deliberately the smallest useful task state machine, and
//! every payload is frozen the moment it ships.
//!
//! Status is a two-state machine (`Open` ⇄ `Done`); reopen is a compensating
//! event, so there is no task deletion.
//!
//! See `docs/domain-model.md` for the command/event table and transition matrix.

use crate::aggregate::{AggregatePlan, Execution, event_from_command};
use crate::envelope::{Command, Event, Payload};
use crate::error::DomainError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// `task.create` — open a new task.
pub const CREATE: &str = "task.create";

/// `task.created` — a task was opened.
pub const CREATED: &str = "task.created";

/// `task.rename` — replace the task's title.
pub const RENAME: &str = "task.rename";

/// `task.renamed` — the title was replaced.
pub const RENAMED: &str = "task.renamed";

/// `task.complete` — mark the task done.
pub const COMPLETE: &str = "task.complete";

/// `task.completed` — the task was marked done.
pub const COMPLETED: &str = "task.completed";

/// `task.reopen` — return a done task to open.
pub const REOPEN: &str = "task.reopen";

/// `task.reopened` — the task was returned to open.
pub const REOPENED: &str = "task.reopened";

/// The maximum task-title length, in bytes (D10).
pub const MAX_TITLE_BYTES: usize = 500;

/// The task's lifecycle status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// The task is open and can be worked on.
    #[default]
    Open,
    /// The task has been completed.
    Done,
}

/// The payload of `task.create`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Create {
    /// The task's title.
    pub title: String,
}

/// The payload of `task.created`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Created {
    /// The task's title.
    pub title: String,
}

/// The payload of `task.rename`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rename {
    /// The task's new title.
    pub title: String,
}

/// The payload of `task.renamed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Renamed {
    /// The task's new title.
    pub title: String,
}

/// The (empty) payload of `task.complete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Complete {}

/// The (empty) payload of `task.completed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completed {}

/// The (empty) payload of `task.reopen`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reopen {}

/// The (empty) payload of `task.reopened`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reopened {}

/// Everything the task aggregate remembers.
///
/// `#[serde(default)]` keeps older snapshots decodable as fields are added.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TaskState {
    /// The task's title, once created.
    pub title: Option<String>,
    /// The task's lifecycle status.
    pub status: TaskStatus,
}

impl TaskState {
    /// Whether the task exists. `title` is the creation marker.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.title.is_some()
    }

    /// Whether the task is open.
    #[must_use]
    pub const fn is_open(&self) -> bool {
        matches!(self.status, TaskStatus::Open)
    }
}

/// Why the task plan refused a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCode {
    /// The command is not one this plan handles.
    UnknownCommand,
    /// The command's payload is not a well-formed shape for its type.
    InvalidPayload,
    /// The event payload could not be serialized.
    UnserializableEvent,
    /// The task has already been created.
    AlreadyCreated,
    /// The task has not been created yet.
    NotCreated,
    /// The task has already been completed.
    AlreadyDone,
    /// The task is not done, so it cannot be reopened.
    NotDone,
    /// The requested title is empty or longer than [`MAX_TITLE_BYTES`].
    InvalidTitle,
}

/// The task aggregate's plan — a zero-sized marker; the type is the plan.
pub struct Task;

impl AggregatePlan<TaskState, TaskCode> for Task {
    fn prepare(state: TaskState, command: Command) -> Result<Execution, DomainError<TaskCode>> {
        match command.command_type.as_str() {
            CREATE => create(&state, &command),
            RENAME => rename(&state, &command),
            COMPLETE => complete(&state, &command),
            REOPEN => reopen(&state, &command),
            _ => Err(reject(
                TaskCode::UnknownCommand,
                "the task plan handles task.create, task.rename, task.complete and task.reopen only",
            )),
        }
    }

    fn apply(state: TaskState, event: Event) -> TaskState {
        match event.event_type.as_str() {
            CREATED => match decode::<Created>(&event.payload.data) {
                Ok(created) => TaskState {
                    title: Some(created.title),
                    status: TaskStatus::Open,
                },
                Err(_) => state,
            },
            RENAMED => match decode::<Renamed>(&event.payload.data) {
                Ok(renamed) => TaskState {
                    title: Some(renamed.title),
                    ..state
                },
                Err(_) => state,
            },
            COMPLETED => TaskState {
                status: TaskStatus::Done,
                ..state
            },
            REOPENED => TaskState {
                status: TaskStatus::Open,
                ..state
            },
            // An event this plan cannot read is not ours to fold.
            _ => state,
        }
    }
}

/// Open a task. Re-creating is a different intent and is rejected; the dedup
/// window handles the retry.
fn create(state: &TaskState, command: &Command) -> Result<Execution, DomainError<TaskCode>> {
    if state.is_created() {
        return Err(reject(
            TaskCode::AlreadyCreated,
            "the task has already been created",
        ));
    }

    let payload: Create = decode_command(command)?;
    if !valid_title(&payload.title) {
        return Err(reject(
            TaskCode::InvalidTitle,
            "the task title must be non-empty and within the length bound",
        ));
    }

    let data = encode(
        &Created {
            title: payload.title,
        },
        CREATED,
    )?;
    Ok(execution(command, CREATED, data))
}

/// Replace the title. Allowed while the task exists, open or done.
fn rename(state: &TaskState, command: &Command) -> Result<Execution, DomainError<TaskCode>> {
    if !state.is_created() {
        return Err(reject(
            TaskCode::NotCreated,
            "the task has not been created yet",
        ));
    }

    let payload: Rename = decode_command(command)?;
    if !valid_title(&payload.title) {
        return Err(reject(
            TaskCode::InvalidTitle,
            "the task title must be non-empty and within the length bound",
        ));
    }

    let data = encode(
        &Renamed {
            title: payload.title,
        },
        RENAMED,
    )?;
    Ok(execution(command, RENAMED, data))
}

/// Mark the task done.
fn complete(state: &TaskState, command: &Command) -> Result<Execution, DomainError<TaskCode>> {
    if !state.is_created() {
        return Err(reject(
            TaskCode::NotCreated,
            "the task has not been created yet",
        ));
    }
    if !state.is_open() {
        return Err(reject(
            TaskCode::AlreadyDone,
            "the task has already been completed",
        ));
    }

    let _: Complete = decode_command(command)?;
    let data = encode(&Completed {}, COMPLETED)?;
    Ok(execution(command, COMPLETED, data))
}

/// Return a done task to open.
fn reopen(state: &TaskState, command: &Command) -> Result<Execution, DomainError<TaskCode>> {
    if !state.is_created() {
        return Err(reject(
            TaskCode::NotCreated,
            "the task has not been created yet",
        ));
    }
    if state.is_open() {
        return Err(reject(
            TaskCode::NotDone,
            "the task is not done, so it cannot be reopened",
        ));
    }

    let _: Reopen = decode_command(command)?;
    let data = encode(&Reopened {}, REOPENED)?;
    Ok(execution(command, REOPENED, data))
}

/// Whether `title` is a legal task title: non-blank and within
/// [`MAX_TITLE_BYTES`].
fn valid_title(title: &str) -> bool {
    !title.trim().is_empty() && title.len() <= MAX_TITLE_BYTES
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

/// Decodes a command's payload, mapping failures to [`TaskCode::InvalidPayload`].
fn decode_command<T: DeserializeOwned>(command: &Command) -> Result<T, DomainError<TaskCode>> {
    decode(&command.payload.data)
}

/// Decodes a payload string.
fn decode<T: DeserializeOwned>(data: &str) -> Result<T, DomainError<TaskCode>> {
    serde_json::from_str(data).map_err(|cause| {
        DomainError::with_cause(
            TaskCode::InvalidPayload,
            "the task payload is malformed",
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Serializes an event payload, mapping failures to
/// [`TaskCode::UnserializableEvent`].
fn encode<T: Serialize>(value: &T, event_type: &str) -> Result<String, DomainError<TaskCode>> {
    serde_json::to_string(value).map_err(|cause| {
        DomainError::with_cause(
            TaskCode::UnserializableEvent,
            &format!("could not serialize {event_type}"),
            Some(anyhow::Error::new(cause)),
        )
    })
}

/// Builds a domain error for `code`.
fn reject(code: TaskCode, message: &str) -> DomainError<TaskCode> {
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
            id: Id::from("cmd-5"),
            aggregate_id: Id::from("task-1"),
            organization_id: Id::from("org-1"),
            workspace_id: Some(Id::from("ws-1")),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, &format!("{command_type}:{payload}")),
            correlation_key: Key::new(&KEY_NS, "board"),
            actor: Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload.to_owned(),
            },
        }
    }

    fn create(title: &str) -> Command {
        command(CREATE, &format!(r#"{{"title":"{title}"}}"#))
    }

    fn rename_to(title: &str) -> Command {
        command(RENAME, &format!(r#"{{"title":"{title}"}}"#))
    }

    fn complete_it() -> Command {
        command(COMPLETE, "{}")
    }

    fn reopen_it() -> Command {
        command(REOPEN, "{}")
    }

    fn code_of<T>(result: Result<T, DomainError<TaskCode>>) -> Option<TaskCode> {
        result.err().map(|error| error.code)
    }

    fn advance(state: TaskState, command: Command) -> TaskState {
        let execution = Task::prepare(state.clone(), command).unwrap();
        execution.events.into_iter().fold(state, Task::apply)
    }

    fn open() -> TaskState {
        TaskState {
            title: Some("write the docs".to_owned()),
            status: TaskStatus::Open,
        }
    }

    fn done() -> TaskState {
        TaskState {
            status: TaskStatus::Done,
            ..open()
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

    // --- create / rename -------------------------------------------------

    #[test]
    fn create_builds_the_event_and_opens_on_apply() {
        let command = create("write the docs");
        let execution = Task::prepare(TaskState::default(), command.clone()).unwrap();
        let event = &execution.events[0];

        assert_eq!(event.event_type, CREATED);
        assert_eq!(event.id, command.event_id(0));
        assert_eq!(event.workspace_id, Some(Id::from("ws-1")));

        let state = Task::apply(TaskState::default(), event.clone());
        assert_eq!(state.title.as_deref(), Some("write the docs"));
        assert_eq!(state.status, TaskStatus::Open);
    }

    #[test]
    fn create_rejects_an_existing_task_and_a_blank_title() {
        assert_eq!(
            code_of(Task::prepare(open(), create("other"))),
            Some(TaskCode::AlreadyCreated)
        );
        assert_eq!(
            code_of(Task::prepare(TaskState::default(), create("   "))),
            Some(TaskCode::InvalidTitle)
        );
    }

    #[test]
    fn rename_replaces_the_title_in_either_status() {
        let renamed = advance(open(), rename_to("write better docs"));
        assert_eq!(renamed.title.as_deref(), Some("write better docs"));

        let renamed_done = advance(done(), rename_to("write better docs"));
        assert_eq!(renamed_done.title.as_deref(), Some("write better docs"));
        assert_eq!(renamed_done.status, TaskStatus::Done);
    }

    #[test]
    fn rename_requires_a_task_and_a_valid_title() {
        assert_eq!(
            code_of(Task::prepare(TaskState::default(), rename_to("x"))),
            Some(TaskCode::NotCreated)
        );
        assert_eq!(
            code_of(Task::prepare(open(), rename_to("   "))),
            Some(TaskCode::InvalidTitle)
        );
    }

    // --- complete / reopen -----------------------------------------------

    #[test]
    fn complete_and_reopen_toggle_the_status() {
        assert_eq!(advance(open(), complete_it()).status, TaskStatus::Done);
        assert_eq!(advance(done(), reopen_it()).status, TaskStatus::Open);
    }

    #[test]
    fn complete_and_reopen_require_the_right_status() {
        assert_eq!(
            code_of(Task::prepare(TaskState::default(), complete_it())),
            Some(TaskCode::NotCreated)
        );
        assert_eq!(
            code_of(Task::prepare(done(), complete_it())),
            Some(TaskCode::AlreadyDone)
        );
        assert_eq!(
            code_of(Task::prepare(open(), reopen_it())),
            Some(TaskCode::NotDone)
        );
    }

    // --- unknown / malformed ---------------------------------------------

    #[test]
    fn unknown_commands_and_malformed_payloads_are_rejected() {
        assert_eq!(
            code_of(Task::prepare(open(), command("task.assign", "{}"))),
            Some(TaskCode::UnknownCommand)
        );

        let mut malformed = create("write the docs");
        malformed.payload.data = "not json".to_owned();
        let error = Task::prepare(TaskState::default(), malformed).unwrap_err();
        assert_eq!(error.code, TaskCode::InvalidPayload);
        assert!(error.cause.is_some());
    }

    #[test]
    fn apply_ignores_foreign_and_malformed_events() {
        let foreign = typed(create("write the docs"), "workspace.created");
        assert_eq!(
            Task::apply(TaskState::default(), foreign),
            TaskState::default()
        );

        let malformed = Event {
            payload: Payload {
                version: 1,
                data: "not json".to_owned(),
            },
            ..typed(create("write the docs"), CREATED)
        };
        assert_eq!(
            Task::apply(TaskState::default(), malformed),
            TaskState::default()
        );
    }

    // --- transition matrix -----------------------------------------------

    #[test]
    fn transition_matrix_matches_the_documented_table() {
        let fresh = TaskState::default();
        let live = open();
        let finished = done();

        // create
        assert!(Task::prepare(fresh.clone(), create("write the docs")).is_ok());
        assert_eq!(
            code_of(Task::prepare(live.clone(), create("write the docs"))),
            Some(TaskCode::AlreadyCreated)
        );
        assert_eq!(
            code_of(Task::prepare(finished.clone(), create("write the docs"))),
            Some(TaskCode::AlreadyCreated)
        );

        // rename
        assert_eq!(
            code_of(Task::prepare(fresh.clone(), rename_to("x"))),
            Some(TaskCode::NotCreated)
        );
        assert!(Task::prepare(live.clone(), rename_to("x")).is_ok());
        assert!(Task::prepare(finished.clone(), rename_to("x")).is_ok());

        // complete
        assert_eq!(
            code_of(Task::prepare(fresh.clone(), complete_it())),
            Some(TaskCode::NotCreated)
        );
        assert!(Task::prepare(live.clone(), complete_it()).is_ok());
        assert_eq!(
            code_of(Task::prepare(finished.clone(), complete_it())),
            Some(TaskCode::AlreadyDone)
        );

        // reopen
        assert_eq!(
            code_of(Task::prepare(fresh, reopen_it())),
            Some(TaskCode::NotCreated)
        );
        assert_eq!(
            code_of(Task::prepare(live, reopen_it())),
            Some(TaskCode::NotDone)
        );
        assert!(Task::prepare(finished, reopen_it()).is_ok());
    }

    // --- property tests --------------------------------------------------

    fn operation() -> impl Strategy<Value = u8> {
        0u8..6
    }

    fn command_for(op: u8) -> Command {
        match op {
            0 => create("write the docs"),
            1 => create(""),
            2 => rename_to("write better docs"),
            3 => complete_it(),
            4 => reopen_it(),
            _ => command("task.assign", "{}"),
        }
    }

    proptest! {
        // prepare/apply never panic, and a task is never `Done` before it is
        // created; the title never disappears once set.
        #[test]
        fn arbitrary_command_sequences_respect_the_invariants(
            ops in prop::collection::vec(operation(), 0..64),
        ) {
            let mut state = TaskState::default();
            let mut created_seen = false;

            for op in ops {
                if let Ok(execution) = Task::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Task::apply(state, event);
                    }
                }

                if state.is_created() {
                    created_seen = true;
                }
                prop_assert!(state.is_open() || state.is_created());
                prop_assert!(!created_seen || state.is_created());
            }
        }

        // Replaying the same events always yields the same state.
        #[test]
        fn replay_is_deterministic(ops in prop::collection::vec(operation(), 0..64)) {
            let mut state = TaskState::default();
            let mut events = Vec::new();

            for op in ops {
                if let Ok(execution) = Task::prepare(state.clone(), command_for(op)) {
                    for event in execution.events {
                        state = Task::apply(state, event.clone());
                        events.push(event);
                    }
                }
            }

            let replayed = events
                .iter()
                .cloned()
                .fold(TaskState::default(), Task::apply);
            prop_assert_eq!(replayed, state);
        }
    }
}
