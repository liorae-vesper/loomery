// SPDX-License-Identifier: MPL-2.0

//! The script itself: the three commands in order, and how much of them a group
//! has already committed.
//!
//! The whole plan is a pure function of [`Bootstrap`] plus the group's
//! committed events, so the shell's worker loop is:
//!
//! ```text
//! loop {
//!     let progress = bootstrap.progress(&events_of(group));
//!     match bootstrap.next_command(progress)? {
//!         Some(command) => propose(command).await,   // then refresh `events_of`
//!         None => break,                             // genesis is complete
//!     }
//! }
//! ```
//!
//! [`Progress`] is read from the log by **causation key**, not by event type:
//! the script recognizes its own work without knowing anything about the
//! aggregate crates that produce the events (D12).

use crate::identity::{
    DEFAULT_WORKSPACE_NAME, Step, bootstrap_actor, bootstrap_correlation_key, command_id,
    default_workspace_id, owner_membership_id, step_key,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trellis_core::envelope::{Command, Event, Payload, Version};
use trellis_core::id::Id;
use trellis_core::timestamp::Timestamp;

/// The envelope version every genesis command is written with.
const ENVELOPE_VERSION: Version = 1;

/// The payload version of every genesis command.
const PAYLOAD_VERSION: Version = 1;

/// ① The creator takes leadership of the organization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignLeader {
    /// The user who becomes the organization's leader.
    pub user_id: Id,
}

/// ② The organization's default workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkspace {
    /// The id the workspace must be created with — derived, not minted, so a
    /// resumed attempt asks for the same entity (D12).
    pub workspace_id: Id,
    /// The workspace's name.
    pub name: String,
}

/// ③ The creator joins that workspace as its Owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddOwner {
    /// The user who becomes the workspace Owner.
    pub user_id: Id,
}

impl Step {
    /// The `command_type` this step issues.
    ///
    /// **Part of the control-plane wire contract:** when `trellis-core` grows
    /// the `organization` / `workspace` / `membership` aggregates, they must
    /// accept these names (or this crate imports theirs — one source of truth
    /// either way).
    #[must_use]
    pub const fn command_type(self) -> &'static str {
        match self {
            Self::AssignLeader => "organization.assign_leader",
            Self::CreateWorkspace => "workspace.create",
            Self::AddOwner => "membership.add_owner",
        }
    }

    /// The `event_type` this step's command must produce.
    ///
    /// Stated for consumers, projections and tests. [`Progress`] deliberately
    /// does **not** use it: it matches causation keys, so the script stays
    /// independent of the aggregate crates.
    #[must_use]
    pub const fn event_type(self) -> &'static str {
        match self {
            Self::AssignLeader => "organization.leader_assigned",
            Self::CreateWorkspace => "workspace.created",
            Self::AddOwner => "membership.owner_added",
        }
    }
}

/// What a group has already committed of the script.
///
/// The fields are public so a shell that projects genesis from its own state
/// machine can construct the value without re-reading the log; [`Bootstrap::progress`]
/// derives it from the events themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Progress {
    /// ① has been committed.
    pub assigned_leader: bool,
    /// ② has been committed.
    pub created_workspace: bool,
    /// ③ has been committed.
    pub added_owner: bool,
}

impl Progress {
    /// Whether `step` has been committed.
    #[must_use]
    pub const fn is_done(self, step: Step) -> bool {
        match step {
            Step::AssignLeader => self.assigned_leader,
            Step::CreateWorkspace => self.created_workspace,
            Step::AddOwner => self.added_owner,
        }
    }

    /// The next step to run, in commit order.
    ///
    /// The script is sequential — an Owner cannot be added to a workspace that
    /// does not exist yet — so this is also the only step that may run.
    #[must_use]
    pub const fn next(self) -> Option<Step> {
        if !self.assigned_leader {
            Some(Step::AssignLeader)
        } else if !self.created_workspace {
            Some(Step::CreateWorkspace)
        } else if !self.added_owner {
            Some(Step::AddOwner)
        } else {
            None
        }
    }

    /// Whether every step is committed.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.assigned_leader && self.created_workspace && self.added_owner
    }

    /// Records `step` as committed.
    fn mark(&mut self, step: Step) {
        match step {
            Step::AssignLeader => self.assigned_leader = true,
            Step::CreateWorkspace => self.created_workspace = true,
            Step::AddOwner => self.added_owner = true,
        }
    }
}

/// The inputs of one bootstrap attempt.
///
/// `occurred_at` is the only nondeterministic input: the shell stamps it once
/// and everything else derives from the organization and the step, so two
/// attempts at the same step produce identical commands (D12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bootstrap {
    /// The organization being born.
    pub organization_id: Id,
    /// The user who created it, and becomes its leader and the workspace Owner.
    pub leader_user_id: Id,
    /// The injected timestamp carried by every event of the script.
    pub occurred_at: Timestamp,
}

impl Bootstrap {
    /// Builds the command for `step`.
    ///
    /// # Errors
    ///
    /// [`Error::Payload`] if the step's payload cannot be serialized — it is
    /// ids and strings only, so this cannot happen in practice.
    pub fn command(&self, step: Step) -> Result<Command, Error> {
        Ok(Command {
            envelope_version: ENVELOPE_VERSION,
            id: command_id(&self.organization_id, step),
            aggregate_id: self.aggregate_id(step),
            organization_id: self.organization_id.clone(),
            workspace_id: self.workspace_scope(step),
            occurred_at: self.occurred_at.clone(),
            causation_key: step_key(&self.organization_id, step),
            correlation_key: bootstrap_correlation_key(&self.organization_id),
            actor: bootstrap_actor(),
            command_type: step.command_type().to_owned(),
            payload: Payload {
                version: PAYLOAD_VERSION,
                data: self.payload(step)?,
            },
        })
    }

    /// The next command to propose, or `None` when genesis is complete.
    ///
    /// # Errors
    ///
    /// As [`Bootstrap::command`].
    pub fn next_command(&self, progress: Progress) -> Result<Option<Command>, Error> {
        progress.next().map(|step| self.command(step)).transpose()
    }

    /// Reads a group's committed events and reports which steps are in.
    ///
    /// A step counts as committed when an event carries its causation key, so
    /// the answer comes from the log itself — after a crash, "did my earlier
    /// attempt commit?" is a fact, not a guess.
    #[must_use]
    pub fn progress(&self, events: &[Event]) -> Progress {
        let mut progress = Progress::default();

        for event in events {
            for step in Step::ALL {
                if event.causation_key == step_key(&self.organization_id, step) {
                    progress.mark(step);
                }
            }
        }

        progress
    }

    /// The aggregate the step's command targets.
    fn aggregate_id(&self, step: Step) -> Id {
        match step {
            // ① is an event on the organization aggregate itself.
            Step::AssignLeader => self.organization_id.clone(),
            // ② creates the workspace: the aggregate *is* the new workspace.
            Step::CreateWorkspace => default_workspace_id(&self.organization_id),
            // ③ is the Owner membership entity of the leader.
            Step::AddOwner => owner_membership_id(&self.organization_id, &self.leader_user_id),
        }
    }

    /// The workspace the step's command is scoped to, if any.
    ///
    /// ① runs before a workspace exists; ② and ③ are scoped to the workspace
    /// they create and populate.
    fn workspace_scope(&self, step: Step) -> Option<Id> {
        match step {
            Step::AssignLeader => None,
            Step::CreateWorkspace | Step::AddOwner => {
                Some(default_workspace_id(&self.organization_id))
            }
        }
    }

    /// Serializes the step's payload.
    fn payload(&self, step: Step) -> Result<String, Error> {
        let serialized = match step {
            Step::AssignLeader => serde_json::to_string(&AssignLeader {
                user_id: self.leader_user_id.clone(),
            }),
            Step::CreateWorkspace => serde_json::to_string(&CreateWorkspace {
                workspace_id: default_workspace_id(&self.organization_id),
                name: DEFAULT_WORKSPACE_NAME.to_owned(),
            }),
            Step::AddOwner => serde_json::to_string(&AddOwner {
                user_id: self.leader_user_id.clone(),
            }),
        };

        serialized.map_err(|source| Error::Payload { step, source })
    }
}

/// Why the script could not produce a command.
#[derive(Debug, Error)]
pub enum Error {
    /// A step's payload could not be serialized.
    #[error("could not serialize the {step:?} payload")]
    Payload {
        /// The step whose payload failed.
        step: Step,
        /// The serializer's error.
        #[source]
        source: serde_json::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis_core::actor::Actor;
    use trellis_core::key::Key;

    fn organization() -> Id {
        Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9")
    }

    fn leader() -> Id {
        Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0")
    }

    fn bootstrap() -> Bootstrap {
        Bootstrap {
            organization_id: organization(),
            leader_user_id: leader(),
            occurred_at: Timestamp::from(1_700_000_000_000),
        }
    }

    /// An event as the log would carry it: the only field the script reads is
    /// the causation key.
    fn event_with(causation_key: Key) -> Event {
        Event {
            envelope_version: 1,
            id: Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f2"),
            aggregate_id: organization(),
            organization_id: organization(),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key,
            correlation_key: bootstrap_correlation_key(&organization()),
            actor: Actor::System,
            event_type: "genesis.test".to_owned(),
            payload: Payload {
                version: 1,
                data: "{}".to_owned(),
            },
        }
    }

    /// Marks `steps` as committed by handing `progress` events carrying their
    /// causation keys.
    fn committing(steps: &[Step]) -> Vec<Event> {
        steps
            .iter()
            .map(|step| event_with(step_key(&organization(), *step)))
            .collect()
    }

    #[test]
    fn the_script_runs_the_steps_in_order() {
        let mut progress = Progress::default();

        for expected in Step::ALL {
            assert_eq!(progress.next(), Some(expected));
            progress.mark(expected);
        }

        assert_eq!(progress.next(), None);
        assert!(progress.is_complete());
    }

    #[test]
    fn progress_defaults_to_the_first_step() {
        assert_eq!(Progress::default().next(), Step::ALL.first().copied());
        assert_eq!(Progress::default().next(), Some(Step::AssignLeader));
        assert!(!Progress::default().is_complete());
    }

    #[test]
    fn every_command_carries_the_derived_identity() {
        let bootstrap = bootstrap();

        for step in Step::ALL {
            let command = bootstrap.command(step).unwrap();

            assert_eq!(command.causation_key, step_key(&organization(), step));
            assert_eq!(
                command.correlation_key,
                bootstrap_correlation_key(&organization())
            );
            assert_eq!(command.id, command_id(&organization(), step));
            assert_eq!(command.actor, bootstrap_actor());
            assert_eq!(command.command_type, step.command_type());
            assert_eq!(command.organization_id, organization());
            assert_eq!(command.envelope_version, 1);
            assert_eq!(command.payload.version, 1);
            assert_eq!(command.occurred_at, bootstrap.occurred_at);
        }
    }

    #[test]
    fn the_steps_are_scoped_to_the_entities_they_touch() {
        let bootstrap = bootstrap();
        let workspace = default_workspace_id(&organization());

        let assign_leader = bootstrap.command(Step::AssignLeader).unwrap();
        assert_eq!(assign_leader.aggregate_id, organization());
        assert_eq!(assign_leader.workspace_id, None);

        let workspace_command = bootstrap.command(Step::CreateWorkspace).unwrap();
        assert_eq!(workspace_command.aggregate_id, workspace);
        assert_eq!(workspace_command.workspace_id, Some(workspace.clone()));

        let add_owner = bootstrap.command(Step::AddOwner).unwrap();
        assert_eq!(
            add_owner.aggregate_id,
            owner_membership_id(&organization(), &leader())
        );
        assert_eq!(add_owner.workspace_id, Some(workspace));
    }

    #[test]
    fn the_payloads_carry_the_injected_ids() {
        let bootstrap = bootstrap();

        let leader: AssignLeader =
            serde_json::from_str(&bootstrap.command(Step::AssignLeader).unwrap().payload.data)
                .unwrap();
        assert_eq!(
            leader,
            AssignLeader {
                user_id: bootstrap.leader_user_id.clone()
            }
        );

        let workspace: CreateWorkspace = serde_json::from_str(
            &bootstrap
                .command(Step::CreateWorkspace)
                .unwrap()
                .payload
                .data,
        )
        .unwrap();
        assert_eq!(workspace.name, DEFAULT_WORKSPACE_NAME);
        assert_eq!(
            workspace.workspace_id,
            default_workspace_id(&organization())
        );

        let owner: AddOwner =
            serde_json::from_str(&bootstrap.command(Step::AddOwner).unwrap().payload.data).unwrap();
        assert_eq!(
            owner,
            AddOwner {
                user_id: bootstrap.leader_user_id.clone()
            }
        );
    }

    #[test]
    fn two_attempts_propose_byte_identical_commands() {
        // Nothing but `occurred_at` is injected, so a retry — a fresh
        // `Bootstrap` value built after a restart — proposes the same envelope.
        for step in Step::ALL {
            assert_eq!(
                bootstrap().command(step).unwrap(),
                bootstrap().command(step).unwrap()
            );
        }
    }

    #[test]
    fn progress_is_read_from_the_committed_events() {
        let bootstrap = bootstrap();

        assert_eq!(bootstrap.progress(&[]), Progress::default());

        let after_one = bootstrap.progress(&committing(&[Step::AssignLeader]));
        assert!(after_one.is_done(Step::AssignLeader));
        assert!(!after_one.is_done(Step::CreateWorkspace));

        let complete = bootstrap.progress(&committing(&Step::ALL));
        assert!(complete.is_complete());
    }

    #[test]
    fn progress_ignores_events_that_are_not_genesis() {
        let bootstrap = bootstrap();
        let foreign = event_with(Key::new(
            &trellis_core::Uuid::from_u128(1),
            "someone-elses-intent",
        ));

        assert_eq!(bootstrap.progress(&[foreign]), Progress::default());
    }

    #[test]
    fn progress_is_scoped_to_the_organization() {
        let bootstrap = bootstrap();
        let other_organization = Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f8");
        let foreign = event_with(step_key(&other_organization, Step::AssignLeader));

        // The derived key carries the organization, so another group's genesis
        // cannot mark this one's progress.
        assert_eq!(bootstrap.progress(&[foreign]), Progress::default());
    }

    #[test]
    fn a_resumed_bootstrap_continues_where_it_stopped() {
        let bootstrap = bootstrap();

        // Crash after ① committed: the resume proposes ②, not a second ①.
        let progress = bootstrap.progress(&committing(&[Step::AssignLeader]));
        let next = bootstrap.next_command(progress).unwrap().unwrap();
        assert_eq!(next.command_type, Step::CreateWorkspace.command_type());

        // Crash after ②: ③ follows.
        let progress =
            bootstrap.progress(&committing(&[Step::AssignLeader, Step::CreateWorkspace]));
        let next = bootstrap.next_command(progress).unwrap().unwrap();
        assert_eq!(next.command_type, Step::AddOwner.command_type());

        // Everything committed: nothing left to propose.
        let progress = bootstrap.progress(&committing(&Step::ALL));
        assert!(bootstrap.next_command(progress).unwrap().is_none());
    }

    #[test]
    fn the_wire_names_are_frozen() {
        let commands: Vec<&str> = Step::ALL.iter().map(|step| step.command_type()).collect();
        let events: Vec<&str> = Step::ALL.iter().map(|step| step.event_type()).collect();

        assert_eq!(
            commands,
            [
                "organization.assign_leader",
                "workspace.create",
                "membership.add_owner"
            ]
        );
        assert_eq!(
            events,
            [
                "organization.leader_assigned",
                "workspace.created",
                "membership.owner_added"
            ]
        );
    }
}
