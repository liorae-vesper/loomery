// SPDX-License-Identifier: MPL-2.0

//! The genesis bootstrap worker — the first client of [`crate::group::GroupOps`].
//!
//! Drive the script to completion for one group. The worker is I/O only: it
//! never mints, never reads a clock, and never decides what to do next beyond
//! "ask the script". Progress is read back from the group's own log, so a run
//! interrupted at any point resumes without any bookkeeping.

use crate::group::{GroupOps, ProposeOutcome};
use loomery_genesis::{Bootstrap, Error as PlanError, Progress, Step};

/// How far the worker got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Genesis {
    /// The steps *this run* appended — a resumed run appends only what the log
    /// was missing.
    pub appended: Vec<Step>,
    /// Where the script stands afterwards; complete on every `Ok` return.
    pub progress: Progress,
}

/// Drives the script to completion for one group.
///
/// Safe to run again at any time, from any process: a completed group appends
/// nothing, and an interrupted run resumes at the first step the log does not
/// have yet.
///
/// # Errors
///
/// [`Error`] when the group cannot be read, a step cannot be appended, or the
/// plan refuses to build a command.
pub async fn run(port: &mut impl GroupOps, bootstrap: &Bootstrap) -> Result<Genesis, Error> {
    let mut appended = Vec::new();

    loop {
        // Always re-read: the previous iteration — or a previous process — may
        // have committed a step without us ever learning the result.
        let events = port
            .committed_events(&bootstrap.organization_id)
            .await
            .map_err(Error::Read)?;
        let progress = bootstrap.progress(&events);

        let Some(step) = progress.next() else {
            // Every step is in the log: genesis is complete for this group.
            return Ok(Genesis { appended, progress });
        };

        let command = bootstrap.command(step)?;

        match port
            .propose(command)
            .await
            .map_err(|source| Error::Propose { step, source })?
        {
            ProposeOutcome::Appended { .. } => appended.push(step),
            // Another attempt — or an earlier one of ours — already committed
            // this step. The next read confirms it.
            ProposeOutcome::Replayed { .. } => {}
        }
    }
}

/// What went wrong while driving the script.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The group's committed events could not be read.
    #[error("could not read the group's committed events")]
    Read(#[source] anyhow::Error),
    /// A step's command could not be appended.
    #[error("could not append the {step:?} command")]
    Propose {
        /// The step whose command failed.
        step: Step,
        /// The failure the group reported.
        #[source]
        source: anyhow::Error,
    },
    /// The plan itself refused to build a command.
    #[error(transparent)]
    Plan(#[from] PlanError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeGroup, bootstrap_value, committed};
    use loomery_genesis::Step;

    #[tokio::test]
    async fn the_happy_path_commits_the_three_steps_in_order() {
        let mut group = FakeGroup::default();
        let outcome = run(&mut group, &bootstrap_value()).await.unwrap();

        assert_eq!(outcome.appended, Step::ALL);
        assert!(outcome.progress.is_complete());

        let types: Vec<&str> = group
            .appended
            .iter()
            .map(|command| command.command_type.as_str())
            .collect();
        assert_eq!(
            types,
            [
                "organization.assign_leader",
                "workspace.create",
                "membership.add_owner"
            ]
        );
    }

    #[tokio::test]
    async fn every_step_is_committed_exactly_once() {
        let mut group = FakeGroup::default();
        run(&mut group, &bootstrap_value()).await.unwrap();

        for step in Step::ALL {
            assert_eq!(committed(&group, step), 1, "{step:?}");
        }
    }

    #[tokio::test]
    async fn a_completed_group_appends_nothing() {
        let mut group = FakeGroup::default();
        run(&mut group, &bootstrap_value()).await.unwrap();
        group.appended.clear();

        let outcome = run(&mut group, &bootstrap_value()).await.unwrap();

        assert!(outcome.appended.is_empty());
        assert!(outcome.progress.is_complete());
        assert!(group.appended.is_empty());
    }

    #[tokio::test]
    async fn a_lost_response_does_not_duplicate_the_step() {
        let mut group = FakeGroup {
            lose_response_after: Some(1),
            ..FakeGroup::default()
        };

        // The append succeeded; the worker never heard back.
        assert!(run(&mut group, &bootstrap_value()).await.is_err());
        assert_eq!(committed(&group, Step::AssignLeader), 1);

        // Resuming proposes 2 — not a second 1.
        let outcome = run(&mut group, &bootstrap_value()).await.unwrap();
        assert_eq!(
            outcome.appended,
            [Step::CreateWorkspace, Step::AddOwner],
            "the resumed run must not repeat 1"
        );
        for step in Step::ALL {
            assert_eq!(committed(&group, step), 1, "{step:?}");
        }
    }

    #[tokio::test]
    async fn a_worker_started_mid_script_picks_up_the_remaining_steps() {
        let mut group = FakeGroup::default();
        for step in [Step::AssignLeader, Step::CreateWorkspace] {
            group
                .test_propose(&bootstrap_value().command(step).unwrap())
                .unwrap();
        }

        let outcome = run(&mut group, &bootstrap_value()).await.unwrap();

        assert_eq!(outcome.appended, [Step::AddOwner]);
        assert_eq!(group.appended.len(), 3);
    }

    #[tokio::test]
    async fn retrying_the_worker_never_duplicates_genesis() {
        let mut group = FakeGroup::default();

        for _ in 0..5 {
            run(&mut group, &bootstrap_value()).await.unwrap();
        }

        for step in Step::ALL {
            assert_eq!(committed(&group, step), 1, "{step:?}");
        }
        assert_eq!(group.appended.len(), 3);
    }

    #[tokio::test]
    async fn two_attempts_propose_byte_identical_commands() {
        // Two processes, same inputs: identical envelopes, so the group cannot
        // end up with two of anything.
        let mut first = FakeGroup::default();
        let mut second = FakeGroup::default();

        run(&mut first, &bootstrap_value()).await.unwrap();
        run(&mut second, &bootstrap_value()).await.unwrap();

        assert_eq!(first.appended, second.appended);
    }

    #[tokio::test]
    async fn a_failed_propose_reports_the_step() {
        struct AlwaysFails;

        impl GroupOps for AlwaysFails {
            fn committed_events(
                &self,
                _organization_id: &loomery_core::id::Id,
            ) -> impl std::future::Future<
                Output = anyhow::Result<Vec<loomery_core::envelope::Event>>,
            > + Send {
                std::future::ready(Ok(Vec::new()))
            }

            fn propose(
                &mut self,
                _command: loomery_core::envelope::Command,
            ) -> impl std::future::Future<Output = anyhow::Result<ProposeOutcome>> + Send
            {
                std::future::ready(Err(anyhow::anyhow!("leader gone")))
            }
        }

        let error = run(&mut AlwaysFails, &bootstrap_value()).await.unwrap_err();

        assert!(matches!(
            error,
            Error::Propose {
                step: Step::AssignLeader,
                ..
            }
        ));
    }
}
