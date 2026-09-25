// SPDX-License-Identifier: MPL-2.0

//! The group port — the shell's consensus boundary.
//!
//! One Raft group owns one `organization_id` (or the control group). Everything
//! in the shell that wants to *write* to a group or *read* what it committed
//! goes through [`GroupOps`]; nothing else needs to know about Raft, storage or
//! leadership.

use loomery_core::envelope::{Command, Event};
use loomery_core::id::Id;

/// What a proposal did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeOutcome {
    /// The command was appended and applied.
    Appended {
        /// Log index of the first event the command produced.
        first_log_index: u64,
    },
    /// The group had already processed this `causation_key`: nothing new was
    /// appended, and the recorded result is the authority.
    Replayed {
        /// Log index of the first event the *original* command produced.
        first_log_index: u64,
    },
}

/// The group operations the rest of the shell needs.
pub trait GroupOps {
    /// Every event the group has committed *and applied*, in log order.
    ///
    /// Answers must reflect *applied* state, not just the log tail: a caller
    /// asking "did my command commit?" must not be told "no" because a replica
    /// has not applied it yet.
    ///
    /// # Errors
    ///
    /// Whatever the group's store reports: I/O, a corrupt segment, a snapshot
    /// that will not load.
    fn committed_events(&self, organization_id: &Id) -> anyhow::Result<Vec<Event>>;

    /// Appends `command` through consensus and waits for it to be applied.
    ///
    /// Idempotent by construction: the identity carried by the command is
    /// derived, so re-submitting it answers [`ProposeOutcome::Replayed`]
    /// instead of committing it twice.
    ///
    /// # Errors
    ///
    /// Consensus failures — the proposal timed out, leadership moved, the group
    /// is unavailable. Every error means *unknown outcome*, never "nothing
    /// happened": re-read [`GroupOps::committed_events`] before proposing again.
    fn propose(&mut self, command: Command) -> anyhow::Result<ProposeOutcome>;
}
