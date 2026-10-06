// SPDX-License-Identifier: MPL-2.0

//! The group port — the shell's consensus boundary.
//!
//! One Raft group owns one `organization_id` (or the control group). Everything
//! in the shell that wants to *write* to a group or *read* what it committed
//! goes through [`GroupOps`]; nothing else needs to know about Raft, storage or
//! leadership.

use std::future::Future;

use loomery_core::envelope::{Command, Event};
use loomery_core::id::Id;
use loomery_core::key::Key;

/// What a proposal did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeOutcome {
    /// The command was appended and applied.
    Appended {
        /// Raft index containing the command; batched commands share this index.
        first_log_index: u64,
    },
    /// The group had already processed this `causation_key`: nothing new was
    /// applied, and the recorded result is the authority. A retry can still
    /// occupy another Raft entry without producing another domain event.
    Replayed {
        /// Raft index containing the original command.
        first_log_index: u64,
        /// Fingerprint the original command recorded. A caller that gets a
        /// replay compares this with its own command's fingerprint: a mismatch
        /// means the key was reused for a different intent (D12), which is a
        /// conflict, not a replay.
        fingerprint: Key,
    },
}

/// The group operations the rest of the shell needs.
///
/// The futures are declared explicitly (`-> impl Future<Output = T> + Send`)
/// rather than as `async fn`: the worker is `tokio::spawn`ed, and `async fn` in
/// a trait gives no `Send` guarantee (and warns, `async_fn_in_trait`).
pub trait GroupOps: Send + Sync {
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
    fn committed_events(
        &self,
        organization_id: &Id,
    ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send;

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
    fn propose(
        &mut self,
        command: Command,
    ) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send;
}
