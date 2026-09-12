// SPDX-License-Identifier: MPL-2.0

//! The genesis script — the deterministic first deployment of a tenant group.
//!
//! A group is born with three commands committed in order (① assign leader →
//! ② create default workspace → ③ add Owner), attributed to the control-plane
//! bootstrap saga. Nothing here reads a clock or generates randomness: every
//! identity is *derived* (`UUIDv5` — [`step_key`], [`bootstrap_correlation_key`],
//! [`default_workspace_id`], [`command_id`]), so a worker resumed after a crash
//! proposes exactly the commands the crashed attempt proposed, including in the
//! window where an event is committed but its dedup entry is not recorded yet.
//! D12 in `docs/design.md` is the reasoning.
//!
//! # Layout
//!
//! - [`identity`] — the derivations: generation namespace, [`Step`], keys, ids.
//! - [`script`] — [`Bootstrap`], [`Progress`], the command payloads, and the
//!   planning itself: [`Bootstrap::next_command`] is the whole state machine.
//!
//! The shell owns the rest — proposing the commands, waiting for each commit,
//! and re-reading the group's events between steps:
//!
//! ```text
//! let bootstrap = Bootstrap { organization_id, leader_user_id, occurred_at };
//! loop {
//!     let progress = bootstrap.progress(&events_of(group));
//!     match bootstrap.next_command(progress)? {
//!         Some(command) => propose(command).await,
//!         None => break,
//!     }
//! }
//! ```
//!
//! Scaffold: the worker loop around this (Raft client, crash-resume wiring) is
//! Phase 1 — see `docs/design.md` §5 and `docs/CONTINUE.md`.

// Strict lints (unwrap/expect/panicking slicing/overflowing math) are denied in
// production code — test code may use them freely, via a single crate-level
// escape hatch active only under `cfg(test)`.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects
    )
)]

pub mod identity;
pub mod script;

pub use identity::{
    DEFAULT_WORKSPACE_NAME, SCRIPT_VERSION, Step, bootstrap_actor, bootstrap_correlation_key,
    command_id, default_workspace_id, default_workspace_key, owner_membership_id, step_key,
};
pub use script::{AddOwner, AssignLeader, Bootstrap, CreateWorkspace, Error, Progress};
