// SPDX-License-Identifier: MPL-2.0

//! The Loomery Raft group — an in-process `OpenRaft` 0.9 group.
//!
//! This module is the storage half of [`crate::group::GroupOps`]: an in-memory
//! [`MemLogStore`] (the replicated log + vote), an in-memory
//! [`MemStateMachine`] (apply + snapshots), and [`RaftGroup`], the adapter that
//! maps `Raft::client_write` onto [`crate::group::GroupOps`].
//!
//! [`RaftGroup::boot_single_node`] retains the in-memory spike. Persistent
//! replicas use [`RaftGroup::boot_persistent`], [`RocksLogStore`] and the
//! multiplexed tonic service in [`transport`]. User tuning lives in
//! [`crate::config::GroupConfig`].
//!
//! The state machine is where the **pure core** runs: every committed command
//! is decoded and dispatched through
//! [`AggregatePlan::process`](loomery_core::aggregate::process), and the
//! resulting events are folded into per-aggregate state. The dedup window
//! ([`loomery_core::dedup::Registry`]) is applied state too, so a replayed
//! command answers [`Applied::Replayed`] instead of committing twice.
//!
//! State that the pure core does not name yet — the applied events, the
//! per-stream aggregate states, and the dedup mirror used for snapshots — lives
//! in the state-machine module.

use loomery_core::envelope::Command;
use loomery_core::key::Key;
use openraft::TokioRuntime;
use serde::{Deserialize, Serialize};

mod disk;
mod log_store;
mod network;
mod port;
mod proposal;
mod rocks_log_store;
mod state_machine;
mod tls;
#[cfg(test)]
mod tls_tests;
pub mod transport;

#[cfg(test)]
mod append_tests;
#[cfg(test)]
mod persistent_tests;
#[cfg(test)]
mod proposal_tests;
#[cfg(test)]
mod suite;

pub use log_store::MemLogStore;
pub use network::NoopNetworkFactory;
pub use port::{ProposeError, RaftGroup};
pub use proposal::ProposalWriter;
pub use rocks_log_store::RocksLogStore;
pub use state_machine::AppliedEvent;
pub use state_machine::MemStateMachine;

/// What clients write to a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)] // Preserve the existing Command constructor; its size is unchanged.
pub enum AppData {
    /// A domain command for the pure core.
    Command(Command),
    /// Ordered independent commands sharing a durable Raft entry. Every replica
    /// must support this variant before a leader enables proposal batching.
    Batch(Vec<Command>),
}

/// What the state machine answers a writer with.
///
/// [`Applied::Appended`] and [`Applied::Replayed`] map one-to-one onto
/// [`ProposeOutcome`](crate::group::ProposeOutcome). [`Applied::Rejected`] has
/// no port equivalent — it is the defensive case where a committed command's
/// payload does not decode (validation is supposed to happen at the gateway,
/// before the command enters consensus), and the port surfaces it as an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Applied {
    /// One outcome per command in a batch, in the original command order.
    Batch(Vec<Applied>),
    /// The command produced events in this Raft entry.
    Appended {
        /// Raft index containing the command; batched commands share this index.
        first_log_index: u64,
    },
    /// The causation key was already in the dedup window.
    Replayed {
        /// Raft index containing the original command.
        first_log_index: u64,
        /// Fingerprint the original command recorded, so a caller can tell a
        /// replay from a reused key carrying a different intent (D12).
        fingerprint: Key,
    },
    /// The command was committed but the aggregate refused it.
    Rejected {
        /// Machine-readable discriminator from the aggregate's code enum.
        code: String,
        /// Human-readable reason.
        message: String,
    },
}

openraft::declare_raft_types!(
    /// The Raft type configuration of a Loomery group.
    ///
    /// `NodeId` is a process-local `u64` (the shell's router maps
    /// `organization_id -> group`, and the node id identifies a replica);
    /// `Node` carries its address once a network exists.
    pub TypeConfig:
        D            = AppData,
        R            = Applied,
        NodeId       = u64,
        Node         = openraft::BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);
