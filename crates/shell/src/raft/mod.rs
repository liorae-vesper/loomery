// SPDX-License-Identifier: MPL-2.0

//! The Loomery Raft group — an in-process `OpenRaft` 0.10 group.
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
mod hardening_tests;
#[cfg(test)]
mod interruption_tests;
#[cfg(test)]
mod persistent_tests;
#[cfg(test)]
mod proposal_tests;
#[cfg(test)]
mod suite;
#[cfg(test)]
mod test_disk;

pub use log_store::MemLogStore;
pub use network::NoopNetworkFactory;
pub use port::{ProposeError, RaftGroup};
pub use proposal::ProposalWriter;
pub use rocks_log_store::RocksLogStore;
pub use state_machine::AppliedEvent;
pub use state_machine::MemStateMachine;

/// `OpenRaft` 0.10 type aliases bound to [`TypeConfig`].
///
/// 0.10 parameterises `LogId`, `Entry`, `Vote`, `StoredMembership`, `SnapshotMeta`
/// and `Snapshot` by associated types of the config rather than by `NodeId`, so
/// spelling them out at every use site would be noise. Binding them once here
/// keeps signatures readable and pins the concrete types the compiler expects.
pub mod alias {
    /// A log id, whose leader id is a committed `LeaderId` (term + node id).
    pub type LogIdOf = openraft::type_config::alias::LogIdOf<super::TypeConfig>;
    /// A log entry carrying this config's payload.
    pub type EntryOf = openraft::type_config::alias::EntryOf<super::TypeConfig>;
    /// A vote, identified by a `LeaderId` rather than a node id.
    pub type VoteOf = openraft::type_config::alias::VoteOf<super::TypeConfig>;
    /// The last membership a state machine applied.
    pub type StoredMembershipOf =
        openraft::type_config::alias::StoredMembershipOf<super::TypeConfig>;
    /// Snapshot metadata: log id plus membership, with no transfer id.
    pub type SnapshotMetaOf = openraft::type_config::alias::SnapshotMetaOf<super::TypeConfig>;
    /// A snapshot plus its metadata, parameterised by the snapshot data type.
    pub type SnapshotOf<SD> = openraft::type_config::alias::SnapshotOf<super::TypeConfig, SD>;
}

/// The `OpenRaft` handle of one group.
///
/// 0.10 makes `Raft` generic over the state-machine type, and the trait is
/// implemented on the shared `Arc<MemStateMachine>` — so every signature that
/// carries a handle names it once here instead of repeating both parameters.
pub type RaftHandle = openraft::Raft<TypeConfig, std::sync::Arc<state_machine::MemStateMachine>>;

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

/// `OpenRaft` 0.10 requires `RaftTypeConfig::D: Display`. It is only ever
/// interpolated into consensus logs, so it names the command types and never
/// the payload (which is caller data and can be large).
impl std::fmt::Display for AppData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Command(command) => f.write_str(&command.command_type),
            Self::Batch(commands) => {
                f.write_str("batch[")?;
                for (position, command) in commands.iter().enumerate() {
                    if position > 0 {
                        f.write_str(",")?;
                    }
                    f.write_str(&command.command_type)?;
                }
                f.write_str("]")
            }
        }
    }
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
    ///
    /// In 0.10 the remaining associated types come from the macro's defaults:
    /// `Term = u64` and the advanced `LeaderId` (`term` plus `node_id`, which
    /// is what 0.9's `Vote<NID>` serialized as). `Entry` is linked to the
    /// configured `Payload` by the macro, and `SnapshotData` is no longer a
    /// `RaftTypeConfig` type at all — it lives on `RaftStateMachine` (and, for
    /// the wire, on `RaftNetworkV2`).
    pub TypeConfig:
        D            = AppData,
        R            = Applied,
        NodeId       = u64,
        Node         = openraft::BasicNode,
        AsyncRuntime = TokioRuntime,
);
