// SPDX-License-Identifier: MPL-2.0

//! The no-network factory for a single-node group.
//!
//! Level 1 of the spike is one node in one process: replication never runs, so
//! the network is never called. This factory exists only to satisfy
//! `Raft::new`; every RPC it could hand out fails, loudly, rather than
//! silently pretending a peer answered. Level 3 replaces it with a real
//! `RaftNetwork` (tonic gRPC) — see `docs/tutorials/openraft-spike.md` §7.

use std::future::Future;

use openraft::BasicNode;
use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use super::TypeConfig;

/// The factory for a group that has no peers.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopNetworkFactory;

/// A network client to a peer that does not exist.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopNetwork;

/// The error every no-peer RPC returns.
#[derive(Debug, thiserror::Error)]
#[error("this single-node group has no peers")]
pub struct NoPeers;

impl RaftNetworkFactory<TypeConfig> for NoopNetworkFactory {
    type Network = NoopNetwork;

    fn new_client(
        &mut self,
        _target: u64,
        _node: &BasicNode,
    ) -> impl Future<Output = NoopNetwork> + Send {
        std::future::ready(NoopNetwork)
    }
}

impl RaftNetwork<TypeConfig> for NoopNetwork {
    fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> impl Future<
        Output = Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>,
    > + Send {
        std::future::ready(Err(RPCError::Unreachable(Unreachable::new(&NoPeers))))
    }

    fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> impl Future<
        Output = Result<
            InstallSnapshotResponse<u64>,
            RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
        >,
    > + Send {
        std::future::ready(Err(RPCError::Unreachable(Unreachable::new(&NoPeers))))
    }

    fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> impl Future<Output = Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>> + Send
    {
        std::future::ready(Err(RPCError::Unreachable(Unreachable::new(&NoPeers))))
    }
}
