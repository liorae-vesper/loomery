// SPDX-License-Identifier: MPL-2.0

//! The no-network factory for a single-node group.
//!
//! Level 1 of the spike is one node in one process: replication never runs, so
//! the network is never called. This factory exists only to satisfy
//! `Raft::new`; every RPC it could hand out fails, loudly, rather than
//! silently pretending a peer answered. Level 3 replaces it with a real
//! `RaftNetworkV2` (tonic gRPC) — see `docs/tutorials/openraft-spike.md` §7.

use std::future::Future;
use std::io::Cursor;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::network::v2::RaftNetworkV2;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::VoteOf;

use super::TypeConfig;
use super::alias::SnapshotOf;

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

/// The "there is no peer" error, ready to wrap in either error family.
fn no_peers() -> Unreachable<TypeConfig> {
    Unreachable::new(&NoPeers)
}

#[allow(clippy::unused_async_trait_impl)] // the factory builds a value, it awaits nothing
impl RaftNetworkFactory<TypeConfig> for NoopNetworkFactory {
    type Network = NoopNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> NoopNetwork {
        NoopNetwork
    }
}

#[allow(clippy::unused_async_trait_impl)] // every no-peer RPC fails without awaiting
impl RaftNetworkV2<TypeConfig> for NoopNetwork {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(RPCError::Unreachable(no_peers()))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(RPCError::Unreachable(no_peers()))
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<TypeConfig>,
        _snapshot: SnapshotOf<Cursor<Vec<u8>>>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(StreamingError::Unreachable(no_peers()))
    }
}
