// SPDX-License-Identifier: MPL-2.0
//! Multiplexed tonic transport for co-resident Raft groups.
use std::collections::BTreeMap;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::error::RPCError;
use openraft::error::RaftError;
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

use super::alias::SnapshotMetaOf;
use super::alias::SnapshotOf;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Server;

use super::RaftHandle;
use super::TypeConfig;
use crate::config::TransportConfig;

/// Version-pinned protobuf transport envelope and service definitions.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod wire {
    tonic::include_proto!("loomery.raft.v1");
}
use wire::Envelope;
use wire::transport_client::TransportClient;
use wire::transport_server::Transport;
use wire::transport_server::TransportServer;

/// One fragment of a snapshot transfer, as JSON inside an [`Envelope`].
///
/// 0.10 fragments snapshots in the network implementation rather than in the
/// core (the chunked `InstallSnapshotRequest` moved to `openraft-legacy`), so
/// this is our own wire shape on the existing `InstallSnapshot` RPC. A
/// transfer is identified by `vote`: every fragment of one transfer carries the
/// same leader vote, and `offset` is where its bytes start.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SnapshotChunk {
    /// The leader's vote for the transfer this fragment belongs to.
    vote: VoteOf<TypeConfig>,
    /// Snapshot metadata; repeated on every fragment so the receiver needs no
    /// out-of-band context.
    meta: SnapshotMetaOf,
    /// Byte offset of `data` within the snapshot.
    offset: u64,
    /// Whether this fragment completes the snapshot.
    done: bool,
    /// This fragment's bytes.
    data: Vec<u8>,
}

/// What a follower answers each snapshot fragment with.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SnapshotAck {
    /// The install response, present only on the fragment that ends the
    /// transfer.
    response: Option<SnapshotResponse<TypeConfig>>,
}

/// Identifies one in-flight snapshot transfer: a group plus the sending leader.
type TransferKey = (String, u64, u64);

/// A partially received snapshot.
#[derive(Default)]
struct Transfer {
    /// Bytes accumulated so far; its length is the next expected offset.
    data: Vec<u8>,
}

/// A registry shared by the transport server and locally booted groups.
#[derive(Clone, Default)]
pub struct TonicTransport {
    groups: Arc<RwLock<BTreeMap<String, RaftHandle>>>,
    /// Half-received snapshot transfers.
    ///
    /// Receiving is stateless in 0.9 because the core owned the reassembly
    /// buffer; in 0.10 the network owns it, so it lives here, keyed by group and
    /// leader. A fragment at offset 0 starts a fresh transfer (which is also how
    /// a retried transfer replaces an aborted one).
    transfers: Arc<Mutex<BTreeMap<TransferKey, Transfer>>>,
}
impl TonicTransport {
    /// Registers a group; rejects accidental replacement of a running group.
    /// # Errors
    /// Returns an error if the group is already registered.
    pub async fn register(&self, group: String, raft: RaftHandle) -> anyhow::Result<()> {
        let mut groups = self.groups.write().await;
        anyhow::ensure!(!groups.contains_key(&group), "group already registered");
        groups.insert(group, raft);
        Ok(())
    }
    /// Removes a stopped group from routing.
    pub async fn unregister(&self, group: &str) {
        self.groups.write().await.remove(group);
    }
    async fn route<T: DeserializeOwned>(
        &self,
        request: Request<Envelope>,
    ) -> Result<(String, RaftHandle, T), Status> {
        let envelope = request.into_inner();
        let raft = self
            .groups
            .read()
            .await
            .get(&envelope.group_id)
            .cloned()
            .ok_or_else(|| Status::not_found("unknown Raft group"))?;
        let rpc = serde_json::from_slice(&envelope.json)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok((envelope.group_id, raft, rpc))
    }
    /// Appends one fragment, answering with the whole snapshot once it is
    /// complete.
    ///
    /// An out-of-order or late fragment drops the transfer, so the leader's next
    /// attempt restarts from offset 0 instead of installing a spliced snapshot.
    async fn assemble(
        &self,
        key: TransferKey,
        chunk: SnapshotChunk,
    ) -> Result<Option<(VoteOf<TypeConfig>, SnapshotMetaOf, Vec<u8>)>, Status> {
        let mut transfers = self.transfers.lock().await;
        if chunk.offset == 0 {
            transfers.insert(key.clone(), Transfer::default());
        }
        let Some(transfer) = transfers.get_mut(&key) else {
            return Err(Status::failed_precondition(
                "snapshot fragment arrived before the first fragment",
            ));
        };
        let expected = u64::try_from(transfer.data.len()).unwrap_or(u64::MAX);
        if chunk.offset != expected {
            transfers.remove(&key);
            return Err(Status::failed_precondition(
                "snapshot fragment out of order",
            ));
        }
        transfer.data.extend_from_slice(&chunk.data);
        if !chunk.done {
            return Ok(None);
        }
        let transfer = transfers.remove(&key).unwrap_or_default();
        Ok(Some((chunk.vote, chunk.meta, transfer.data)))
    }
    /// Serves on a pre-bound listener until shutdown resolves.
    /// # Errors
    /// Returns a transport error if serving fails.
    pub async fn serve(
        &self,
        listener: tokio::net::TcpListener,
        config: TransportConfig,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        config.validate()?;
        let mut server = Server::builder();
        if let Some(tls) = &config.server_tls {
            server = server.tls_config(
                super::tls::server(tls, Duration::from_millis(config.connect_timeout_ms)).await?,
            )?;
        }
        let service = TransportServer::new(self.clone())
            .max_decoding_message_size(config.max_message_bytes)
            .max_encoding_message_size(config.max_message_bytes);
        // Custom incoming streams bypass the server builder's socket options.
        let incoming = tonic::transport::server::TcpIncoming::from(listener)
            .with_nodelay(Some(true))
            .with_keepalive(Some(Duration::from_millis(config.tcp_keepalive_ms)));
        server
            .initial_stream_window_size(config.stream_window_bytes)
            .initial_connection_window_size(config.connection_window_bytes)
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await?;
        Ok(())
    }
}
fn answer(value: &impl Serialize) -> Result<Response<Envelope>, Status> {
    Ok(Response::new(Envelope {
        group_id: String::new(),
        json: serde_json::to_vec(value).map_err(|e| Status::internal(e.to_string()))?,
    }))
}
#[tonic::async_trait]
impl Transport for TonicTransport {
    async fn append_entries(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let (_, raft, rpc) = self
            .route::<AppendEntriesRequest<TypeConfig>>(request)
            .await?;
        answer(&raft.append_entries(rpc).await)
    }
    async fn vote(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        let (_, raft, rpc) = self.route::<VoteRequest<TypeConfig>>(request).await?;
        answer(&raft.vote(rpc).await)
    }
    async fn install_snapshot(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let (group, raft, chunk) = self.route::<SnapshotChunk>(request).await?;
        let key = (
            group,
            chunk.vote.leader_id.term,
            chunk.vote.leader_id.node_id,
        );
        let Some((vote, meta, data)) = self.assemble(key, chunk).await? else {
            return answer(&Ok::<SnapshotAck, RaftError<TypeConfig>>(SnapshotAck {
                response: None,
            }));
        };
        let snapshot = SnapshotOf {
            meta,
            snapshot: Cursor::new(data),
        };
        let installed: Result<SnapshotAck, RaftError<TypeConfig>> = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map(|response| SnapshotAck {
                response: Some(response),
            })
            .map_err(RaftError::Fatal);
        answer(&installed)
    }
}
/// Creates reusable gRPC channels to addresses in Raft membership metadata.
#[derive(Clone)]
pub struct TonicNetworkFactory {
    /// Group routing key, identical on all replicas.
    pub group_id: String,
    /// Transport tuning.
    pub config: TransportConfig,
}
/// A peer connection (lazily connected and automatically reconnected by tonic).
pub struct TonicNetwork {
    node: BasicNode,
    factory: TonicNetworkFactory,
    client: Option<TransportClient<Channel>>,
}
impl RaftNetworkFactory<TypeConfig> for TonicNetworkFactory {
    type Network = TonicNetwork;
    #[allow(clippy::unused_async_trait_impl)]
    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        TonicNetwork {
            node: node.clone(),
            factory: self.clone(),
            client: None,
        }
    }
}
impl TonicNetwork {
    async fn client(&mut self) -> anyhow::Result<TransportClient<Channel>> {
        if let Some(client) = &self.client {
            return Ok(client.clone());
        }
        let config = &self.factory.config;
        config.validate()?;
        let mut endpoint = Endpoint::from_shared(self.node.addr.clone())?;
        let scheme = endpoint.uri().scheme_str();
        anyhow::ensure!(
            scheme
                == Some(if config.client_tls.is_some() {
                    "https"
                } else {
                    "http"
                }),
            "peer URI scheme must match the configured TLS mode"
        );
        if let Some(tls) = &config.client_tls {
            endpoint = endpoint.tls_config(
                super::tls::client(tls, Duration::from_millis(config.connect_timeout_ms)).await?,
            )?;
        }
        let channel = endpoint
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
            .tcp_keepalive(Some(Duration::from_millis(config.tcp_keepalive_ms)))
            .initial_stream_window_size(config.stream_window_bytes)
            .initial_connection_window_size(config.connection_window_bytes)
            .connect_lazy();
        let client = TransportClient::new(channel)
            .max_decoding_message_size(config.max_message_bytes)
            .max_encoding_message_size(config.max_message_bytes);
        self.client = Some(client.clone());
        Ok(client)
    }
    fn request(
        &self,
        rpc: &impl Serialize,
        option: &RPCOption,
    ) -> Result<Request<Envelope>, serde_json::Error> {
        let mut request = Request::new(Envelope {
            group_id: self.factory.group_id.clone(),
            json: serde_json::to_vec(rpc)?,
        });
        request.set_timeout(option.hard_ttl().min(Duration::from_millis(
            self.factory.config.request_timeout_ms,
        )));
        Ok(request)
    }
    /// Decodes a peer's `Result<_, RaftError<_>>` answer into [`RPCError`].
    ///
    /// 0.10's `RPCError<C>` fixes its error parameter to `Infallible`, so a
    /// remote API error has no `RemoteError` to live in; it is reported as
    /// unreachable, which is the mapping openraft's own example network uses.
    /// `AppendEntries` and `Vote` do not legitimately fail on a healthy peer, so
    /// nothing actionable is lost.
    fn decode<T: DeserializeOwned>(
        response: Response<Envelope>,
    ) -> Result<T, RPCError<TypeConfig>> {
        let result: Result<T, RaftError<TypeConfig>> =
            serde_json::from_slice(&response.into_inner().json)
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        result.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}
/// Generates one unary RPC method on [`TonicNetwork`] for a Raft RPC.
///
/// `$name` is our method, `$client` the generated tonic one. The timeout is
/// 0.9's: the hard TTL capped by the configured request timeout. A stream sets
/// its own idle policy in leg 5.
macro_rules! unary {
    ($name:ident, $client:ident, $request:ty, $response:ty) => {
        async fn $name(
            &mut self,
            rpc: $request,
            option: &RPCOption,
        ) -> Result<$response, RPCError<TypeConfig>> {
            let request = self
                .request(&rpc, option)
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            let mut client = self.client().await.map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::other(e.to_string())))
            })?;
            let response = tokio::time::timeout(
                option.hard_ttl().min(Duration::from_millis(
                    self.factory.config.request_timeout_ms,
                )),
                client.$client(request),
            )
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            Self::decode(response)
        }
    };
}
impl TonicNetwork {
    unary!(
        call_append_entries,
        append_entries,
        AppendEntriesRequest<TypeConfig>,
        AppendEntriesResponse<TypeConfig>
    );
    unary!(
        call_vote,
        vote,
        VoteRequest<TypeConfig>,
        VoteResponse<TypeConfig>
    );

    /// Sends one snapshot fragment.
    ///
    /// Separate from [`unary!`] because the answer is a `Result` the streaming
    /// error family has no remote variant for, so a rejected fragment is
    /// reported as unreachable: the transfer failed and the leader should back
    /// off and retry it from the start.
    async fn call_install_snapshot(
        &mut self,
        rpc: SnapshotChunk,
        option: &RPCOption,
    ) -> Result<SnapshotAck, StreamingError<TypeConfig>> {
        let request = self
            .request(&rpc, option)
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        let mut client = self.client().await.map_err(|e| {
            StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(e.to_string())))
        })?;
        let response = tokio::time::timeout(
            option.hard_ttl().min(Duration::from_millis(
                self.factory.config.request_timeout_ms,
            )),
            client.install_snapshot(request),
        )
        .await
        .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?
        .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        let result: Result<SnapshotAck, RaftError<TypeConfig>> =
            serde_json::from_slice(&response.into_inner().json)
                .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        result.map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))
    }
}

/// The byte ranges a snapshot of `total` bytes is sent in.
///
/// A zero-length snapshot still needs one fragment, so the result is never
/// empty and its last range always ends at `total`.
fn fragments(total: usize, chunk_size: usize) -> Vec<std::ops::Range<usize>> {
    let chunk = chunk_size.max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    loop {
        let end = start.saturating_add(chunk).min(total);
        ranges.push(start..end);
        if end >= total {
            return ranges;
        }
        start = end;
    }
}

impl RaftNetworkV2<TypeConfig> for TonicNetwork {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.call_append_entries(rpc, &option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.call_vote(rpc, &option).await
    }

    /// Sends the snapshot to the follower in fragments over the existing
    /// `InstallSnapshot` RPC.
    ///
    /// 0.10 gives the network the whole snapshot and makes it responsible for
    /// fragmenting, so the chunking 0.9 did inside the core happens here. The
    /// fragment size is the core's advised `snapshot_chunk_size`; a transport
    /// that has no advice sends it whole.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<Cursor<Vec<u8>>>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let SnapshotOf {
            meta,
            snapshot: data,
        } = snapshot;
        let bytes = data.into_inner();
        let total = bytes.len();
        let chunk_size = option.snapshot_chunk_size().unwrap_or(total).max(1);
        tokio::pin!(cancel);

        let ranges = fragments(total, chunk_size);
        let last = ranges.len().saturating_sub(1);
        for (position, range) in ranges.into_iter().enumerate() {
            let done = position == last;
            let offset = range.start;
            let fragment = bytes
                .get(range)
                .ok_or_else(|| {
                    StreamingError::Closed(ReplicationClosed::new("snapshot fragment out of range"))
                })?
                .to_vec();
            let chunk = SnapshotChunk {
                vote,
                meta: meta.clone(),
                offset: u64::try_from(offset).unwrap_or(u64::MAX),
                done,
                data: fragment,
            };
            let ack = tokio::select! {
                closed = &mut cancel => return Err(StreamingError::Closed(closed)),
                sent = self.call_install_snapshot(chunk, &option) => sent?,
            };
            if done {
                return ack.response.ok_or_else(|| {
                    StreamingError::Closed(ReplicationClosed::new(
                        "follower acknowledged the final snapshot fragment without installing it",
                    ))
                });
            }
        }

        // `fragments` always marks its last range `done`, so the loop returns;
        // this keeps the function total without an unreachable panic.
        Err(StreamingError::Closed(ReplicationClosed::new(
            "snapshot transfer ended without a final fragment",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::alias::StoredMembershipOf;

    /// One fragment of a transfer led by node 1 in term 1.
    fn chunk(offset: u64, data: &[u8], done: bool) -> SnapshotChunk {
        SnapshotChunk {
            vote: openraft::Vote::new_committed(1, 1),
            meta: SnapshotMetaOf {
                last_log_id: None,
                last_membership: StoredMembershipOf::default(),
            },
            offset,
            done,
            data: data.to_vec(),
        }
    }

    fn key() -> TransferKey {
        ("group".to_owned(), 1, 1)
    }

    #[test]
    fn fragments_cover_the_snapshot_exactly() {
        assert_eq!(fragments(0, 4), vec![0..0]);
        assert_eq!(fragments(4, 4), vec![0..4]);
        assert_eq!(fragments(5, 2), vec![0..2, 2..4, 4..5]);
        // A zero fragment size must still make progress.
        assert_eq!(fragments(2, 0), vec![0..1, 1..2]);
    }

    #[tokio::test]
    async fn fragments_are_reassembled_in_order() {
        let transport = TonicTransport::default();
        let key = key();

        assert!(
            transport
                .assemble(key.clone(), chunk(0, b"abc", false))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            transport
                .assemble(key.clone(), chunk(3, b"de", false))
                .await
                .unwrap()
                .is_none()
        );
        let (_, _, data) = transport
            .assemble(key.clone(), chunk(5, b"f", true))
            .await
            .unwrap()
            .expect("the last fragment completes the transfer");
        assert_eq!(data, b"abcdef");
        assert!(transport.transfers.lock().await.is_empty());
    }

    #[tokio::test]
    async fn a_fragment_at_zero_starts_a_fresh_transfer() {
        let transport = TonicTransport::default();
        let key = key();

        transport
            .assemble(key.clone(), chunk(0, b"abc", false))
            .await
            .unwrap();
        // A retried transfer replaces the aborted one instead of appending to it.
        let (_, _, data) = transport
            .assemble(key.clone(), chunk(0, b"xy", true))
            .await
            .unwrap()
            .expect("the retried transfer completes");
        assert_eq!(data, b"xy");
    }

    #[tokio::test]
    async fn an_out_of_order_fragment_drops_the_transfer() {
        let transport = TonicTransport::default();
        let key = key();

        transport
            .assemble(key.clone(), chunk(0, b"abc", false))
            .await
            .unwrap();
        let error = transport
            .assemble(key.clone(), chunk(9, b"z", false))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            transport.transfers.lock().await.is_empty(),
            "a gap must make the leader restart the transfer"
        );
    }

    #[tokio::test]
    async fn a_fragment_without_a_first_fragment_is_rejected() {
        let transport = TonicTransport::default();

        let error = transport
            .assemble(key(), chunk(3, b"z", false))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
}
