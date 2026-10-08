// SPDX-License-Identifier: MPL-2.0
//! Multiplexed tonic transport for co-resident Raft groups.
use std::collections::BTreeMap;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::base::BoxFuture;
use openraft::base::BoxStream;
use openraft::error::Fatal;
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
use openraft::raft::StreamAppendResult;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::VoteOf;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Server;

use super::RaftHandle;
use super::TypeConfig;
use super::alias::SnapshotMetaOf;
use super::alias::SnapshotOf;
use crate::config::TransportConfig;

/// Version-pinned protobuf transport envelope and service definitions.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod wire {
    tonic::include_proto!("loomery.raft.v1");
}
use wire::Envelope;
use wire::SnapshotChunk;
use wire::transport_client::TransportClient;
use wire::transport_server::Transport;
use wire::transport_server::TransportServer;

/// How many requests or results may be in flight on one append stream.
///
/// Matched to openraft's own pipeline depth (`PIPELINE_BUFFER_SIZE`, 64) so the
/// transport is never the tighter bottleneck; the bound is what stops a leader
/// from running unboundedly ahead of a slow follower.
const STREAM_BUFFER: usize = 64;

/// A snapshot transfer's opening metadata, JSON on the first fragment.
///
/// The vote is not part of `SnapshotMeta`, and it must reach the follower to
/// install the snapshot, so it travels with the metadata.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Opening {
    /// The leader's vote for the transfer.
    vote: VoteOf<TypeConfig>,
    /// The snapshot's position and membership.
    meta: SnapshotMetaOf,
}

/// Serializes the opening metadata of a snapshot transfer.
///
/// Shared by the client and the transport tests so both frame a transfer the
/// same way.
pub(super) fn opening_json(
    vote: VoteOf<TypeConfig>,
    meta: &SnapshotMetaOf,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&Opening {
        vote,
        meta: meta.clone(),
    })
}

/// Accumulates the fragments of one snapshot transfer.
///
/// The buffer is scoped to a single RPC, which is the point of streaming it: a
/// transfer that dies mid-way leaves nothing behind, and two transfers cannot
/// collide. (The unary first cut of this transport kept reassembly in a shared
/// map keyed by group and leader, which leaked a partial transfer whenever the
/// sender died.)
#[derive(Default)]
pub(super) struct SnapshotAssembly {
    /// The routing key, learned from the first fragment.
    group: Option<String>,
    /// The opening metadata, learned from the first fragment.
    opening: Option<Opening>,
    /// The bytes received so far.
    data: Vec<u8>,
}

impl SnapshotAssembly {
    /// Adds one fragment, answering with the whole snapshot when it completes.
    fn push(&mut self, chunk: SnapshotChunk) -> Result<Option<(String, Opening, Vec<u8>)>, Status> {
        if !chunk.group_id.is_empty() && self.group.as_deref() != Some(chunk.group_id.as_str()) {
            if self.group.is_some() {
                return Err(Status::invalid_argument(
                    "snapshot fragments name different groups",
                ));
            }
            self.group = Some(chunk.group_id);
        }
        if self.opening.is_none() && !chunk.opening.is_empty() {
            self.opening = Some(
                serde_json::from_slice(&chunk.opening)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
            );
        }
        self.data.extend_from_slice(&chunk.data);
        if !chunk.done {
            return Ok(None);
        }
        let Some(group) = self.group.take() else {
            return Err(Status::invalid_argument(
                "snapshot fragments carry no group id",
            ));
        };
        let Some(opening) = self.opening.take() else {
            return Err(Status::invalid_argument(
                "snapshot fragments carry no metadata",
            ));
        };
        Ok(Some((group, opening, std::mem::take(&mut self.data))))
    }
}

/// A registry shared by the transport server and locally booted groups.
#[derive(Clone, Default)]
pub struct TonicTransport {
    groups: Arc<RwLock<BTreeMap<String, RaftHandle>>>,
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
    /// Looks up a registered group by routing key.
    async fn group(&self, group_id: &str) -> Result<RaftHandle, Status> {
        self.groups
            .read()
            .await
            .get(group_id)
            .cloned()
            .ok_or_else(|| Status::not_found("unknown Raft group"))
    }
    async fn route<T: DeserializeOwned>(
        &self,
        request: Request<Envelope>,
    ) -> Result<(String, RaftHandle, T), Status> {
        let envelope = request.into_inner();
        let raft = self.group(&envelope.group_id).await?;
        let rpc = serde_json::from_slice(&envelope.json)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok((envelope.group_id, raft, rpc))
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
    /// The follower's results, relayed as the core produces them.
    type StreamAppendStream = ReceiverStream<Result<Envelope, Status>>;

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
    /// Receives a snapshot stream and installs it once it is complete.
    ///
    /// The assembled bytes live in this frame, so a sender that goes away cannot
    /// leave a half-transfer behind, and a stream that ends without the final
    /// fragment is reported as aborted rather than installed.
    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<Envelope>, Status> {
        let mut stream = request.into_inner();
        let mut assembly = SnapshotAssembly::default();
        while let Some(chunk) = stream.message().await? {
            let Some((group, opening, data)) = assembly.push(chunk)? else {
                continue;
            };
            let raft = self.group(&group).await?;
            let snapshot = SnapshotOf {
                meta: opening.meta,
                snapshot: Cursor::new(data),
            };
            let installed: Result<SnapshotResponse<TypeConfig>, RaftError<TypeConfig>> = raft
                .install_full_snapshot(opening.vote, snapshot)
                .await
                .map_err(RaftError::Fatal);
            return answer(&installed);
        }
        Err(Status::aborted(
            "snapshot stream ended before the final fragment",
        ))
    }
    /// Relays an append stream to this node's `Raft` and its results back.
    ///
    /// The handler returns as soon as the stream is routed, then works in both
    /// directions at once: requests are decoded into `Raft::stream_append`'s
    /// input, its output is encoded back. Ordering is HTTP/2's on a single stream,
    /// so nothing here reorders, and a stalled peer is bounded by the configured
    /// TCP keepalive rather than by a guessed timer.
    async fn stream_append(
        &self,
        request: Request<Streaming<Envelope>>,
    ) -> Result<Response<Self::StreamAppendStream>, Status> {
        let mut inbound = request.into_inner();
        let Some(first) = inbound.message().await? else {
            // No request, no group to route: answer with an empty stream.
            let (_open, closed) = mpsc::channel(1);
            return Ok(Response::new(ReceiverStream::new(closed)));
        };
        let raft = self.group(&first.group_id).await?;
        let (results_tx, results_rx) = mpsc::channel(STREAM_BUFFER);
        let (requests_tx, requests_rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(decode_append_requests(
            first,
            inbound,
            requests_tx,
            results_tx.clone(),
        ));
        let outbound = raft.stream_append(ReceiverStream::new(requests_rx));
        tokio::spawn(encode_append_results(outbound, results_tx));
        Ok(Response::new(ReceiverStream::new(results_rx)))
    }
}
/// Decodes inbound envelopes into [`Raft::stream_append`] input.
///
/// A request that cannot be decoded cannot be answered, so it is reported on the
/// results stream and the input ends: the leader sees the error and
/// re-establishes replication. Dropping the results receiver (a cancelled
/// stream) stops this at the next send.
async fn decode_append_requests(
    first: Envelope,
    mut inbound: Streaming<Envelope>,
    requests: mpsc::Sender<AppendEntriesRequest<TypeConfig>>,
    results: mpsc::Sender<Result<Envelope, Status>>,
) {
    let mut next = Some(first);
    loop {
        let envelope = match next.take() {
            Some(envelope) => envelope,
            None => match inbound.message().await {
                Ok(Some(envelope)) => envelope,
                Ok(None) => return,
                Err(status) => {
                    let _ = results.send(Err(status)).await;
                    return;
                }
            },
        };
        match serde_json::from_slice::<AppendEntriesRequest<TypeConfig>>(&envelope.json) {
            Ok(rpc) => {
                // Stop the moment the caller is gone. A request handed to the core
                // after the leader has abandoned this stream can only be applied
                // behind the leader's back, and re-ordered against the stream it
                // restarts with.
                tokio::select! {
                    sent = requests.send(rpc) => {
                        if sent.is_err() {
                            return;
                        }
                    }
                    () = results.closed() => return,
                }
            }
            Err(e) => {
                let _ = results
                    .send(Err(Status::invalid_argument(e.to_string())))
                    .await;
                return;
            }
        }
    }
}
/// Encodes the core's results onto the wire.
///
/// The wire carries openraft's own `Result<StreamAppendResult, Fatal>` for this
/// call, exactly as [`Raft::stream_append`] yields it. The stream ends when the
/// core's does, which is how the follower's answer for the last request still
/// gets through: the core drains accepted requests before it ends.
async fn encode_append_results<S>(outbound: S, results: mpsc::Sender<Result<Envelope, Status>>)
where
    S: Stream<Item = Result<StreamAppendResult<TypeConfig>, Fatal<TypeConfig>>> + OptionalSend,
{
    let mut outbound = std::pin::pin!(outbound);
    while let Some(item) = outbound.next().await {
        let json = match serde_json::to_vec(&item) {
            Ok(json) => json,
            Err(e) => {
                let _ = results.send(Err(Status::internal(e.to_string()))).await;
                return;
            }
        };
        let envelope = Envelope {
            group_id: String::new(),
            json,
        };
        if results.send(Ok(envelope)).await.is_err() {
            return;
        }
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
/// 0.9's: the hard TTL capped by the configured request timeout. The snapshot
/// stream is deliberately not unary and sets no such deadline; see
/// [`TonicNetwork::call_install_snapshot`].
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

    /// Sends a snapshot as one client stream and decodes the install result.
    ///
    /// There is deliberately no whole-RPC deadline. Openraft passes
    /// `Config::install_snapshot_timeout` — 200 ms by default — as `hard_ttl`,
    /// and 0.9 applied that to each chunk-sized RPC; applying it to a whole
    /// stream would abort every transfer that is not tiny, and openraft would
    /// restart it from the beginning. The bounds here are openraft's `cancel`
    /// future, the configured TCP keepalive, and `max_message_bytes` per
    /// fragment. An idle-timeout policy that distinguishes "slow link" from
    /// "stalled peer" needs a per-fragment progress signal the request stream
    /// does not expose; it belongs with the `StreamAppend` work, where the
    /// response side is a stream.
    async fn call_install_snapshot(
        &mut self,
        chunks: mpsc::Receiver<SnapshotChunk>,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let mut client = self.client().await.map_err(|e| {
            StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(e.to_string())))
        })?;
        let response = client
            .install_snapshot(ReceiverStream::new(chunks))
            .await
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        let result: Result<SnapshotResponse<TypeConfig>, RaftError<TypeConfig>> =
            serde_json::from_slice(&response.into_inner().json)
                .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        result.map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))
    }
}

/// The byte ranges a snapshot of `total` bytes is sent in.
///
/// A zero-length snapshot still needs one fragment, so the result is never
/// empty and its last range always ends at `total`.
pub(super) fn fragments(total: usize, chunk_size: usize) -> Vec<std::ops::Range<usize>> {
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

/// One fragment of a snapshot transfer.
///
/// The routing key and the opening metadata travel on the first fragment only,
/// and `first` is what decides that — so the client's pump and the transport
/// tests frame a snapshot identically.
fn fragment(
    bytes: &[u8],
    range: std::ops::Range<usize>,
    first: bool,
    done: bool,
    group_id: &str,
    opening: &[u8],
) -> SnapshotChunk {
    SnapshotChunk {
        group_id: if first {
            group_id.to_owned()
        } else {
            String::new()
        },
        opening: if first { opening.to_vec() } else { Vec::new() },
        data: bytes.get(range).unwrap_or_default().to_vec(),
        done,
    }
}

/// Sends a snapshot's fragments into `chunks`.
///
/// Fragments are copied one at a time, so a snapshot is never duplicated in
/// memory on the sender; the bounded channel and HTTP/2 flow control pace the
/// stream, and returning early when the receiver is gone is what makes
/// cancelling the RPC stop the copying too.
pub(super) async fn pump_fragments(
    bytes: Vec<u8>,
    ranges: Vec<std::ops::Range<usize>>,
    group_id: String,
    opening: Vec<u8>,
    chunks: mpsc::Sender<SnapshotChunk>,
) {
    let last = ranges.len().saturating_sub(1);
    for (position, range) in ranges.into_iter().enumerate() {
        let chunk = fragment(
            &bytes,
            range,
            position == 0,
            position == last,
            &group_id,
            &opening,
        );
        if chunks.send(chunk).await.is_err() {
            return;
        }
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

    /// Streams `AppendEntries` requests to the follower and yields its results in
    /// the order they were asked.
    ///
    /// One bidirectional HTTP/2 stream carries both directions, and HTTP/2 keeps
    /// each direction ordered, so the trait's in-order contract needs no sequence
    /// numbers: whatever the follower answers arrives in request order.
    ///
    /// Neither bound comes from openraft's stream TTL, deliberately. Opening the
    /// stream carries the configured RPC deadline, so a peer that never sends
    /// response headers cannot hang replication; `hard_ttl` is explicitly not a
    /// stream deadline and on this path it is only the heartbeat interval.
    /// Each awaited result then carries
    /// [`stream_stall_timeout_ms`](crate::config::TransportConfig::stream_stall_timeout_ms):
    /// a *stall* detector, not a latency budget. An earlier version used
    /// `soft_ttl` (three quarters of the heartbeat interval, so 75 ms in a real
    /// deployment) as a per-result bound, and that wedged replication under load:
    /// tearing a stream down mid-burst leaves the leader's progress ahead of the
    /// follower, and openraft discards the conflict that would repair it (see
    /// `progress::entry::update::Updater::update_conflicting`), so replication
    /// never resumes. A stall bound is long enough that a slow fsync is not one.
    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        _option: RPCOption,
    ) -> BoxFuture<
        's,
        Result<
            BoxStream<'s, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
            RPCError<TypeConfig>,
        >,
    >
    where
        S: Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static,
    {
        let fu = async move {
            let mut client = self.client().await.map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::other(e.to_string())))
            })?;
            let (requests_tx, requests_rx) = mpsc::channel(STREAM_BUFFER);
            tokio::spawn(pump_append_requests(
                input,
                self.factory.group_id.clone(),
                requests_tx,
            ));

            // Setup only: `hard_ttl` is explicitly not a stream deadline (on the
            // replication path it is the heartbeat interval), so opening the stream
            // is bounded by the configured RPC deadline instead.
            let open_timeout = Duration::from_millis(self.factory.config.request_timeout_ms);
            let response = tokio::time::timeout(
                open_timeout,
                client.stream_append(ReceiverStream::new(requests_rx)),
            )
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

            let (results_tx, results_rx) = mpsc::channel(STREAM_BUFFER);
            tokio::spawn(pump_append_results(
                response.into_inner(),
                Duration::from_millis(self.factory.config.stream_stall_timeout_ms),
                results_tx,
            ));
            let stream: BoxStream<'s, _> = Box::pin(ReceiverStream::new(results_rx));
            Ok(stream)
        };
        Box::pin(fu)
    }

    /// Sends the snapshot to the follower as one client stream.
    ///
    /// 0.10 gives the network the whole snapshot and makes fragmentation its job,
    /// so the chunking 0.9 did inside the core happens here. The fragment size is
    /// the core's advised `snapshot_chunk_size`; a transport that has no advice
    /// sends it whole.
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
        let opening = opening_json(vote, &meta)
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        let ranges = fragments(total, chunk_size);

        // Two fragments of headroom: enough that the pump is never the
        // bottleneck, small enough that the snapshot is not duplicated.
        let (chunks_tx, chunks_rx) = mpsc::channel(2);
        let pump = tokio::spawn(pump_fragments(
            bytes,
            ranges,
            self.factory.group_id.clone(),
            opening,
            chunks_tx,
        ));

        tokio::pin!(cancel);
        let result = tokio::select! {
            closed = &mut cancel => Err(StreamingError::Closed(closed)),
            sent = self.call_install_snapshot(chunks_rx) => sent,
        };
        pump.abort();
        result
    }
}

/// Encodes the caller's requests onto the wire.
async fn pump_append_requests<S>(mut input: S, group_id: String, requests: mpsc::Sender<Envelope>)
where
    S: Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static,
{
    while let Some(rpc) = input.next().await {
        let Ok(json) = serde_json::to_vec(&rpc) else {
            // A request that cannot be encoded cannot be asked about, so the
            // stream ends and replication re-establishes.
            return;
        };
        let envelope = Envelope {
            group_id: group_id.clone(),
            json,
        };
        if requests.send(envelope).await.is_err() {
            return;
        }
    }
}

/// Decodes the follower's results into the ordered stream the trait promises.
///
/// `RPCError<C>` fixes its error parameter to `Infallible`, so a remote `Fatal`
/// has no `RemoteError` to live in and is reported as unreachable — the mapping
/// the unary path uses. Like `stream_append_sequential`, an error ends the stream
/// after it has been delivered.
async fn pump_append_results(
    mut inbound: Streaming<Envelope>,
    idle: Duration,
    results: mpsc::Sender<Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
) {
    loop {
        let message = match tokio::time::timeout(idle, inbound.message()).await {
            Ok(Ok(Some(envelope))) => envelope,
            Ok(Ok(None)) => return,
            Ok(Err(status)) => {
                let _ = results
                    .send(Err(RPCError::Unreachable(Unreachable::new(&status))))
                    .await;
                return;
            }
            Err(_elapsed) => {
                let _ = results
                    .send(Err(RPCError::Unreachable(Unreachable::from_string(
                        "append stream made no progress",
                    ))))
                    .await;
                return;
            }
        };
        let decoded: Result<StreamAppendResult<TypeConfig>, Fatal<TypeConfig>> =
            match serde_json::from_slice(&message.json) {
                Ok(decoded) => decoded,
                Err(e) => {
                    let _ = results
                        .send(Err(RPCError::Unreachable(Unreachable::new(&e))))
                        .await;
                    return;
                }
            };
        // `Conflict` and `HigherVote` are protocol answers, not transport
        // failures: they travel as `Ok(..)` and end the stream after delivery,
        // which is what `stream_append_sequential` does with them.
        let (item, stop) = match decoded {
            Ok(result) => {
                let stop = result.is_err();
                (Ok(result), stop)
            }
            Err(fatal) => (Err(RPCError::Unreachable(Unreachable::new(&fatal))), true),
        };
        if results.send(item).await.is_err() || stop {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::alias::StoredMembershipOf;

    /// Opening metadata for a transfer led by node 1 in term 1.
    fn opening() -> Vec<u8> {
        opening_json(
            openraft::Vote::new_committed(1, 1),
            &SnapshotMetaOf {
                last_log_id: None,
                last_membership: StoredMembershipOf::default(),
            },
        )
        .unwrap()
    }

    /// One fragment carrying exactly `data`.
    fn chunk(group: &str, data: &[u8], first: bool, done: bool) -> SnapshotChunk {
        fragment(data, 0..data.len(), first, done, group, &opening())
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
    async fn the_pump_frames_a_snapshot_for_one_transfer() {
        let opening = opening();
        let (tx, mut rx) = mpsc::channel(8);
        pump_fragments(
            b"abcdef".to_vec(),
            fragments(6, 2),
            "group".to_owned(),
            opening.clone(),
            tx,
        )
        .await;

        let mut chunks = Vec::new();
        while let Some(chunk) = rx.recv().await {
            chunks.push(chunk);
        }
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].group_id, "group");
        assert_eq!(chunks[0].opening, opening);
        assert!(chunks[1].group_id.is_empty() && chunks[1].opening.is_empty());
        assert_eq!(chunks[0].data, b"ab");
        assert_eq!(chunks[2].data, b"ef");
        assert_eq!(
            chunks.iter().map(|c| c.done).collect::<Vec<_>>(),
            vec![false, false, true]
        );
    }

    #[test]
    fn an_assembly_joins_fragments_in_order() {
        let mut assembly = SnapshotAssembly::default();

        assert!(
            assembly
                .push(chunk("group", b"abc", true, false))
                .unwrap()
                .is_none()
        );
        assert!(
            assembly
                .push(chunk("", b"de", false, false))
                .unwrap()
                .is_none()
        );
        let (group, _, data) = assembly
            .push(chunk("", b"f", false, true))
            .unwrap()
            .expect("the last fragment completes the transfer");
        assert_eq!(group, "group");
        assert_eq!(data, b"abcdef");
    }

    #[test]
    fn an_assembly_rejects_a_transfer_with_no_group() {
        let mut assembly = SnapshotAssembly::default();
        let error = assembly.push(chunk("", b"abc", true, true)).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn an_assembly_rejects_a_transfer_with_no_metadata() {
        let mut assembly = SnapshotAssembly::default();
        let chunk = SnapshotChunk {
            group_id: "group".to_owned(),
            opening: Vec::new(),
            data: b"abc".to_vec(),
            done: true,
        };
        let error = assembly.push(chunk).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn an_assembly_rejects_fragments_from_two_groups() {
        let mut assembly = SnapshotAssembly::default();
        assembly.push(chunk("group", b"abc", true, false)).unwrap();
        // A later fragment that names a *different* group is not a continuation
        // of this transfer.
        let other = SnapshotChunk {
            group_id: "other".to_owned(),
            opening: Vec::new(),
            data: b"def".to_vec(),
            done: true,
        };
        let error = assembly.push(other).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
