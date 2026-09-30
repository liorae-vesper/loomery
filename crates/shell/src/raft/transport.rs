// SPDX-License-Identifier: MPL-2.0
//! Multiplexed tonic transport for co-resident Raft groups.
use super::TypeConfig;
use crate::config::TransportConfig;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft};
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint, Server},
};

/// Version-pinned protobuf transport envelope and service definitions.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod wire {
    tonic::include_proto!("loomery.raft.v1");
}
use wire::{
    Envelope,
    transport_client::TransportClient,
    transport_server::{Transport, TransportServer},
};

/// A registry shared by the transport server and locally booted groups.
#[derive(Clone, Default)]
pub struct TonicTransport {
    groups: Arc<RwLock<BTreeMap<String, Raft<TypeConfig>>>>,
}
impl TonicTransport {
    /// Registers a group; rejects accidental replacement of a running group.
    /// # Errors
    /// Returns an error if the group is already registered.
    pub async fn register(&self, group: String, raft: Raft<TypeConfig>) -> anyhow::Result<()> {
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
    ) -> Result<(Raft<TypeConfig>, T), Status> {
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
        Ok((raft, rpc))
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
        let (raft, rpc) = self
            .route::<AppendEntriesRequest<TypeConfig>>(request)
            .await?;
        answer(&raft.append_entries(rpc).await)
    }
    async fn vote(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        let (raft, rpc) = self.route::<VoteRequest<u64>>(request).await?;
        answer(&raft.vote(rpc).await)
    }
    async fn install_snapshot(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let (raft, rpc) = self
            .route::<InstallSnapshotRequest<TypeConfig>>(request)
            .await?;
        answer(&raft.install_snapshot(rpc).await)
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
    target: u64,
    node: BasicNode,
    factory: TonicNetworkFactory,
    client: Option<TransportClient<Channel>>,
}
impl RaftNetworkFactory<TypeConfig> for TonicNetworkFactory {
    type Network = TonicNetwork;
    #[allow(clippy::unused_async_trait_impl)]
    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        TonicNetwork {
            target,
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
    #[allow(clippy::result_large_err)] // OpenRaft fixes the RPC error type.
    fn decode<T: DeserializeOwned, E: DeserializeOwned + std::error::Error>(
        &self,
        response: Response<Envelope>,
    ) -> Result<T, RPCError<u64, BasicNode, E>> {
        let result: Result<T, E> = serde_json::from_slice(&response.into_inner().json)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        result.map_err(|e| {
            RPCError::RemoteError(RemoteError::new_with_node(
                self.target,
                self.node.clone(),
                e,
            ))
        })
    }
}
macro_rules! rpc {
    ($name:ident, $request:ty, $response:ty, $error:ty) => {
        async fn $name(
            &mut self,
            rpc: $request,
            option: RPCOption,
        ) -> Result<$response, RPCError<u64, BasicNode, $error>> {
            let request = self
                .request(&rpc, &option)
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            let mut client = self.client().await.map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::other(e.to_string())))
            })?;
            let response = tokio::time::timeout(
                option.hard_ttl().min(Duration::from_millis(
                    self.factory.config.request_timeout_ms,
                )),
                client.$name(request),
            )
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            self.decode(response)
        }
    };
}
impl RaftNetwork<TypeConfig> for TonicNetwork {
    rpc!(
        append_entries,
        AppendEntriesRequest<TypeConfig>,
        AppendEntriesResponse<u64>,
        RaftError<u64>
    );
    rpc!(vote, VoteRequest<u64>, VoteResponse<u64>, RaftError<u64>);
    rpc!(install_snapshot, InstallSnapshotRequest<TypeConfig>, InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>);
}
