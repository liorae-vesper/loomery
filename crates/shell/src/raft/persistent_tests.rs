// SPDX-License-Identifier: MPL-2.0
//! Network replication and disk recovery acceptance tests.
use super::transport::TonicTransport;
use super::*;
use crate::{
    bootstrap,
    config::GroupConfig,
    group::GroupOps,
    test_support::{bootstrap_value, organization},
};
use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, StorageError,
    testing::{StoreBuilder, Suite},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

struct Builder;
impl StoreBuilder<TypeConfig, RocksLogStore, Arc<MemStateMachine>, tempfile::TempDir> for Builder {
    async fn build(
        &self,
    ) -> Result<(tempfile::TempDir, RocksLogStore, Arc<MemStateMachine>), StorageError<u64>> {
        let dir = tempfile::tempdir().unwrap();
        let disk = super::disk::Disk::open(dir.path(), &crate::config::StorageConfig::default())
            .await
            .unwrap();
        let log = RocksLogStore::open(disk.clone());
        let machine = MemStateMachine::open(disk).await.unwrap();
        Ok((dir, log, machine))
    }
}
#[test]
fn rocksdb_passes_openraft_storage_suite() {
    Suite::<TypeConfig, RocksLogStore, Arc<MemStateMachine>, Builder, tempfile::TempDir>::test_all(
        Builder,
    )
    .unwrap();
}
fn config() -> GroupConfig {
    let mut config = GroupConfig::default();
    config.raft.heartbeat_interval = 50;
    config.raft.election_timeout_min = 150;
    config.raft.election_timeout_max = 300;
    config
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn three_replicas_commit_and_recover_genesis() {
    let root = tempfile::tempdir().unwrap();
    let mut groups = Vec::new();
    let mut servers = Vec::new();
    let mut members = BTreeMap::new();
    for node in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            node,
            BasicNode::new(format!("http://{}", listener.local_addr().unwrap())),
        );
        let group = RaftGroup::boot_persistent(
            node,
            "tenant".into(),
            &root.path().join(node.to_string()),
            config(),
        )
        .await
        .unwrap();
        let transport = TonicTransport::default();
        transport
            .register("tenant".into(), group.raft())
            .await
            .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            transport
                .serve(listener, config().transport, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        servers.push((stop, server));
        groups.push(group);
    }
    groups[0]
        .raft()
        .initialize(BTreeMap::from([(1, members[&1].clone())]))
        .await
        .unwrap();
    groups[0]
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    for node in 2..=3 {
        groups[0]
            .raft()
            .add_learner(node, members[&node].clone(), true)
            .await
            .unwrap();
    }
    groups[0]
        .raft()
        .change_membership(BTreeSet::from([1, 2, 3]), false)
        .await
        .unwrap();
    bootstrap::run(&mut groups[0], &bootstrap_value())
        .await
        .unwrap();
    let last = groups[0]
        .raft()
        .metrics()
        .borrow()
        .last_applied
        .unwrap()
        .index;
    for group in &groups {
        group
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .applied_index(Some(last), "replicated")
            .await
            .unwrap();
        assert_eq!(
            group.committed_events(&organization()).await.unwrap().len(),
            3
        );
    }
    // Force snapshot persistence before reopening a real database handle.
    groups[0].raft().trigger().snapshot().await.unwrap();
    for group in &groups {
        group.shutdown().await.unwrap();
    }
    for (stop, server) in servers {
        stop.send(()).unwrap();
        server.await.unwrap();
    }
    drop(groups);
    let recovered =
        RaftGroup::boot_persistent(1, "tenant".into(), &root.path().join("1"), config())
            .await
            .unwrap();
    assert_eq!(
        recovered
            .committed_events(&organization())
            .await
            .unwrap()
            .len(),
        3
    );
    // Durable dedup can be inspected through the state machine without a live quorum.
    let mut machine = recovered.state_machine.clone();
    let entry = openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(2, 1), last + 1),
        payload: openraft::EntryPayload::Normal(AppData::Command(
            bootstrap_value()
                .command(loomery_genesis::Step::AssignLeader)
                .unwrap(),
        )),
    };

    assert!(matches!(
        machine.apply([entry]).await.unwrap()[0],
        Applied::Replayed { .. }
    ));
    recovered.shutdown().await.unwrap();
}
#[test]
fn configuration_defaults_and_validation() {
    let config: GroupConfig = serde_json::from_str("{}").unwrap();
    config.validate().unwrap();
    let bytes = serde_json::to_vec(&config).unwrap();
    serde_json::from_slice::<GroupConfig>(&bytes)
        .unwrap()
        .validate()
        .unwrap();
    let mut invalid = config;
    invalid.transport.max_message_bytes = 0;
    assert!(invalid.validate().is_err());
    assert!(serde_json::from_str::<GroupConfig>(r#"{"storage":{"unknown":1}}"#).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshots_cross_tonic_and_unknown_groups_are_rejected() {
    use openraft::{
        RaftSnapshotBuilder,
        network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    };
    let root = tempfile::tempdir().unwrap();
    let mut source = RaftGroup::boot_single_node(1).await.unwrap();
    source
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "source leader")
        .await
        .unwrap();
    bootstrap::run(&mut source, &bootstrap_value())
        .await
        .unwrap();
    let snapshot = source.state_machine.clone().build_snapshot().await.unwrap();
    let target = RaftGroup::boot_persistent(2, "tenant".into(), root.path(), config())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let transport = TonicTransport::default();
    transport
        .register("tenant".into(), target.raft())
        .await
        .unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        transport
            .serve(listener, config().transport, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let mut factory = transport::TonicNetworkFactory {
        group_id: "tenant".into(),
        config: config().transport,
    };
    let mut client = factory.new_client(2, &BasicNode::new(&address)).await;
    let rpc = openraft::raft::InstallSnapshotRequest {
        vote: openraft::Vote::new_committed(10, 1),
        meta: snapshot.meta,
        offset: 0,
        data: snapshot.snapshot.into_inner(),
        done: true,
    };
    client
        .install_snapshot(rpc, RPCOption::new(Duration::from_secs(3)))
        .await
        .unwrap();
    assert_eq!(
        target
            .committed_events(&organization())
            .await
            .unwrap()
            .len(),
        3
    );
    let mut raw = transport::wire::transport_client::TransportClient::connect(address)
        .await
        .unwrap();
    let error = raw
        .vote(transport::wire::Envelope {
            group_id: "missing".into(),
            json: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
    let error = raw
        .vote(transport::wire::Envelope {
            group_id: "tenant".into(),
            json: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    target.shutdown().await.unwrap();
    source.shutdown().await.unwrap();
    stop.send(()).unwrap();
    server.await.unwrap();
}
