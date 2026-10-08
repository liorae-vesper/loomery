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

struct Builder(crate::config::StatePersistence);
impl StoreBuilder<TypeConfig, RocksLogStore, Arc<MemStateMachine>, tempfile::TempDir> for Builder {
    async fn build(
        &self,
    ) -> Result<(tempfile::TempDir, RocksLogStore, Arc<MemStateMachine>), StorageError<u64>> {
        let dir = tempfile::tempdir().unwrap();
        let disk = super::disk::Disk::open(dir.path(), &crate::config::StorageConfig::default())
            .await
            .unwrap();
        let log = RocksLogStore::open(disk.clone());
        let machine = MemStateMachine::open(disk, self.0).await.unwrap();
        Ok((dir, log, machine))
    }
}
#[test]
fn rocksdb_passes_openraft_storage_suite() {
    Suite::<TypeConfig, RocksLogStore, Arc<MemStateMachine>, Builder, tempfile::TempDir>::test_all(
        Builder(crate::config::StatePersistence::Checkpoint),
    )
    .unwrap();
}
#[test]
fn snapshot_backed_rocksdb_passes_openraft_storage_suite() {
    Suite::<TypeConfig, RocksLogStore, Arc<MemStateMachine>, Builder, tempfile::TempDir>::test_all(
        Builder(crate::config::StatePersistence::Snapshot),
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
/// Reopens a persistent group, waiting out the release of the previous handle.
///
/// The rule and the deadline are in [`super::test_disk`]: a store is released when
/// the last handle to it goes away, and `Raft::shutdown` only *aborts* the tasks
/// holding others, so a reopen can race that release (this flaked in CI, not
/// locally). A store still locked after the deadline is a real leak, and its error
/// surfaces.
async fn reopen(
    node_id: u64,
    directory: &std::path::Path,
    config: GroupConfig,
) -> anyhow::Result<RaftGroup> {
    let deadline = tokio::time::Instant::now() + test_disk::RELEASE_TIMEOUT;
    loop {
        match RaftGroup::boot_persistent(node_id, "tenant".into(), directory, config.clone()).await
        {
            Ok(group) => return Ok(group),
            Err(error) if tokio::time::Instant::now() < deadline => {
                eprintln!("reopen attempt failed, retrying: {error}");
                tokio::time::sleep(test_disk::RELEASE_POLL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn three_replicas_commit_and_recover_genesis() {
    let root = tempfile::tempdir().unwrap();
    let (mut server_tls, client_tls) = super::tls_tests::certificates(root.path());
    server_tls.client_ca_certificate = Some(client_tls.ca_certificate.clone());
    let mut group_config = config();
    group_config.transport.server_tls = Some(server_tls);
    group_config.transport.client_tls = Some(client_tls);
    let mut groups = Vec::new();
    let mut servers = Vec::new();
    let mut members = BTreeMap::new();
    for node in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            node,
            BasicNode::new(format!("https://{}", listener.local_addr().unwrap())),
        );
        let group = RaftGroup::boot_persistent(
            node,
            "tenant".into(),
            &root.path().join(node.to_string()),
            group_config.clone(),
        )
        .await
        .unwrap();
        let transport = TonicTransport::default();
        transport
            .register("tenant".into(), group.raft())
            .await
            .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server_config = group_config.transport.clone();
        let server = tokio::spawn(async move {
            transport
                .serve(listener, server_config, async {
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
            .applied_index_at_least(Some(last), "replicated")
            .await
            .unwrap();
        // What matters is the *state*, and a replica's applied-index metric can be
        // observed ahead of the fold it reports. Wait for the state itself, bounded,
        // and dump what arrived if it never does — a wait that can only fail loudly,
        // never pass quietly.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let events = group.committed_events(&organization()).await.unwrap();
            if events.len() == 3 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "a replica never folded all three genesis events: {events:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    let recovered = reopen(1, &root.path().join("1"), group_config)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)] // Recovery scenarios deliberately share the full lifecycle.
async fn snapshot_mode_recovers_log_only_and_snapshot_with_purged_prefix() {
    use crate::config::StatePersistence;
    use openraft::storage::RaftLogStorage;
    for snapshot_first in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut settings = config();
        settings.storage.state_persistence = StatePersistence::Snapshot;
        let mut group =
            RaftGroup::boot_persistent(1, "tenant".into(), root.path(), settings.clone())
                .await
                .unwrap();
        group
            .raft()
            .initialize(BTreeMap::from([(1, BasicNode::default())]))
            .await
            .unwrap();
        group
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        let commands = bootstrap_value();
        group
            .propose(
                commands
                    .command(loomery_genesis::Step::AssignLeader)
                    .unwrap(),
            )
            .await
            .unwrap();
        if snapshot_first {
            let covered = group.raft().metrics().borrow().last_applied.unwrap();
            group.raft().trigger().snapshot().await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if group
                        .raft()
                        .metrics()
                        .borrow()
                        .snapshot
                        .is_some_and(|id| id.index >= covered.index)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            // Exercise recovery after the snapshot-covered prefix is gone.
            group
                .raft()
                .trigger()
                .purge_log(covered.index)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if group
                        .raft()
                        .metrics()
                        .borrow()
                        .purged
                        .is_some_and(|id| id.index >= covered.index)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        bootstrap::run(&mut group, &commands).await.unwrap();
        let index = group.raft().metrics().borrow().last_applied.unwrap().index;
        group.shutdown().await.unwrap();
        drop(group);
        let mut restored =
            RaftGroup::boot_persistent(1, "tenant".into(), root.path(), settings.clone())
                .await
                .unwrap();
        restored
            .raft()
            .wait(Some(Duration::from_secs(5)))
            // "At least": a restored node re-elects itself and appends the
            // election's blank entry, which can push the applied index past the
            // one this test recorded. What it is waiting for is that the log has
            // been replayed, and the state assertion below is what proves it.
            .applied_index_at_least(Some(index), "replay")
            .await
            .unwrap();
        assert_eq!(
            restored
                .committed_events(&organization())
                .await
                .unwrap()
                .len(),
            3
        );
        restored
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "restored leader")
            .await
            .unwrap();
        bootstrap::run(&mut restored, &commands).await.unwrap();
        assert_eq!(
            restored
                .committed_events(&organization())
                .await
                .unwrap()
                .len(),
            3
        );
        restored.shutdown().await.unwrap();
        drop(restored);
        let disk = super::disk::Disk::open(root.path(), &settings.storage)
            .await
            .unwrap();
        assert!(
            disk.get(super::disk::Family::State, b"state")
                .await
                .unwrap()
                .is_none()
        );
        let mut log = RocksLogStore::open(disk.clone());
        assert!(log.read_committed().await.unwrap().is_some());
        assert!(
            MemStateMachine::open(disk, StatePersistence::Checkpoint)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn persistence_mode_is_fixed_across_restarts_in_both_directions() {
    use crate::config::{StatePersistence, StorageConfig};
    for (selected, rejected) in [
        (StatePersistence::Checkpoint, StatePersistence::Snapshot),
        (StatePersistence::Snapshot, StatePersistence::Checkpoint),
    ] {
        let root = tempfile::tempdir().unwrap();
        let settings = StorageConfig::default();
        let disk = super::disk::Disk::open(root.path(), &settings)
            .await
            .unwrap();
        let machine = MemStateMachine::open(disk.clone(), selected).await.unwrap();
        let marker = disk
            .get(super::disk::Family::Default, b"state_persistence")
            .await
            .unwrap()
            .unwrap();
        drop(machine);
        drop(disk);
        let disk = super::disk::Disk::open(root.path(), &settings)
            .await
            .unwrap();
        let error = MemStateMachine::open(disk.clone(), rejected)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mode cannot change"));
        assert_eq!(
            disk.get(super::disk::Family::Default, b"state_persistence")
                .await
                .unwrap(),
            Some(marker)
        );
        MemStateMachine::open(disk, selected).await.unwrap();
    }
}

#[tokio::test]
async fn another_layout_or_a_missing_marker_is_refused() {
    use crate::config::StorageConfig;
    let settings = StorageConfig::default();

    // A database written before column families existed: one family, no layout of
    // ours. Refused, rather than read as if it were this layout.
    let legacy = tempfile::tempdir().unwrap();
    {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let database = rocksdb::DB::open(&options, legacy.path()).unwrap();
        database.put(b"vote", b"legacy vote").unwrap();
    }
    assert!(
        super::disk::Disk::open(legacy.path(), &settings)
            .await
            .is_err(),
        "a database from another layout is refused"
    );

    // Our families, but no layout marker: refused too, so a hand-made or
    // half-written directory cannot pass for one this build wrote.
    let unmarked = tempfile::tempdir().unwrap();
    let disk = super::disk::Disk::open(unmarked.path(), &settings)
        .await
        .unwrap();
    disk.run(|db| {
        db.delete_cf(super::disk::Family::Default.handle(db)?, b"format")?;
        Ok(())
    })
    .await
    .unwrap();
    drop(disk);
    assert!(
        super::disk::Disk::open(unmarked.path(), &settings)
            .await
            .is_err(),
        "a database without a layout marker is refused"
    );

    // And a database carrying another layout version: the marker is what makes a
    // future format change fail closed instead of being misread.
    let future = tempfile::tempdir().unwrap();
    let disk = super::disk::Disk::open(future.path(), &settings)
        .await
        .unwrap();
    disk.put(
        super::disk::Family::Default,
        b"format".to_vec(),
        serde_json::to_vec(&super::disk::FORMAT.saturating_add(1)).unwrap(),
    )
    .await
    .unwrap();
    drop(disk);
    assert!(
        super::disk::Disk::open(future.path(), &settings)
            .await
            .is_err(),
        "another layout version is refused"
    );
}

#[tokio::test]
async fn invalid_persistence_marker_fails_closed() {
    use crate::config::{StatePersistence, StorageConfig};
    let root = tempfile::tempdir().unwrap();
    let disk = super::disk::Disk::open(root.path(), &StorageConfig::default())
        .await
        .unwrap();
    let invalid = b"\"unknown_mode\"".to_vec();
    disk.put(
        super::disk::Family::Default,
        b"state_persistence".to_vec(),
        invalid.clone(),
    )
    .await
    .unwrap();
    for mode in [StatePersistence::Checkpoint, StatePersistence::Snapshot] {
        assert!(MemStateMachine::open(disk.clone(), mode).await.is_err());
        assert_eq!(
            disk.get(super::disk::Family::Default, b"state_persistence")
                .await
                .unwrap(),
            Some(invalid.clone())
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_database_only_after_state_handles_are_dropped() {
    use crate::config::StatePersistence;
    for mode in [StatePersistence::Checkpoint, StatePersistence::Snapshot] {
        let root = tempfile::tempdir().unwrap();
        let mut settings = config();
        settings.storage.state_persistence = mode;
        let mut group =
            RaftGroup::boot_persistent(1, "tenant".into(), root.path(), settings.clone())
                .await
                .unwrap();
        group
            .raft()
            .initialize(BTreeMap::from([(1, BasicNode::default())]))
            .await
            .unwrap();
        group
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "leader")
            .await
            .unwrap();
        bootstrap::run(&mut group, &bootstrap_value())
            .await
            .unwrap();
        let index = group.raft().metrics().borrow().last_applied.unwrap().index;
        let retained_state = group.state_machine.clone();
        group.shutdown().await.unwrap();
        drop(group);
        let error = super::disk::Disk::open(root.path(), &settings.storage)
            .await
            .unwrap_err();
        assert!(error.to_string().to_lowercase().contains("lock"));
        assert_eq!(
            retained_state.committed_events(&organization()).await.len(),
            3
        );
        drop(retained_state);
        let mut recovered = RaftGroup::boot_persistent(1, "tenant".into(), root.path(), settings)
            .await
            .unwrap();
        recovered
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(index), "recovered")
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
        recovered
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "recovered leader")
            .await
            .unwrap();
        bootstrap::run(&mut recovered, &bootstrap_value())
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
        recovered.shutdown().await.unwrap();
    }
}
