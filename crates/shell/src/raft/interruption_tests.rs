// SPDX-License-Identifier: MPL-2.0

//! What an *interrupted* durable write leaves behind.
//!
//! The storage suites pin the happy path (build, install, purge, restart). These
//! tests inject a real `RocksDB` write rejection — a read-only handle, the same
//! technique `append_tests` uses — at the two places the design's recovery story
//! depends on atomicity:
//!
//! * a **snapshot build** whose persist fails: the previous snapshot must survive,
//!   nothing may be installed, recovery must still work, and a retry must succeed.
//!   This is the "abandoned build" path: the design relies on the builder being
//!   idempotent and keyed by `last_applied_index`
//!   (`docs/research/checkpoint-policy.md`), which these tests now exercise under
//!   injection rather than assume.
//! * a **purge** whose batch fails: the covered entries and the purge floor move
//!   together or not at all, because they are one synchronous WAL batch. A crash
//!   cannot land between them, so the failing-write case is what there is to test.
//!
//! Findings are recorded in `docs/benchmarks/persistence-hardening.md`.

use std::collections::BTreeMap;
use std::time::Duration;

use loomery_core::Uuid;
use loomery_core::actor::Actor;
use loomery_core::envelope::Command;
use loomery_core::envelope::Event;
use loomery_core::envelope::Payload;
use loomery_core::id::Id;
use loomery_core::key::Key;
use loomery_core::timestamp::Timestamp;
use openraft::BasicNode;
use openraft::LogId;

use super::alias::LogIdOf;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use rocksdb::IteratorMode;
use rocksdb::WriteBatch;
use rocksdb::WriteOptions;

use crate::config::GroupConfig;
use crate::config::StatePersistence;
use crate::config::StorageConfig;
use crate::group::GroupOps;

use super::RaftGroup;
use super::RocksLogStore;
use super::disk::Family;
use super::state_machine::MemStateMachine;
use super::test_disk;

const ORGANIZATION: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9";
const NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);
const COMMANDS: u64 = 20;

fn config(mode: StatePersistence) -> GroupConfig {
    let mut config = GroupConfig::default();
    config.raft.heartbeat_interval = 50;
    config.raft.election_timeout_min = 150;
    config.raft.election_timeout_max = 300;
    config.storage.state_persistence = mode;
    config
}

fn task_command(index: u64) -> Command {
    Command {
        envelope_version: 1,
        id: Id::new(),
        aggregate_id: Id::from(format!("task-{index}")),
        organization_id: Id::from(ORGANIZATION),
        workspace_id: Some(Id::from("ws-1")),
        occurred_at: Timestamp::from(1_700_000_000_000),
        causation_key: Key::new(&NS, &format!("task-{index}")),
        correlation_key: Key::new(&NS, "interruption"),
        actor: Actor::System,
        command_type: "task.create".to_owned(),
        payload: Payload {
            version: 1,
            data: format!(r#"{{"title":"task {index}"}}"#),
        },
    }
}

/// Boots a persistent replica, applies [`COMMANDS`] commands and takes a
/// snapshot, then closes everything so the database is free for another handle.
///
/// Returns the stored snapshot bytes and the log id that snapshot covers.
async fn boot_apply_and_snapshot(
    path: &std::path::Path,
    mode: StatePersistence,
) -> (Vec<u8>, LogIdOf) {
    let mut group = RaftGroup::boot_persistent(1, "tenant".to_owned(), path, config(mode))
        .await
        .unwrap();
    group
        .raft()
        .initialize(BTreeMap::from([(1u64, BasicNode::default())]))
        .await
        .unwrap();
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();

    for index in 0..COMMANDS {
        group.propose(task_command(index)).await.unwrap();
    }
    assert_eq!(
        group
            .committed_events(&Id::from(ORGANIZATION))
            .await
            .unwrap()
            .len() as u64,
        COMMANDS
    );

    // A completed build: this is the snapshot the interrupted one must not damage.
    // Built on the state machine directly, so the test does not depend on Raft's
    // snapshot scheduling; the handle is scoped because a live clone keeps the
    // database locked (the rule
    // `shutdown_releases_database_only_after_state_handles_are_dropped` pins).
    let covered = {
        let machine = group.state_machine();
        let mut builder = machine.clone();
        let built = builder.build_snapshot().await.unwrap();
        built.meta.last_log_id.expect("a snapshot covers the log")
    };
    group.shutdown().await.unwrap();
    drop(group);

    let disk = test_disk::open(path, &StorageConfig::default())
        .await
        .unwrap();
    let bytes = disk
        .get(Family::State, b"snapshot")
        .await
        .unwrap()
        .expect("a completed build persists a snapshot");
    (bytes, covered)
}

/// The log index the stored snapshot covers.
///
/// Read as JSON because the stored type is private to the state machine. 0.10's
/// `SnapshotMeta` carries no transfer id — a snapshot is identified by the
/// position it covers — so this index is the whole identity to read back.
async fn stored_snapshot_index(path: &std::path::Path) -> u64 {
    let bytes = stored_snapshot(path).await.expect("a snapshot is stored");
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["meta"]["last_log_id"]["index"]
        .as_u64()
        .expect("a covered index")
}

/// The serialized state the stored record carries in its `data` field.
async fn stored_snapshot_data(path: &std::path::Path) -> Vec<u8> {
    let bytes = stored_snapshot(path).await.expect("a snapshot is stored");
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    serde_json::from_value(value["data"].clone()).expect("the record carries the snapshot bytes")
}

/// The stored snapshot bytes, read through a fresh handle.
async fn stored_snapshot(path: &std::path::Path) -> Option<Vec<u8>> {
    test_disk::open(path, &StorageConfig::default())
        .await
        .unwrap()
        .get(Family::State, b"snapshot")
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_abandoned_snapshot_build_keeps_the_previous_one_and_stays_retryable() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_path_buf();
    let (before, covered) = boot_apply_and_snapshot(&path, StatePersistence::Snapshot).await;

    // The build runs against a read-only database: serialization succeeds, the
    // persist is rejected, so the build is abandoned midway.
    let machine = MemStateMachine::open(
        test_disk::open_read_only(&path).await.unwrap(),
        StatePersistence::Snapshot,
    )
    .await
    .unwrap();
    let mut building = machine.clone();
    let mut builder = building.get_snapshot_builder().await;
    let error = builder
        .build_snapshot()
        .await
        .expect_err("a read-only database cannot persist a snapshot");
    // The injected failure is the store's own write rejection. `RocksDB` words it
    // differently for a put ("read only") than for a batch on a compacted
    // read-only database ("not supported in compacted db mode"). In 0.10 the
    // storage API returns `io::Error`, so a logic error cannot arrive here by
    // type; what is worth asserting is that the store's own rejection is what
    // surfaced.
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("read only") || message.contains("not supported"),
        "the injected failure is the store's write rejection: {error}"
    );

    // Nothing was installed: the machine still holds the snapshot it recovered,
    // and the stored bytes are untouched.
    let mut reading = machine.clone();
    let current = reading
        .get_current_snapshot()
        .await
        .unwrap()
        .expect("the recovered snapshot is still current");
    assert_eq!(
        current.meta.last_log_id,
        Some(covered),
        "the abandoned build did not advance the installed snapshot"
    );

    // Close the read-only handle before looking at the database again: a read-only
    // `RocksDB` handle holds the database lock too, so a second open (read-write or
    // read-only) cannot succeed while it is alive.
    drop(reading);
    drop(builder);
    drop(building);
    drop(machine);
    assert_eq!(
        stored_snapshot(&path).await.as_deref(),
        Some(before.as_slice()),
        "the abandoned build did not overwrite the stored snapshot"
    );

    // Recovery after the abandoned build: the group still comes back whole.
    let mut group = RaftGroup::boot_persistent(
        1,
        "tenant".to_owned(),
        &path,
        config(StatePersistence::Snapshot),
    )
    .await
    .unwrap();
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    assert_eq!(
        group
            .committed_events(&Id::from(ORGANIZATION))
            .await
            .unwrap()
            .len() as u64,
        COMMANDS,
        "recovery after an abandoned build is unaffected"
    );

    // The retry the design promises: a build after the interrupted one is keyed by
    // the applied index, so it succeeds and covers the log as it now stands.
    // Built on the state machine directly, and read back only once the group is
    // closed — a live group holds the database, so a second handle cannot open it.
    group.propose(task_command(COMMANDS)).await.unwrap();
    let retried = {
        let machine = group.state_machine();
        let mut builder = machine.clone();
        builder.build_snapshot().await.unwrap()
    };
    let retried_index = retried
        .meta
        .last_log_id
        .expect("a snapshot covers the log")
        .index;
    assert_eq!(
        retried_index,
        covered.index + 1,
        "the retry covers the command applied after the abandoned build"
    );
    let retried_bytes = retried.snapshot.into_inner();
    group.shutdown().await.unwrap();
    drop(group);

    assert_eq!(
        stored_snapshot_index(&path).await,
        covered.index + 1,
        "the retry persisted a snapshot covering the newer log"
    );
    // 0.10 removed the transfer id from `SnapshotMeta`, so the stored bytes —
    // not an id — are what show that the retry, and not the snapshot it replaced,
    // is what got persisted.
    assert_eq!(
        stored_snapshot_data(&path).await,
        retried_bytes,
        "the stored snapshot is the one the retry built"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_purge_moves_neither_the_entries_nor_the_floor() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_path_buf();
    let mut group = RaftGroup::boot_persistent(
        1,
        "tenant".to_owned(),
        &path,
        config(StatePersistence::Checkpoint),
    )
    .await
    .unwrap();
    group
        .raft()
        .initialize(BTreeMap::from([(1u64, BasicNode::default())]))
        .await
        .unwrap();
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    for index in 0..COMMANDS {
        group.propose(task_command(index)).await.unwrap();
    }
    group.shutdown().await.unwrap();
    drop(group);

    // Purge against a read-only database: the batch is rejected, so the covered
    // entries and the purge floor must both still be absent from the database.
    let mut store = RocksLogStore::open(test_disk::open_read_only(&path).await.unwrap());
    let entries_before = store.try_get_log_entries(..).await.unwrap().len();
    let state = store.get_log_state().await.unwrap();
    let last = state.last_log_id.expect("a log exists");
    let floor = LogId::new(last.leader_id, last.index / 2);
    let error = store
        .purge(floor)
        .await
        .expect_err("a read-only database cannot purge");
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("read only") || message.contains("not supported"),
        "the injected failure is the store's write rejection: {error}"
    );

    let state = store.get_log_state().await.unwrap();
    assert_eq!(
        state.last_purged_log_id, None,
        "a failed purge does not move the floor"
    );
    let untouched = store.try_get_log_entries(..).await.unwrap();
    assert_eq!(
        untouched.len(),
        entries_before,
        "a failed purge deletes no entry"
    );

    // The positive control: a purge that commits moves both, and the floor
    // survives a reopen — a crash after the batch cannot lose it.
    drop(store);
    let mut store = RocksLogStore::open(
        test_disk::open(&path, &StorageConfig::default())
            .await
            .unwrap(),
    );
    let before = store.try_get_log_entries(..).await.unwrap();
    let covered = before
        .iter()
        .filter(|entry| entry.log_id.index <= floor.index)
        .count();
    store.purge(floor).await.unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(floor));
    let remaining = store.try_get_log_entries(..).await.unwrap();
    assert_eq!(
        remaining.len(),
        before.len() - covered,
        "the entries above the floor are kept, the ones below are gone"
    );
    assert!(
        remaining
            .iter()
            .all(|entry| entry.log_id.index > floor.index),
        "no covered entry survived the purge"
    );

    drop(store);
    let mut store = RocksLogStore::open(
        test_disk::open(&path, &StorageConfig::default())
            .await
            .unwrap(),
    );
    assert_eq!(
        store.get_log_state().await.unwrap().last_purged_log_id,
        Some(floor),
        "the purge floor is durable on its own"
    );
}

/// Boots a single-replica checkpoint group and waits for it to lead.
///
/// `initialize` is only for a database that has never held a cluster: a reopened one
/// recovers its membership from its own log, and asking again is an error.
async fn boot_checkpoint(path: &std::path::Path, initialize: bool) -> RaftGroup {
    let group = RaftGroup::boot_persistent(
        1,
        "tenant".to_owned(),
        path,
        config(StatePersistence::Checkpoint),
    )
    .await
    .unwrap();
    if initialize {
        group
            .raft()
            .initialize(BTreeMap::from([(1u64, BasicNode::default())]))
            .await
            .unwrap();
    }
    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    group
}

/// Applies [`COMMANDS`] commands and stops the replica, freeing the database.
async fn apply_and_stop(path: &std::path::Path) -> Vec<Event> {
    let mut group = boot_checkpoint(path, true).await;
    for index in 0..COMMANDS {
        group.propose(task_command(index)).await.unwrap();
    }
    let record = group
        .committed_events(&Id::from(ORGANIZATION))
        .await
        .unwrap();
    assert_eq!(record.len() as u64, COMMANDS);
    group.shutdown().await.unwrap();
    drop(group);
    record
}

/// Deletes what an apply wrote, so the replica comes back behind its log.
///
/// The state family's aggregates, dedup entries and markers go — and, when `record`
/// is set, the append-only record they were written *with*, which is the same
/// `WriteBatch`. `state_persistence` and `snapshot` survive: their own synced paths
/// wrote them, and they are not what a lost apply takes with it.
async fn drop_applied_writes(path: &std::path::Path, record: bool) {
    let disk = test_disk::open(path, &StorageConfig::default())
        .await
        .unwrap();
    disk.run(move |db| {
        let mut batch = WriteBatch::default();
        let state = Family::State.handle(db)?;
        for item in db.iterator_cf(state, IteratorMode::Start) {
            let (key, _) = item?;
            if key.as_ref() != b"state_persistence" && key.as_ref() != b"snapshot" {
                batch.delete_cf(state, key);
            }
        }
        if record {
            let events = Family::Events.handle(db)?;
            for item in db.iterator_cf(events, IteratorMode::Start) {
                let (key, _) = item?;
                batch.delete_cf(events, key);
            }
        }
        db.write_opt(batch, &WriteOptions::default())?;
        Ok(())
    })
    .await
    .unwrap();
    drop(disk);
}

/// Reopens the replica and requires the record to come back exactly once.
async fn reopen_and_expect(path: &std::path::Path, expected: &[Event]) {
    let group = boot_checkpoint(path, false).await;
    group
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |m| m.last_applied.map(|id| id.index) == m.last_log_index,
            "replayed",
        )
        .await
        .unwrap();
    let recovered = group
        .committed_events(&Id::from(ORGANIZATION))
        .await
        .unwrap();
    assert_eq!(
        recovered.len(),
        expected.len(),
        "recovery produced a different number of events than were applied"
    );
    assert_eq!(
        recovered, expected,
        "recovery rebuilt the record, once, from the log"
    );
    group.shutdown().await.unwrap();
}

/// A lost apply is repaired by replaying the log.
///
/// The checkpoint write is not the durability boundary — the Raft log is, and it is
/// synced before a command is committed. A lost unsynced batch therefore leaves the
/// replica behind its log but still *consistent*, because an apply writes its state,
/// its record and its marker in one `WriteBatch`: the marker moves exactly as far as
/// the record does. `OpenRaft` then replays from the marker it finds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_state_write_is_repaired_by_replaying_the_log() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();

    let expected = apply_and_stop(path).await;
    drop_applied_writes(path, true).await;
    reopen_and_expect(path, &expected).await;
}

/// A store whose record has no marker is refused rather than guessed at.
///
/// An apply writes the record and its marker in one `WriteBatch`, so this is not a
/// store a crash can produce. Both available guesses corrupt the record — keeping it
/// duplicates every event on replay, dropping it loses events no log may still hold —
/// so opening fails closed, the same choice the database's layout marker makes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_without_its_marker_is_refused_rather_than_guessed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();

    let _applied = apply_and_stop(path).await;
    drop_applied_writes(path, false).await;

    let error = RaftGroup::boot_persistent(
        1,
        "tenant".to_owned(),
        path,
        config(StatePersistence::Checkpoint),
    )
    .await
    .err()
    .expect("an inconsistent store must not be opened");
    assert!(
        error.to_string().contains("no applied marker"),
        "unexpected error: {error}"
    );
}
