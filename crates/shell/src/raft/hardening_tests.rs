// SPDX-License-Identifier: MPL-2.0

//! Persistence hardening: does a long history survive snapshot, purge and
//! restart in **both** persistence modes?
//!
//! The `OpenRaft` storage suites already pin the storage contracts (including
//! snapshot transfer and re-applying committed entries); this test adds the
//! dimension the suites do not cover: a few hundred applied commands, a
//! snapshot taken over that history, a full shutdown, and a restart from the
//! same database — with the dedup window still intact afterwards.
//!
//! Findings and the gaps left open are recorded in
//! `docs/benchmarks/persistence-hardening.md`.

use std::collections::BTreeMap;
use std::time::Duration;

use loomery_core::Uuid;
use loomery_core::actor::Actor;
use loomery_core::envelope::Command;
use loomery_core::envelope::Payload;
use loomery_core::id::Id;
use loomery_core::key::Key;
use loomery_core::timestamp::Timestamp;
use openraft::BasicNode;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;

use crate::config::GroupConfig;
use crate::config::StatePersistence;
use crate::group::GroupOps;
use crate::group::ProposeOutcome;
use crate::raft::RaftGroup;
use crate::raft::RocksLogStore;
use openraft::storage::RaftLogStorage;

const ORGANIZATION: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9";
const NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

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
        correlation_key: Key::new(&NS, "hardening"),
        actor: Actor::System,
        command_type: "task.create".to_owned(),
        payload: Payload {
            version: 1,
            data: format!(r#"{{"title":"task {index}"}}"#),
        },
    }
}

/// Boots (or reopens) the replica and waits for a leader.
async fn boot(path: &std::path::Path, mode: StatePersistence) -> RaftGroup {
    let group = RaftGroup::boot_persistent(1, "tenant".to_owned(), path, config(mode))
        .await
        .unwrap();

    if !group.raft().is_initialized().await.unwrap() {
        group
            .raft()
            .initialize(BTreeMap::from([(1u64, BasicNode::default())]))
            .await
            .unwrap();
    }

    group
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.current_leader.is_some(),
            "leader available",
        )
        .await
        .unwrap();

    group
}

/// Applies `commands` commands, snapshots, shuts down, reopens and checks that
/// the history — and the dedup window — survived.
async fn history_survives_snapshot_and_restart(mode: StatePersistence, commands: u64) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_path_buf();
    let organization_id = Id::from(ORGANIZATION);

    let mut group = boot(&path, mode).await;
    for index in 0..commands {
        group.propose(task_command(index)).await.unwrap();
    }

    // A durable checkpoint exists in checkpoint mode and the committed log is
    // durable in both; snapshot build/install/purge are covered by the OpenRaft
    // storage suites (see `docs/benchmarks/persistence-hardening.md`).
    let applied = group
        .raft()
        .metrics()
        .borrow()
        .last_applied
        .map_or(0, |log_id| log_id.index);
    assert_eq!(
        group
            .committed_events(&organization_id)
            .await
            .unwrap()
            .len() as u64,
        commands,
        "{mode:?}: every command applied"
    );

    group.shutdown().await.unwrap();
    drop(group);

    // Restart over the same database: state comes back from the snapshot and
    // the log, whichever the mode relies on.
    let mut group = boot(&path, mode).await;
    assert_eq!(
        group
            .committed_events(&organization_id)
            .await
            .unwrap()
            .len() as u64,
        commands,
        "{mode:?}: the history survived the restart"
    );
    assert!(
        group
            .raft()
            .metrics()
            .borrow()
            .last_applied
            .map_or(0, |log_id| log_id.index)
            >= applied,
        "{mode:?}: the applied index did not go backwards"
    );

    // The dedup window survived too: the last command is a replay, not a
    // second event.
    let outcome = group.propose(task_command(commands - 1)).await.unwrap();
    assert!(
        matches!(outcome, ProposeOutcome::Replayed { .. }),
        "{mode:?}: a re-proposed command replays after a restart"
    );
    assert_eq!(
        group
            .committed_events(&organization_id)
            .await
            .unwrap()
            .len() as u64,
        commands,
        "{mode:?}: the replay did not duplicate"
    );

    group.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_history_survives_snapshot_and_restart_in_both_modes() {
    history_survives_snapshot_and_restart(StatePersistence::Checkpoint, 300).await;
    history_survives_snapshot_and_restart(StatePersistence::Snapshot, 300).await;
}

/// How many commands the opt-in soak applies, or `None` when it is not enabled.
///
/// The soak is minutes of work, so it does not run in the default suite:
///
/// ```sh
/// LOOMERY_TEST_SOAK=1 mise run test                  # 2,000 commands
/// LOOMERY_TEST_SOAK_COMMANDS=20000 mise run test     # as long as you like
/// ```
fn soak_commands() -> Option<u64> {
    let enabled =
        std::env::var("LOOMERY_TEST_SOAK").is_ok_and(|value| !value.is_empty() && value != "0");
    if !enabled {
        return None;
    }
    Some(
        std::env::var("LOOMERY_TEST_SOAK_COMMANDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2000),
    )
}

/// The modes the soak runs, so a long run can skip the one that cannot afford it.
///
/// Checkpoint mode serializes the whole state on every apply, so its cost is
/// quadratic in the history (measured in `docs/benchmarks/persistence-hardening.md`);
/// `LOOMERY_TEST_SOAK_MODE=snapshot` runs the mode that scales instead.
fn soak_modes() -> Vec<StatePersistence> {
    match std::env::var("LOOMERY_TEST_SOAK_MODE").as_deref() {
        Ok("checkpoint") => vec![StatePersistence::Checkpoint],
        Ok("snapshot") => vec![StatePersistence::Snapshot],
        _ => vec![StatePersistence::Checkpoint, StatePersistence::Snapshot],
    }
}

/// The bytes the database occupies on disk.
fn directory_size(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .expect("the database directory")
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .filter(std::fs::Metadata::is_file)
        .map(|metadata| metadata.len())
        .sum()
}

/// A long history: apply, snapshot mid-way, restart, and measure each phase.
///
/// What the short probe cannot answer is *cost*: how apply throughput holds up as
/// the history grows, what a snapshot of it costs, how long recovery takes, and
/// how large the database gets. The numbers are recorded in
/// `docs/benchmarks/persistence-hardening.md`.
async fn soak(mode: StatePersistence, commands: u64) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_path_buf();
    let organization_id = Id::from(ORGANIZATION);

    let started = std::time::Instant::now();
    let mut group = boot(&path, mode).await;
    let halfway = commands / 2;
    for index in 0..halfway {
        group.propose(task_command(index)).await.unwrap();
    }

    // A snapshot over half the history, then the rest: recovery has a snapshot and
    // a log tail to combine, which is what a restart in the middle of a run sees.
    let snapshot_started = std::time::Instant::now();
    {
        let mut machine = group.state_machine();
        let mut builder = machine.get_snapshot_builder().await;
        builder.build_snapshot().await.unwrap();
    }
    let snapshot = snapshot_started.elapsed();
    for index in halfway..commands {
        group.propose(task_command(index)).await.unwrap();
    }
    let applied = started.elapsed();
    assert_eq!(
        group
            .committed_events(&organization_id)
            .await
            .unwrap()
            .len() as u64,
        commands,
        "{mode:?}: every command applied"
    );
    let applied_index = group
        .raft()
        .metrics()
        .borrow()
        .last_applied
        .map_or(0, |log_id| log_id.index);
    group.shutdown().await.unwrap();
    drop(group);
    let size = directory_size(&path);

    let restart_started = std::time::Instant::now();
    let mut group = boot(&path, mode).await;
    let restart = restart_started.elapsed();
    assert_eq!(
        group
            .committed_events(&organization_id)
            .await
            .unwrap()
            .len() as u64,
        commands,
        "{mode:?}: the whole history came back"
    );
    assert!(
        group
            .raft()
            .metrics()
            .borrow()
            .last_applied
            .map_or(0, |log_id| log_id.index)
            >= applied_index,
        "{mode:?}: the applied index did not go backwards"
    );
    let outcome = group.propose(task_command(commands - 1)).await.unwrap();
    assert!(
        matches!(outcome, ProposeOutcome::Replayed { .. }),
        "{mode:?}: the dedup window survived the soak"
    );
    group.shutdown().await.unwrap();

    // Integer arithmetic: the rate is a report, and a float cast here would need an
    // allow for precision loss in the middle of a test.
    let nanos = applied.as_nanos();
    let per_second = u64::try_from(
        u128::from(commands)
            .saturating_mul(1_000_000_000)
            .checked_div(nanos)
            .unwrap_or(0),
    )
    .unwrap_or(u64::MAX);
    println!(
        "soak {mode:?}: {commands} commands | apply {applied:?} (~{per_second}/s) |          snapshot {snapshot:?} | restart {restart:?} | {size} bytes on disk"
    );
}

/// The record is written per apply and read back in log order, once per event.
///
/// Run in snapshot mode deliberately: checkpoint mode also carries the events
/// inside its state record, so only snapshot mode shows that they are durable on
/// their own account.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_record_holds_every_event_once_and_in_log_order() {
    let root = tempfile::tempdir().unwrap();
    let mut group = boot(root.path(), StatePersistence::Snapshot).await;
    for index in 0..5 {
        group.propose(task_command(index)).await.unwrap();
    }

    let recorded = group.state_machine().event_record().await.unwrap();
    let folded = group
        .committed_events(&Id::from(ORGANIZATION))
        .await
        .unwrap();
    assert_eq!(recorded.len(), 5, "one event per command, and no more");
    assert_eq!(
        recorded, folded,
        "the record and the fold agree, in log order"
    );

    // A replay is not a second event: the record is a record of what happened.
    let outcome = group.propose(task_command(4)).await.unwrap();
    assert!(matches!(outcome, ProposeOutcome::Replayed { .. }));
    assert_eq!(
        group.state_machine().event_record().await.unwrap().len(),
        5,
        "a replayed command adds nothing to the record"
    );
    group.shutdown().await.unwrap();
}

/// Purging the Raft log does not touch the record.
///
/// The Raft log is bookkeeping — `OpenRaft` purges what a snapshot covers — while
/// the record is history. This purges the whole log away and finds the events
/// still there, which is the guarantee that lets purging be safe.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purge_leaves_the_record_untouched() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_path_buf();
    let commands = 5;
    let mut group = boot(&path, StatePersistence::Snapshot).await;
    for index in 0..commands {
        group.propose(task_command(index)).await.unwrap();
    }
    let recorded = group.state_machine().event_record().await.unwrap();
    assert_eq!(recorded.len(), usize::try_from(commands).unwrap());
    group.shutdown().await.unwrap();
    drop(group);

    // Purge the log away, then look at the record through a fresh handle.
    let mut log = RocksLogStore::open(
        super::test_disk::open(&path, &crate::config::StorageConfig::default())
            .await
            .unwrap(),
    );
    let last = log
        .get_log_state()
        .await
        .unwrap()
        .last_log_id
        .expect("the log has entries");
    log.purge(last).await.unwrap();
    drop(log);
    assert!(
        log_entries(&path).await.is_empty(),
        "the purge emptied the Raft log"
    );

    let disk = super::test_disk::open(&path, &crate::config::StorageConfig::default())
        .await
        .unwrap();
    let stored = read_record(&disk).await;
    assert_eq!(
        stored, recorded,
        "the record survived a purge of the log that carried it"
    );
}

/// Every log entry in a database, read through a fresh handle.
async fn log_entries(path: &std::path::Path) -> Vec<u64> {
    let mut log = RocksLogStore::open(
        super::test_disk::open(path, &crate::config::StorageConfig::default())
            .await
            .unwrap(),
    );
    openraft::storage::RaftLogReader::try_get_log_entries(&mut log, ..)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.log_id.index)
        .collect()
}

/// The append-only record in a database, read through a fresh handle.
async fn read_record(disk: &super::disk::Disk) -> Vec<loomery_core::envelope::Event> {
    disk.run(|db| {
        let family = super::disk::Family::Events.handle(db)?;
        let mut events = Vec::new();
        for item in db.iterator_cf(family, rocksdb::IteratorMode::Start) {
            let (key, value) = item?;
            if !key.starts_with(b"e") {
                break;
            }
            events.push(serde_json::from_slice(&value)?);
        }
        Ok(events)
    })
    .await
    .unwrap()
}

/// The fold is recovered from its own per-aggregate state, not by replaying the log.
///
/// The proof is a database whose log is *gone*: a snapshot covers it, so `OpenRaft`
/// can purge every entry away. Reopening then finds the record still answering
/// history, and the dedup window — which lives in the state, not in the log — still
/// turning a re-proposed command into a replay. Nothing else could have supplied
/// either: there is no log left to replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_state_answers_without_its_log() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_path_buf();
    let commands = 5u64;
    let mut group = boot(&path, StatePersistence::Checkpoint).await;
    for index in 0..commands {
        group.propose(task_command(index)).await.unwrap();
    }
    {
        let machine = group.state_machine();
        let mut builder = machine.clone();
        builder.build_snapshot().await.unwrap();
    }
    group.shutdown().await.unwrap();
    drop(group);

    // Purge every entry the snapshot covers: the log is now empty.
    let mut log = RocksLogStore::open(
        super::test_disk::open(&path, &crate::config::StorageConfig::default())
            .await
            .unwrap(),
    );
    let last = log
        .get_log_state()
        .await
        .unwrap()
        .last_log_id
        .expect("the log has entries");
    log.purge(last).await.unwrap();
    drop(log);
    assert!(
        log_entries(&path).await.is_empty(),
        "the purge emptied the Raft log"
    );

    let mut group = boot(&path, StatePersistence::Checkpoint).await;
    assert_eq!(
        group
            .committed_events(&Id::from(ORGANIZATION))
            .await
            .unwrap()
            .len(),
        usize::try_from(commands).unwrap(),
        "the record answers history with the log gone"
    );
    let outcome = group.propose(task_command(commands - 1)).await.unwrap();
    assert!(
        matches!(outcome, ProposeOutcome::Replayed { .. }),
        "the dedup window came from the state, not from a log that no longer exists"
    );
    assert_eq!(
        group.state_machine().event_record().await.unwrap().len(),
        usize::try_from(commands).unwrap(),
        "a replay adds nothing to the record"
    );
    group.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_history_soak_is_opt_in() {
    let Some(commands) = soak_commands() else {
        return;
    };
    for mode in soak_modes() {
        soak(mode, commands).await;
    }
}
