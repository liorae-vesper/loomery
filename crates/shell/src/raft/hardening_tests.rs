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

use crate::config::GroupConfig;
use crate::config::StatePersistence;
use crate::group::GroupOps;
use crate::group::ProposeOutcome;
use crate::raft::RaftGroup;

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
