// SPDX-License-Identifier: MPL-2.0
//! Shared batch writer, bounds, lifecycle and durable replay acceptance checks.
use super::*;
use crate::{
    config::{GroupConfig, StatePersistence},
    group::ProposeOutcome,
    test_support::{bootstrap_value, organization},
};
use openraft::BasicNode;
use openraft::type_config::async_runtime::WatchReceiver;
use std::{collections::BTreeMap, time::Duration};

fn config(mode: StatePersistence) -> GroupConfig {
    let mut config = GroupConfig::default();
    config.storage.state_persistence = mode;
    config.proposals.max_batch_commands = 3;
    config.proposals.max_delay_ms = 20;
    config.raft.heartbeat_interval = 50;
    config.raft.election_timeout_min = 150;
    config.raft.election_timeout_max = 300;
    config
}
async fn boot(path: &std::path::Path, config: GroupConfig) -> RaftGroup {
    let group = RaftGroup::boot_persistent(1, "batch-test".into(), path, config)
        .await
        .unwrap();
    if !group.raft().is_initialized().await.unwrap() {
        group
            .raft()
            .initialize(BTreeMap::from([(1, BasicNode::default())]))
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
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |m| m.last_applied.map(|id| id.index) == m.last_log_index,
            "startup applied",
        )
        .await
        .unwrap();
    group
}
fn commands() -> [loomery_core::envelope::Command; 3] {
    loomery_genesis::Step::ALL.map(|step| bootstrap_value().command(step).unwrap())
}

#[tokio::test]
async fn concurrent_writers_share_an_entry_and_recover_each_command_in_both_modes() {
    for mode in [StatePersistence::Checkpoint, StatePersistence::Snapshot] {
        let directory = tempfile::tempdir().unwrap();
        let group = boot(directory.path(), config(mode)).await;
        let before = group
            .raft()
            .metrics()
            .borrow_watched()
            .last_log_index
            .unwrap();
        let writer = group.writer();
        let other = writer.clone();
        let [first, second, third] = commands();
        let (a, b, c) = tokio::join!(
            writer.propose(first.clone()),
            other.propose(second.clone()),
            writer.propose(third.clone())
        );
        let expected = ProposeOutcome::Appended {
            first_log_index: before + 1,
        };
        assert_eq!(a.unwrap(), expected);
        assert_eq!(b.unwrap(), expected);
        assert_eq!(c.unwrap(), expected);
        let events = group.state_machine.committed_events(&organization()).await;
        assert_eq!(
            events
                .iter()
                .map(|event| event.causation_key.clone())
                .collect::<Vec<_>>(),
            vec![
                first.causation_key,
                second.causation_key,
                third.causation_key
            ]
        );
        group.shutdown().await.unwrap();
        assert!(writer.propose(commands()[0].clone()).await.is_err());
        drop(writer);
        drop(other);
        drop(group);

        // Current binaries must replay historical batches even after operators
        // disable the production of new batches.
        let mut recovery_config = config(mode);
        recovery_config.proposals.max_batch_commands = 1;
        let recovered = boot(directory.path(), recovery_config).await;
        let writer = recovered.writer();
        let [first, second, third] = commands();
        let fingerprints = [
            first.fingerprint(),
            second.fingerprint(),
            third.fingerprint(),
        ];
        let (a, b, c) = tokio::join!(
            writer.propose(first),
            writer.propose(second),
            writer.propose(third)
        );
        for (outcome, fingerprint) in [(a, 0), (b, 1), (c, 2)] {
            assert_eq!(
                outcome.unwrap(),
                ProposeOutcome::Replayed {
                    first_log_index: before + 1,
                    fingerprint: fingerprints[fingerprint].clone(),
                }
            );
        }
        assert_eq!(
            recovered
                .state_machine
                .committed_events(&organization())
                .await
                .len(),
            3
        );
        recovered.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn count_and_byte_limits_split_batches_without_losing_commands() {
    for byte_limited in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut config = config(StatePersistence::Snapshot);
        let commands = commands();
        let largest = commands
            .iter()
            .map(|command| serde_json::to_vec(command).unwrap().len())
            .max()
            .unwrap();
        if byte_limited {
            config.proposals.max_batch_bytes = largest;
        } else {
            config.proposals.max_batch_commands = 2;
        }
        config.proposals.queue_capacity = 1;
        let group = boot(directory.path(), config).await;
        let before = group
            .raft()
            .metrics()
            .borrow_watched()
            .last_log_index
            .unwrap();
        let writer = group.writer();
        let [first, second, third] = commands;
        let (a, b, c) = tokio::join!(
            writer.propose(first),
            writer.propose(second),
            writer.propose(third)
        );
        assert!(a.is_ok() && b.is_ok() && c.is_ok());
        let after = group
            .raft()
            .metrics()
            .borrow_watched()
            .last_log_index
            .unwrap();
        assert_eq!(after - before, if byte_limited { 3 } else { 2 });
        assert_eq!(
            group
                .state_machine
                .committed_events(&organization())
                .await
                .len(),
            3
        );
        group.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn a_lone_command_finishes_and_oversized_commands_never_enter_raft() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(StatePersistence::Snapshot);
    let command = commands()[0].clone();
    config.proposals.max_batch_bytes = serde_json::to_vec(&command).unwrap().len();
    let group = boot(directory.path(), config).await;
    let writer = group.writer();
    tokio::time::timeout(Duration::from_secs(5), writer.propose(command.clone()))
        .await
        .unwrap()
        .unwrap();
    let before = group.raft().metrics().borrow_watched().last_log_index;
    let mut oversized = command;
    oversized.payload.data.push_str(&"x".repeat(1000));
    assert!(
        writer
            .propose(oversized)
            .await
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );
    assert_eq!(
        group.raft().metrics().borrow_watched().last_log_index,
        before
    );
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_domain_rejection_is_returned_only_to_its_own_batched_caller() {
    let directory = tempfile::tempdir().unwrap();
    let group = boot(directory.path(), config(StatePersistence::Checkpoint)).await;
    let writer = group.writer();
    let [first, second, _] = commands();
    let mut rejected = first.clone();
    rejected.command_type = "task.create".to_owned();
    let (a, b, c) = tokio::join!(
        writer.propose(first),
        writer.propose(rejected),
        writer.propose(second)
    );
    assert_eq!(a.unwrap(), c.unwrap());
    assert!(matches!(
        b.unwrap_err().downcast_ref::<ProposeError>(),
        Some(ProposeError::Rejected { .. })
    ));
    assert_eq!(
        group
            .state_machine
            .committed_events(&organization())
            .await
            .len(),
        2
    );
    group.shutdown().await.unwrap();
}

#[test]
fn proposal_configuration_defaults_to_legacy_and_rejects_invalid_bounds() {
    let defaults: GroupConfig = serde_json::from_str("{}").unwrap();
    assert_eq!(defaults.proposals.max_batch_commands, 1);
    for setting in ["max_batch_commands", "max_batch_bytes", "queue_capacity"] {
        let config: GroupConfig =
            serde_json::from_str(&format!("{{\"proposals\":{{\"{setting}\":0}}}}")).unwrap();
        assert!(config.validate().is_err());
    }
    let mut config = GroupConfig::default();
    config.proposals.max_batch_commands = 8;
    config.proposals.max_batch_bytes = config.transport.max_message_bytes;
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn shutdown_interrupts_queued_proposals_and_releases_the_worker() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(StatePersistence::Snapshot);
    config.proposals.max_delay_ms = 10_000;
    let group = boot(directory.path(), config).await;
    let writer = group.writer();
    let submitted = writer.clone();
    let pending = tokio::spawn(async move { submitted.propose(commands()[0].clone()).await });
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    group.shutdown().await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProposeError>(),
        Some(ProposeError::Unknown(_))
    ));
    assert!(writer.propose(commands()[0].clone()).await.is_err());
}

#[tokio::test]
async fn a_rejected_batch_preserves_consensus_error_classification_for_each_caller() {
    let raft = openraft::Raft::new(
        1,
        std::sync::Arc::new(openraft::Config::default().validate().unwrap()),
        NoopNetworkFactory,
        MemLogStore::default(),
        std::sync::Arc::new(MemStateMachine::default()),
    )
    .await
    .unwrap();
    let config = crate::config::ProposalConfig {
        max_batch_commands: 2,
        max_delay_ms: 20,
        ..crate::config::ProposalConfig::default()
    };
    let writer = ProposalWriter::new(raft.clone(), config);
    let [first, second, _] = commands();
    let (a, b) = tokio::join!(writer.propose(first), writer.propose(second));
    for error in [a.unwrap_err(), b.unwrap_err()] {
        assert!(matches!(
            error.downcast_ref::<ProposeError>(),
            Some(ProposeError::ForwardToLeader { .. })
        ));
    }
    writer.stop().await;
    raft.shutdown().await.unwrap();
}
