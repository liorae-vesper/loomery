// SPDX-License-Identifier: MPL-2.0

//! The Raft-backed [`GroupOps`] adapter.
//!
//! [`RaftGroup`] maps [`openraft::Raft::client_write`] onto
//! [`GroupOps::propose`]: the state machine's [`Applied`] response distinguishes
//! appended from replayed, and everything else (timeouts, leadership movement)
//! is an error that means *unknown outcome* — the caller re-reads
//! [`GroupOps::committed_events`] before proposing again.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use loomery_core::envelope::Command;
use loomery_core::envelope::Event;
use loomery_core::id::Id;
use openraft::BasicNode;
use openraft::Raft;
use openraft::error::ClientWriteError;
use openraft::error::RaftError;

use super::AppData;
use super::Applied;
use super::TypeConfig;
use super::log_store::MemLogStore;
use super::network::NoopNetworkFactory;
use super::state_machine::MemStateMachine;
use crate::group::GroupOps;
use crate::group::ProposeOutcome;

/// One group's handle: the Raft client plus the state machine its events live
/// in.
///
/// [`GroupOps::propose`] goes through consensus; [`GroupOps::committed_events`]
/// reads the state machine's *applied* state directly, so a caller that just
/// proposed and is asking "did it commit?" gets the applied answer, not the log
/// tail's.
pub struct RaftGroup {
    raft: Raft<TypeConfig>,
    pub(super) state_machine: Arc<MemStateMachine>,
}

impl RaftGroup {
    /// Wraps an already-booted Raft handle and its state machine.
    #[must_use]
    pub fn new(raft: Raft<TypeConfig>, state_machine: Arc<MemStateMachine>) -> Self {
        Self {
            raft,
            state_machine,
        }
    }

    /// Boots a fresh, single-node, in-memory group and initializes it.
    ///
    /// This is spike level 1: no network, no persistence, one voter.
    ///
    /// # Errors
    ///
    /// Whatever `OpenRaft` reports while building the node or initializing the
    /// one-node cluster.
    pub async fn boot_single_node(node_id: u64) -> anyhow::Result<Self> {
        Self::boot(
            node_id,
            MemLogStore::default(),
            Arc::new(MemStateMachine::default()),
        )
        .await
    }

    /// Boots a group over an existing log store and state machine, initializing
    /// the one-node cluster only if the log is still pristine.
    ///
    /// Passing the *same* log store with a *fresh* state machine is the spike's
    /// restart test: `OpenRaft` re-applies the committed log into the new state
    /// machine, so the group's applied events survive a process restart.
    ///
    /// # Errors
    ///
    /// Whatever `OpenRaft` reports while building the node or initializing the
    /// one-node cluster.
    pub async fn boot(
        node_id: u64,
        log_store: MemLogStore,
        state_machine: Arc<MemStateMachine>,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(openraft::Config::default().validate()?);

        let raft = Raft::new(
            node_id,
            config,
            NoopNetworkFactory,
            log_store,
            Arc::clone(&state_machine),
        )
        .await?;

        if !raft.is_initialized().await? {
            raft.initialize(BTreeMap::from([(node_id, BasicNode::default())]))
                .await?;
        }

        Ok(Self::new(raft, state_machine))
    }

    /// Boots a persistent networked replica without initializing membership.
    /// Call `initialize` only on the designated bootstrap node of a new cluster.
    /// # Errors
    /// Returns configuration, database, recovery or consensus startup errors.
    pub async fn boot_persistent(
        node_id: u64,
        group_id: String,
        path: &std::path::Path,
        config: crate::config::GroupConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        super::tls::preflight(&config.transport).await?;
        anyhow::ensure!(!group_id.is_empty(), "group id must not be empty");
        let disk = super::disk::Disk::open(path, &config.storage).await?;
        let log = super::RocksLogStore::open(disk.clone());
        let machine = MemStateMachine::open(disk).await?;
        let network = super::transport::TonicNetworkFactory {
            group_id,
            config: config.transport,
        };
        let raft = Raft::new(
            node_id,
            Arc::new(config.raft.validate()?),
            network,
            log,
            machine.clone(),
        )
        .await?;
        Ok(Self::new(raft, machine))
    }

    /// Consensus handle for transport registration, metrics, initialization,
    /// learner admission and membership changes.
    #[must_use]
    pub fn raft(&self) -> Raft<TypeConfig> {
        self.raft.clone()
    }

    /// Stops the group's `OpenRaft` task.
    ///
    /// # Errors
    ///
    /// The runtime's join error, if the Raft task could not be awaited.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.raft.shutdown().await?;
        Ok(())
    }
}

impl GroupOps for RaftGroup {
    fn committed_events(
        &self,
        organization_id: &Id,
    ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send {
        let state_machine = Arc::clone(&self.state_machine);
        let organization_id = organization_id.clone();

        async move { Ok(state_machine.committed_events(&organization_id).await) }
    }

    fn propose(
        &mut self,
        command: Command,
    ) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send {
        let raft = self.raft.clone();

        async move {
            // `client_write` (not `client_write_ff`): genesis must know ①
            // committed before proposing ②.
            let response = raft
                .client_write(AppData::Command(command))
                .await
                .map_err(classify)?;

            Ok(match response.data {
                Applied::Appended { first_log_index } => {
                    ProposeOutcome::Appended { first_log_index }
                }
                Applied::Replayed { first_log_index } => {
                    ProposeOutcome::Replayed { first_log_index }
                }
                Applied::Rejected { code, message } => {
                    return Err(anyhow::Error::new(ProposeError::Rejected { code, message }));
                }
            })
        }
    }
}

/// Why a proposal could not be completed.
///
/// Every variant except [`ProposeError::Rejected`] means the outcome is
/// **unknown**: re-read the group's committed events and retry.
#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    /// Another node holds leadership; the shell can route there and retry.
    #[error("the group is not on this node; the leader is {leader:?}")]
    ForwardToLeader {
        /// The leader's node id, when known.
        leader: Option<u64>,
    },
    /// A membership change failed — blind retry is unsafe.
    #[error("a membership change failed")]
    Fatal(#[source] anyhow::Error),
    /// The command committed but the state machine refused it.
    #[error("the state machine rejected the command: {message} ({code})")]
    Rejected {
        /// Machine-readable discriminator from the aggregate's code enum.
        code: String,
        /// Human-readable reason.
        message: String,
    },
    /// A transport or consensus failure with an unknown outcome.
    #[error("the proposal failed; whether it committed is unknown — re-read the log")]
    Unknown(#[source] anyhow::Error),
}

/// Classifies a `client_write` failure for the caller's retry policy.
///
/// Backoff and routing belong *outside* the worker: this only says what the
/// failure was, and every non-membership variant is retryable in the sense that
/// re-reading the log first is always safe.
fn classify(error: RaftError<u64, ClientWriteError<u64, BasicNode>>) -> anyhow::Error {
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            anyhow::Error::new(ProposeError::ForwardToLeader {
                leader: forward.leader_id,
            })
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(cause)) => {
            anyhow::Error::new(ProposeError::Fatal(anyhow::Error::new(cause)))
        }
        other => anyhow::Error::new(ProposeError::Unknown(anyhow::Error::new(other))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap;
    use crate::test_support::bootstrap_value;
    use crate::test_support::organization;
    use loomery_genesis::Step;
    use loomery_genesis::bootstrap_actor;
    use loomery_genesis::bootstrap_correlation_key;
    use loomery_genesis::default_workspace_id;
    use loomery_genesis::step_key;

    /// How many applied events carry `step`'s causation key.
    fn committed(events: &[Event], organization_id: &Id, step: Step) -> usize {
        let key = step_key(organization_id, step);
        events
            .iter()
            .filter(|event| event.causation_key == key)
            .count()
    }

    /// The events the script produced, in order.
    fn genesis_events(events: &[Event], organization_id: &Id) -> Vec<Event> {
        events
            .iter()
            .filter(|event| {
                Step::ALL
                    .iter()
                    .any(|step| event.causation_key == step_key(organization_id, *step))
            })
            .cloned()
            .collect()
    }

    /// A counting wrapper, so a test can assert on *proposals* rather than only
    /// on the events they produced.
    struct CountingGroup {
        inner: RaftGroup,
        proposals: usize,
    }

    impl GroupOps for CountingGroup {
        fn committed_events(
            &self,
            organization_id: &Id,
        ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send {
            self.inner.committed_events(organization_id)
        }

        fn propose(
            &mut self,
            command: Command,
        ) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send {
            self.proposals += 1;
            self.inner.propose(command)
        }
    }

    #[tokio::test]
    async fn genesis_is_born_with_the_three_steps_in_order() {
        let mut group = RaftGroup::boot_single_node(1).await.unwrap();
        let organization_id = organization();
        let bootstrap = bootstrap_value();

        let outcome = bootstrap::run(&mut group, &bootstrap).await.unwrap();
        assert_eq!(outcome.appended, Step::ALL);
        assert!(outcome.progress.is_complete());

        let events = group.committed_events(&organization_id).await.unwrap();
        let genesis = genesis_events(&events, &organization_id);

        // 1. exactly three genesis events, in ①②③ order.
        assert_eq!(
            genesis
                .iter()
                .map(|event| event.causation_key.clone())
                .collect::<Vec<_>>(),
            Step::ALL
                .iter()
                .map(|step| step_key(&organization_id, *step))
                .collect::<Vec<_>>()
        );

        // 2. the workspace exists, with the derived id, and the creator is its
        //    Owner.
        assert!(genesis.iter().any(|event| {
            event.event_type == "workspace.created"
                && event.workspace_id.as_deref() == Some(&*default_workspace_id(&organization_id))
        }));
        assert!(genesis.iter().any(|event| {
            event.event_type == "membership.owner_added" && event.actor == bootstrap_actor()
        }));

        // 3. every genesis event carries the bootstrap actor and the
        //    organization's derived correlation key.
        for event in &genesis {
            assert_eq!(event.actor, bootstrap_actor());
            assert_eq!(
                event.correlation_key,
                bootstrap_correlation_key(&organization_id)
            );
        }
    }

    #[tokio::test]
    async fn a_crash_between_steps_resumes_without_a_second_genesis() {
        let mut group = RaftGroup::boot_single_node(1).await.unwrap();
        let organization_id = organization();
        let bootstrap = bootstrap_value();

        // A previous attempt committed ① and died before ②. (The first
        // client write lands after the initialization membership entry, so the
        // index is 2, not 1.)
        let first = group
            .propose(bootstrap.command(Step::AssignLeader).unwrap())
            .await
            .unwrap();
        assert_eq!(first, ProposeOutcome::Appended { first_log_index: 2 });

        // The resume proposes ② and ③ — never a second ①.
        let outcome = bootstrap::run(&mut group, &bootstrap).await.unwrap();
        assert_eq!(outcome.appended, [Step::CreateWorkspace, Step::AddOwner]);

        let events = group.committed_events(&organization_id).await.unwrap();
        for step in Step::ALL {
            assert_eq!(committed(&events, &organization_id, step), 1, "{step:?}");
        }
    }

    #[tokio::test]
    async fn re_proposing_a_committed_command_replays() {
        let mut group = RaftGroup::boot_single_node(1).await.unwrap();
        let bootstrap = bootstrap_value();
        let command = bootstrap.command(Step::AssignLeader).unwrap();

        let first = group.propose(command.clone()).await.unwrap();
        let second = group.propose(command).await.unwrap();

        assert!(matches!(first, ProposeOutcome::Appended { .. }));
        assert!(matches!(second, ProposeOutcome::Replayed { .. }));
        assert_eq!(
            committed(
                &group.committed_events(&organization()).await.unwrap(),
                &organization(),
                Step::AssignLeader
            ),
            1
        );
    }

    #[tokio::test]
    async fn a_restart_over_the_same_log_rebuilds_the_state() {
        let organization_id = organization();
        let bootstrap = bootstrap_value();
        let log_store = MemLogStore::default();

        // First run: a group that commits all three steps, then stops.
        let mut first = RaftGroup::boot(1, log_store.clone(), Arc::new(MemStateMachine::default()))
            .await
            .unwrap();
        bootstrap::run(&mut first, &bootstrap).await.unwrap();
        assert_eq!(
            first
                .committed_events(&organization_id)
                .await
                .unwrap()
                .len(),
            3
        );
        first.shutdown().await.unwrap();

        // Restart: the same log, a fresh state machine. OpenRaft re-applies the
        // committed entries, so the events are back.
        let mut restarted = RaftGroup::boot(1, log_store, Arc::new(MemStateMachine::default()))
            .await
            .unwrap();

        let events = restarted.committed_events(&organization_id).await.unwrap();
        assert_eq!(events.len(), 3);
        for step in Step::ALL {
            assert_eq!(committed(&events, &organization_id, step), 1, "{step:?}");
        }

        // And a resumed worker has nothing left to do.
        let outcome = bootstrap::run(&mut restarted, &bootstrap).await.unwrap();
        assert!(outcome.appended.is_empty());
    }

    #[tokio::test]
    async fn re_running_a_completed_group_proposes_nothing() {
        let mut group = RaftGroup::boot_single_node(1).await.unwrap();
        let bootstrap = bootstrap_value();
        bootstrap::run(&mut group, &bootstrap).await.unwrap();

        let mut counting = CountingGroup {
            inner: group,
            proposals: 0,
        };
        let outcome = bootstrap::run(&mut counting, &bootstrap).await.unwrap();

        assert_eq!(counting.proposals, 0, "a completed group appends nothing");
        assert!(outcome.appended.is_empty());
    }
}
