// SPDX-License-Identifier: MPL-2.0

//! The in-memory Raft state machine — where the pure core runs.
//!
//! Every committed command is decoded and dispatched through
//! [`AggregatePlan::process`](loomery_core::aggregate::process), the resulting
//! events are folded into per-aggregate state
//! ([`AggregateState`]) and appended to the group's applied-event log, and the
//! command's `causation_key` is recorded in the dedup window — **after** the
//! events are applied, per the core's critical-section contract.
//!
//! Three things the genesis worker depends on, all satisfied here:
//!
//! 1. the pure core does the deciding (`process`/`apply`), so a replayed
//!    command is deterministic;
//! 2. a dedup hit answers [`Applied::Replayed`], never a second set of events;
//! 3. [`MemStateMachine::committed_events`] answers from *applied* state.
//!
//! The whole state (including the dedup window) is serialized into snapshots,
//! so installing a snapshot restores the same dedup window a fresh log replay
//! would rebuild.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use tokio::sync::watch;

use loomery_core::aggregate::AggregatePlan;
use loomery_core::aggregate::Processed;
use loomery_core::aggregate::Processed::Executed;
use loomery_core::aggregate::Processed::Replayed;
use loomery_core::aggregate::process;
use loomery_core::dedup::Registry;
use loomery_core::envelope::Command;
use loomery_core::envelope::Event;
use loomery_core::id::Id;
use loomery_core::invitation;
use loomery_core::invitation::Invitation;
use loomery_core::invitation::InvitationCode;
use loomery_core::invitation::InvitationState;
use loomery_core::key::Key;
use loomery_core::membership;
use loomery_core::membership::ADD_OWNER;
use loomery_core::membership::AssignmentCode;
use loomery_core::membership::MembershipCode;
use loomery_core::membership::OrganizationAssignment;
use loomery_core::membership::OrganizationAssignmentState;
use loomery_core::membership::Role;
use loomery_core::membership::WorkspaceMembership;
use loomery_core::membership::WorkspaceMembershipState;
use loomery_core::org;
use loomery_core::org::ASSIGN_LEADER;
use loomery_core::org::Organization;
use loomery_core::org::OrganizationCode;
use loomery_core::org::OrganizationState;
use loomery_core::task;
use loomery_core::task::Task;
use loomery_core::task::TaskCode;
use loomery_core::task::TaskState;
use loomery_core::tenant;
use loomery_core::tenant::Tenant;
use loomery_core::tenant::TenantCode;
use loomery_core::tenant::TenantState;
use loomery_core::user;
use loomery_core::user::User;
use loomery_core::user::UserCode;
use loomery_core::user::UserState;
use loomery_core::workspace;
use loomery_core::workspace::CREATE as CREATE_WORKSPACE;
use loomery_core::workspace::Workspace;
use loomery_core::workspace::WorkspaceCode;
use loomery_core::workspace::WorkspaceState;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use super::AppData;
use super::Applied;
use super::TypeConfig;

/// How many processed causation keys the dedup window keeps.
///
/// Bounded on purpose (the log is the durable answer, the window is a cache);
/// the value is also the size a snapshot's window is rebuilt with.
pub const DEDUP_WINDOW: usize = 4096;

/// The state machine of one group: applied aggregate state plus the group's
/// applied-event log and dedup window.
#[derive(Debug)]
pub struct MemStateMachine {
    /// The folded state, behind an async lock (`OpenRaft` applies from its task,
    /// the [`crate::raft::RaftGroup`] port reads from another).
    state: RwLock<GroupState>,
    disk: Option<super::disk::Disk>,
    persistence: crate::config::StatePersistence,
    /// Identifier counter for snapshots (snapshot ids need only be unique).
    snapshot_idx: AtomicU64,
    /// The last snapshot this replica built or received.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    /// Notifies the host's workers that the applied log moved.
    ///
    /// A `watch` rather than a broadcast because the value *is* the state a
    /// worker needs (the latest applied index): a worker that wakes late still
    /// sees the current index and catches up from its own cursor.
    applied: watch::Sender<u64>,
}

impl Default for MemStateMachine {
    fn default() -> Self {
        Self {
            state: RwLock::new(GroupState::default()),
            disk: None,
            persistence: crate::config::StatePersistence::Checkpoint,
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            applied: watch::channel(0).0,
        }
    }
}

/// Everything the group's state machine folds.
#[derive(Debug)]
struct GroupState {
    /// The last log id applied.
    last_applied_log: Option<LogId<u64>>,
    /// The last membership applied.
    last_membership: StoredMembership<u64, BasicNode>,
    /// One entry per aggregate stream, keyed by `aggregate_id`.
    streams: BTreeMap<Id, AggregateState>,
    /// Every event the group has applied, in log order — what
    /// [`MemStateMachine::committed_events`] answers from.
    applied: Vec<AppliedEvent>,
    /// The dedup window (not serializable; see [`GroupState::dedup`]).
    registry: Registry,
    /// The dedup window in insertion order, mirrored for snapshots.
    dedup: Vec<(Key, Key, usize)>,
    /// Who belongs to which organization, and with which workspace roles.
    ///
    /// Derived from the applied log — never serialized, rebuilt from it after a
    /// snapshot install — so the gateway's authorization queries are a map lookup
    /// instead of a scan over every aggregate stream (most of which are tasks).
    members: Members,
}

/// The membership index: one record per `(organization, user)`.
type Members = BTreeMap<(Id, Id), MemberRecord>;

/// What one `(organization, user)` pair is: assigned to the organization, and
/// the roles held in its workspaces.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct MemberRecord {
    /// Whether an `organization.member_assigned` stands (removal clears it).
    assigned: bool,
    /// The role held in each workspace of the organization.
    roles: BTreeMap<Id, Role>,
}

impl MemberRecord {
    /// Whether the user belongs to the organization at all.
    fn is_member(&self) -> bool {
        self.assigned || !self.roles.is_empty()
    }

    /// Whether the user owns any workspace of the organization.
    fn is_owner(&self) -> bool {
        self.roles.values().any(|role| *role == Role::Owner)
    }
}

impl GroupState {
    /// Appends an applied event to the group's log **and** to the membership
    /// index.
    fn append(&mut self, log_index: u64, event: Event) {
        note_membership(&mut self.members, &event);
        self.applied.push(AppliedEvent { log_index, event });
    }

    /// Rebuilds the membership index from the applied log.
    ///
    /// A snapshot carries the log but not the index (it is derived), so a
    /// snapshot install rebuilds it here rather than trusting a serialized copy.
    fn rebuild_members(&mut self) {
        let mut members = Members::new();
        for entry in &self.applied {
            note_membership(&mut members, &entry.event);
        }
        self.members = members;
    }
}

/// Folds one event into the membership index.
///
/// These are the events the membership plans produce; one that does not decode
/// is skipped, exactly as `apply` skips it.
fn note_membership(members: &mut Members, event: &Event) {
    let organization_id = event.organization_id.clone();
    let workspace_id = event.workspace_id.clone();

    match event.event_type.as_str() {
        membership::MEMBER_ASSIGNED => {
            if let Ok(assigned) =
                serde_json::from_str::<membership::MemberAssigned>(&event.payload.data)
            {
                update(members, &organization_id, &assigned.user_id, |record| {
                    record.assigned = true;
                });
            }
        }
        membership::ORG_MEMBER_REMOVED => {
            if let Ok(removed) =
                serde_json::from_str::<membership::OrgMemberRemoved>(&event.payload.data)
            {
                update(members, &organization_id, &removed.user_id, |record| {
                    record.assigned = false;
                });
            }
        }
        membership::OWNER_ADDED => {
            if let (Some(workspace_id), Ok(added)) = (
                workspace_id,
                serde_json::from_str::<membership::OwnerAdded>(&event.payload.data),
            ) {
                update(members, &organization_id, &added.user_id, |record| {
                    record.roles.insert(workspace_id, Role::Owner);
                });
            }
        }
        membership::MEMBER_ADDED => {
            if let (Some(workspace_id), Ok(added)) = (
                workspace_id,
                serde_json::from_str::<membership::MemberAdded>(&event.payload.data),
            ) {
                update(members, &organization_id, &added.user_id, |record| {
                    record.roles.insert(workspace_id, added.role);
                });
            }
        }
        membership::ROLE_CHANGED => {
            if let (Some(workspace_id), Ok(changed)) = (
                workspace_id,
                serde_json::from_str::<membership::RoleChanged>(&event.payload.data),
            ) {
                update(members, &organization_id, &changed.user_id, |record| {
                    record.roles.insert(workspace_id, changed.role);
                });
            }
        }
        membership::MEMBER_REMOVED => {
            if let (Some(workspace_id), Ok(removed)) = (
                workspace_id,
                serde_json::from_str::<membership::MemberRemoved>(&event.payload.data),
            ) {
                update(members, &organization_id, &removed.user_id, |record| {
                    record.roles.remove(&workspace_id);
                });
            }
        }
        _ => {}
    }
}

/// Applies `change` to one `(organization, user)` record, creating it on demand.
fn update(
    members: &mut Members,
    organization_id: &Id,
    user_id: &Id,
    change: impl FnOnce(&mut MemberRecord),
) {
    change(
        members
            .entry((organization_id.clone(), user_id.clone()))
            .or_default(),
    );
}

impl Default for GroupState {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            streams: BTreeMap::new(),
            applied: Vec::new(),
            registry: Registry::new(DEDUP_WINDOW),
            dedup: Vec::new(),
            members: Members::new(),
        }
    }
}

/// One applied event and the log index it landed at.
///
/// The outbox needs the index to derive `Nats-Msg-Id` and to keep a resume
/// cursor (D11); the genesis worker and other readers only want the event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedEvent {
    /// The Raft log index the event's command was applied at (shared by all
    /// events one command produced).
    pub log_index: u64,
    /// The applied event.
    pub event: Event,
}

/// The per-stream state of an aggregate, tagged by its plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum AggregateState {
    /// The organization aggregate (genesis ①).
    Organization(OrganizationState),
    /// The workspace aggregate (genesis ②).
    Workspace(WorkspaceState),
    /// The workspace-membership aggregate (genesis ③).
    Membership(WorkspaceMembershipState),
    /// The control-plane user aggregate.
    User(UserState),
    /// The organization-assignment aggregate (onboarding).
    Assignment(OrganizationAssignmentState),
    /// The task aggregate (the workspace board).
    Task(TaskState),
    /// The control-plane tenant-placement aggregate.
    Tenant(TenantState),
    /// The invitation aggregate (email onboarding).
    Invitation(InvitationState),
}

/// The serializable projection of [`GroupState`] used as snapshot data.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotData {
    /// Frozen snapshot format version; absent on legacy in-memory snapshots.
    #[serde(default = "snapshot_version")]
    version: u32,
    /// The last applied log id at snapshot time.
    last_applied_log: Option<LogId<u64>>,
    /// The last applied membership at snapshot time.
    last_membership: StoredMembership<u64, BasicNode>,
    /// The per-stream states.
    streams: BTreeMap<Id, AggregateState>,
    /// The applied-event log.
    applied: Vec<AppliedEvent>,
    /// The dedup window, in insertion order.
    dedup: Vec<(Key, Key, usize)>,
}

fn snapshot_version() -> u32 {
    1
}

/// A snapshot as stored by the state machine.
#[derive(Debug, Serialize, Deserialize)]
struct StoredSnapshot {
    /// The snapshot's metadata.
    meta: SnapshotMeta<u64, BasicNode>,
    /// The serialized [`SnapshotData`].
    data: Vec<u8>,
}

impl MemStateMachine {
    pub(crate) async fn open(
        disk: super::disk::Disk,
        persistence: crate::config::StatePersistence,
    ) -> anyhow::Result<Arc<Self>> {
        let saved = disk.get(b"state_persistence").await?;
        if let Some(bytes) = saved {
            let previous: crate::config::StatePersistence = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                previous == persistence,
                "state persistence mode cannot change on an existing database: stored {previous:?}, requested {persistence:?}"
            );
        } else {
            // Databases predating this marker used checkpoint recovery.
            anyhow::ensure!(
                persistence == crate::config::StatePersistence::Checkpoint
                    || disk
                        .run(|db| {
                            Ok(db
                                .iterator(rocksdb::IteratorMode::Start)
                                .next()
                                .transpose()?
                                .is_none())
                        })
                        .await?,
                "existing unmarked database requires checkpoint mode"
            );
            disk.put(b"state_persistence", serde_json::to_vec(&persistence)?)
                .await?;
        }
        let mut machine = Arc::new(Self::default());
        let recovery_key: &'static [u8] = match persistence {
            crate::config::StatePersistence::Checkpoint => b"state",
            crate::config::StatePersistence::Snapshot => b"snapshot",
        };
        if let Some(bytes) = disk.get(recovery_key).await? {
            let stored: StoredSnapshot = serde_json::from_slice(&bytes)?;
            machine
                .clone()
                .install_snapshot(&stored.meta, Box::new(Cursor::new(stored.data)))
                .await?;
        }
        *machine.current_snapshot.write().await = disk
            .get(b"snapshot")
            .await?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()?;
        let recovered = Arc::get_mut(&mut machine)
            .ok_or_else(|| anyhow::anyhow!("state machine unexpectedly shared during recovery"))?;
        recovered.disk = Some(disk);
        recovered.persistence = persistence;
        Ok(machine)
    }
    #[allow(clippy::result_large_err)] // OpenRaft fixes the storage error type.
    async fn persist(
        &self,
        key: &'static [u8],
        stored: &StoredSnapshot,
    ) -> Result<(), StorageError<u64>> {
        if let Some(disk) = &self.disk {
            let bytes =
                serde_json::to_vec(stored).map_err(|e| StorageIOError::write_state_machine(&e))?;
            disk.put(key, bytes).await.map_err(|e| {
                StorageIOError::write_state_machine(&std::io::Error::other(e.to_string()))
            })?;
        }
        Ok(())
    }

    /// Every event the group has applied, filtered to `organization_id`, in log
    /// order.
    ///
    /// One Raft group owns one organization, so in practice this is the group's
    /// whole applied-event log; the filter keeps the answer honest if the group
    /// is ever shared.
    pub async fn committed_events(&self, organization_id: &Id) -> Vec<Event> {
        self.applied_events(organization_id)
            .await
            .into_iter()
            .map(|applied| applied.event)
            .collect()
    }

    /// Every event the group has applied, in log order, **with its log index**.
    ///
    /// This is what the outbox tailer consumes: the index is part of the
    /// message identity (`<group>:<log_index>:e<pos>`, D11) and of the cursor
    /// that makes a restart resume instead of re-publishing.
    pub async fn applied_events(&self, organization_id: &Id) -> Vec<AppliedEvent> {
        self.state
            .read()
            .await
            .applied
            .iter()
            .filter(|applied| &applied.event.organization_id == organization_id)
            .cloned()
            .collect()
    }

    /// The events of one workspace, in log order.
    ///
    /// A workspace's own events (`workspace.created`, `renamed`, `archived`) carry
    /// the workspace as their *aggregate*, while the work inside it (tasks,
    /// memberships) carries it as their `workspace_id`; a read scoped to the
    /// workspace sees both, and nothing from another workspace.
    pub async fn workspace_events(&self, organization_id: &Id, workspace_id: &Id) -> Vec<Event> {
        self.state
            .read()
            .await
            .applied
            .iter()
            .filter(|applied| &applied.event.organization_id == organization_id)
            .filter(|applied| {
                applied.event.workspace_id.as_ref() == Some(workspace_id)
                    || &applied.event.aggregate_id == workspace_id
            })
            .map(|applied| applied.event.clone())
            .collect()
    }

    /// Whether `user_id` belongs to `organization_id`.
    ///
    /// Membership has two sources: the organization assignment the invitation saga
    /// writes, and a role in one of the organization's workspaces — genesis ③
    /// gives the owner theirs, `membership.add_member` gives later members theirs.
    /// Both are folded into the index as events are applied.
    pub async fn is_organization_member(&self, organization_id: &Id, user_id: &Id) -> bool {
        self.state
            .read()
            .await
            .members
            .get(&(organization_id.clone(), user_id.clone()))
            .is_some_and(MemberRecord::is_member)
    }

    /// Whether `user_id` owns any workspace of `organization_id`.
    ///
    /// The organization has no role of its own: roles live on workspace
    /// memberships, so "may administer this organization" means owning at least
    /// one of its workspaces. The admin claim is a separate, global bypass that
    /// the gateway applies.
    pub async fn is_organization_owner(&self, organization_id: &Id, user_id: &Id) -> bool {
        self.state
            .read()
            .await
            .members
            .get(&(organization_id.clone(), user_id.clone()))
            .is_some_and(MemberRecord::is_owner)
    }

    /// The role `user_id` holds in `workspace_id`, if any.
    pub async fn workspace_role(
        &self,
        organization_id: &Id,
        workspace_id: &Id,
        user_id: &Id,
    ) -> Option<Role> {
        self.state
            .read()
            .await
            .members
            .get(&(organization_id.clone(), user_id.clone()))
            .and_then(|record| record.roles.get(workspace_id).copied())
    }

    /// The control group's tenant records, keyed by organization id, for the
    /// router projection.
    ///
    /// The router maps `organization_id → group` from these; an unregistered
    /// organization simply does not appear.
    pub async fn tenants(&self) -> Vec<(Id, TenantState)> {
        self.state
            .read()
            .await
            .streams
            .iter()
            .filter_map(|(organization_id, state)| match state {
                AggregateState::Tenant(tenant) => Some((organization_id.clone(), tenant.clone())),
                _ => None,
            })
            .collect()
    }

    /// A receiver that changes whenever the applied log advances.
    ///
    /// The host's workers wait on this instead of polling. The value is the last
    /// applied log index, so a worker that wakes late still catches up (from its
    /// own cursor) rather than missing entries.
    #[must_use]
    pub fn applied_watch(&self) -> watch::Receiver<u64> {
        self.applied.subscribe()
    }

    /// Applied events at or after `log_index`, in log order.
    ///
    /// The outbox tailer resumes from its persisted cursor. Passing the cursor's
    /// `log_index` returns the boundary entry too, and the outbox skips the
    /// positions it has already published.
    pub async fn applied_events_since(
        &self,
        organization_id: &Id,
        log_index: u64,
    ) -> Vec<AppliedEvent> {
        self.state
            .read()
            .await
            .applied
            .iter()
            .filter(|applied| {
                &applied.event.organization_id == organization_id && applied.log_index >= log_index
            })
            .cloned()
            .collect()
    }
}

/// Applies one committed command, dispatching to the aggregate plan its
/// `command_type` names.
fn apply_command(group: &mut GroupState, command: Command, log_index: u64) -> Applied {
    match command.command_type.as_str() {
        ASSIGN_LEADER | org::RENAME | org::ARCHIVE => apply_organization(group, command, log_index),
        CREATE_WORKSPACE | workspace::RENAME | workspace::ARCHIVE => {
            apply_workspace(group, command, log_index)
        }
        ADD_OWNER
        | membership::ADD_MEMBER
        | membership::CHANGE_ROLE
        | membership::REMOVE_MEMBER => apply_membership(group, command, log_index),
        user::PROVISION | user::UPDATE_PROFILE | user::DEACTIVATE => {
            apply_user(group, command, log_index)
        }
        membership::ASSIGN_MEMBER | membership::ORG_REMOVE_MEMBER => {
            apply_assignment(group, command, log_index)
        }
        task::CREATE | task::RENAME | task::COMPLETE | task::REOPEN => {
            apply_task(group, command, log_index)
        }
        tenant::REGISTER | tenant::ACTIVATE | tenant::TOMBSTONE => {
            apply_tenant(group, command, log_index)
        }
        invitation::CREATE | invitation::ACCEPT | invitation::EXPIRE => {
            apply_invitation(group, command, log_index)
        }
        other => Applied::Rejected {
            code: "unknown_command".to_owned(),
            message: format!("no aggregate plan handles `{other}`"),
        },
    }
}

/// Drives the organization plan for one command.
fn apply_organization(group: &mut GroupState, command: Command, log_index: u64) -> Applied {
    let aggregate_id = command.aggregate_id.clone();
    let mut state = match group.streams.get(&aggregate_id) {
        Some(AggregateState::Organization(state)) => state.clone(),
        Some(_) => return kind_mismatch(&aggregate_id),
        None => OrganizationState::default(),
    };

    let key = command.causation_key.clone();
    let fingerprint = command.fingerprint();

    match process::<OrganizationState, OrganizationCode, Organization>(
        state.clone(),
        &group.registry,
        command,
    ) {
        Executed(execution) => {
            for event in execution.events {
                state = Organization::apply(state, event.clone());
                group.append(log_index, event);
            }
            group
                .streams
                .insert(aggregate_id, AggregateState::Organization(state));
            record(group, key, fingerprint, log_index);
            Applied::Appended {
                first_log_index: log_index,
            }
        }
        Replayed { index } => Applied::Replayed {
            first_log_index: to_u64(index),
            fingerprint: recorded_fingerprint(group, &key, fingerprint.clone()),
        },
        loomery_core::aggregate::Processed::Error(error) => rejected(error.code, &error.message),
    }
}

/// Drives the workspace plan for one command.
fn apply_workspace(group: &mut GroupState, command: Command, log_index: u64) -> Applied {
    let aggregate_id = command.aggregate_id.clone();
    let mut state = match group.streams.get(&aggregate_id) {
        Some(AggregateState::Workspace(state)) => state.clone(),
        Some(_) => return kind_mismatch(&aggregate_id),
        None => WorkspaceState::default(),
    };

    let key = command.causation_key.clone();
    let fingerprint = command.fingerprint();

    match process::<WorkspaceState, WorkspaceCode, Workspace>(
        state.clone(),
        &group.registry,
        command,
    ) {
        Executed(execution) => {
            for event in execution.events {
                state = Workspace::apply(state, event.clone());
                group.append(log_index, event);
            }
            group
                .streams
                .insert(aggregate_id, AggregateState::Workspace(state));
            record(group, key, fingerprint, log_index);
            Applied::Appended {
                first_log_index: log_index,
            }
        }
        Replayed { index } => Applied::Replayed {
            first_log_index: to_u64(index),
            fingerprint: recorded_fingerprint(group, &key, fingerprint.clone()),
        },
        loomery_core::aggregate::Processed::Error(error) => rejected(error.code, &error.message),
    }
}

/// Drives the membership plan for one command.
fn apply_membership(group: &mut GroupState, command: Command, log_index: u64) -> Applied {
    let aggregate_id = command.aggregate_id.clone();
    let mut state = match group.streams.get(&aggregate_id) {
        Some(AggregateState::Membership(state)) => state.clone(),
        Some(_) => return kind_mismatch(&aggregate_id),
        None => WorkspaceMembershipState::default(),
    };

    let key = command.causation_key.clone();
    let fingerprint = command.fingerprint();

    match process::<WorkspaceMembershipState, MembershipCode, WorkspaceMembership>(
        state.clone(),
        &group.registry,
        command,
    ) {
        Executed(execution) => {
            for event in execution.events {
                state = WorkspaceMembership::apply(state, event.clone());
                group.append(log_index, event);
            }
            group
                .streams
                .insert(aggregate_id, AggregateState::Membership(state));
            record(group, key, fingerprint, log_index);
            Applied::Appended {
                first_log_index: log_index,
            }
        }
        Replayed { index } => Applied::Replayed {
            first_log_index: to_u64(index),
            fingerprint: recorded_fingerprint(group, &key, fingerprint.clone()),
        },
        loomery_core::aggregate::Processed::Error(error) => rejected(error.code, &error.message),
    }
}

/// Drives one aggregate plan through the pure core and folds what it produced.
///
/// The plan decides (`process`), the events fold into the stream
/// (`Plan::apply`), and the dedup entry is recorded *after* the events are
/// applied — the core's critical-section contract.
macro_rules! drive_plan {
    ($name:ident, $variant:ident, $state:ty, $code:ty, $plan:ty) => {
        /// Applies one committed command through its aggregate plan.
        fn $name(group: &mut GroupState, command: Command, log_index: u64) -> Applied {
            let aggregate_id = command.aggregate_id.clone();
            let mut state = match group.streams.get(&aggregate_id) {
                Some(AggregateState::$variant(state)) => state.clone(),
                Some(_) => return kind_mismatch(&aggregate_id),
                None => <$state>::default(),
            };

            let key = command.causation_key.clone();
            let fingerprint = command.fingerprint();

            match process::<$state, $code, $plan>(state.clone(), &group.registry, command) {
                Executed(execution) => {
                    for event in execution.events {
                        state = <$plan>::apply(state, event.clone());
                        group.append(log_index, event);
                    }
                    group
                        .streams
                        .insert(aggregate_id, AggregateState::$variant(state));
                    record(group, key, fingerprint, log_index);
                    Applied::Appended {
                        first_log_index: log_index,
                    }
                }
                Replayed { index } => Applied::Replayed {
                    first_log_index: to_u64(index),
                    fingerprint: recorded_fingerprint(group, &key, fingerprint.clone()),
                },
                Processed::Error(error) => rejected(error.code, &error.message),
            }
        }
    };
}

drive_plan!(apply_user, User, UserState, UserCode, User);
drive_plan!(
    apply_assignment,
    Assignment,
    OrganizationAssignmentState,
    AssignmentCode,
    OrganizationAssignment
);
drive_plan!(apply_task, Task, TaskState, TaskCode, Task);
drive_plan!(apply_tenant, Tenant, TenantState, TenantCode, Tenant);
drive_plan!(
    apply_invitation,
    Invitation,
    InvitationState,
    InvitationCode,
    Invitation
);

/// The fingerprint recorded for `key`, or `fallback` when the registry has no
/// entry (a replay always has one; this only guards a raced eviction).
fn recorded_fingerprint(group: &GroupState, key: &Key, fallback: Key) -> Key {
    group
        .registry
        .lookup(key)
        .map_or(fallback, |entry| entry.fingerprint().clone())
}

/// Records a committed command in the dedup window and mirrors it for snapshots.
fn record(group: &mut GroupState, key: Key, fingerprint: Key, log_index: u64) {
    group.registry.insert(
        key,
        fingerprint,
        usize::try_from(log_index).unwrap_or(usize::MAX),
    );
    group.dedup = group.registry.window_entries();
}

/// A rejection carrying an aggregate's code enum.
fn rejected(code: impl std::fmt::Debug, message: &str) -> Applied {
    Applied::Rejected {
        code: format!("{code:?}"),
        message: message.to_owned(),
    }
}

/// A command whose `aggregate_id` already belongs to another aggregate kind.
fn kind_mismatch(aggregate_id: &Id) -> Applied {
    Applied::Rejected {
        code: "aggregate_kind_mismatch".to_owned(),
        message: format!("`{aggregate_id}` is already another aggregate kind"),
    }
}

/// Widens a dedup index for the wire response (usize -> u64 never truncates on
/// the platforms this spike targets; impossible values saturate).
fn to_u64(index: usize) -> u64 {
    u64::try_from(index).unwrap_or(u64::MAX)
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<MemStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let group = self.state.read().await;

        let data = SnapshotData {
            version: snapshot_version(),
            last_applied_log: group.last_applied_log,
            last_membership: group.last_membership.clone(),
            streams: group.streams.clone(),
            applied: group.applied.clone(),
            dedup: group.dedup.clone(),
        };
        let last_applied_log = data.last_applied_log;
        let last_membership = data.last_membership.clone();
        let bytes = tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
            .await
            .map_err(|error| {
                StorageIOError::read_state_machine(&std::io::Error::other(error.to_string()))
            })?
            .map_err(|error| StorageIOError::read_state_machine(&error))?;

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed);
        let snapshot_id = match last_applied_log {
            Some(last) => format!("{}-{}-{snapshot_idx}", last.leader_id, last.index),
            None => format!("--{snapshot_idx}"),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        // Take the snapshot lock *before* releasing the state lock, so a newer
        // build cannot install over this one (the reference store's ordering).
        let mut current = self.current_snapshot.write().await;
        drop(group);
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        self.persist(b"snapshot", &stored).await?;
        *current = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<MemStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let group = self.state.read().await;
        Ok((group.last_applied_log, group.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Applied>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        let mut group = self.state.write().await;

        for entry in entries {
            let log_index = entry.log_id.index;
            group.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => responses.push(Applied::Appended {
                    first_log_index: log_index,
                }),
                EntryPayload::Membership(membership) => {
                    group.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(Applied::Appended {
                        first_log_index: log_index,
                    });
                }
                EntryPayload::Normal(AppData::Command(command)) => {
                    responses.push(apply_command(&mut group, command, log_index));
                }
                EntryPayload::Normal(AppData::Batch(commands)) => {
                    responses.push(Applied::Batch(
                        commands
                            .into_iter()
                            .map(|command| apply_command(&mut group, command, log_index))
                            .collect(),
                    ));
                }
            }
        }

        // Wake the host's workers: the applied log moved (outbox tailer, sagas).
        // `send_replace` never blocks and always leaves the latest index behind.
        if let Some(last_applied) = group.last_applied_log {
            self.applied.send_replace(last_applied.index);
        }
        drop(group);
        if self.disk.is_some() && self.persistence == crate::config::StatePersistence::Checkpoint {
            let group = self.state.read().await;
            let data = SnapshotData {
                version: snapshot_version(),
                last_applied_log: group.last_applied_log,
                last_membership: group.last_membership.clone(),
                streams: group.streams.clone(),
                applied: group.applied.clone(),
                dedup: group.dedup.clone(),
            };
            let stored = StoredSnapshot {
                meta: SnapshotMeta {
                    last_log_id: data.last_applied_log,
                    last_membership: data.last_membership.clone(),
                    snapshot_id: "checkpoint".into(),
                },
                data: serde_json::to_vec(&data)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?,
            };
            self.persist(b"state", &stored).await?;
        }
        Ok(responses)
    }

    fn get_snapshot_builder(&mut self) -> impl Future<Output = Self::SnapshotBuilder> + Send {
        std::future::ready(Arc::clone(self))
    }
    fn begin_receiving_snapshot(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Box<Cursor<Vec<u8>>>, StorageError<u64>>> + Send
    {
        // Nothing to prepare: a fresh, empty buffer is the receiving handle.
        std::future::ready(Ok(Box::new(Cursor::new(Vec::new()))))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        let data: SnapshotData = serde_json::from_slice(&bytes)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;

        if data.version != snapshot_version() {
            return Err(StorageIOError::read_snapshot(
                Some(meta.signature()),
                &std::io::Error::other("unsupported snapshot format version"),
            )
            .into());
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        if let Some(disk) = &self.disk {
            let bytes =
                serde_json::to_vec(&stored).map_err(|e| StorageIOError::write_state_machine(&e))?;
            disk.run(move |db| {
                let mut batch = rocksdb::WriteBatch::default();
                batch.put(b"state", &bytes);
                batch.put(b"snapshot", &bytes);
                let mut options = rocksdb::WriteOptions::default();
                options.set_sync(true);
                db.write_opt(batch, &options)?;
                Ok(())
            })
            .await
            .map_err(|e| {
                StorageIOError::write_state_machine(&std::io::Error::other(e.to_string()))
            })?;
        }
        let mut group = self.state.write().await;
        group.last_applied_log = meta.last_log_id;
        group.last_membership = meta.last_membership.clone();
        group.streams = data.streams;
        group.applied = data.applied;

        // Rebuild the dedup window from its serialized insertion order.
        let mut registry = Registry::new(DEDUP_WINDOW);
        for (key, fingerprint, index) in &data.dedup {
            registry.insert(key.clone(), fingerprint.clone(), *index);
        }
        group.registry = registry;
        group.dedup = data.dedup;
        group.rebuild_members();
        if let Some(last_applied) = meta.last_log_id {
            self.applied.send_replace(last_applied.index);
        }
        drop(group);

        let mut current = self.current_snapshot.write().await;
        *current = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        match &*self.current_snapshot.read().await {
            Some(stored) => Ok(Some(Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{bootstrap_value, organization};
    use loomery_genesis::Step;

    /// Applies one command the way `OpenRaft` would, then reads the response.
    async fn apply_one(machine: &Arc<MemStateMachine>, command: Command) -> Applied {
        let entry = Entry {
            log_id: LogId::new(
                openraft::CommittedLeaderId::new(1, 1),
                command_index(machine).await,
            ),
            payload: EntryPayload::Normal(AppData::Command(command)),
        };
        machine.clone().apply([entry]).await.unwrap().remove(0)
    }

    /// The next index for a test entry — derived from what has been applied.
    async fn command_index(machine: &Arc<MemStateMachine>) -> u64 {
        let group = machine.state.read().await;
        group.dedup.len() as u64
    }

    #[tokio::test]
    async fn applying_a_command_appends_its_event() {
        let machine = Arc::new(MemStateMachine::default());
        let bootstrap = bootstrap_value();
        let command = bootstrap.command(Step::AssignLeader).unwrap();

        let outcome = apply_one(&machine, command.clone()).await;
        assert_eq!(outcome, Applied::Appended { first_log_index: 0 });

        let events = machine.committed_events(&organization()).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, Step::AssignLeader.event_type());
        assert_eq!(events[0].causation_key, command.causation_key);
    }

    #[tokio::test]
    async fn re_applying_the_same_command_replays() {
        let machine = Arc::new(MemStateMachine::default());
        let command = bootstrap_value().command(Step::AssignLeader).unwrap();

        apply_one(&machine, command.clone()).await;
        let outcome = apply_one(&machine, command).await;

        assert!(matches!(outcome, Applied::Replayed { .. }));
        assert_eq!(machine.committed_events(&organization()).await.len(), 1);
    }

    #[tokio::test]
    async fn a_batch_applies_in_order_with_independent_dedup_and_rejections() {
        let machine = Arc::new(MemStateMachine::default());
        let bootstrap = bootstrap_value();
        let first = bootstrap.command(Step::AssignLeader).unwrap();
        let mut rejected = first.clone();
        rejected.command_type = "task.create".to_owned();
        let second = bootstrap.command(Step::CreateWorkspace).unwrap();
        let commands = vec![first.clone(), first.clone(), rejected, second.clone()];
        let entry = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 42),
            payload: EntryPayload::Normal(AppData::Batch(commands)),
        };
        let results = machine.clone().apply([entry]).await.unwrap();
        let results = match results.into_iter().next() {
            Some(Applied::Batch(results)) => results,
            _ => Vec::new(),
        };
        assert_eq!(results.len(), 4);
        assert_eq!(
            results[0],
            Applied::Appended {
                first_log_index: 42
            }
        );
        assert_eq!(
            results[1],
            Applied::Replayed {
                first_log_index: 42,
                fingerprint: first.fingerprint(),
            }
        );
        assert!(matches!(results[2], Applied::Rejected { .. }));
        assert_eq!(
            results[3],
            Applied::Appended {
                first_log_index: 42
            }
        );
        let events = machine.committed_events(&organization()).await;
        assert_eq!(
            events
                .iter()
                .map(|event| event.causation_key.clone())
                .collect::<Vec<_>>(),
            vec![first.causation_key, second.causation_key]
        );
    }

    #[tokio::test]
    async fn an_unknown_command_is_rejected_without_events() {
        let machine = Arc::new(MemStateMachine::default());
        let mut command = bootstrap_value().command(Step::AssignLeader).unwrap();
        command.command_type = "task.create".to_owned();

        let outcome = apply_one(&machine, command).await;

        assert!(matches!(outcome, Applied::Rejected { .. }));
        assert!(machine.committed_events(&organization()).await.is_empty());
    }

    #[tokio::test]
    async fn a_snapshot_round_trips_state_and_dedup() {
        let machine = Arc::new(MemStateMachine::default());
        let command = bootstrap_value().command(Step::AssignLeader).unwrap();
        apply_one(&machine, command.clone()).await;

        let snapshot = machine.clone().build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id.map(|id| id.index), Some(0));

        // Fresh machine: installing the snapshot restores state and dedup.
        let restored = Arc::new(MemStateMachine::default());
        restored
            .clone()
            .install_snapshot(
                &snapshot.meta,
                Box::new(Cursor::new(snapshot.snapshot.into_inner())),
            )
            .await
            .unwrap();

        assert_eq!(restored.committed_events(&organization()).await.len(), 1);
        let replay = apply_one(&restored, command).await;
        assert!(matches!(replay, Applied::Replayed { .. }));
    }
}

#[cfg(test)]
mod membership_index_tests {
    use super::*;
    use crate::group::GroupOps;
    use crate::raft::RaftGroup;
    use crate::test_support::{assign_member, join_workspace};
    use loomery_core::Uuid;
    use loomery_core::envelope::Payload;
    use loomery_core::key::Key;
    use loomery_core::timestamp::Timestamp;

    /// The namespace these tests' derived keys use.
    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    /// The index is derived from applied events, so it must answer membership,
    /// roles and ownership without touching the aggregate streams.
    #[tokio::test]
    async fn the_membership_index_answers_roles_and_ownership() {
        let mut group = RaftGroup::boot_single_node(1).await.unwrap();
        let organization_id = Id::from("org-1");
        let workspace_id = Id::from("ws-1");
        let member = Id::from("user-1");
        let owner = Id::from("user-0");
        let state = group.state_machine();

        assert!(
            !state
                .is_organization_member(&organization_id, &member)
                .await,
            "nothing is assigned yet"
        );

        assign_member(&mut group, &organization_id, &member).await;
        join_workspace(
            &mut group,
            &organization_id,
            &workspace_id,
            &member,
            Role::Viewer,
        )
        .await;
        join_workspace(
            &mut group,
            &organization_id,
            &workspace_id,
            &owner,
            Role::Owner,
        )
        .await;

        assert!(
            state
                .is_organization_member(&organization_id, &member)
                .await
        );
        assert_eq!(
            state
                .workspace_role(&organization_id, &workspace_id, &member)
                .await,
            Some(Role::Viewer)
        );
        assert!(state.is_organization_owner(&organization_id, &owner).await);
        assert!(
            !state.is_organization_owner(&organization_id, &member).await,
            "a Viewer is not an owner"
        );
        assert_eq!(
            state
                .workspace_role(&organization_id, &Id::from("other-ws"), &member)
                .await,
            None
        );
        assert!(
            !state
                .is_organization_member(&organization_id, &Id::from("stranger"))
                .await
        );

        // A role change is folded in place...
        change_role(
            &mut group,
            &organization_id,
            &workspace_id,
            &member,
            Role::Member,
        )
        .await;
        assert_eq!(
            state
                .workspace_role(&organization_id, &workspace_id, &member)
                .await,
            Some(Role::Member)
        );

        // ...removing the workspace role leaves the organization assignment...
        leave_workspace(&mut group, &organization_id, &workspace_id, &member).await;
        assert_eq!(
            state
                .workspace_role(&organization_id, &workspace_id, &member)
                .await,
            None
        );
        assert!(
            state
                .is_organization_member(&organization_id, &member)
                .await,
            "the assignment still stands"
        );

        // ...and leaving the organization clears it.
        leave_organization(&mut group, &organization_id, &member).await;
        assert!(
            !state
                .is_organization_member(&organization_id, &member)
                .await
        );
        assert!(
            state.is_organization_owner(&organization_id, &owner).await,
            "the other Owner is untouched"
        );
    }

    /// Proposes a membership command straight into the group, as the saga does.
    async fn propose(
        group: &mut RaftGroup,
        organization_id: &Id,
        workspace_id: Option<&Id>,
        user_id: &Id,
        command_type: &str,
        payload: String,
    ) {
        let aggregate_id = match workspace_id {
            Some(workspace_id) => {
                membership::workspace_membership_id(organization_id, workspace_id, user_id)
            }
            None => membership::organization_assignment_id(organization_id, user_id),
        };
        let command = Command {
            envelope_version: 1,
            id: Id::new(),
            aggregate_id,
            organization_id: organization_id.clone(),
            workspace_id: workspace_id.cloned(),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&KEY_NS, &format!("{command_type}:{user_id}")),
            correlation_key: Key::new(&KEY_NS, "membership-index"),
            actor: loomery_core::actor::Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload,
            },
        };
        group.propose(command).await.unwrap();
    }

    async fn change_role(
        group: &mut RaftGroup,
        organization_id: &Id,
        workspace_id: &Id,
        user_id: &Id,
        role: Role,
    ) {
        propose(
            group,
            organization_id,
            Some(workspace_id),
            user_id,
            membership::CHANGE_ROLE,
            format!(r#"{{"user_id":"{user_id}","role":"{}"}}"#, role_name(role)),
        )
        .await;
    }

    async fn leave_workspace(
        group: &mut RaftGroup,
        organization_id: &Id,
        workspace_id: &Id,
        user_id: &Id,
    ) {
        propose(
            group,
            organization_id,
            Some(workspace_id),
            user_id,
            membership::REMOVE_MEMBER,
            format!(r#"{{"user_id":"{user_id}"}}"#),
        )
        .await;
    }

    async fn leave_organization(group: &mut RaftGroup, organization_id: &Id, user_id: &Id) {
        propose(
            group,
            organization_id,
            None,
            user_id,
            membership::ORG_REMOVE_MEMBER,
            format!(r#"{{"user_id":"{user_id}"}}"#),
        )
        .await;
    }

    fn role_name(role: Role) -> &'static str {
        match role {
            Role::Owner => "Owner",
            Role::Member => "Member",
            Role::Viewer => "Viewer",
        }
    }
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use loomery_core::actor::Actor;
    use loomery_core::envelope::Payload;
    use loomery_core::tenant::TenantStatus;
    use loomery_core::timestamp::Timestamp;
    use openraft::CommittedLeaderId;

    fn tenant_command(command_type: &str, payload: &str) -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-tenant"),
            aggregate_id: Id::from("org-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(
                &loomery_core::Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9),
                payload,
            ),
            correlation_key: Key::new(
                &loomery_core::Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9),
                "control-plane",
            ),
            actor: Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload.to_owned(),
            },
        }
    }

    async fn apply_one(machine: &Arc<MemStateMachine>, command: Command) -> Applied {
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(AppData::Command(command)),
        };
        machine.clone().apply([entry]).await.unwrap().remove(0)
    }

    #[tokio::test]
    async fn a_tenant_registration_is_applied_and_visible_to_the_router() {
        let machine = Arc::new(MemStateMachine::default());
        let register = tenant_command(
            tenant::REGISTER,
            r#"{"group_id":"tenant-1","replicas":[{"node_id":1,"address":"http://127.0.0.1:7001"}],"leader_user_id":"018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0"}"#,
        );

        let applied = apply_one(&machine, register).await;
        assert_eq!(applied, Applied::Appended { first_log_index: 1 });

        let tenants = machine.tenants().await;
        assert_eq!(tenants.len(), 1);
        assert_eq!(tenants[0].0, Id::from("org-1"));
        assert_eq!(tenants[0].1.group_id.as_deref(), Some("tenant-1"));
        assert_eq!(tenants[0].1.status, TenantStatus::Registering);
        assert_eq!(tenants[0].1.replicas.len(), 1);
    }

    #[tokio::test]
    async fn an_unknown_control_command_is_rejected() {
        let machine = Arc::new(MemStateMachine::default());
        let applied = apply_one(&machine, tenant_command("tenant.move", "{}")).await;

        assert!(matches!(applied, Applied::Rejected { .. }));
        assert!(machine.tenants().await.is_empty());
    }
}
