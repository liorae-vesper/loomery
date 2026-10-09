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
use std::io;
use std::io::Cursor;
use std::sync::Arc;

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
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use rocksdb::{WriteBatch, WriteOptions};
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

use super::AppData;
use super::Applied;
use super::TypeConfig;
use super::alias::EntryOf;
use super::alias::LogIdOf;
use super::alias::SnapshotMetaOf;
use super::alias::SnapshotOf;
use super::alias::StoredMembershipOf;
use super::disk::Family;

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
            current_snapshot: RwLock::new(None),
            applied: watch::channel(0).0,
        }
    }
}

/// Everything the group's state machine folds.
#[derive(Debug)]
struct GroupState {
    /// The last log id applied.
    last_applied_log: Option<LogIdOf>,
    /// The last membership applied.
    last_membership: StoredMembershipOf,
    /// One entry per aggregate stream, keyed by `aggregate_id`.
    streams: BTreeMap<Id, AggregateState>,
    /// Every event the group has applied, in log order — what
    /// [`MemStateMachine::committed_events`] answers from.
    applied: Vec<AppliedEvent>,
    /// The dedup window (not serializable; see [`GroupState::dedup`]).
    registry: Registry,
    /// Dedup entries this batch added, and keys it evicted. Cleared per apply.
    added_dedup: Vec<(Key, Key, usize)>,
    evicted_dedup: Vec<Key>,
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

    /// Forgets what the last batch touched, so the next one's deltas stand alone.
    fn begin_batch(&mut self) {
        self.added_dedup.clear();
        self.evicted_dedup.clear();
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
            let Ok(assigned) =
                serde_json::from_str::<membership::MemberAssigned>(&event.payload.data)
            else {
                return;
            };
            update(members, &organization_id, &assigned.user_id, |record| {
                record.assigned = true;
            });
        }
        membership::ORG_MEMBER_REMOVED => {
            let Ok(removed) =
                serde_json::from_str::<membership::OrgMemberRemoved>(&event.payload.data)
            else {
                return;
            };
            update(members, &organization_id, &removed.user_id, |record| {
                record.assigned = false;
            });
        }
        membership::OWNER_ADDED => {
            let Ok(added) = serde_json::from_str::<membership::OwnerAdded>(&event.payload.data)
            else {
                return;
            };
            let Some(workspace_id) = workspace_id else {
                return;
            };
            update(members, &organization_id, &added.user_id, |record| {
                record.roles.insert(workspace_id, Role::Owner);
            });
        }
        membership::MEMBER_ADDED => {
            let Ok(added) = serde_json::from_str::<membership::MemberAdded>(&event.payload.data)
            else {
                return;
            };
            let Some(workspace_id) = workspace_id else {
                return;
            };
            update(members, &organization_id, &added.user_id, |record| {
                record.roles.insert(workspace_id, added.role);
            });
        }
        membership::ROLE_CHANGED => {
            let Ok(changed) = serde_json::from_str::<membership::RoleChanged>(&event.payload.data)
            else {
                return;
            };
            let Some(workspace_id) = workspace_id else {
                return;
            };
            update(members, &organization_id, &changed.user_id, |record| {
                record.roles.insert(workspace_id, changed.role);
            });
        }
        membership::MEMBER_REMOVED => {
            let Ok(removed) =
                serde_json::from_str::<membership::MemberRemoved>(&event.payload.data)
            else {
                return;
            };
            let Some(workspace_id) = workspace_id else {
                return;
            };
            update(members, &organization_id, &removed.user_id, |record| {
                record.roles.remove(&workspace_id);
            });
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
            last_membership: StoredMembershipOf::default(),
            streams: BTreeMap::new(),
            applied: Vec::new(),
            registry: Registry::new(DEDUP_WINDOW),
            added_dedup: Vec::new(),
            evicted_dedup: Vec::new(),
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
    /// The organization aggregate (genesis 1).
    Organization(OrganizationState),
    /// The workspace aggregate (genesis ②).
    Workspace(WorkspaceState),
    /// The workspace-membership aggregate (genesis 3).
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
    last_applied_log: Option<LogIdOf>,
    /// The last applied membership at snapshot time.
    last_membership: StoredMembershipOf,
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
    meta: SnapshotMetaOf,
    /// The serialized [`SnapshotData`].
    data: Vec<u8>,
}

/// The state a checkpoint-mode database holds, read back key by key.
struct LoadedState {
    streams: BTreeMap<Id, AggregateState>,
    applied_log: Option<LogIdOf>,
    membership: Option<StoredMembershipOf>,
    /// The dedup window, ordered by the log index each entry was recorded at.
    dedup: Vec<(Key, Key, usize)>,
}

/// Reads the per-aggregate state a checkpoint-mode apply wrote.
fn load_state(db: &rocksdb::DB) -> anyhow::Result<LoadedState> {
    let family = Family::State.handle(db)?;
    let mut loaded = LoadedState {
        streams: BTreeMap::new(),
        applied_log: None,
        membership: None,
        dedup: Vec::new(),
    };
    for item in db.iterator_cf(family, rocksdb::IteratorMode::Start) {
        let (key, value) = item?;
        if key.starts_with(b"agg:") {
            let (aggregate_id, state): (Id, AggregateState) = serde_json::from_slice(&value)?;
            loaded.streams.insert(aggregate_id, state);
        } else if key.as_ref() == APPLIED_LOG_KEY {
            loaded.applied_log = serde_json::from_slice(&value)?;
        } else if key.as_ref() == MEMBERSHIP_KEY {
            loaded.membership = Some(serde_json::from_slice(&value)?);
        } else if key.starts_with(b"dedup:") {
            loaded.dedup.push(serde_json::from_slice(&value)?);
        }
    }
    // The window is FIFO and bounded, and an entry's log index is its position in
    // it: the order an apply recorded them in is the order they are evicted in.
    loaded.dedup.sort_by_key(|(_, _, index)| *index);
    Ok(loaded)
}

/// Reads the append-only record back as the applied history, in log order.
fn load_record(db: &rocksdb::DB) -> anyhow::Result<Vec<AppliedEvent>> {
    let family = Family::Events.handle(db)?;
    let mut applied = Vec::new();
    for item in db.iterator_cf(family, rocksdb::IteratorMode::Start) {
        let (key, value) = item?;
        let Some(index) = key.strip_prefix(b"e").and_then(|key| key.get(..16)) else {
            break;
        };
        applied.push(AppliedEvent {
            log_index: u64::from_str_radix(std::str::from_utf8(index)?, 16)?,
            event: serde_json::from_slice(&value)?,
        });
    }
    Ok(applied)
}

impl MemStateMachine {
    pub(crate) async fn open(
        disk: super::disk::Disk,
        persistence: crate::config::StatePersistence,
    ) -> anyhow::Result<Arc<Self>> {
        let saved = disk.get(Family::Default, b"state_persistence").await?;
        if let Some(bytes) = saved {
            let previous: crate::config::StatePersistence = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                previous == persistence,
                "state persistence mode cannot change on an existing database: stored {previous:?}, requested {persistence:?}"
            );
        } else {
            // No mode marker: a database this build just created, since `Disk::open`
            // refuses an existing database that carries no layout marker. Pin the
            // mode now — a database's mode is fixed for its lifetime.
            disk.put(
                Family::Default,
                b"state_persistence".to_vec(),
                serde_json::to_vec(&persistence)?,
            )
            .await?;
        }
        let mut machine = Arc::new(Self::default());
        let snapshot = disk
            .get(Family::State, b"snapshot")
            .await?
            .map(|bytes| serde_json::from_slice::<StoredSnapshot>(&bytes))
            .transpose()?;

        if persistence == crate::config::StatePersistence::Snapshot {
            // The snapshot is this mode's state: it carries the fold, the dedup
            // window and the events up to its index, and the log tail after it is
            // replayed by Raft once this returns.
            if let Some(stored) = &snapshot {
                machine
                    .clone()
                    .install_snapshot(&stored.meta, Cursor::new(stored.data.clone()))
                    .await?;
            }
        } else {
            // Every aggregate the state holds, keyed as the applies wrote them. No
            // whole-state record exists to read: an apply persists only its deltas.
            let loaded = disk.run(load_state).await?;
            let mut state = machine.state.write().await;
            state.streams = loaded.streams;
            state.last_applied_log = loaded.applied_log;
            state.last_membership = loaded.membership.unwrap_or_default();
            state.registry = Registry::new(DEDUP_WINDOW);
            for (key, fingerprint, index) in &loaded.dedup {
                state
                    .registry
                    .insert(key.clone(), fingerprint.clone(), *index);
            }
        }

        if persistence == crate::config::StatePersistence::Checkpoint {
            // Checkpoint mode has no record of *state*: the fold is the per-aggregate
            // keys read above, and the history is the append-only record. Loading the
            // record here is what lets reads answer from it — and Raft replays only
            // the entries after `meta:applied`, so nothing is counted twice.
            //
            // Snapshot mode is different: its fold comes from a snapshot that can
            // lag the record, and Raft advances that fold by replaying the log tail,
            // appending the events it produces. Pre-loading the record there would
            // double-count exactly those events, so this mode restores the
            // snapshot's own history and lets the replay extend it.
            let mut recorded = disk.run(load_record).await?;
            let mut state = machine.state.write().await;
            // The marker says how much of the record is already folded, and Raft
            // resumes after it, so anything past it is re-derived. An apply writes its
            // events and its marker in one atomic `WriteBatch`, so a crash cannot
            // leave the record ahead of the marker — but if one ever did, the replay
            // would append those events a second time, and a duplicated D13 record is
            // silent corruption. Keeping only what the marker covers makes the replay
            // idempotent by construction.
            if let Some(through) = state.last_applied_log.map(|id| id.index) {
                // The marker says how much of the record is already folded, and Raft
                // resumes after it, so anything past it is re-derived: keeping it
                // would append those events a second time.
                recorded.retain(|applied| applied.log_index <= through);
            } else if !recorded.is_empty() {
                // An apply writes the record and its marker in one `WriteBatch`, so a
                // record with no marker is not a store a crash can produce — and both
                // guesses corrupt it: keeping the record duplicates every event on
                // replay, dropping it loses events no log may still hold. Fail closed,
                // as the layout marker does.
                anyhow::bail!(
                    "the applied record has events but no applied marker: refusing to guess"
                );
            }
            if !recorded.is_empty() {
                state.applied = recorded;
                state.rebuild_members();
            }
        }
        *machine.current_snapshot.write().await = snapshot;
        let recovered = Arc::get_mut(&mut machine)
            .ok_or_else(|| anyhow::anyhow!("state machine unexpectedly shared during recovery"))?;
        recovered.disk = Some(disk);
        recovered.persistence = persistence;
        Ok(machine)
    }
    /// Writes what this apply must make durable, as one synchronous batch.
    ///
    /// The record is written in *both* persistence modes: it is the history, not the
    /// state, and `OpenRaft` may purge the entries that carried these events as soon
    /// as a snapshot covers them.
    ///
    /// In checkpoint mode the same batch carries **only what this batch changed** —
    /// the aggregate states it touched, the dedup entries it added and evicted, and
    /// the applied index — instead of the whole state. That is what makes an apply
    /// cost the size of the batch rather than the size of the history (D13 step 2).
    /// Snapshot mode persists no state per apply: recovery replays the log into the
    /// state a snapshot carried.
    ///
    /// **This batch is not synced, and does not need to be.** The Raft log is, and it
    /// is durable before the entry it carries is committed and applied, so the log —
    /// with the snapshots behind it — is the durability boundary in *both* modes.
    /// Losing this batch to a power cut therefore leaves the replica behind its log
    /// but consistent with itself, because everything below is one `WriteBatch`: the
    /// record, the state and the applied marker move together or not at all. Recovery
    /// replays the difference, which trades an fsync per apply for one fsync-window
    /// of replay — 9.5% of throughput on the batched path, measured in
    /// `workpad/benchmarks/deployment-scale.md`. Restoring `set_sync(true)` here buys
    /// back the shorter replay and nothing else; the tests that pin the recovery
    /// repair are `raft/interruption_tests.rs`.
    async fn write_applied(&self, batch: Vec<AppliedEvent>) -> Result<(), io::Error> {
        let Some(disk) = &self.disk else {
            return Ok(());
        };
        let checkpoint = self.persistence == crate::config::StatePersistence::Checkpoint;
        let serialize_started = timings::enabled().then(std::time::Instant::now);

        let mut events = Vec::with_capacity(batch.len());
        for applied in &batch {
            events.push((
                event_key(applied.log_index, position_in(&batch, applied)),
                serde_json::to_vec(&applied.event).map_err(io::Error::other)?,
            ));
        }

        let delta = if checkpoint {
            let group = self.state.read().await;
            // Only the aggregates this batch's events *are about*: a stream changes
            // only by an event, so the events name exactly what to write.
            let mut touched = std::collections::BTreeSet::new();
            for applied in &batch {
                touched.insert(applied.event.aggregate_id.clone());
            }
            let mut writes = Vec::with_capacity(touched.len().saturating_add(4));
            for aggregate_id in touched {
                if let Some(state) = group.streams.get(&aggregate_id) {
                    // The id travels in the value, not in the key: deriving an id
                    // back out of a key would mean parsing it, and ids are not all
                    // canonical UUIDs (a test, or an imported aggregate, can carry
                    // any string). The key is only there to place it in the family.
                    writes.push((
                        aggregate_key(&aggregate_id),
                        serde_json::to_vec(&(&aggregate_id, state)).map_err(io::Error::other)?,
                    ));
                }
            }
            writes.push((
                APPLIED_LOG_KEY.to_vec(),
                serde_json::to_vec(&group.last_applied_log).map_err(io::Error::other)?,
            ));
            writes.push((
                MEMBERSHIP_KEY.to_vec(),
                serde_json::to_vec(&group.last_membership).map_err(io::Error::other)?,
            ));
            for (key, fingerprint, first_log_index) in &group.added_dedup {
                // The same triple the window reports, so a reader can rebuild it
                // (key, fingerprint, first log index) without parsing the key back.
                writes.push((
                    dedup_key(key),
                    serde_json::to_vec(&(key.clone(), fingerprint.clone(), *first_log_index))
                        .map_err(io::Error::other)?,
                ));
            }
            let deletes = group
                .evicted_dedup
                .iter()
                .map(dedup_key)
                .collect::<Vec<_>>();
            Some((writes, deletes))
        } else {
            None
        };

        if let Some(started) = serialize_started {
            timings::add(&timings::SERIALIZE_NS, started);
        }
        let write_started = timings::enabled().then(std::time::Instant::now);
        disk.run(move |db| {
            let mut batch = WriteBatch::default();
            if let Some((writes, deletes)) = delta {
                let family = Family::State.handle(db)?;
                for (key, value) in writes {
                    batch.put_cf(family, key, value);
                }
                for key in deletes {
                    batch.delete_cf(family, key);
                }
            }
            if !events.is_empty() {
                let family = Family::Events.handle(db)?;
                for (key, value) in events {
                    batch.put_cf(family, key, value);
                }
            }
            // Deliberately unsynced: the Raft log is the durability boundary, and it
            // is durable before this entry was committed. See the doc comment.
            let mut options = WriteOptions::default();
            options.set_sync(false);
            db.write_opt(batch, &options)?;
            Ok(())
        })
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
        if let Some(started) = write_started {
            timings::add(&timings::WRITE_NS, started);
        }
        Ok(())
    }

    async fn persist(&self, key: &'static [u8], stored: &StoredSnapshot) -> Result<(), io::Error> {
        if let Some(disk) = &self.disk {
            let bytes = serde_json::to_vec(stored).map_err(io::Error::other)?;
            disk.put(Family::State, key.to_vec(), bytes)
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;
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

    /// Every event in the append-only record, in log order.
    ///
    /// The record is what happened, written before any snapshot covered the Raft
    /// entries that carried it, so it outlives the log. Reads still answer from the
    /// folded state — history reads move here in step 3 of
    /// `docs/storage-layout.md` — and this is what lets the layout be tested at
    /// all: the record stands on its own, without the fold and without the log.
    ///
    /// A group with no database has no record, and answers with nothing.
    ///
    /// # Errors
    ///
    /// The record cannot be read.
    pub async fn event_record(&self) -> anyhow::Result<Vec<Event>> {
        let Some(disk) = &self.disk else {
            return Ok(Vec::new());
        };
        disk.run(|db| {
            let family = Family::Events.handle(db)?;
            let mut events = Vec::new();
            for item in db.iterator_cf(family, rocksdb::IteratorMode::Start) {
                let (key, value) = item?;
                if !key.starts_with(b"e") {
                    break;
                }
                events.push(serde_json::from_slice::<Event>(&value)?);
            }
            Ok(events)
        })
        .await
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
    /// writes, and a role in one of the organization's workspaces — genesis 3
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

/// The keys one apply writes besides the aggregates.
///
/// The aggregate states are keyed per aggregate — `agg:{aggregate_id}` — which is
/// what makes an apply write only what it changed instead of the whole state
/// (D13 step 2). The tenant is the database, so nothing here names the tenant.
const APPLIED_LOG_KEY: &[u8] = b"meta:applied";
const MEMBERSHIP_KEY: &[u8] = b"meta:membership";

fn aggregate_key(aggregate_id: &Id) -> Vec<u8> {
    format!("agg:{aggregate_id}").into_bytes()
}

fn dedup_key(causation_key: &Key) -> Vec<u8> {
    format!("dedup:{causation_key}").into_bytes()
}

/// The key an event occupies in the append-only record: `e{log_index}:{position}`.
///
/// Big-endian, so a range scan over the family is history in log order, and
/// `position` distinguishes several events produced by one batched entry.
fn event_key(log_index: u64, position: u32) -> Vec<u8> {
    format!("e{log_index:016x}:{position:02x}").into_bytes()
}

/// Where one event sits among the events of its own log entry.
///
/// A batched entry produces several events, and the key has to tell them apart.
fn position_in(batch: &[AppliedEvent], applied: &AppliedEvent) -> u32 {
    batch
        .iter()
        .filter(|other| other.log_index == applied.log_index)
        .position(|other| std::ptr::eq(other, applied))
        .map_or(0, |position| u32::try_from(position).unwrap_or(u32::MAX))
}

/// Records a committed command in the dedup window and mirrors it for snapshots.
fn record(group: &mut GroupState, key: Key, fingerprint: Key, log_index: u64) {
    let first_log_index = usize::try_from(log_index).unwrap_or(usize::MAX);
    // The window is bounded and FIFO, so the only entry an insert can evict is the
    // one at the front. Asking the registry for it keeps this O(1): reading the
    // whole window back here (which is what this used to do, to maintain a mirror)
    // costs the window's size on *every* command, and while the window is filling
    // that cost grows with the history — quadratic, in both persistence modes.
    let full = group.registry.len() >= DEDUP_WINDOW;
    let oldest = group.registry.oldest();
    let known = group.registry.lookup(&key).is_some();
    group
        .registry
        .insert(key.clone(), fingerprint.clone(), first_log_index);
    if !known {
        group.added_dedup.push((key, fingerprint, first_log_index));
        if let Some(evicted) = full.then_some(oldest).flatten() {
            group.evicted_dedup.push(evicted);
        }
    }
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

/// Applies one batch of entries and returns one response per entry, in input
/// order.
///
/// Keeping the batch logic out of the [`RaftStateMachine`] impl is what lets the
/// batching, dedup and persistence behaviour be tested without an `OpenRaft`
/// responder: 0.10 only hands responders to the trait method.
/// Where the apply path spends its time, for the benchmark harness.
///
/// There is no profiler in these environments (`perf` is absent, and
/// `perf_event_paranoid` would refuse it), so the phases are timed directly: one
/// `Instant::now()` on each side of a batch, collected in process-wide counters and
/// read back through [`report`]. Collection is off unless `LOOMERY_APPLY_TIMINGS` is
/// set, and the calls are skipped entirely when it is off, so an unmeasured run
/// executes the production path unchanged.
///
/// The split is deliberately coarse — parse, apply, serialise, write — because those
/// are the four things a fix can change independently: the JSON the log store reads,
/// the core's work per command, the JSON the state machine writes, and `RocksDB`'s
/// write including its fsync.
pub mod timings {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    static ENABLED: OnceLock<bool> = OnceLock::new();

    /// Parsing log entries out of JSON, on the log store's read path.
    pub static PARSE_NS: AtomicU64 = AtomicU64::new(0);
    /// The core apply for a batch: decode a command, validate, transition, build the
    /// event; includes copying the batch's own events out of the history.
    pub static APPLY_NS: AtomicU64 = AtomicU64::new(0);
    /// Building the durable write's payloads: the events, the touched aggregates'
    /// state, the dedup triples.
    pub static SERIALIZE_NS: AtomicU64 = AtomicU64::new(0);
    /// The `RocksDB` write itself, which is synchronous — one fsync per batch.
    pub static WRITE_NS: AtomicU64 = AtomicU64::new(0);
    /// Encoding the log entry for the Raft log, on the way in.
    pub static LOG_SERIALIZE_NS: AtomicU64 = AtomicU64::new(0);
    /// The Raft log's own synchronous write — the second fsync per batch.
    pub static LOG_WRITE_NS: AtomicU64 = AtomicU64::new(0);
    /// Bytes of encoded log entries, against their commands.
    pub static LOG_BYTES: AtomicU64 = AtomicU64::new(0);
    /// What the per-command rates are divided by.
    pub static ENTRIES: AtomicU64 = AtomicU64::new(0);
    /// How many commands the timed entries carried.
    pub static COMMANDS: AtomicU64 = AtomicU64::new(0);

    /// Whether the counters are being collected.
    pub fn enabled() -> bool {
        *ENABLED.get_or_init(|| std::env::var_os("LOOMERY_APPLY_TIMINGS").is_some())
    }

    /// Adds the time since `start` to `counter`.
    #[inline]
    pub fn add(counter: &AtomicU64, start: Instant) {
        let nanos = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        counter.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Adds to a counter.
    #[inline]
    pub fn add_count(counter: &AtomicU64, count: u64) {
        counter.fetch_add(count, Ordering::Relaxed);
    }

    /// The totals as microseconds per command — the unit the benchmark reports in —
    /// with the counts they were divided by.
    ///
    /// `cast_precision_loss` is allowed because this is a diagnostic: at millisecond
    /// totals the mantissa's width is irrelevant, and reporting in `f64` is the point.
    #[allow(clippy::cast_precision_loss)]
    pub fn report() -> String {
        let commands = COMMANDS.load(Ordering::Relaxed).max(1);
        let per_command =
            |counter: &AtomicU64| counter.load(Ordering::Relaxed) as f64 / 1000.0 / commands as f64;
        let (parse, apply, serialize, write) = (
            per_command(&PARSE_NS),
            per_command(&APPLY_NS),
            per_command(&SERIALIZE_NS),
            per_command(&WRITE_NS),
        );
        let (log_serialize, log_write) =
            (per_command(&LOG_SERIALIZE_NS), per_command(&LOG_WRITE_NS));
        let commands = COMMANDS.load(Ordering::Relaxed).max(1);
        format!(
            "{} entries, {} commands, {} B/command in the log | us/command: parse {parse:.2} \
             apply {apply:.2} serialize {serialize:.2} write {write:.2} | log: encode \
             {log_serialize:.2} write {log_write:.2} | total {:.2}",
            ENTRIES.load(Ordering::Relaxed),
            COMMANDS.load(Ordering::Relaxed),
            LOG_BYTES
                .load(Ordering::Relaxed)
                .checked_div(commands)
                .unwrap_or(0),
            parse + apply + serialize + write + log_serialize + log_write,
        )
    }

    /// Zeroes every counter, so the next report covers only what follows.
    pub fn reset() {
        for counter in [
            &PARSE_NS,
            &APPLY_NS,
            &SERIALIZE_NS,
            &WRITE_NS,
            &LOG_SERIALIZE_NS,
            &LOG_WRITE_NS,
            &LOG_BYTES,
            &ENTRIES,
            &COMMANDS,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod timings_tests {
    use super::timings;
    use std::time::Instant;

    /// The report is per command, and a report ends the phase it describes.
    ///
    /// The harness is the only other caller, so without this the arithmetic and the
    /// reset would be exercised only by a benchmark run — and the counters are
    /// process-wide, which is exactly the kind of state a test should pin.
    #[test]
    fn the_report_is_per_command_and_resets() {
        timings::reset();
        timings::add_count(&timings::ENTRIES, 1);
        timings::add_count(&timings::COMMANDS, 4);
        timings::add(&timings::APPLY_NS, Instant::now());
        let report = timings::report();
        assert!(report.starts_with("1 entries, 4 commands"), "{report}");
        assert!(report.contains("us/command: parse"), "{report}");
        assert!(report.contains("| total "), "{report}");

        timings::reset();
        let cleared = timings::report();
        assert!(cleared.starts_with("0 entries, 0 commands"), "{cleared}");
        assert!(cleared.contains("| total 0.00"), "{cleared}");
    }
}

pub(super) async fn apply_batch(
    machine: &MemStateMachine,
    entries: Vec<EntryOf>,
) -> Result<Vec<Applied>, io::Error> {
    let timed = timings::enabled();
    let mut responses = Vec::new();
    let mut group = machine.state.write().await;
    // Everything appended to the in-memory history while applying this batch is
    // this batch's share of the record.
    let recorded_from = group.applied.len();
    group.begin_batch();
    let started = timed.then(std::time::Instant::now);
    let (mut entries_seen, mut commands_seen) = (0_u64, 0_u64);

    for entry in entries {
        entries_seen = entries_seen.saturating_add(1);
        match &entry.payload {
            EntryPayload::Normal(AppData::Command(_)) => {
                commands_seen = commands_seen.saturating_add(1);
            }
            EntryPayload::Normal(AppData::Batch(commands)) => {
                let count = u64::try_from(commands.len()).unwrap_or(u64::MAX);
                commands_seen = commands_seen.saturating_add(count);
            }
            EntryPayload::Blank | EntryPayload::Membership(_) => {}
        }
        let log_index = entry.log_id.index;
        group.last_applied_log = Some(entry.log_id);

        match entry.payload {
            EntryPayload::Blank => responses.push(Applied::Appended {
                first_log_index: log_index,
            }),
            EntryPayload::Membership(membership) => {
                group.last_membership = StoredMembershipOf::new(Some(entry.log_id), membership);
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
        machine.applied.send_replace(last_applied.index);
    }
    let batch = group
        .applied
        .get(recorded_from..)
        .unwrap_or_default()
        .to_vec();
    drop(group);
    if let Some(started) = started {
        timings::add(&timings::APPLY_NS, started);
        timings::add_count(&timings::ENTRIES, entries_seen);
        timings::add_count(&timings::COMMANDS, commands_seen);
    }
    machine.write_applied(batch).await?;
    Ok(responses)
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<MemStateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<Cursor<Vec<u8>>>, io::Error> {
        let group = self.state.read().await;

        let data = SnapshotData {
            version: snapshot_version(),
            last_applied_log: group.last_applied_log,
            last_membership: group.last_membership.clone(),
            streams: group.streams.clone(),
            applied: group.applied.clone(),
            dedup: group.registry.window_entries(),
        };
        let last_applied_log = data.last_applied_log;
        let last_membership = data.last_membership.clone();
        let bytes = tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
            .await
            .map_err(|error| io::Error::other(error.to_string()))?
            .map_err(io::Error::other)?;

        // 0.10 drops `snapshot_id` from the metadata: a snapshot *is* the position
        // it covers, so a transfer id belongs on the wire (leg 5), not in the
        // record every state machine writes.
        let meta = SnapshotMetaOf {
            last_log_id: last_applied_log,
            last_membership,
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

        Ok(SnapshotOf {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<MemStateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogIdOf>, StoredMembershipOf), io::Error> {
        let group = self.state.read().await;
        Ok((group.last_applied_log, group.last_membership.clone()))
    }

    /// 0.10 hands each entry its own responder and expects the response to be
    /// sent per entry. The stream the core feeds here is one finite
    /// `first..=last` range, so it is collected and applied through
    /// [`apply_batch`] in one critical section, exactly as before; the responses
    /// go out only afterwards, because a failed durable write must not tell a
    /// writer its command committed.
    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut entries = entries;
        let mut batch = Vec::new();
        let mut responders = Vec::new();
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            batch.push(entry);
            responders.push(responder);
        }

        let responses = apply_batch(self, batch).await?;

        // `apply_batch` answers exactly once per entry, so these stay aligned.
        for (responder, response) in responders.into_iter().zip(responses) {
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    fn get_snapshot_builder(&mut self) -> impl Future<Output = Self::SnapshotBuilder> + Send {
        std::future::ready(Arc::clone(self))
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let bytes = snapshot.into_inner();
        let data: SnapshotData = serde_json::from_slice(&bytes).map_err(io::Error::other)?;

        if data.version != snapshot_version() {
            return Err(io::Error::other("unsupported snapshot format version"));
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        if let Some(disk) = &self.disk {
            let bytes = serde_json::to_vec(&stored).map_err(io::Error::other)?;
            // The received snapshot is written the way an apply writes: one key per
            // aggregate, plus the markers the dedup window. Its events go into the
            // record too — a replica that installs a snapshot never applies the
            // entries it covered, so the snapshot is the only copy of them it sees.
            let mut writes = Vec::new();
            for (aggregate_id, state) in &data.streams {
                writes.push((
                    aggregate_key(aggregate_id),
                    serde_json::to_vec(&(aggregate_id, state)).map_err(io::Error::other)?,
                ));
            }
            writes.push((
                APPLIED_LOG_KEY.to_vec(),
                serde_json::to_vec(&data.last_applied_log).map_err(io::Error::other)?,
            ));
            writes.push((
                MEMBERSHIP_KEY.to_vec(),
                serde_json::to_vec(&data.last_membership).map_err(io::Error::other)?,
            ));
            for (key, fingerprint, index) in &data.dedup {
                writes.push((
                    dedup_key(key),
                    serde_json::to_vec(&(key.clone(), fingerprint.clone(), *index))
                        .map_err(io::Error::other)?,
                ));
            }
            let mut events = Vec::with_capacity(data.applied.len());
            for applied in &data.applied {
                events.push((
                    event_key(applied.log_index, position_in(&data.applied, applied)),
                    serde_json::to_vec(&applied.event).map_err(io::Error::other)?,
                ));
            }
            disk.run(move |db| {
                let mut batch = WriteBatch::default();
                let family = Family::State.handle(db)?;
                for (key, value) in writes {
                    batch.put_cf(family, key, value);
                }
                batch.put_cf(family, b"snapshot", &bytes);
                if !events.is_empty() {
                    let family = Family::Events.handle(db)?;
                    for (key, value) in events {
                        batch.put_cf(family, key, value);
                    }
                }
                let mut options = WriteOptions::default();
                options.set_sync(true);
                db.write_opt(batch, &options)?;
                Ok(())
            })
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
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
    ) -> Result<Option<SnapshotOf<Cursor<Vec<u8>>>>, io::Error> {
        match &*self.current_snapshot.read().await {
            Some(stored) => Ok(Some(SnapshotOf {
                meta: stored.meta.clone(),
                snapshot: Cursor::new(stored.data.clone()),
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

    /// A log id in term 1, proposed by node 1.
    fn log_id(index: u64) -> LogIdOf {
        use openraft::vote::RaftLeaderId;
        use openraft::vote::leader_id_adv::LeaderId;
        LogIdOf::new(LeaderId::new(1, 1), index)
    }

    /// Applies one command the way `OpenRaft` would, then reads the response.
    async fn apply_one(machine: &Arc<MemStateMachine>, command: Command) -> Applied {
        let entry = EntryOf {
            log_id: log_id(command_index(machine).await),
            payload: EntryPayload::Normal(AppData::Command(command)),
        };
        apply_batch(machine, vec![entry]).await.unwrap().remove(0)
    }

    /// The next index for a test entry — derived from what has been applied.
    async fn command_index(machine: &Arc<MemStateMachine>) -> u64 {
        let group = machine.state.read().await;
        u64::try_from(group.registry.len()).unwrap_or(u64::MAX)
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
        let entry = EntryOf {
            log_id: log_id(42),
            payload: EntryPayload::Normal(AppData::Batch(commands)),
        };
        let results = apply_batch(&machine, vec![entry]).await.unwrap();
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
            .install_snapshot(&snapshot.meta, Cursor::new(snapshot.snapshot.into_inner()))
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

    /// A log id in term 1, proposed by node 1.
    fn log_id(index: u64) -> LogIdOf {
        use openraft::vote::RaftLeaderId;
        use openraft::vote::leader_id_adv::LeaderId;
        LogIdOf::new(LeaderId::new(1, 1), index)
    }

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
        let entry = EntryOf {
            log_id: log_id(1),
            payload: EntryPayload::Normal(AppData::Command(command)),
        };
        apply_batch(machine, vec![entry]).await.unwrap().remove(0)
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
