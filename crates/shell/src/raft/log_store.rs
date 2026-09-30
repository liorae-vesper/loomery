// SPDX-License-Identifier: MPL-2.0

//! The in-memory Raft log store — the replicated log and the vote.
//!
//! Correctness notes that matter even in memory (they are the bugs the spike
//! exists to catch):
//!
//! * [`MemLogStore::get_log_state`] never consults the state machine's applied
//!   index: the log must remember more than the state machine has applied.
//! * [`MemLogStore::append`] drops any conflicting suffix before writing, so a
//!   re-written tail after a leadership change cannot leave a hole.
//! * [`MemLogStore::purge`] only reports what it removed as `last_purged`; the
//!   state machine is responsible for never needing those entries again.
//!
//! All state is behind a `tokio::sync::RwLock`, so the storage methods are
//! genuinely async (`OpenRaft` calls them from its own task).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::Entry;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftLogId;
use openraft::StorageError;
use openraft::Vote;
use openraft::storage::LogFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use tokio::sync::RwLock;

use super::TypeConfig;

/// The in-memory replicated log, vote and commit pointer of one group.
#[derive(Debug, Clone)]
pub struct MemLogStore {
    inner: Arc<RwLock<LogStoreState>>,
}

/// The log store's data, guarded as a whole (writes are serialized by the lock).
#[derive(Debug, Default)]
struct LogStoreState {
    /// The last vote this replica persisted.
    vote: Option<Vote<u64>>,
    /// The log entries, keyed by index.
    entries: BTreeMap<u64, Entry<TypeConfig>>,
    /// The committed pointer, when it has been saved.
    committed: Option<LogId<u64>>,
    /// Entries at or below this id have been purged and must not be read again.
    last_purged: Option<LogId<u64>>,
}

impl Default for MemLogStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(LogStoreState::default())),
        }
    }
}

impl RaftLogReader<TypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let state = self.inner.read().await;
        Ok(state
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let state = self.inner.read().await;

        // The last present entry wins; with an empty log the answer is the
        // last purged id, per the trait contract.
        let last_log_id = state
            .entries
            .values()
            .next_back()
            .map(|entry| *entry.get_log_id())
            .or(state.last_purged);

        Ok(LogState {
            last_purged_log_id: state.last_purged,
            last_log_id,
        })
    }

    fn get_log_reader(&mut self) -> impl Future<Output = Self::LogReader> + Send {
        std::future::ready(self.clone())
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.inner.write().await;
        state.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.read().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut state = self.inner.write().await;
        state.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.read().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();

        {
            let mut state = self.inner.write().await;

            // A conflict means the log's tail is being rewritten: drop
            // everything from the first incoming index onwards before writing,
            // so no hole and no stale suffix can survive.
            if let Some(first) = entries.first() {
                let first_index = first.log_id.index;
                state.entries.retain(|index, _| *index < first_index);
            }

            for entry in entries {
                state.entries.insert(entry.log_id.index, entry);
            }
        }

        // In-memory writes are durable as soon as the lock is released.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.inner.write().await;
        state.entries.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.inner.write().await;
        state.entries.retain(|index, _| *index > log_id.index);
        state.last_purged = Some(log_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::blank_ent;

    fn store() -> MemLogStore {
        MemLogStore::default()
    }

    fn blank(index: u64) -> Entry<TypeConfig> {
        blank_ent::<TypeConfig>(1, 1, index)
    }

    /// Seeds entries behind the lock. `append` itself needs an `OpenRaft`-built
    /// `LogFlushed` callback, so it is exercised through the real group in the
    /// acceptance tests; the read/purge semantics are tested here directly.
    async fn seed(store: &MemLogStore, indexes: &[u64]) {
        let mut state = store.inner.write().await;
        for index in indexes {
            state.entries.insert(*index, blank(*index));
        }
    }

    #[tokio::test]
    async fn an_empty_log_reports_no_state() {
        let mut store = store();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, None);
        assert_eq!(state.last_purged_log_id, None);
    }

    #[tokio::test]
    async fn reading_returns_the_requested_range() {
        let mut store = store();
        seed(&store, &[1, 2]).await;

        let read = store.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[1].log_id.index, 2);

        let log_state = store.get_log_state().await.unwrap();
        assert_eq!(log_state.last_log_id.map(|id| id.index), Some(2));
        assert!(matches!(read[0].payload, openraft::EntryPayload::Blank));
    }

    #[tokio::test]
    async fn truncate_drops_the_conflicting_suffix() {
        let mut store = store();
        seed(&store, &[1, 2, 3]).await;

        store
            .truncate(LogId::new(blank(2).log_id.leader_id, 2))
            .await
            .unwrap();

        assert_eq!(
            store
                .get_log_state()
                .await
                .unwrap()
                .last_log_id
                .map(|id| id.index),
            Some(1)
        );
        assert_eq!(store.try_get_log_entries(1..4).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn purge_moves_the_floor() {
        let mut store = store();
        seed(&store, &[1, 2, 3]).await;

        store
            .purge(LogId::new(blank(2).log_id.leader_id, 2))
            .await
            .unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.map(|id| id.index), Some(2));
        assert_eq!(state.last_log_id.map(|id| id.index), Some(3));
        assert_eq!(store.try_get_log_entries(1..4).await.unwrap().len(), 1);
    }
}
