// SPDX-License-Identifier: MPL-2.0
//! `RocksDB` replicated log. Big-endian index keys preserve log ordering;
//! atomic synchronous WAL batches persist truncation and purge metadata.
//!
//! The log lives in its own column family ([`Family::RaftLog`]). Entry keys start
//! with `l`, and the bookkeeping keys (`purged`, `vote`, `committed`) sort *after*
//! every entry, because `p` and `v` are greater than `l`. So a forward scan stops
//! at the first key that is not an entry, and finding the last entry is a reverse
//! scan from `m`: the greatest key below `m` is the greatest `l…`, while the
//! bookkeeping keys sit above it.
use super::{TypeConfig, disk::Disk, disk::Family};
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, StorageIOError, Vote};
use rocksdb::{IteratorMode, WriteBatch, WriteOptions};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fmt::Debug,
    ops::{Bound, RangeBounds},
};

/// Durable replicated log, vote and commit pointer of one group.
#[derive(Clone, Debug)]
pub struct RocksLogStore {
    disk: Disk,
}
fn io(error: &anyhow::Error) -> StorageError<u64> {
    StorageIOError::write_logs(&std::io::Error::other(error.to_string())).into()
}
fn key(index: u64) -> Vec<u8> {
    let mut key = vec![b'l'];
    key.extend_from_slice(&index.to_be_bytes());
    key
}
fn write(db: &rocksdb::DB, batch: WriteBatch) -> anyhow::Result<()> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options)?;
    Ok(())
}
fn meta<T: DeserializeOwned>(
    db: &rocksdb::DB,
    family: Family,
    key: &[u8],
) -> anyhow::Result<Option<T>> {
    db.get_cf(family.handle(db)?, key)?
        .map(|bytes| serde_json::from_slice(&bytes))
        .transpose()
        .map_err(Into::into)
}
impl RocksLogStore {
    pub(crate) fn open(disk: Disk) -> Self {
        Self { disk }
    }
    async fn save(&self, key: &'static [u8], value: impl Serialize) -> anyhow::Result<()> {
        self.disk
            .put(Family::RaftLog, key.to_vec(), serde_json::to_vec(&value)?)
            .await
    }
    async fn remove(&self, index: u64, purge: Option<LogId<u64>>) -> anyhow::Result<()> {
        self.disk
            .run(move |db| {
                let family = Family::RaftLog.handle(db)?;
                let mut batch = WriteBatch::default();
                for item in db.prefix_iterator_cf(family, b"l") {
                    let (key, value) = item?;
                    if !key.starts_with(b"l") {
                        break;
                    }
                    let entry: Entry<TypeConfig> = serde_json::from_slice(&value)?;
                    if if purge.is_some() {
                        entry.log_id.index <= index
                    } else {
                        entry.log_id.index >= index
                    } {
                        batch.delete_cf(family, key);
                    }
                }
                if let Some(id) = purge {
                    batch.put_cf(family, b"purged", serde_json::to_vec(&id)?);
                }
                write(db, batch)
            })
            .await
    }
}
impl RaftLogReader<TypeConfig> for RocksLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        self.disk
            .run(move |db| {
                let family = Family::RaftLog.handle(db)?;
                let first = match start {
                    Bound::Included(n) | Bound::Excluded(n) => n,
                    Bound::Unbounded => 0,
                };
                let mut entries = Vec::new();
                for item in db.iterator_cf(
                    family,
                    IteratorMode::From(&key(first), rocksdb::Direction::Forward),
                ) {
                    let (key, value) = item?;
                    if !key.starts_with(b"l") {
                        break;
                    }
                    let entry: Entry<TypeConfig> = serde_json::from_slice(&value)?;
                    let index = entry.log_id.index;
                    if match end {
                        Bound::Included(n) => index > n,
                        Bound::Excluded(n) => index >= n,
                        Bound::Unbounded => false,
                    } {
                        break;
                    }
                    if matches!(start, Bound::Excluded(n) if index == n) {
                        continue;
                    }
                    entries.push(entry);
                }
                Ok(entries)
            })
            .await
            .map_err(|e| io(&e))
    }
}
impl RaftLogStorage<TypeConfig> for RocksLogStore {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        self.disk
            .run(|db| {
                let family = Family::RaftLog.handle(db)?;
                let purged = meta::<LogId<u64>>(db, Family::RaftLog, b"purged")?;
                let mut last = None;
                // `m` is above every entry key and below the bookkeeping keys, so
                // the greatest key below it is the greatest entry.
                if let Some(item) = db
                    .iterator_cf(
                        family,
                        IteratorMode::From(b"m", rocksdb::Direction::Reverse),
                    )
                    .next()
                {
                    let (key, value) = item?;
                    if key.starts_with(b"l") {
                        last = Some(serde_json::from_slice::<Entry<TypeConfig>>(&value)?.log_id);
                    }
                }
                Ok(LogState {
                    last_purged_log_id: purged,
                    last_log_id: last.or(purged),
                })
            })
            .await
            .map_err(|e| io(&e))
    }
    fn get_log_reader(&mut self) -> impl std::future::Future<Output = Self> + Send {
        std::future::ready(self.clone())
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.save(b"vote", vote).await.map_err(|e| io(&e))
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.disk
            .run(|db| meta(db, Family::RaftLog, b"vote"))
            .await
            .map_err(|e| io(&e))
    }
    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.save(b"committed", committed).await.map_err(|e| io(&e))
    }
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        self.disk
            .run(|db| Ok(meta::<Option<LogId<u64>>>(db, Family::RaftLog, b"committed")?.flatten()))
            .await
            .map_err(|e| io(&e))
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
        let entries: Vec<_> = entries.into_iter().collect();
        let result = self
            .disk
            .run(move |db| {
                let family = Family::RaftLog.handle(db)?;
                let mut batch = WriteBatch::default();
                for entry in entries {
                    batch.put_cf(family, key(entry.log_id.index), serde_json::to_vec(&entry)?);
                }
                write(db, batch)
            })
            .await;
        match result {
            Ok(()) => {
                // The synchronous WAL batch has completed on the blocking pool.
                // OpenRaft permits this callback before append returns. Its 0.9.25
                // core awaits the callback too, so merely detaching the sync would
                // not pipeline appends from this group.
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
                Err(io(&error))
            }
        }
    }
    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.remove(log_id.index, None).await.map_err(|e| io(&e))
    }
    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.remove(log_id.index, Some(log_id))
            .await
            .map_err(|e| io(&e))
    }
}
