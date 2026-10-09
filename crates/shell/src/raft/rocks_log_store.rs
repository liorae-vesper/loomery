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
use super::{TypeConfig, alias::EntryOf, alias::LogIdOf, alias::VoteOf, disk::Disk, disk::Family};
use openraft::OptionalSend;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use rocksdb::{IteratorMode, WriteBatch, WriteOptions};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fmt::Debug,
    io,
    ops::{Bound, RangeBounds},
};

/// Durable replicated log, vote and commit pointer of one group.
#[derive(Clone, Debug)]
pub struct RocksLogStore {
    disk: Disk,
}
fn io(error: &anyhow::Error) -> io::Error {
    io::Error::other(error.to_string())
}
fn key(index: u64) -> Vec<u8> {
    let mut key = vec![b'l'];
    key.extend_from_slice(&index.to_be_bytes());
    key
}
fn write(db: &rocksdb::DB, batch: WriteBatch) -> anyhow::Result<()> {
    let started = super::state_machine::timings::enabled().then(std::time::Instant::now);
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options)?;
    if let Some(started) = started {
        super::state_machine::timings::add(&super::state_machine::timings::LOG_WRITE_NS, started);
    }
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
    async fn remove(&self, index: u64, purge: Option<LogIdOf>) -> anyhow::Result<()> {
        self.disk
            .run(move |db| {
                let family = Family::RaftLog.handle(db)?;
                let mut batch = WriteBatch::default();
                for item in db.prefix_iterator_cf(family, b"l") {
                    let (key, value) = item?;
                    if !key.starts_with(b"l") {
                        break;
                    }
                    let entry: EntryOf = serde_json::from_slice(&value)?;
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
    ) -> Result<Vec<EntryOf>, io::Error> {
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
                let timed = super::state_machine::timings::enabled();
                for item in db.iterator_cf(
                    family,
                    IteratorMode::From(&key(first), rocksdb::Direction::Forward),
                ) {
                    let (key, value) = item?;
                    if !key.starts_with(b"l") {
                        break;
                    }
                    let parse_started = timed.then(std::time::Instant::now);
                    let entry: EntryOf = serde_json::from_slice(&value)?;
                    if let Some(started) = parse_started {
                        super::state_machine::timings::add(
                            &super::state_machine::timings::PARSE_NS,
                            started,
                        );
                    }
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

    // The vote is part of the log, but reading it is the *reader's* job in 0.10:
    // replication reads it to check that a log stream still belongs to the leader.
    async fn read_vote(&mut self) -> Result<Option<VoteOf>, io::Error> {
        self.disk
            .run(|db| meta(db, Family::RaftLog, b"vote"))
            .await
            .map_err(|e| io(&e))
    }
}
impl RaftLogStorage<TypeConfig> for RocksLogStore {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        self.disk
            .run(|db| {
                let family = Family::RaftLog.handle(db)?;
                let purged = meta::<LogIdOf>(db, Family::RaftLog, b"purged")?;
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
                        last = Some(serde_json::from_slice::<EntryOf>(&value)?.log_id);
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
    async fn save_vote(&mut self, vote: &VoteOf) -> Result<(), io::Error> {
        self.save(b"vote", vote).await.map_err(|e| io(&e))
    }
    async fn save_committed(&mut self, committed: Option<LogIdOf>) -> Result<(), io::Error> {
        self.save(b"committed", committed).await.map_err(|e| io(&e))
    }
    async fn read_committed(&mut self) -> Result<Option<LogIdOf>, io::Error> {
        self.disk
            .run(|db| Ok(meta::<Option<LogIdOf>>(db, Family::RaftLog, b"committed")?.flatten()))
            .await
            .map_err(|e| io(&e))
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let result = self
            .disk
            .run(move |db| {
                let family = Family::RaftLog.handle(db)?;
                let mut batch = WriteBatch::default();
                let timed = super::state_machine::timings::enabled();
                let encode_started = timed.then(std::time::Instant::now);
                for entry in entries {
                    let value = serde_json::to_vec(&entry)?;
                    if timed {
                        super::state_machine::timings::add_count(
                            &super::state_machine::timings::LOG_BYTES,
                            value.len() as u64,
                        );
                    }
                    batch.put_cf(family, key(entry.log_id.index), value);
                }
                if let Some(started) = encode_started {
                    super::state_machine::timings::add(
                        &super::state_machine::timings::LOG_SERIALIZE_NS,
                        started,
                    );
                }
                write(db, batch)
            })
            .await;
        match result {
            Ok(()) => {
                // The synchronous WAL batch has completed on the blocking pool.
                // OpenRaft permits this callback before append returns. Its core
                // awaits the callback too, so merely detaching the sync would not
                // pipeline appends from this group.
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.io_completed(Err(io::Error::other(error.to_string())));
                Err(io(&error))
            }
        }
    }

    /// Removes every entry after `last_log_id` (exclusive), which is 0.10's
    /// spelling of 0.9's "truncate since `log_id`, inclusive".
    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf>) -> Result<(), io::Error> {
        match last_log_id {
            // `None` truncates everything; a kept id at the index ceiling cannot
            // be followed by anything, so there is nothing to delete.
            None => self.remove(0, None).await.map_err(|e| io(&e)),
            Some(id) => match id.index.checked_add(1) {
                Some(after) => self.remove(after, None).await.map_err(|e| io(&e)),
                None => Ok(()),
            },
        }
    }
    async fn purge(&mut self, log_id: LogIdOf) -> Result<(), io::Error> {
        self.remove(log_id.index, Some(log_id))
            .await
            .map_err(|e| io(&e))
    }
}
