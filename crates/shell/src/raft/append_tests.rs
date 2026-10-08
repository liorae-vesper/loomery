// SPDX-License-Identifier: MPL-2.0
//! Append return/flush-callback checks beyond `OpenRaft`'s storage suite.
use super::{
    AppData, MemLogStore, MemStateMachine, NoopNetworkFactory, RocksLogStore, TypeConfig,
    disk::Disk,
};
use crate::{config::StorageConfig, test_support::bootstrap_value};
use openraft::{
    OptionalSend, StorageError,
    storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftLogStorageExt},
    testing::blank_ent,
};
use std::{collections::BTreeMap, fmt::Debug, io, ops::RangeBounds, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};

use super::alias::{EntryOf, LogIdOf, VoteOf};

type Completion = JoinHandle<Result<(), StorageError<TypeConfig>>>;

// IOFlushed::new is private. Use the public blocking_append helper to obtain
// a genuine OpenRaft callback, keeping its receiver alive in a separate task.
// This fixture delays captured callbacks; log visibility uses an in-memory store.
struct CallbackSource {
    reader: MemLogStore,
    sender: mpsc::UnboundedSender<IOFlushed<TypeConfig>>,
    commands_only: bool,
}
impl RaftLogReader<TypeConfig> for CallbackSource {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf>, io::Error> {
        self.reader.try_get_log_entries(range).await
    }
    async fn read_vote(&mut self) -> Result<Option<VoteOf>, io::Error> {
        self.reader.read_vote().await
    }
}
impl RaftLogStorage<TypeConfig> for CallbackSource {
    type LogReader = MemLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        self.reader.get_log_state().await
    }
    fn get_log_reader(&mut self) -> impl std::future::Future<Output = Self::LogReader> + Send {
        std::future::ready(self.reader.clone())
    }
    async fn save_vote(&mut self, vote: &VoteOf) -> Result<(), io::Error> {
        self.reader.save_vote(vote).await
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
        if !self.commands_only
            || entries
                .iter()
                .any(|entry| matches!(entry.payload, openraft::EntryPayload::Normal(_)))
        {
            // Entries become readable before returning, but the original callback
            // stays pending until the test acknowledges it explicitly.
            self.reader
                .blocking_append(entries)
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
            assert!(self.sender.send(callback).is_ok());
            Ok(())
        } else {
            self.reader.append(entries, callback).await
        }
    }
    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf>) -> Result<(), io::Error> {
        self.reader.truncate_after(last_log_id).await
    }
    async fn purge(&mut self, log_id: LogIdOf) -> Result<(), io::Error> {
        self.reader.purge(log_id).await
    }
}

async fn callback() -> (IOFlushed<TypeConfig>, Completion) {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let mut source = CallbackSource {
        reader: MemLogStore::default(),
        sender,
        commands_only: false,
    };
    let completion = tokio::spawn(async move { source.blocking_append([]).await });
    let callback = timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !completion.is_finished(),
        "append return alone is not a flush"
    );
    (callback, completion)
}

/// 0.9 serialized local appends behind the previous flush; 0.10 does not — the
/// core tracks IO completion with a watermark instead, so a second client write
/// reaches `RaftLogStorage::append` while the first is still unflushed. That is
/// the pipelining this branch exists for. What must not change is that an entry
/// is readable as soon as `append` returns, and that a client is answered only
/// once a flush covering its entry has completed.
#[tokio::test]
async fn a_second_append_is_not_serialized_behind_the_first_flush() {
    let (sender, mut callbacks) = mpsc::unbounded_channel();
    let mut reader = MemLogStore::default();
    let store = CallbackSource {
        reader: reader.clone(),
        sender,
        commands_only: true,
    };
    let config = openraft::Config {
        enable_tick: false,
        ..openraft::Config::default()
    };
    let raft = openraft::Raft::new(
        1,
        Arc::new(config.validate().unwrap()),
        NoopNetworkFactory,
        store,
        Arc::new(MemStateMachine::default()),
    )
    .await
    .unwrap();
    raft.initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();

    let command = AppData::Command(
        bootstrap_value()
            .command(loomery_genesis::Step::ALL[0])
            .unwrap(),
    );
    let first_raft = raft.clone();
    let first_command = command.clone();
    let mut first = tokio::spawn(async move { first_raft.client_write(first_command).await });
    let first_callback = timeout(Duration::from_secs(5), callbacks.recv())
        .await
        .unwrap()
        .unwrap();
    let second_raft = raft.clone();
    let second = tokio::spawn(async move { second_raft.client_write(command).await });

    // 0.10 appends the next entry without waiting for the previous flush: both
    // callbacks are outstanding at once.
    let second_callback = timeout(Duration::from_secs(5), callbacks.recv())
        .await
        .expect("0.10 does not wait for the previous flush before the next append")
        .unwrap();

    // Both entries are readable before either flush: `append` makes them visible,
    // only the callback makes them durable.
    let entries = reader.try_get_log_entries(..).await.unwrap();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| matches!(entry.payload, openraft::EntryPayload::Normal(_)))
            .count(),
        2
    );
    // Neither client may be answered while its entry is unflushed.
    assert!(
        timeout(Duration::from_millis(100), &mut first)
            .await
            .is_err()
    );
    assert!(!second.is_finished());

    // The first flush covers the first entry, so that client is answered; the
    // second entry is still unflushed, so its client is not.
    first_callback.io_completed(Ok(()));
    timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!second.is_finished());
    second_callback.io_completed(Ok(()));
    timeout(Duration::from_secs(5), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    raft.shutdown().await.unwrap();
}

async fn completed(completion: Completion) -> Result<(), Box<StorageError<TypeConfig>>> {
    timeout(Duration::from_secs(5), completion)
        .await
        .expect("append must complete its flush callback")
        .unwrap()
        .map_err(Box::new)
}

#[tokio::test]
async fn batched_clients_wait_for_their_shared_flush_callback() {
    let (sender, mut callbacks) = mpsc::unbounded_channel();
    let mut reader = MemLogStore::default();
    let store = CallbackSource {
        reader: reader.clone(),
        sender,
        commands_only: true,
    };
    let config = openraft::Config {
        enable_tick: false,
        ..openraft::Config::default()
    };
    let raft = openraft::Raft::new(
        1,
        Arc::new(config.validate().unwrap()),
        NoopNetworkFactory,
        store,
        Arc::new(MemStateMachine::default()),
    )
    .await
    .unwrap();
    raft.initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    let writer = super::ProposalWriter::new(
        raft.clone(),
        crate::config::ProposalConfig {
            max_batch_commands: 2,
            max_delay_ms: 20,
            ..crate::config::ProposalConfig::default()
        },
    );
    let submitted = writer.clone();
    let first = bootstrap_value()
        .command(loomery_genesis::Step::AssignLeader)
        .unwrap();
    let second = bootstrap_value()
        .command(loomery_genesis::Step::CreateWorkspace)
        .unwrap();
    let mut clients =
        tokio::spawn(
            async move { tokio::join!(submitted.propose(first), submitted.propose(second)) },
        );
    let callback = timeout(Duration::from_secs(5), callbacks.recv())
        .await
        .unwrap()
        .unwrap();
    let entries = reader.try_get_log_entries(..).await.unwrap();
    let batches: Vec<_> = entries
        .iter()
        .filter_map(|entry| match &entry.payload {
            openraft::EntryPayload::Normal(AppData::Batch(commands)) => {
                Some((entry.log_id.index, commands.len()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].1, 2);
    assert!(
        timeout(Duration::from_millis(100), &mut clients)
            .await
            .is_err()
    );
    assert!(callbacks.try_recv().is_err());
    callback.io_completed(Ok(()));
    let (first, second) = timeout(Duration::from_secs(5), clients)
        .await
        .unwrap()
        .unwrap();
    let expected = crate::group::ProposeOutcome::Appended {
        first_log_index: batches[0].0,
    };
    assert_eq!(first.unwrap(), expected);
    assert_eq!(second.unwrap(), expected);
    writer.stop().await;
    raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_flush_never_acknowledges_or_applies_any_batched_command() {
    let (sender, mut callbacks) = mpsc::unbounded_channel();
    let store = CallbackSource {
        reader: MemLogStore::default(),
        sender,
        commands_only: true,
    };
    let machine = Arc::new(MemStateMachine::default());
    let raft = openraft::Raft::new(
        1,
        Arc::new(
            openraft::Config {
                enable_tick: false,
                ..openraft::Config::default()
            }
            .validate()
            .unwrap(),
        ),
        NoopNetworkFactory,
        store,
        machine.clone(),
    )
    .await
    .unwrap();
    raft.initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(1, "leader")
        .await
        .unwrap();
    let writer = super::ProposalWriter::new(
        raft.clone(),
        crate::config::ProposalConfig {
            max_batch_commands: 2,
            max_delay_ms: 20,
            ..crate::config::ProposalConfig::default()
        },
    );
    let submitted = writer.clone();
    let first = bootstrap_value()
        .command(loomery_genesis::Step::AssignLeader)
        .unwrap();
    let second = bootstrap_value()
        .command(loomery_genesis::Step::CreateWorkspace)
        .unwrap();
    let clients =
        tokio::spawn(
            async move { tokio::join!(submitted.propose(first), submitted.propose(second)) },
        );
    let callback = timeout(Duration::from_secs(5), callbacks.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(!clients.is_finished());
    // The entries are visible, but durability failed. Returning append Ok must
    // never turn this callback error into a successful client acknowledgement.
    callback.io_completed(Err(std::io::Error::other("injected WAL sync failure")));
    let (first, second) = timeout(Duration::from_secs(5), clients)
        .await
        .unwrap()
        .unwrap();
    for error in [first.unwrap_err(), second.unwrap_err()] {
        assert!(
            matches!(
                error.downcast_ref::<super::ProposeError>(),
                Some(super::ProposeError::Unknown(_))
            ),
            "unexpected client error: {error:#}"
        );
    }
    assert!(
        machine
            .committed_events(&crate::test_support::organization())
            .await
            .is_empty()
    );
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.running_state.is_err(),
            "flush failure stops Raft",
        )
        .await
        .unwrap();
    writer.stop().await;
    raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn append_success_completes_callback_and_is_readable_and_reopenable() {
    let dir = tempfile::tempdir().unwrap();
    let disk = Disk::open(dir.path(), &StorageConfig::default())
        .await
        .unwrap();
    let mut store = RocksLogStore::open(disk);
    let mut reader = store.get_log_reader().await;
    let entries = [
        blank_ent::<TypeConfig>(1, 1, 0),
        blank_ent::<TypeConfig>(1, 1, 1),
    ];
    let expected: Vec<_> = entries.iter().map(|entry| entry.log_id).collect();
    let (callback, completion) = callback().await;

    store.append(entries, callback).await.unwrap();
    // Read through a handle obtained before append, directly after it returns.
    let actual = reader.try_get_log_entries(..).await.unwrap();
    assert_eq!(
        actual.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
        expected
    );
    completed(completion).await.unwrap();

    drop(reader);
    drop(store);
    let disk = Disk::open(dir.path(), &StorageConfig::default())
        .await
        .unwrap();
    let mut reopened = RocksLogStore::open(disk);
    let actual = reopened.try_get_log_entries(..).await.unwrap();
    assert_eq!(
        actual.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
        expected
    );
}

#[tokio::test]
async fn failed_append_reports_error_through_both_return_and_callback() {
    let dir = tempfile::tempdir().unwrap();
    let disk = Disk::open(dir.path(), &StorageConfig::default())
        .await
        .unwrap();
    let mut store = RocksLogStore::open(disk);
    store
        .blocking_append([blank_ent::<TypeConfig>(1, 1, 0)])
        .await
        .unwrap();
    drop(store);

    // A real RocksDB write rejection, rather than a mocked callback outcome.
    let path = dir.path().to_owned();
    let database = tokio::task::spawn_blocking(move || {
        rocksdb::DB::open_cf_descriptors_read_only(
            &rocksdb::Options::default(),
            path,
            super::disk::Disk::descriptors(&StorageConfig::default()),
            false,
        )
    })
    .await
    .unwrap()
    .unwrap();
    let mut store = RocksLogStore::open(Disk(Arc::new(database)));
    let (callback, completion) = callback().await;
    let returned = store
        .append([blank_ent::<TypeConfig>(1, 1, 1)], callback)
        .await
        .unwrap_err();
    let flushed = completed(completion).await.unwrap_err();

    assert!(returned.to_string().to_lowercase().contains("read only"));
    assert!(flushed.to_string().to_lowercase().contains("read only"));
    let entries = store.try_get_log_entries(..).await.unwrap();
    assert_eq!(
        entries.len(),
        1,
        "failed append must preserve the existing log"
    );
    assert_eq!(entries[0].log_id.index, 0);
}
