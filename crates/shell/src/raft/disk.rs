// SPDX-License-Identifier: MPL-2.0
//! Durable `RocksDB` access, dispatched off Tokio's runtime threads.
//!
//! One database per group, split into **column families** rather than one key
//! space (see `docs/storage-layout.md`): the tenant is the database, so nothing
//! inside it needs a tenant prefix, and the kinds of data are separated by family —
//! the Raft log, the fold, the append-only record, and the read models.
//!
//! Opening is **fail closed**. A database that does not carry this layout — an
//! older one, or one whose family set is not exactly [`Family::ALL`] — is refused
//! rather than half-opened, and the layout version marker is what makes that
//! decision. Pre-release there is no migration: the data directory is discarded and
//! the tenant re-onboarded.
use crate::config::StorageConfig;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB, Options, WriteOptions,
};
use std::{path::Path, sync::Arc};

/// The layout version this build writes.
///
/// Bump it whenever the family set or a family's key shape changes; every opener
/// refuses a database that carries a different one.
pub(crate) const FORMAT: u32 = 1;

/// The key the layout version is stored under, in [`Family::Default`].
const FORMAT_KEY: &[u8] = b"format";

/// A column family of one group's database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Family {
    /// Markers: the layout version and the state persistence mode.
    Default,
    /// The Raft log and its bookkeeping: entries, vote, committed, purge floor.
    RaftLog,
    /// The fold: aggregate state, membership, applied index, dedup, snapshot.
    State,
    /// The append-only record: every applied event, in order.
    Events,
    /// Read models (D9). Created with the database so the layout is complete.
    Projections,
}

impl Family {
    /// Every family, in the order the database is opened with.
    pub(crate) const ALL: [Family; 5] = [
        Family::Default,
        Family::RaftLog,
        Family::State,
        Family::Events,
        Family::Projections,
    ];

    /// The name `RocksDB` knows this family by.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Family::Default => "default",
            Family::RaftLog => "raft_log",
            Family::State => "state",
            Family::Events => "events",
            Family::Projections => "projections",
        }
    }

    /// The family's handle in `db`.
    ///
    /// # Errors
    ///
    /// The database was not opened with this family. That is a refusal, not
    /// something to create silently behind the caller's back: a family that is
    /// missing means the layout is not the one this build writes.
    pub(crate) fn handle(self, db: &DB) -> anyhow::Result<&ColumnFamily> {
        db.cf_handle(self.name()).ok_or_else(|| {
            anyhow::anyhow!(
                "column family `{}` is missing: this database was not opened with the layout this build writes",
                self.name()
            )
        })
    }
}

/// Durable access to one group's database.
#[derive(Clone, Debug)]
pub(crate) struct Disk(pub Arc<DB>);

impl Disk {
    /// Opens (or creates) the database at `path`, with the full family layout.
    ///
    /// A directory without a database in it is new — the families and the layout
    /// marker are created. An existing one is opened exactly as it stands: missing
    /// families are *not* created, and the marker must match, so an unknown layout
    /// fails here instead of being read as if it were this one.
    ///
    /// # Errors
    ///
    /// The database cannot be opened, the marker is absent or from another layout,
    /// or the marker cannot be written.
    pub async fn open(path: &Path, config: &StorageConfig) -> anyhow::Result<Self> {
        let path = path.to_owned();
        let config = config.clone();
        let (disk, fresh) = tokio::task::spawn_blocking(move || {
            let fresh = !path.join("CURRENT").exists();

            let mut options = Options::default();
            if fresh {
                options.create_if_missing(true);
                options.create_missing_column_families(true);
            }
            options.set_max_background_jobs(config.max_background_jobs);
            options.set_max_open_files(config.max_open_files);

            let families = Self::descriptors(&config);
            let db = DB::open_cf_descriptors(&options, path, families)?;
            Ok::<_, anyhow::Error>((Self(Arc::new(db)), fresh))
        })
        .await??;

        disk.check_format(fresh).await?;
        Ok(disk)
    }

    /// The column-family descriptors of this layout, with one block cache shared by
    /// every family — the configured block cache is the whole database's budget, not
    /// a budget per family.
    ///
    /// Shared with the read-only opener the tests use, so the layout lives in one
    /// place: a database can only be opened by something that lists every family.
    pub(crate) fn descriptors(config: &StorageConfig) -> Vec<ColumnFamilyDescriptor> {
        let cache = Cache::new_lru_cache(config.block_cache_bytes);
        Family::ALL
            .iter()
            .map(|family| {
                let mut table = BlockBasedOptions::default();
                table.set_block_cache(&cache);
                let mut family_options = Options::default();
                family_options.set_write_buffer_size(config.write_buffer_bytes);
                family_options.set_max_write_buffer_number(config.max_write_buffers);
                family_options.set_compression_type(rocksdb::DBCompressionType::Lz4);
                family_options.set_block_based_table_factory(&table);
                ColumnFamilyDescriptor::new(family.name(), family_options)
            })
            .collect()
    }

    /// Reads the layout marker, or writes it for a database that was just created.
    ///
    /// # Errors
    ///
    /// An existing database without a marker, or with one this build does not
    /// write; or the marker cannot be written.
    async fn check_format(&self, fresh: bool) -> anyhow::Result<()> {
        let Some(bytes) = self.get(Family::Default, FORMAT_KEY).await? else {
            anyhow::ensure!(
                fresh,
                "an existing database carries no storage layout marker: \
                 pre-release, delete the data directory and re-onboard"
            );
            return self
                .put(
                    Family::Default,
                    FORMAT_KEY.to_vec(),
                    serde_json::to_vec(&FORMAT)?,
                )
                .await;
        };
        let found: u32 = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            found == FORMAT,
            "storage layout {found} is not the layout this build writes ({FORMAT}): \
             pre-release, delete the data directory and re-onboard"
        );
        Ok(())
    }

    /// Runs `f` against the database on the blocking pool.
    ///
    /// The closure gets the whole database, so it can reach several families in one
    /// atomic batch — which is how an apply writes its state and its events as one
    /// durable fact.
    ///
    /// # Errors
    ///
    /// Whatever `f` reports, or the blocking task fails.
    pub async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&DB) -> anyhow::Result<T> + Send + 'static,
    ) -> anyhow::Result<T> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || f(&db)).await?
    }

    /// Reads `key` from `family`.
    ///
    /// # Errors
    ///
    /// The family is missing from this database, or the read fails.
    pub async fn get(&self, family: Family, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let key = key.to_vec();
        self.run(move |db| Ok(db.get_cf(family.handle(db)?, key)?))
            .await
    }

    /// Writes `value` to `key` in `family`, synchronously.
    ///
    /// For a lone key. An apply writes its state and its events in one batch
    /// instead, so a crash cannot separate them.
    ///
    /// # Errors
    ///
    /// The family is missing from this database, or the write fails.
    pub async fn put(&self, family: Family, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.run(move |db| {
            let mut options = WriteOptions::default();
            options.set_sync(true);
            db.put_cf_opt(family.handle(db)?, key, value, &options)?;
            Ok(())
        })
        .await
    }
}
