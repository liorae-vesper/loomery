// SPDX-License-Identifier: MPL-2.0
//! Durable `RocksDB` access, dispatched off Tokio's runtime threads.
use crate::config::StorageConfig;
use rocksdb::{BlockBasedOptions, Cache, DB, Options, WriteOptions};
use std::{path::Path, sync::Arc};

#[derive(Clone, Debug)]
pub(crate) struct Disk(pub Arc<DB>);
impl Disk {
    pub async fn open(path: &Path, config: &StorageConfig) -> anyhow::Result<Self> {
        let path = path.to_owned();
        let config = config.clone();
        tokio::task::spawn_blocking(move || {
            let mut options = Options::default();
            options.create_if_missing(true);
            options.set_write_buffer_size(config.write_buffer_bytes);
            options.set_max_write_buffer_number(config.max_write_buffers);
            options.set_max_background_jobs(config.max_background_jobs);
            options.set_max_open_files(config.max_open_files);
            options.set_compression_type(rocksdb::DBCompressionType::Lz4);
            let mut table = BlockBasedOptions::default();
            table.set_block_cache(&Cache::new_lru_cache(config.block_cache_bytes));
            options.set_block_based_table_factory(&table);
            Ok(Self(Arc::new(DB::open(&options, path)?)))
        })
        .await?
    }
    pub async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&DB) -> anyhow::Result<T> + Send + 'static,
    ) -> anyhow::Result<T> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || f(&db)).await?
    }
    pub async fn put(&self, key: &'static [u8], bytes: Vec<u8>) -> anyhow::Result<()> {
        self.run(move |db| {
            let mut options = WriteOptions::default();
            options.set_sync(true);
            db.put_opt(key, bytes, &options)?;
            Ok(())
        })
        .await
    }
    pub async fn get(&self, key: &'static [u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.run(move |db| Ok(db.get(key)?)).await
    }
}
