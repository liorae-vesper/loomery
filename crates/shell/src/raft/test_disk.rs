// SPDX-License-Identifier: MPL-2.0

//! Opening a group's database again in tests.
//!
//! A `RocksDB` database is released when the last handle to it goes away. The
//! state machine owns one handle, and `Raft::shutdown` only *aborts* the tasks
//! holding others — an abort takes effect when the task is next polled, so a
//! reopen races the release. It is a race a test can lose under load (it flaked in
//! CI, not locally), so the wait here is explicit and bounded: a handle still held
//! after [`RELEASE_TIMEOUT`] is a real leak, and its error is returned rather than
//! retried away.
//!
//! The rule itself is pinned by
//! `persistent_tests::shutdown_releases_database_only_after_state_handles_are_dropped`.

use std::path::Path;
use std::time::Duration;

use tokio::time::Instant;

use super::disk::Disk;
use crate::config::StorageConfig;

/// How long a reopen waits for the previous handle's release.
pub(crate) const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long between attempts while waiting.
pub(crate) const RELEASE_POLL: Duration = Duration::from_millis(50);

/// Opens the database for reading and writing, waiting out the release.
pub(crate) async fn open(path: &Path, config: &StorageConfig) -> anyhow::Result<Disk> {
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    loop {
        match Disk::open(path, config).await {
            Ok(disk) => return Ok(disk),
            Err(error) if Instant::now() < deadline => {
                eprintln!("database still held, retrying: {error}");
                tokio::time::sleep(RELEASE_POLL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Opens the database **read only**, so every write is rejected by `RocksDB`
/// itself rather than by a mocked seam.
///
/// This is how the interruption tests inject a durable write failure: the failure
/// is a real storage rejection, not a callback told to report one.
pub(crate) async fn open_read_only(path: &Path) -> anyhow::Result<Disk> {
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    loop {
        let path = path.to_owned();
        let attempt = tokio::task::spawn_blocking(move || {
            rocksdb::DB::open_for_read_only(&rocksdb::Options::default(), path, false)
        })
        .await?;
        match attempt {
            Ok(database) => return Ok(Disk(std::sync::Arc::new(database))),
            Err(error) if Instant::now() < deadline => {
                eprintln!("database still held, retrying: {error}");
                tokio::time::sleep(RELEASE_POLL).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}
