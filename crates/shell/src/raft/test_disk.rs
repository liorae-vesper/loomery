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
//!
//! A test boots a group through [`boot`] — and, when the boot belongs to product code
//! that a test cannot route through it, waits with [`wait_for_release`] — so the wait
//! cannot be forgotten at a call site. It *was* forgotten: `boot_persistent` called
//! directly after a `shutdown` lost this race in CI builds #9, #12 and #23.

use std::path::Path;
use std::time::Duration;

use tokio::time::Instant;

use super::RaftGroup;
use super::disk::Disk;
use crate::config::GroupConfig;
use crate::config::StorageConfig;

/// How long a reopen waits for the previous handle's release.
pub(crate) const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long between attempts while waiting.
pub(crate) const RELEASE_POLL: Duration = Duration::from_millis(50);

/// Whether an error is the store's lock being held, rather than a real failure.
///
/// The wait exists for one specific error, and it must not swallow others: a boot that
/// fails for any other reason is a failure, and retrying it only delays the report.
fn is_lock_held(error: &anyhow::Error) -> bool {
    error.to_string().to_lowercase().contains("lock")
}

/// Boots a persistent group, waiting out the release of a previous handle.
///
/// A test that boots a group again after `shutdown` is modelling a *new process*: there
/// the previous handle is simply gone, so waiting for its release is what a fresh
/// process sees. `Raft::shutdown` only *aborts* the tasks holding handles, and an abort
/// takes effect when the task is next polled, so the boot races that release. A store
/// still held after [`RELEASE_TIMEOUT`] is a real leak, and its error is returned rather
/// than retried away.
pub(crate) async fn boot(
    node_id: u64,
    group_id: &str,
    path: &Path,
    config: GroupConfig,
) -> anyhow::Result<RaftGroup> {
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    loop {
        match RaftGroup::boot_persistent(node_id, group_id.to_owned(), path, config.clone()).await {
            Ok(group) => return Ok(group),
            Err(error) if is_lock_held(&error) && Instant::now() < deadline => {
                eprintln!("group still held, retrying the boot: {error}");
                tokio::time::sleep(RELEASE_POLL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Waits until the store at `path` can be opened, then releases it again.
///
/// For a test that hands the boot to product code and so cannot route it through
/// [`boot`]. Opening is the only probe available: the lock is the release's observable
/// effect.
pub(crate) async fn wait_for_release(path: &Path, config: &StorageConfig) -> anyhow::Result<()> {
    drop(open(path, config).await?);
    Ok(())
}

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
/// is a real storage rejection, not a callback told to report one. A read-only open
/// must list every column family, so it shares the layout with [`Disk::open`].
pub(crate) async fn open_read_only(path: &Path) -> anyhow::Result<Disk> {
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    loop {
        let path = path.to_owned();
        let attempt = tokio::task::spawn_blocking(move || {
            rocksdb::DB::open_cf_descriptors_read_only(
                &rocksdb::Options::default(),
                path,
                Disk::descriptors(&StorageConfig::default()),
                false,
            )
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
