// SPDX-License-Identifier: MPL-2.0

//! `OpenRaft`'s own storage conformance suite, run against the in-memory store.
//!
//! [`Suite::test_all`] drives the log store and state machine the way `OpenRaft`
//! itself does — membership-in-log, purge/tail semantics, snapshot transfer,
//! re-applying committed-but-unapplied entries, vote round-trips — so the spike
//! is held to the crate's contract and not only to its own tests. This is where
//! a storage bug shows up before a cluster exists.

use std::future::Future;
use std::sync::Arc;

use openraft::StorageError;
use openraft::testing::StoreBuilder;
use openraft::testing::Suite;

use super::TypeConfig;
use super::log_store::MemLogStore;
use super::state_machine::MemStateMachine;

/// Builds a fresh, empty store + state machine for each suite case.
struct Builder;

impl StoreBuilder<TypeConfig, MemLogStore, Arc<MemStateMachine>, ()> for Builder {
    fn build(
        &self,
    ) -> impl Future<Output = Result<((), MemLogStore, Arc<MemStateMachine>), StorageError<u64>>> + Send
    {
        std::future::ready(Ok((
            (),
            MemLogStore::default(),
            Arc::new(MemStateMachine::default()),
        )))
    }
}

#[test]
fn the_in_memory_store_passes_the_openraft_suite() {
    Suite::<TypeConfig, MemLogStore, Arc<MemStateMachine>, Builder, ()>::test_all(Builder).unwrap();
}
