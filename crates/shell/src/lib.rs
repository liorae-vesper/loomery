// SPDX-License-Identifier: MPL-2.0

//! The imperative shell around the pure core: consensus, storage, the gateway,
//! and the workers that drive the pure scripts.
//!
//! See `docs/shell.md` for the reference: the [`group::GroupOps`] port, the
//! [`bootstrap`] worker, and the in-process Raft group in [`raft`].

// Strict lints (unwrap/expect/panicking slicing/overflowing math) are denied in
// production code — test code may use them freely, via a single crate-level
// escape hatch active only under `cfg(test)`.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects
    )
)]

pub mod bootstrap;
pub mod config;
pub mod control;
pub mod gateway;
pub mod group;
pub mod outbox;
pub mod raft;

#[cfg(test)]
pub(crate) mod test_support;
