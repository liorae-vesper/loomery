// SPDX-License-Identifier: MPL-2.0

//! The pure, deterministic core of Loomery.
//!
//! This crate holds the domain model and the envelope machinery. It is
//! deliberately free of I/O, wall-clock reads, and randomness: identities
//! and timestamps are injected through the command envelope by the shell.
//!
//! See the architecture, contracts and decisions in `docs/design.md` at the
//! repository root.

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

pub mod actor;
pub mod aggregate;
pub mod dedup;
pub mod envelope;
pub mod error;
pub mod id;
pub mod key;
pub mod timestamp;
pub mod versioning;

/// The `Uuid` type this crate's API speaks in — [`key::Key::new`] takes a
/// namespace `&Uuid`.
///
/// Re-exported so callers declare namespaces with *this* version of the
/// `uuid` crate instead of adding their own dependency and risking two
/// incompatible copies in one graph.
pub use uuid::Uuid;
