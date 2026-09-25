// SPDX-License-Identifier: MPL-2.0

//! The imperative shell around the pure core: consensus, storage, the gateway,
//! and the workers that drive the pure scripts.

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
pub mod group;

#[cfg(test)]
pub(crate) mod test_support;
