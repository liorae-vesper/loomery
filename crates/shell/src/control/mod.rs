// SPDX-License-Identifier: MPL-2.0

//! The control plane: the control group's read models and tenant lifecycle.
//!
//! The control group holds the tenant-placement records
//! ([`loomery_core::tenant`]).

mod router;

pub use router::Route;
pub use router::Router;
