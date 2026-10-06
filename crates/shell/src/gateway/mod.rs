// SPDX-License-Identifier: MPL-2.0

//! The gateway: HTTP entry, identity, edge pre-compute and read-your-writes.
//!
//! The gateway is the only place untrusted input meets the system. It
//! authenticates the caller, enforces admin-only commands, runs edge
//! pre-computation (blocking-but-pure work such as password hashing, before a
//! command enters consensus), mints the command's identity, routes it to the
//! organization's group, and applies the read-your-writes gate to reads.
//!
//! See `docs/control-plane.md` for routing and `docs/shell.md` for the port.

mod ryw;

pub use ryw::RywOutcome;
pub use ryw::ensure_min_index;
