// SPDX-License-Identifier: MPL-2.0

//! The gateway: HTTP entry, identity, edge pre-compute and read-your-writes.
//!
//! The gateway is the only place untrusted input meets the system. It
//! authenticates the caller, enforces admin-only commands, runs edge
//! pre-computation (blocking-but-pure work such as password hashing, before a
//! command enters consensus), mints the command's identity, routes it to the
//! organization's group, and applies the read-your-writes gate to reads.
//!
//! * [`CommandPlane`] — the framework-agnostic command path.
//! * [`router`] — the axum adapter.
//! * [`ensure_min_index`] — the `X-Min-Index` gate.
//!
//! See `docs/control-plane.md` for routing and `docs/shell.md` for the port.

pub mod command;
pub mod http;
pub mod identity;
pub mod precompute;
pub mod ryw;

pub use command::CommandError;
pub use command::CommandOutcome;
pub use command::CommandPlane;
pub use command::CommandRequest;
pub use command::GroupRegistry;
pub use http::router;
pub use identity::AuthError;
pub use identity::Authenticator;
pub use identity::Identity;
pub use identity::StaticAuthenticator;
pub use identity::is_admin_only;
pub use precompute::PASSWORD_FIELD;
pub use precompute::PASSWORD_HASH_FIELD;
pub use precompute::PreComputeError;
pub use precompute::hash_password;
pub use precompute::verify_password;
pub use ryw::RywOutcome;
pub use ryw::ensure_min_index;
