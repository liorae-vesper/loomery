// SPDX-License-Identifier: MPL-2.0

//! Per-aggregate upcast chains for frozen payloads — shared contract.
//!
//! Every aggregate owns the versions of its own payloads as a **closed enum**
//! (`KnownPayload`), maps runtime `(event_type, version)` pairs onto it via
//! `TryFrom`, and then `upcast`s with an **exhaustive match**. Adding a new
//! payload version means: add a variant → the compiler refuses to build until
//! both the `TryFrom` entry *and* the `upcast` arm land. Unknown pairs are a
//! first-class error, never a silent pass-through.
//!
//! Stored bytes are never rewritten:
//! frozen `V{n}` structs and their chains stay in the codebase forever so old
//! events decode forever.
//!
//! # Pattern (per aggregate, e.g. `crate::task`)
//!
//! ```ignore
//! pub enum KnownPayload {
//!     TaskCreatedV1,
//!     TaskCreatedV2, // new version ⇒ new variant ⇒ compiler forces the arms
//! }
//!
//! impl TryFrom<(&str, Version)> for KnownPayload {
//!     type Error = UpcastCode;
//!     fn try_from((event_type, version): (&str, Version)) -> Result<Self, Self::Error> {
//!         match (event_type, version) {
//!             ("task.created", 1) => Ok(Self::TaskCreatedV1),
//!             ("task.created", 2) => Ok(Self::TaskCreatedV2),
//!             _ => Err(UpcastCode::UnknownVersion),
//!         }
//!     }
//! }
//!
//! pub fn upcast(payload: &Payload) -> Result<Payload, DomainError<UpcastCode>> {
//!     // event_type is verified by the caller's KnownPayload lookup;
//!     // here only the version matters:
//!     let known = KnownPayload::try_from(("task.created", payload.version))?;
//!     match known {
//!         KnownPayload::TaskCreatedV1 => task_created_v1_to_v2(payload),
//!         KnownPayload::TaskCreatedV2 => Ok(payload.clone()), // latest = identity
//!     }
//! }
//! ```

use crate::envelope::Payload;
use crate::error::DomainError;

/// Error codes for payload upcasting.
#[derive(Debug, PartialEq)]
pub enum UpcastCode {
    /// No upcast chain is registered for this `(event_type, version)` pair.
    UnknownVersion,
    /// An upcast step failed to decode or transform the payload.
    UpcastFailed,
}

/// A single-step upcast transformation: frozen `V{n}` payload → next shape.
///
/// Pure data → data; the core never performs I/O, so every replica
/// transforms identically. Aggregate modules declare these as plain `fn`s
/// (e.g. `task_created_v1_to_v2`) and wire them into their exhaustive
/// `upcast` match.
pub type Upcaster = fn(payload: &Payload) -> Result<Payload, DomainError<UpcastCode>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_version_code_is_distinct_from_failed_chain() {
        assert_ne!(UpcastCode::UnknownVersion, UpcastCode::UpcastFailed);
    }
}
