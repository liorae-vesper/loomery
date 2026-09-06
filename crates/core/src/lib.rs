//! The pure, deterministic core of Trellis.
//!
//! This crate holds the domain model and the envelope machinery. It is
//! deliberately free of I/O, wall-clock reads, and randomness: identities
//! and timestamps are injected through the command envelope by the shell.
//!
//! See the architecture, contracts and decisions in `docs/design.md` at the
//! repository root.

#![warn(missing_docs)]

pub mod actor;
pub mod envelope;
pub mod id;
pub mod timestamp;
