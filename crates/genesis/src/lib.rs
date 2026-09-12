// SPDX-License-Identifier: MPL-2.0

//! The genesis script — the deterministic first deployment of a tenant group.
//!
//! A newly created group is born with its first three events committed
//! (assign leader → create default workspace → add Owner), attributed to the
//! control-plane bootstrap saga
//! (`Actor::Saga { user_id: None, name: "control-plane:Bootstrap" }`) and
//! keyed by deterministic causation, so a crash mid-provisioning resumes
//! without committing genesis twice.
//!
//! Nothing here reads a clock or generates randomness: identity comes from
//! [`trellis_core::id::Key`], which derives a stable `UUIDv5` from a
//! namespace plus data, and from ids injected through the command envelope.
//!
//! Scaffold: the script itself is not implemented yet — see
//! `docs/design.md` §5 (Phase 1) and `docs/CONTINUE.md`.
