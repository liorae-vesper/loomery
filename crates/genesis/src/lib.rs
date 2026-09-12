// SPDX-License-Identifier: MPL-2.0

//! The genesis script — the deterministic first deployment of a tenant group.
//!
//! A newly created group is born with its first three events committed
//! (① assign leader → ② create default workspace → ③ add Owner), attributed
//! to the control-plane bootstrap saga and keyed by *derived* identity, so a
//! crash mid-provisioning resumes without committing genesis twice.
//!
//! # Why derived identity (D12)
//!
//! Nothing here reads a clock or generates randomness. Every identity is a
//! [`Key`] (`UUIDv5`) derived from `(generation-namespace, organization, step)`
//! — including the default workspace's *entity* id, which goes through
//! [`Id::from`]. The resumed worker therefore proposes exactly the command the
//! crashed attempt proposed, which matters in the window where an event is
//! committed but its dedup entry is not recorded yet: a re-run is recognized
//! as the same creation. A minted [`Id::new`] per attempt could not do that —
//! the retry would be a *different* command, so the group would either grow a
//! second default workspace or reject the retry forever.
//!
//! The three identities have three different jobs (D12):
//!
//! - [`step_key`] — the *intent* of one command (①, ② or ③); a retry is a replay.
//! - [`bootstrap_correlation_key`] — the *workflow*, shared by all three steps,
//!   so the whole bootstrap can be grouped by it.
//! - [`default_workspace_id`] — the *entity* the workflow creates.
//!
//! Two rules keep this sound:
//!
//! - **The generation lives in the namespace, never in the derived data.** A
//!   binary upgraded mid-provisioning must derive the ids the previous attempt
//!   derived; a deliberate re-bootstrap means adding a new namespace constant
//!   (`V2`), after which the `V1` ids stay valid forever.
//! - **The strings below are the identity contract.** [`Step::slug`] and
//!   [`DEFAULT_WORKSPACE_NAME`] are hashed; changing one silently changes
//!   every id derived from it (the golden tests in this module will fail).
//!
//! Scaffold: the commands themselves are not built yet — see `docs/design.md`
//! §5 (Phase 1) and `docs/CONTINUE.md`.

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

use trellis_core::Uuid;
use trellis_core::actor::Actor;
use trellis_core::id::Id;
use trellis_core::key::Key;

/// The namespace of the `v1` genesis script.
///
/// RFC 4122 namespaces are how a generation is versioned: ids derived under a
/// namespace stay stable forever, so a new bootstrap generation adds a new
/// constant instead of mutating this one.
const GENESIS_NAMESPACE_V1: Uuid = Uuid::from_u128(0x8e3f_2a1b_9c4d_4e5f_a6b7_c8d9_e0f1_a2b3);

/// Human-readable version of the genesis script, for logs and diagnostics.
///
/// Informational only — deliberately **not** part of any derived key (see the
/// module docs).
pub const SCRIPT_VERSION: &str = "v1";

/// The name of the workspace genesis creates for every new organization.
pub const DEFAULT_WORKSPACE_NAME: &str = "General";

/// The saga identity every genesis event is attributed to.
#[must_use]
pub fn bootstrap_actor() -> Actor {
    Actor::Saga {
        user_id: None,
        name: "control-plane:Bootstrap".to_owned(),
    }
}

/// The genesis steps, in commit order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    /// ① Grant the organization's creator leadership.
    AssignLeader,
    /// ② Create the organization's default workspace.
    CreateWorkspace,
    /// ③ Add the creator as Owner of that workspace.
    AddOwner,
}

impl Step {
    /// Every step, in commit order.
    pub const ALL: [Step; 3] = [Self::AssignLeader, Self::CreateWorkspace, Self::AddOwner];

    /// The step's name inside a derived key.
    ///
    /// **Part of the identity contract:** changing a slug renames every key
    /// derived from it, so a resumed worker would stop recognizing the
    /// committed ones.
    #[must_use]
    pub const fn slug(self) -> &'static str {
        match self {
            Self::AssignLeader => "assign-leader",
            Self::CreateWorkspace => "create-workspace",
            Self::AddOwner => "add-owner",
        }
    }
}

/// The causation key for `step` of `organization_id`.
///
/// This is the command's idempotency identity: `(organization, step)` always
/// derives the same key, so a resumed worker is a replay rather than a second
/// genesis.
#[must_use]
pub fn step_key(organization_id: &Id, step: Step) -> Key {
    Key::new(
        &GENESIS_NAMESPACE_V1,
        &format!("{}:{}", organization_id, step.slug()),
    )
}

/// The correlation key of the whole bootstrap workflow for `organization_id`.
///
/// One value shared by all three steps, derived from the workflow's *business*
/// identity (the organization) rather than from per-attempt state — so the
/// worker re-derives it after a crash, and any consumer groups an
/// organization's genesis events by it. Contrast [`step_key`], which separates
/// the three intents of that one workflow.
#[must_use]
pub fn bootstrap_correlation_key(organization_id: &Id) -> Key {
    Key::new(
        &GENESIS_NAMESPACE_V1,
        &format!("{organization_id}:bootstrap"),
    )
}

/// The id of the default workspace genesis creates for `organization_id`.
///
/// A *derived* entity id: the workspace is a function of its organization
/// ("the one default workspace of org X"), so every resumer derives the same
/// id and a re-run cannot create a second one.
#[must_use]
pub fn default_workspace_id(organization_id: &Id) -> Id {
    Id::from(default_workspace_key(organization_id))
}

/// The default workspace's [`Key`] — the derivation behind
/// [`default_workspace_id`].
#[must_use]
pub fn default_workspace_key(organization_id: &Id) -> Key {
    Key::new(
        &GENESIS_NAMESPACE_V1,
        &format!("{organization_id}:default-workspace"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// A well-formed organization id for the derivations below.
    fn org() -> Id {
        Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9")
    }

    fn other_org() -> Id {
        Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f8")
    }

    #[test]
    fn steps_are_ordered_and_their_slugs_are_frozen() {
        let slugs: Vec<&str> = Step::ALL.iter().map(|step| step.slug()).collect();
        assert_eq!(slugs, ["assign-leader", "create-workspace", "add-owner"]);
    }

    #[test]
    fn step_keys_are_deterministic() {
        // The crash-resume property: re-deriving after a restart is identical.
        for step in Step::ALL {
            assert_eq!(step_key(&org(), step), step_key(&org(), step));
        }
    }

    #[test]
    fn every_step_and_organization_gets_its_own_key() {
        let mut seen: HashSet<Key> = HashSet::new();
        for organization in [org(), other_org()] {
            for step in Step::ALL {
                assert!(
                    seen.insert(step_key(&organization, step)),
                    "{step:?} collided"
                );
            }
        }
    }

    #[test]
    fn workspace_keys_are_disjoint_from_step_keys() {
        let organization = org();
        let mut seen: HashSet<Key> = Step::ALL
            .iter()
            .map(|step| step_key(&organization, *step))
            .collect();

        assert!(seen.insert(default_workspace_key(&organization)));
    }

    #[test]
    fn the_default_workspace_id_is_deterministic_and_unique_per_organization() {
        assert_eq!(default_workspace_id(&org()), default_workspace_id(&org()));
        assert_ne!(
            default_workspace_id(&org()),
            default_workspace_id(&other_org())
        );
    }

    #[test]
    fn the_default_workspace_id_is_a_canonical_uuid_v5() {
        let id = default_workspace_id(&org());

        assert_eq!(Uuid::parse_str(&id).unwrap().get_version_num(), 5);
        // It also survives the strict id parser every untrusted id goes through.
        assert_eq!(Id::parse(&id).unwrap(), id);
    }

    #[test]
    fn the_bootstrap_correlation_is_one_key_for_the_whole_workflow() {
        let organization = org();
        let correlation = bootstrap_correlation_key(&organization);

        // The workflow's identity is stable across attempts and shared by ①②③.
        assert_eq!(correlation, bootstrap_correlation_key(&organization));
        // Another organization is another workflow.
        assert_ne!(correlation, bootstrap_correlation_key(&other_org()));
    }

    #[test]
    fn the_correlation_is_not_any_of_the_step_keys() {
        let organization = org();
        let correlation = bootstrap_correlation_key(&organization);

        for step in Step::ALL {
            assert_ne!(correlation, step_key(&organization, step), "{step:?}");
            assert_ne!(correlation, default_workspace_key(&organization));
        }
    }

    /// The derivation is an identity contract: ids derived by an earlier
    /// attempt must stay derivable forever. These values are frozen.
    #[test]
    fn the_derivations_are_frozen() {
        let organization = org();

        assert_eq!(
            step_key(&organization, Step::AssignLeader).to_string(),
            "9aa3c832-e22b-5f88-9c0e-51b763ce007d"
        );
        assert_eq!(
            default_workspace_id(&organization).to_string(),
            "369c41d9-6b9f-5fab-a0d6-eca73b7fe5b7"
        );
        assert_eq!(
            bootstrap_correlation_key(&organization).to_string(),
            "7656f657-5a4f-51d8-b4fd-8962113d3b28"
        );
    }

    #[test]
    fn the_bootstrap_actor_is_the_control_plane_saga() {
        assert_eq!(
            bootstrap_actor(),
            Actor::Saga {
                user_id: None,
                name: "control-plane:Bootstrap".to_owned(),
            }
        );
    }

    #[test]
    fn the_script_version_is_informational() {
        // Guards the module-docs claim: the version is not hashed into any
        // identity, so ids are unaffected by it.
        assert_eq!(SCRIPT_VERSION, "v1");
        assert_eq!(DEFAULT_WORKSPACE_NAME, "General");
    }
}
