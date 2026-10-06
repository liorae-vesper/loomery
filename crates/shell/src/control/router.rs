// SPDX-License-Identifier: MPL-2.0

//! The router read model: `organization_id → group`.
//!
//! A **projection** of the control group's tenant records
//! ([`loomery_core::tenant`]). Rebuilding it is a pure function of that log, so
//! a restarted host replays the control group and gets the same table
//! ([`Router::rebuild`]); applying one record is what a live state machine
//! calls as it folds ([`Router::apply`]).
//!
//! The read model is a `DashMap` so the gateway can answer `route()` without a
//! lock while the control group keeps folding in the background.
//!
//! A tenant is routable once it has been *registered*; `Route::active` says
//! whether genesis completed and application traffic is allowed. A tombstoned
//! tenant is withdrawn from the table so a delayed worker cannot address it.

use dashmap::DashMap;
use loomery_core::id::Id;
use loomery_core::tenant::Replica;
use loomery_core::tenant::TenantState;
use loomery_core::tenant::TenantStatus;

/// Where a tenant's group lives, and whether it may carry traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// The tenant group's id (the transport routes by this).
    pub group_id: String,
    /// The group's intended replicas.
    pub replicas: Vec<Replica>,
    /// Whether genesis completed and the tenant may carry application traffic.
    pub active: bool,
}

/// The in-memory `organization_id → group` projection.
#[derive(Debug, Default)]
pub struct Router {
    routes: DashMap<Id, Route>,
}

impl Router {
    /// Creates an empty router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds the whole projection from the control group's tenant records.
    ///
    /// Idempotent: rebuilding from the same records yields the same table, so
    /// this is also the restart path.
    pub fn rebuild(&self, tenants: &[(Id, TenantState)]) {
        self.routes.clear();
        for (organization_id, tenant) in tenants {
            self.apply(organization_id.clone(), tenant);
        }
    }

    /// Applies one tenant record.
    ///
    /// An unregistered or tombstoned tenant is **withdrawn**; a registered one
    /// is upserted (so activation flips `active` in place).
    pub fn apply(&self, organization_id: Id, tenant: &TenantState) {
        match tenant.status {
            TenantStatus::Unregistered | TenantStatus::Tombstoned => {
                self.routes.remove(&organization_id);
            }
            TenantStatus::Registering | TenantStatus::Active => {
                let Some(group_id) = tenant.group_id.clone() else {
                    // A record without a group id cannot be routed; refuse to
                    // invent one.
                    self.routes.remove(&organization_id);
                    return;
                };

                self.routes.insert(
                    organization_id,
                    Route {
                        group_id,
                        replicas: tenant.replicas.clone(),
                        active: tenant.status == TenantStatus::Active,
                    },
                );
            }
        }
    }

    /// The route for an organization, if it is registered and not tombstoned.
    #[must_use]
    pub fn route(&self, organization_id: &Id) -> Option<Route> {
        self.routes.get(organization_id).map(|route| route.clone())
    }

    /// How many organizations are currently routable.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Whether no organization is routable.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loomery_core::tenant::Replica;
    use loomery_core::tenant::TenantStatus;

    fn organization(value: &str) -> Id {
        Id::from(value)
    }

    fn replica(node_id: u64) -> Replica {
        Replica {
            node_id,
            address: format!("http://127.0.0.1:{}", 7000 + node_id),
        }
    }

    fn tenant(group_id: &str, status: TenantStatus) -> TenantState {
        TenantState {
            group_id: Some(group_id.to_owned()),
            replicas: vec![replica(1)],
            status,
        }
    }

    #[test]
    fn a_registered_tenant_becomes_routable_and_activation_flips_the_flag() {
        let router = Router::new();
        let org = organization("org-1");

        router.apply(org.clone(), &tenant("tenant-1", TenantStatus::Registering));
        let route = router.route(&org).unwrap();
        assert_eq!(route.group_id, "tenant-1");
        assert_eq!(route.replicas.len(), 1);
        assert!(!route.active);

        router.apply(org.clone(), &tenant("tenant-1", TenantStatus::Active));
        assert!(router.route(&org).unwrap().active);
        assert_eq!(router.len(), 1);
    }

    #[test]
    fn a_tombstoned_tenant_is_withdrawn() {
        let router = Router::new();
        let org = organization("org-1");
        router.apply(org.clone(), &tenant("tenant-1", TenantStatus::Active));

        router.apply(org.clone(), &tenant("tenant-1", TenantStatus::Tombstoned));

        assert!(router.route(&org).is_none());
        assert!(router.is_empty());
    }

    #[test]
    fn an_unregistered_or_group_less_record_is_not_routable() {
        let router = Router::new();
        let org = organization("org-1");

        router.apply(
            org.clone(),
            &TenantState {
                group_id: Some("tenant-1".to_owned()),
                replicas: vec![replica(1)],
                status: TenantStatus::Unregistered,
            },
        );
        assert!(router.route(&org).is_none());

        router.apply(
            org.clone(),
            &TenantState {
                group_id: None,
                replicas: Vec::new(),
                status: TenantStatus::Registering,
            },
        );
        assert!(router.route(&org).is_none());
    }

    #[test]
    fn rebuilding_from_the_same_records_is_idempotent() {
        let records = vec![
            (
                organization("org-1"),
                tenant("tenant-1", TenantStatus::Active),
            ),
            (
                organization("org-2"),
                tenant("tenant-2", TenantStatus::Registering),
            ),
            (
                organization("org-3"),
                tenant("tenant-3", TenantStatus::Tombstoned),
            ),
        ];

        let router = Router::new();
        router.rebuild(&records);
        let first: Vec<Route> = ["org-1", "org-2", "org-3"]
            .iter()
            .filter_map(|org| router.route(&organization(org)))
            .collect();

        router.rebuild(&records);
        let second: Vec<Route> = ["org-1", "org-2", "org-3"]
            .iter()
            .filter_map(|org| router.route(&organization(org)))
            .collect();

        assert_eq!(first, second);
        assert_eq!(router.len(), 2, "the tombstoned tenant is withdrawn");
        assert!(router.route(&organization("org-1")).unwrap().active);
        assert!(!router.route(&organization("org-2")).unwrap().active);
    }
}
