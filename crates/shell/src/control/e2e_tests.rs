// SPDX-License-Identifier: MPL-2.0

//! End-to-end onboarding acceptance (`design.md` §5, Phase 1 gate).
//!
//! **register org → genesis ①②③ → workspace + Owner**, with no duplicate
//! genesis when the same provisioning is retried. Written against the real
//! shell building blocks (control group, controller, tenant group, genesis
//! worker), not a fake.

use loomery_core::tenant::Replica;
use loomery_genesis::Step;
use loomery_genesis::bootstrap_actor;
use loomery_genesis::bootstrap_correlation_key;
use loomery_genesis::default_workspace_id;
use loomery_genesis::step_key;

use crate::control::Router;
use crate::control::provision;
use crate::group::GroupOps;
use crate::raft::RaftGroup;
use crate::test_support::bootstrap_value;

fn replicas() -> Vec<Replica> {
    vec![Replica {
        node_id: 1,
        address: "http://127.0.0.1:7001".to_owned(),
    }]
}

#[tokio::test]
async fn onboarding_is_born_with_a_workspace_and_an_owner() {
    let mut control = RaftGroup::boot_single_node(1).await.unwrap();
    let mut tenant = RaftGroup::boot_single_node(1).await.unwrap();
    let router = Router::new();
    let bootstrap = bootstrap_value();
    let organization = bootstrap.organization_id.clone();

    provision(
        &mut control,
        &mut tenant,
        &router,
        "tenant-1",
        &replicas(),
        &bootstrap,
    )
    .await
    .unwrap();

    let events = tenant.committed_events(&organization).await.unwrap();
    let genesis: Vec<_> = events
        .iter()
        .filter(|event| {
            Step::ALL
                .iter()
                .any(|step| event.causation_key == step_key(&organization, *step))
        })
        .collect();

    // 1. exactly three genesis events, in ①②③ order.
    assert_eq!(genesis.len(), 3);
    let types: Vec<&str> = genesis
        .iter()
        .map(|event| event.event_type.as_str())
        .collect();
    assert_eq!(
        types,
        [
            "organization.leader_assigned",
            "workspace.created",
            "membership.owner_added"
        ]
    );

    // 2. the workspace exists with its derived id, and the creator is its Owner.
    assert_eq!(
        genesis[1].workspace_id.as_deref(),
        Some(&*default_workspace_id(&organization))
    );
    assert_eq!(genesis[2].actor, bootstrap_actor());

    // 3. every genesis event carries the bootstrap actor and the
    //    organization's derived correlation key.
    for event in &genesis {
        assert_eq!(event.actor, bootstrap_actor());
        assert_eq!(
            event.correlation_key,
            bootstrap_correlation_key(&organization)
        );
    }

    // 4. the tenant is routable — and only because genesis completed.
    let route = router.route(&organization).unwrap();
    assert_eq!(route.group_id, "tenant-1");
    assert!(route.active);

    // 5. re-running provisioning never duplicates genesis.
    let again = provision(
        &mut control,
        &mut tenant,
        &router,
        "tenant-1",
        &replicas(),
        &bootstrap,
    )
    .await
    .unwrap();
    assert!(again.appended.is_empty());

    let events = tenant.committed_events(&organization).await.unwrap();
    for step in Step::ALL {
        let key = step_key(&organization, step);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.causation_key == key)
                .count(),
            1,
            "{step:?}"
        );
    }
}
