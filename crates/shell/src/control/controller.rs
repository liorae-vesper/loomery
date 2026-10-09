// SPDX-License-Identifier: MPL-2.0

//! The tenant-creation controller.
//!
//! Creating a tenant is three durable steps plus a projection:
//!
//! 1. **Record the placement** in the control group (`tenant.register`), so a
//!    crash mid-provisioning leaves a reconcilable record;
//! 2. **Run genesis** on the tenant group (bootstrap 123);
//! 3. **Activate** the tenant in the control group (`tenant.activate`) — only
//!    once genesis committed;
//! 4. **Publish the route** by projecting the control group's records into the
//!    [`Router`].
//!
//! Steps 1–3 are idempotent by **derived identity** (D12): the causation key of
//! each control command is `(organization, action)`, so a retry replays instead
//! of duplicating, and `bootstrap::run` resumes from the tenant group's log.
//! The order matters at step 3/4: a tenant becomes routable only after genesis
//! completed, so a caller can never observe a half-born tenant.
//!
//! Booting the replica processes, registering them with the transport and
//! initializing membership are **host** concerns (see
//! `workpad/implementation.md`); this controller starts from an already booted,
//! initialized tenant group.

#[cfg(test)]
use std::future::Future;

use loomery_core::Uuid;
use loomery_core::actor::Actor;
use loomery_core::envelope::Command;
#[cfg(test)]
use loomery_core::envelope::Event;
use loomery_core::envelope::Payload;
use loomery_core::id::Id;
use loomery_core::key::Key;
use loomery_core::tenant;
use loomery_core::tenant::Replica;
use loomery_core::tenant::TenantState;
use loomery_core::timestamp::Timestamp;
use loomery_genesis::Bootstrap;

use crate::bootstrap;
use crate::bootstrap::Genesis;
use crate::group::GroupOps;
use crate::raft::RaftGroup;

use super::Router;

/// The namespace of control-plane command identities (D12). Changing it
/// re-derives every control causation key, so it is versioned like a script.
const CONTROL_NAMESPACE: Uuid = Uuid::from_u128(0x5f2b_8c41_7d3e_4a90_b1c2_d3e4_f506_1728);

/// The saga identity control-plane commands are attributed to.
const CONTROL_ACTOR: &str = "control-plane:TenantProvisioning";

/// Drives one tenant from registration to a published, active route.
///
/// `control` is the control group; `tenant` is a booted, initialized tenant
/// group; `bootstrap` carries the organization, leader and injected timestamp.
///
/// Returns the genesis run's outcome. Safe to call again after a crash: every
/// step is idempotent, and an already-active tenant only re-projects the route.
///
/// # Errors
///
/// A control-group failure, a genesis failure, or a rejected command.
pub async fn provision<G: GroupOps>(
    control: &mut RaftGroup,
    tenant: &mut G,
    router: &Router,
    group_id: &str,
    replicas: &[Replica],
    bootstrap: &Bootstrap,
) -> anyhow::Result<Genesis> {
    let organization_id = &bootstrap.organization_id;

    // 1. Record the placement before touching the tenant group, so a crash is
    //    reconcilable from the control log alone.
    if !registered(control, organization_id).await {
        let command = control_command(
            organization_id,
            "tenant-register",
            tenant::REGISTER,
            serde_json::to_string(&tenant::Register {
                group_id: group_id.to_owned(),
                replicas: replicas.to_vec(),
                leader_user_id: bootstrap.leader_user_id.clone(),
            })?,
        );
        control.propose(command).await?;
    }

    // 2. Genesis — the tenant group's own idempotent three-step bootstrap.
    let genesis = bootstrap::run(tenant, bootstrap).await?;

    // 3. Activation is the fence: traffic is allowed only after genesis.
    if !activated(control, organization_id).await {
        let command = control_command(
            organization_id,
            "tenant-activate",
            tenant::ACTIVATE,
            serde_json::to_string(&tenant::Activate {})?,
        );
        control.propose(command).await?;
    }

    // 4. Project the control log into the router.
    route(control, router).await;

    Ok(genesis)
}

/// Whether the control group already holds a tenant record for `organization_id`.
async fn registered(control: &RaftGroup, organization_id: &Id) -> bool {
    tenant_record(control, organization_id)
        .await
        .is_some_and(|record| record.is_registered())
}

/// Whether the control group already marks the tenant active.
async fn activated(control: &RaftGroup, organization_id: &Id) -> bool {
    tenant_record(control, organization_id)
        .await
        .is_some_and(|record| record.is_active())
}

/// The control group's current record for `organization_id`.
async fn tenant_record(control: &RaftGroup, organization_id: &Id) -> Option<TenantState> {
    control
        .state_machine()
        .tenants()
        .await
        .into_iter()
        .find(|(id, _)| id == organization_id)
        .map(|(_, record)| record)
}

/// Rebuilds the router from the control group's applied records.
async fn route(control: &RaftGroup, router: &Router) {
    router.rebuild(&control.state_machine().tenants().await);
}

/// Builds a control-plane command with a **derived** causation key (D12).
///
/// The envelope id and timestamp are per-attempt (minted here, shell-side); the
/// identity that makes a retry a replay is the derived causation key, so two
/// attempts at the same action propose equivalent intents.
fn control_command(
    organization_id: &Id,
    action: &str,
    command_type: &str,
    data: String,
) -> Command {
    Command {
        envelope_version: 1,
        id: Id::new(),
        aggregate_id: organization_id.clone(),
        organization_id: organization_id.clone(),
        workspace_id: None,
        occurred_at: Timestamp::now(),
        causation_key: derived_key(organization_id, action),
        correlation_key: derived_key(organization_id, "provisioning"),
        actor: Actor::Saga {
            user_id: None,
            name: CONTROL_ACTOR.to_owned(),
        },
        command_type: command_type.to_owned(),
        payload: Payload { version: 1, data },
    }
}

/// Derives a control-plane key from the organization and the action.
fn derived_key(organization_id: &Id, action: &str) -> Key {
    Key::new(&CONTROL_NAMESPACE, &format!("{organization_id}:{action}"))
}

/// The tenants the control group recorded but has not activated, in id order.
///
/// This is the startup reconciliation and retry sweep's work list: each entry
/// names a group whose genesis must be resumed (or whose activation was never
/// committed). An active or tombstoned tenant is not pending.
/// The genesis bootstrap a tenant record remembers.
///
/// Registration records the leader user id ([`loomery_core::tenant::Register`]),
/// so an interrupted provisioning can be resumed **from state**: the bootstrap is
/// `(organization, leader user, time)`, and the time affects no derived identity
/// (D12), which is why this is the original bootstrap in every way that matters.
///
/// `None` means the record predates the field (or the tenant has no record): its
/// genesis can only be resumed with an operator-supplied
/// [`Bootstrap`].
#[must_use]
pub fn bootstrap_for(organization_id: &Id, tenant: &TenantState) -> Option<Bootstrap> {
    tenant
        .leader_user_id
        .as_ref()
        .map(|leader_user_id| Bootstrap {
            organization_id: organization_id.clone(),
            leader_user_id: leader_user_id.clone(),
            occurred_at: Timestamp::now(),
        })
}

/// Tenants whose provisioning did not finish: registered, not yet active.
pub async fn incomplete(control: &RaftGroup) -> Vec<(Id, TenantState)> {
    let mut pending: Vec<(Id, TenantState)> = control
        .state_machine()
        .tenants()
        .await
        .into_iter()
        .filter(|(_, record)| record.is_registered() && !record.is_active())
        .collect();
    pending.sort_by(|(left, _), (right, _)| left.cmp(right));
    pending
}

/// Resumes one recorded-but-inactive tenant: completes genesis and activates
/// it.
///
/// The host has already reopened the tenant group's replicas (same databases,
/// recovered membership) and must **not** call `initialize` again; this
/// function only proposes the missing work. It is the same idempotent path as
/// [`provision`] minus the registration step.
///
/// # Errors
///
/// A genesis failure or a control-group failure.
pub async fn resume<G: GroupOps>(
    control: &mut RaftGroup,
    tenant: &mut G,
    router: &Router,
    bootstrap: &Bootstrap,
) -> anyhow::Result<Genesis> {
    let genesis = bootstrap::run(tenant, bootstrap).await?;

    if !activated(control, &bootstrap.organization_id).await {
        let command = control_command(
            &bootstrap.organization_id,
            "tenant-activate",
            tenant::ACTIVATE,
            serde_json::to_string(&tenant::Activate {})?,
        );
        control.propose(command).await?;
    }

    route(control, router).await;

    Ok(genesis)
}

/// A [`GroupOps`] tenant that always fails, for the "route is fenced behind
/// genesis" test.
#[cfg(test)]
struct FailingTenant;

#[cfg(test)]
impl GroupOps for FailingTenant {
    fn committed_events(
        &self,
        _organization_id: &Id,
    ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send {
        std::future::ready(Ok(Vec::new()))
    }

    fn propose(
        &mut self,
        _command: Command,
    ) -> impl Future<Output = anyhow::Result<crate::group::ProposeOutcome>> + Send {
        std::future::ready(Err(anyhow::anyhow!("genesis could not be committed")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::bootstrap_value;
    use loomery_genesis::Step;

    fn replicas() -> Vec<Replica> {
        vec![Replica {
            node_id: 1,
            address: "http://127.0.0.1:7001".to_owned(),
        }]
    }

    #[tokio::test]
    async fn provisioning_registers_runs_genesis_and_activates() {
        let mut control = RaftGroup::boot_single_node(1).await.unwrap();
        let mut tenant = RaftGroup::boot_single_node(1).await.unwrap();
        let router = Router::new();
        let bootstrap = bootstrap_value();

        let genesis = provision(
            &mut control,
            &mut tenant,
            &router,
            "tenant-1",
            &replicas(),
            &bootstrap,
        )
        .await
        .unwrap();

        assert_eq!(genesis.appended, Step::ALL);
        assert!(genesis.progress.is_complete());

        let route = router.route(&bootstrap.organization_id).unwrap();
        assert_eq!(route.group_id, "tenant-1");
        assert!(route.active);
    }

    #[tokio::test]
    async fn a_tenant_is_not_routable_before_genesis_completes() {
        let mut control = RaftGroup::boot_single_node(1).await.unwrap();
        let router = Router::new();
        let bootstrap = bootstrap_value();

        let result = provision(
            &mut control,
            &mut FailingTenant,
            &router,
            "tenant-1",
            &replicas(),
            &bootstrap,
        )
        .await;

        assert!(result.is_err());
        assert!(
            router.route(&bootstrap.organization_id).is_none(),
            "a tenant whose genesis failed must not be routable"
        );
    }

    #[tokio::test]
    async fn provisioning_is_idempotent() {
        let mut control = RaftGroup::boot_single_node(1).await.unwrap();
        let mut tenant = RaftGroup::boot_single_node(1).await.unwrap();
        let router = Router::new();
        let bootstrap = bootstrap_value();

        let first = provision(
            &mut control,
            &mut tenant,
            &router,
            "tenant-1",
            &replicas(),
            &bootstrap,
        )
        .await
        .unwrap();
        assert_eq!(first.appended, Step::ALL);

        // A retry appends nothing: the control commands replay and genesis is
        // already complete.
        let second = provision(
            &mut control,
            &mut tenant,
            &router,
            "tenant-1",
            &replicas(),
            &bootstrap,
        )
        .await
        .unwrap();
        assert!(second.appended.is_empty());
        assert!(router.route(&bootstrap.organization_id).unwrap().active);
    }

    #[tokio::test]
    async fn a_tenant_record_remembers_the_bootstrap_it_was_provisioned_with() {
        let mut control = RaftGroup::boot_single_node(1).await.unwrap();
        let mut tenant = RaftGroup::boot_single_node(1).await.unwrap();
        let router = Router::new();
        let bootstrap = bootstrap_value();

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

        let record = control
            .state_machine()
            .tenants()
            .await
            .into_iter()
            .find(|(organization_id, _)| organization_id == &bootstrap.organization_id)
            .map(|(_, record)| record)
            .expect("a tenant record");
        let rebuilt = bootstrap_for(&bootstrap.organization_id, &record).expect("a bootstrap");

        assert_eq!(rebuilt.organization_id, bootstrap.organization_id);
        assert_eq!(
            rebuilt.leader_user_id, bootstrap.leader_user_id,
            "genesis can be resumed without the original caller"
        );
    }

    #[tokio::test]
    async fn a_record_without_a_leader_cannot_rebuild_a_bootstrap() {
        let record = TenantState {
            group_id: Some("tenant-1".to_owned()),
            replicas: replicas(),
            leader_user_id: None,
            status: loomery_core::tenant::TenantStatus::Registering,
        };
        assert!(bootstrap_for(&Id::from("org-1"), &record).is_none());
    }

    #[tokio::test]
    async fn reconciliation_resumes_an_interrupted_tenant() {
        let mut control = RaftGroup::boot_single_node(1).await.unwrap();
        let mut tenant = RaftGroup::boot_single_node(1).await.unwrap();
        let router = Router::new();
        let bootstrap = bootstrap_value();

        // A previous controller recorded the placement and committed only 1.
        control
            .propose(control_command(
                &bootstrap.organization_id,
                "tenant-register",
                tenant::REGISTER,
                serde_json::to_string(&tenant::Register {
                    group_id: "tenant-1".to_owned(),
                    replicas: replicas(),
                    leader_user_id: bootstrap.leader_user_id.clone(),
                })
                .unwrap(),
            ))
            .await
            .unwrap();
        tenant
            .propose(bootstrap.command(Step::AssignLeader).unwrap())
            .await
            .unwrap();

        let pending = incomplete(&control).await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, bootstrap.organization_id);

        // Resuming picks up ②3 and activates; nothing is re-initialized.
        let genesis = resume(&mut control, &mut tenant, &router, &bootstrap)
            .await
            .unwrap();
        assert_eq!(genesis.appended, [Step::CreateWorkspace, Step::AddOwner]);
        assert!(incomplete(&control).await.is_empty());
        assert!(router.route(&bootstrap.organization_id).unwrap().active);
    }
}
