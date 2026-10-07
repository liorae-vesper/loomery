// SPDX-License-Identifier: MPL-2.0

//! The command plane: authenticate → authorize → pre-compute → mint → route →
//! propose.
//!
//! This is the only path from untrusted input to consensus. It
//!
//! 1. **authenticates** the bearer token ([`Authenticator`]);
//! 2. **authorizes** admin-only commands against the admin claim;
//! 3. **pre-computes** edge work (password hashing) so it never enters the log;
//! 4. **mints** the command's identity — a fresh causation key unless the client
//!    supplied a valid one, which is what makes a retry a replay (D12);
//! 5. **routes** to the organization's *active* group ([`Router`] +
//!    [`GroupRegistry`]) and proposes.
//!
//! [`CommandPlane::build_command`] is pure; [`CommandPlane::submit`] adds the
//! routing and the proposal, so the identity and pre-compute rules are testable
//! without a cluster.

use std::sync::Arc;
use std::time::Duration;

use loomery_core::Uuid;
use loomery_core::actor::Actor;
use loomery_core::envelope::Command;
use loomery_core::envelope::Payload;
use loomery_core::id::Id;
use loomery_core::key::Key;
use loomery_core::timestamp::Timestamp;

use crate::control::Router;
use crate::group::GroupOps;
use crate::group::ProposeOutcome;
use crate::raft::RaftGroup;

use super::identity::AuthError;
use super::identity::Authenticator;
use super::identity::Identity;
use super::identity::is_admin_only;
use super::identity::is_membership_exempt;
use super::precompute;
use super::precompute::PreComputeError;

/// The namespace gateway-minted causation keys derive from (D12). Changing it
/// re-derives every minted key, so it is versioned like a script.
const GATEWAY_NAMESPACE: Uuid = Uuid::from_u128(0x9a3c_5e17_6b28_4c9d_8e0f_1a2b_3c4d_5e6f);

/// A command as a client submits it.
#[derive(Debug, Clone)]
pub struct CommandRequest {
    /// The organization (and therefore the group) the command belongs to.
    pub organization_id: Id,
    /// The aggregate the command targets.
    pub aggregate_id: Id,
    /// The workspace scope, when the command has one.
    pub workspace_id: Option<Id>,
    /// The wire command type (`task.create`, `workspace.rename`, …).
    pub command_type: String,
    /// The command payload, as plain JSON (D10 validates it in the plan).
    pub payload: serde_json::Value,
    /// A client-supplied idempotency key (canonical `UUIDv5`); minted when absent.
    pub causation_id: Option<String>,
    /// A saga/workflow correlation key; defaults to the causation key.
    pub correlation_id: Option<String>,
    /// The bearer token, when the request carried one.
    pub token: Option<String>,
}

/// What happened to a submitted command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutcome {
    /// Appended or replayed, with the first event's log index.
    pub outcome: ProposeOutcome,
    /// The identity the command was submitted under.
    pub causation_key: Key,
}

/// A registry of the host's running groups, keyed by group id.
pub trait GroupRegistry: Send + Sync {
    /// The running group with this id, if this host hosts it.
    fn group(&self, group_id: &str) -> Option<RaftGroup>;
}

/// Why a command could not be submitted.
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// Authentication or authorization failed.
    #[error(transparent)]
    Auth(#[from] AuthError),
    /// The organization has no tenant record.
    #[error("the organization is not registered")]
    UnknownOrganization,
    /// The tenant exists but genesis has not completed.
    #[error("the tenant is still being provisioned")]
    NotActive,
    /// This host does not host the tenant's group.
    #[error("the tenant's group is not hosted here")]
    GroupUnavailable,
    /// A supplied id was not a canonical `UUIDv5` key.
    #[error("the supplied key is not a canonical UUIDv5")]
    InvalidKey,
    /// A causation key came back carrying a different intent (D12): a conflict,
    /// not a replay.
    #[error("the causation key was reused for a different command")]
    KeyReused,
    /// Edge pre-computation refused the payload.
    #[error(transparent)]
    PreCompute(#[from] PreComputeError),
    /// The payload could not be serialized for the envelope.
    #[error("the command payload could not be serialized")]
    Serialize(#[from] serde_json::Error),
    /// The proposal failed; the outcome is **unknown**, so a retry must re-read.
    #[error("the command could not be appended")]
    Propose(#[source] anyhow::Error),
}

/// The gateway's command plane.
pub struct CommandPlane {
    router: Arc<Router>,
    groups: Arc<dyn GroupRegistry>,
    authenticator: Arc<dyn Authenticator>,
    ryw_hold: Duration,
}

impl CommandPlane {
    /// Builds a command plane.
    #[must_use]
    pub fn new(
        router: Arc<Router>,
        groups: Arc<dyn GroupRegistry>,
        authenticator: Arc<dyn Authenticator>,
        ryw_hold: Duration,
    ) -> Self {
        Self {
            router,
            groups,
            authenticator,
            ryw_hold,
        }
    }

    /// The read-your-writes hold the HTTP layer applies to `X-Min-Index`.
    #[must_use]
    pub fn ryw_hold(&self) -> Duration {
        self.ryw_hold
    }

    /// Authenticates, authorizes, pre-computes and mints the command.
    ///
    /// Pure: it touches no group and performs no I/O. This is where the
    /// untrusted request becomes a well-formed [`Command`].
    ///
    /// # Errors
    ///
    /// [`CommandError::Auth`] when authentication or the admin claim fails,
    /// [`CommandError::InvalidKey`] for a malformed client key, and
    /// [`CommandError::PreCompute`] if edge hashing fails.
    pub async fn build_command(&self, request: CommandRequest) -> Result<Command, CommandError> {
        Ok(self.build_command_with_identity(request).await?.0)
    }

    /// [`CommandPlane::build_command`], also answering who was authenticated (the
    /// authorization step needs the admin claim, which the actor does not carry).
    async fn build_command_with_identity(
        &self,
        request: CommandRequest,
    ) -> Result<(Command, Identity), CommandError> {
        let identity = self
            .authenticator
            .authenticate(request.token.as_deref())
            .await?;

        if is_admin_only(&request.command_type) && !identity.is_admin {
            return Err(AuthError::Forbidden.into());
        }

        let causation_key = match &request.causation_id {
            Some(id) => Key::try_from(id.as_str()).map_err(|_| CommandError::InvalidKey)?,
            None => mint_key(),
        };
        let correlation_key = match &request.correlation_id {
            Some(id) => Key::try_from(id.as_str()).map_err(|_| CommandError::InvalidKey)?,
            None => causation_key.clone(),
        };

        let mut payload = request.payload;
        precompute::hash_password(&mut payload)?;

        let actor = Actor::User {
            id: identity.user_id.clone(),
        };

        Ok((
            Command {
                envelope_version: 1,
                id: Id::new(),
                aggregate_id: request.aggregate_id,
                organization_id: request.organization_id,
                workspace_id: request.workspace_id,
                occurred_at: Timestamp::now(),
                causation_key,
                correlation_key,
                actor,
                command_type: request.command_type,
                payload: Payload {
                    version: 1,
                    data: serde_json::to_string(&payload)?,
                },
            },
            identity,
        ))
    }

    /// Resolves the organization's **active** group on this host.
    ///
    /// # Errors
    ///
    /// [`CommandError::UnknownOrganization`] when there is no record,
    /// [`CommandError::NotActive`] while the tenant is still provisioning, and
    /// [`CommandError::GroupUnavailable`] when this host does not host it.
    pub fn group_for(&self, organization_id: &Id) -> Result<RaftGroup, CommandError> {
        let route = self
            .router
            .route(organization_id)
            .ok_or(CommandError::UnknownOrganization)?;

        if !route.active {
            return Err(CommandError::NotActive);
        }

        self.groups
            .group(&route.group_id)
            .ok_or(CommandError::GroupUnavailable)
    }

    /// Authenticates a caller without proposing anything — the read path.
    ///
    /// Reads must not be open: an organization's event log is tenant data. This
    /// checks *who* is calling; authorizing them against the organization's
    /// membership is the read-model work (see `docs/gateway.md`).
    ///
    /// # Errors
    ///
    /// [`CommandError::Auth`] when the token is missing or unknown.
    pub async fn authenticate(&self, token: Option<&str>) -> Result<Identity, CommandError> {
        Ok(self.authenticator.authenticate(token).await?)
    }

    /// Whether `identity` may touch `organization_id`'s group.
    ///
    /// The rule is one line of policy: an administrator may, everyone else must be
    /// assigned to the organization (the genesis leader is assigned by ①, invited
    /// users by the acceptance saga). It is deliberately *not* finer-grained —
    /// per-workspace roles are the read-model work.
    ///
    /// # Errors
    ///
    /// [`CommandError::Auth`] with [`AuthError::Forbidden`] when the caller is
    /// neither an administrator nor a member.
    pub async fn authorize(
        &self,
        organization_id: &Id,
        identity: &Identity,
        group: &RaftGroup,
    ) -> Result<(), CommandError> {
        if identity.is_admin
            || group
                .state_machine()
                .is_organization_member(organization_id, &identity.user_id)
                .await
        {
            return Ok(());
        }
        Err(CommandError::Auth(AuthError::Forbidden))
    }

    /// Routes the command to the organization's group and proposes it.
    ///
    /// # Errors
    ///
    /// As [`CommandPlane::build_command`] and [`CommandPlane::group_for`], plus
    /// [`CommandError::Propose`] (an **unknown** outcome — re-read before
    /// retrying).
    pub async fn submit(&self, request: CommandRequest) -> Result<CommandOutcome, CommandError> {
        let (command, identity) = self.build_command_with_identity(request).await?;
        let group = self.group_for(&command.organization_id)?;
        if !is_membership_exempt(&command.command_type) {
            self.authorize(&command.organization_id, &identity, &group)
                .await?;
        }
        let causation_key = command.causation_key.clone();
        let fingerprint = command.fingerprint();

        let mut group = group;
        let outcome = group
            .propose(command)
            .await
            .map_err(CommandError::Propose)?;

        // A replay whose recorded fingerprint differs is a reused key on a
        // different request — answer a conflict, never the recorded result.
        if let ProposeOutcome::Replayed {
            fingerprint: recorded,
            ..
        } = &outcome
            && recorded != &fingerprint
        {
            return Err(CommandError::KeyReused);
        }

        Ok(CommandOutcome {
            outcome,
            causation_key,
        })
    }
}

/// Mints a fresh causation key (D12): a random seed, derived into a `UUIDv5`.
fn mint_key() -> Key {
    Key::new(&GATEWAY_NAMESPACE, &Id::new().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::identity::Identity;
    use crate::gateway::identity::StaticAuthenticator;
    use loomery_core::tenant::Replica;
    use loomery_core::tenant::TenantState;
    use loomery_core::tenant::TenantStatus;
    use serde_json::json;

    /// A registry that hosts nothing.
    struct NoGroups;

    impl GroupRegistry for NoGroups {
        fn group(&self, _group_id: &str) -> Option<RaftGroup> {
            None
        }
    }

    /// A registry that hosts exactly one group.
    struct OneGroup {
        group_id: String,
        group: RaftGroup,
    }

    impl GroupRegistry for OneGroup {
        fn group(&self, group_id: &str) -> Option<RaftGroup> {
            (group_id == self.group_id).then(|| self.group.clone())
        }
    }

    fn command_plane(router: Arc<Router>, groups: Arc<dyn GroupRegistry>) -> CommandPlane {
        let authenticator = StaticAuthenticator::new()
            .with_token(
                "member",
                Identity {
                    user_id: Id::from("user-1"),
                    is_admin: false,
                },
            )
            .with_token(
                "admin",
                Identity {
                    user_id: Id::from("user-0"),
                    is_admin: true,
                },
            );
        CommandPlane::new(
            router,
            groups,
            Arc::new(authenticator),
            Duration::from_millis(50),
        )
    }

    fn command_request(command_type: &str, token: &str) -> CommandRequest {
        CommandRequest {
            organization_id: Id::from("org-1"),
            aggregate_id: Id::from("task-1"),
            workspace_id: Some(Id::from("ws-1")),
            command_type: command_type.to_owned(),
            payload: json!({ "title": "a task" }),
            causation_id: None,
            correlation_id: None,
            token: Some(token.to_owned()),
        }
    }

    fn active_router() -> Arc<Router> {
        let router = Router::new();
        router.apply(
            Id::from("org-1"),
            &TenantState {
                group_id: Some("tenant-1".to_owned()),
                replicas: vec![Replica {
                    node_id: 1,
                    address: "http://127.0.0.1:7001".to_owned(),
                }],
                leader_user_id: None,
                status: TenantStatus::Active,
            },
        );
        Arc::new(router)
    }

    #[tokio::test]
    async fn building_a_command_hashes_the_password_before_consensus() {
        let plane = command_plane(Arc::new(Router::new()), Arc::new(NoGroups));
        let mut request = command_request("user.provision", "member");
        request.payload = json!({ "display_name": "Ada", "password": "s3cret" });

        let command = plane.build_command(request).await.unwrap();

        assert!(command.payload.data.contains("password_hash"));
        assert!(!command.payload.data.contains("s3cret"));
    }

    #[tokio::test]
    async fn a_command_is_minted_with_a_causation_key_and_a_user_actor() {
        let plane = command_plane(Arc::new(Router::new()), Arc::new(NoGroups));

        let command = plane
            .build_command(command_request("task.create", "member"))
            .await
            .unwrap();

        assert_eq!(command.causation_key, command.correlation_key);
        assert_eq!(
            command.actor,
            Actor::User {
                id: Id::from("user-1")
            }
        );
        assert_eq!(command.command_type, "task.create");
    }

    #[tokio::test]
    async fn a_client_supplied_causation_key_is_validated() {
        let plane = command_plane(Arc::new(Router::new()), Arc::new(NoGroups));
        let key = Key::new(&Uuid::from_u128(1), "client-intent");

        let mut request = command_request("task.create", "member");
        request.causation_id = Some(key.to_string());
        assert_eq!(
            plane.build_command(request).await.unwrap().causation_key,
            key
        );

        let mut request = command_request("task.create", "member");
        request.causation_id = Some("not-a-key".to_owned());
        assert!(matches!(
            plane.build_command(request).await,
            Err(CommandError::InvalidKey)
        ));
    }

    #[tokio::test]
    async fn admin_only_commands_require_the_admin_claim() {
        let plane = command_plane(Arc::new(Router::new()), Arc::new(NoGroups));

        assert!(matches!(
            plane
                .build_command(command_request("organization.archive", "member"))
                .await,
            Err(CommandError::Auth(AuthError::Forbidden))
        ));
        assert!(
            plane
                .build_command(command_request("organization.archive", "admin"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_command_is_routed_to_the_tenants_active_group() {
        let mut group = RaftGroup::boot_single_node(1).await.unwrap();
        crate::test_support::assign_member(&mut group, &Id::from("org-1"), &Id::from("user-1"))
            .await;
        let router = active_router();
        let registry = Arc::new(OneGroup {
            group_id: "tenant-1".to_owned(),
            group,
        });
        let plane = command_plane(router, registry);

        let outcome = plane
            .submit(command_request("task.create", "member"))
            .await
            .unwrap();
        assert!(matches!(outcome.outcome, ProposeOutcome::Appended { .. }));

        // The event is in the group it was routed to.
        let target = plane.group_for(&Id::from("org-1")).unwrap();
        let events = target.committed_events(&Id::from("org-1")).await.unwrap();
        let created: Vec<&str> = events
            .iter()
            .filter(|event| event.event_type == "task.created")
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(created, ["task.created"], "the command landed in its group");
    }

    #[tokio::test]
    async fn unknown_and_inactive_tenants_are_refused() {
        let plane = command_plane(Arc::new(Router::new()), Arc::new(NoGroups));
        assert!(matches!(
            plane.submit(command_request("task.create", "member")).await,
            Err(CommandError::UnknownOrganization)
        ));

        let router = Arc::new(Router::new());
        router.apply(
            Id::from("org-1"),
            &TenantState {
                group_id: Some("tenant-1".to_owned()),
                replicas: Vec::new(),
                leader_user_id: None,
                status: TenantStatus::Registering,
            },
        );
        let plane = command_plane(router, Arc::new(NoGroups));
        assert!(matches!(
            plane.submit(command_request("task.create", "member")).await,
            Err(CommandError::NotActive)
        ));
    }

    #[tokio::test]
    async fn a_command_for_an_unhosted_group_is_refused() {
        let plane = command_plane(active_router(), Arc::new(NoGroups));
        assert!(matches!(
            plane.submit(command_request("task.create", "member")).await,
            Err(CommandError::GroupUnavailable)
        ));
    }
}
