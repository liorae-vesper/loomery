// SPDX-License-Identifier: MPL-2.0

//! Caller identity and the admin claim.
//!
//! The gateway authenticates every request and turns it into an [`Identity`]:
//! who is calling, and whether the system-admin claim is present (`design.md`
//! §4, the OIDC `groups` claim). The OIDC implementation is Phase-1 wiring; the
//! port is a trait so the default test suite stays self-contained
//! ([`StaticAuthenticator`] is the test/dev implementation).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use loomery_core::id::Id;
use loomery_core::invitation;
use loomery_core::membership;
use loomery_core::membership::Role;
use loomery_core::org;
use loomery_core::task;
use loomery_core::tenant;
use loomery_core::user;
use loomery_core::workspace;

/// Who is calling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The authenticated user.
    pub user_id: Id,
    /// Whether the caller carries the system-admin claim.
    pub is_admin: bool,
}

/// Why authentication or authorization failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// No bearer token was presented.
    #[error("missing credentials")]
    Missing,
    /// The token is not recognised.
    #[error("unknown credentials")]
    Unknown,
    /// The command requires an administrator.
    #[error("the command requires an administrator")]
    Forbidden,
}

/// The future an [`Authenticator`] returns.
///
/// Boxed because the gateway holds `Arc<dyn Authenticator>` and a real
/// authenticator (Keycloak) performs I/O.
pub type AuthFuture<'a> = Pin<Box<dyn Future<Output = Result<Identity, AuthError>> + Send + 'a>>;

/// Turns a bearer token into an [`Identity`].
pub trait Authenticator: Send + Sync {
    /// Authenticates a bearer token (`None` when the header is absent).
    ///
    /// # Errors
    ///
    /// [`AuthError::Missing`] or [`AuthError::Unknown`].
    fn authenticate(&self, token: Option<&str>) -> AuthFuture<'_>;
}

/// A fixed token table — for tests and local development only.
#[derive(Debug, Default, Clone)]
pub struct StaticAuthenticator {
    tokens: HashMap<String, Identity>,
}

impl StaticAuthenticator {
    /// Creates an empty authenticator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `token` as `identity`.
    #[must_use]
    pub fn with_token(mut self, token: impl Into<String>, identity: Identity) -> Self {
        self.tokens.insert(token.into(), identity);
        self
    }
}

impl Authenticator for StaticAuthenticator {
    fn authenticate(&self, token: Option<&str>) -> AuthFuture<'_> {
        let identity = token
            .ok_or(AuthError::Missing)
            .and_then(|token| self.tokens.get(token).cloned().ok_or(AuthError::Unknown));
        Box::pin(std::future::ready(identity))
    }
}

/// Whether `command_type` requires the admin claim.
///
/// Destructive or control-plane commands are admin-only; ordinary domain
/// commands are not. The list is explicit so adding an admin-only command is a
/// deliberate, reviewable change.
#[must_use]
pub fn is_admin_only(command_type: &str) -> bool {
    matches!(
        command_type,
        org::ARCHIVE | user::DEACTIVATE | tenant::REGISTER | tenant::ACTIVATE | tenant::TOMBSTONE
    )
}

/// The workspace role a command requires, in the workspace it names.
///
/// `None` means the command is not workspace-scoped: organization membership is
/// the whole gate. A scoped command that names **no** workspace is refused rather
/// than treated as unscoped, because there would be no role to check.
#[must_use]
pub fn required_workspace_role(command_type: &str) -> Option<Role> {
    match command_type {
        // Membership management and archiving are the owner's business.
        membership::ADD_OWNER
        | membership::ADD_MEMBER
        | membership::CHANGE_ROLE
        | membership::REMOVE_MEMBER
        | workspace::ARCHIVE => Some(Role::Owner),
        // Everyday work: a Member may, a Viewer may not (a Viewer reads).
        workspace::RENAME | task::CREATE | task::RENAME | task::COMPLETE | task::REOPEN => {
            Some(Role::Member)
        }
        _ => None,
    }
}

/// Whether a command requires owning the organization (or the admin claim).
///
/// Roles live on workspace memberships, so "organization administrator" means
/// owning at least one of its workspaces — which is what inviting people is.
#[must_use]
pub fn requires_organization_ownership(command_type: &str) -> bool {
    matches!(command_type, invitation::CREATE)
}

/// Commands a caller may submit **without belonging to the organization**.
///
/// Onboarding is the one flow that starts before membership: an invitee accepts
/// an invitation while they are still a stranger to the organization, and the
/// acceptance is what makes them a member (the invitation saga then assigns them).
/// Everything else needs membership or the admin claim — including every read.
///
/// Known limit, stated rather than baked in: the invitation plan trusts the
/// `user_id` in the accept payload, so it cannot yet check that the accepting
/// caller *is* the invited person. Binding those two (an email→user lookup, or
/// deriving the invitee from the authenticated actor) is the invitation work;
/// the exemption itself is deliberately one command wide.
#[must_use]
pub fn is_membership_exempt(command_type: &str) -> bool {
    matches!(command_type, invitation::ACCEPT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(admin: bool) -> Identity {
        Identity {
            user_id: Id::from("user-1"),
            is_admin: admin,
        }
    }

    #[tokio::test]
    async fn a_registered_token_authenticates() {
        let authenticator = StaticAuthenticator::new().with_token("t1", identity(false));
        assert_eq!(
            authenticator.authenticate(Some("t1")).await.unwrap(),
            identity(false)
        );
    }

    #[tokio::test]
    async fn missing_and_unknown_tokens_are_rejected() {
        let authenticator = StaticAuthenticator::new().with_token("t1", identity(false));
        assert_eq!(
            authenticator.authenticate(None).await,
            Err(AuthError::Missing)
        );
        assert_eq!(
            authenticator.authenticate(Some("nope")).await,
            Err(AuthError::Unknown)
        );
    }

    #[test]
    fn the_role_policy_is_explicit() {
        assert_eq!(
            required_workspace_role(membership::CHANGE_ROLE),
            Some(Role::Owner)
        );
        assert_eq!(
            required_workspace_role(workspace::ARCHIVE),
            Some(Role::Owner)
        );
        assert_eq!(required_workspace_role(task::CREATE), Some(Role::Member));
        assert_eq!(
            required_workspace_role(workspace::RENAME),
            Some(Role::Member)
        );
        assert_eq!(
            required_workspace_role("task.created"),
            None,
            "events are not commands"
        );
        assert!(requires_organization_ownership(invitation::CREATE));
        assert!(!requires_organization_ownership(invitation::ACCEPT));
    }

    #[test]
    fn membership_is_exempt_for_the_onboarding_command_only() {
        assert!(is_membership_exempt(invitation::ACCEPT));
        assert!(!is_membership_exempt(invitation::CREATE));
        assert!(!is_membership_exempt("task.create"));
        assert!(!is_membership_exempt(org::ARCHIVE));
    }

    #[test]
    fn admin_only_is_explicit_and_domain_commands_are_open() {
        assert!(is_admin_only(org::ARCHIVE));
        assert!(is_admin_only(user::DEACTIVATE));
        assert!(is_admin_only(tenant::TOMBSTONE));
        assert!(!is_admin_only("task.create"));
        assert!(!is_admin_only("workspace.create"));
    }
}
