// SPDX-License-Identifier: MPL-2.0

//! Provisioning: the seam between the gateway's admin route and the host.
//!
//! The gateway knows *who* is calling (the [`CommandPlane`](super::CommandPlane)
//! authenticates and the admin claim authorizes) but nothing about running
//! groups, the control plane or genesis. [`Provisioner`] is the small port that
//! keeps it that way: `shell::host::Host` implements it, and a host that is not
//! wired for provisioning simply answers "unavailable".
//!
//! A provision request is **idempotent**: the control plane derives every key
//! and id from the business tuple (D12), so replaying the same request answers
//! "already there" rather than creating a second organization.

use std::future::Future;
use std::pin::Pin;

use loomery_core::id::Id;

/// A request to provision one tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionRequest {
    /// The organization to create.
    pub organization_id: Id,
    /// The user who owns the organization's genesis (its first member).
    pub leader_user_id: Id,
    /// The tenant group's id; the host derives one when absent.
    pub group_id: Option<String>,
}

/// The future a [`Provisioner`] returns.
///
/// Boxed because the gateway holds `Arc<dyn Provisioner>` and provisioning does
/// I/O (the control group proposes, genesis commits, a database opens).
pub type ProvisionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Provisioned, ProvisionError>> + Send + 'a>>;

/// Where a provisioned tenant ended up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provisioned {
    /// The organization that was provisioned.
    pub organization_id: Id,
    /// The group hosting it.
    pub group_id: String,
}

/// Why provisioning failed.
#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    /// This host is not wired for provisioning (no control plane or no groups).
    #[error("this host cannot provision tenants")]
    Unavailable,
    /// The request itself is wrong (bad ids or group id): retrying cannot help.
    #[error("the request was refused: {0}")]
    Refused(String),
    /// The control plane or genesis failed.
    ///
    /// The outcome may be partial — the placement is recorded before genesis
    /// runs, on purpose — so a retry is both safe and idempotent.
    #[error("provisioning failed")]
    Failed(#[source] anyhow::Error),
}

/// Provisions tenants on a host.
pub trait Provisioner: Send + Sync {
    /// Provisions one tenant, or answers that it already exists.
    ///
    /// # Errors
    ///
    /// A [`ProvisionError`]: refused requests are permanent, the rest are
    /// retryable.
    fn provision(&self, request: ProvisionRequest) -> ProvisionFuture<'_>;
}
