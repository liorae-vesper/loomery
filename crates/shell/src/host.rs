// SPDX-License-Identifier: MPL-2.0

//! The runtime host: the wire between the control plane, the gateway and the
//! workers — everything `loomery-server` assembles.
//!
//! [`Host::boot`] does the long-lived wiring once:
//!
//! 1. it opens the **control group** (a fresh data directory is initialized with
//!    this node as its only voter), waits for a leader, and projects the tenant
//!    records into the [`Router`];
//! 2. it boots one **tenant group** per *active* tenant and registers them in a
//!    [`GroupTable`], which is what the gateway routes through;
//! 3. it warms the identity provider, so a misconfigured `IdP` fails at startup
//!    rather than on the first request;
//! 4. it builds the [`CommandPlane`] — the only path from untrusted input to
//!    consensus.
//!
//! [`Host::start_workers`] then spawns one **outbox worker** per tenant group (a
//! persisted cursor per group, so a restart resumes instead of republishing) and
//! the **saga runner**. [`Host::serve`] serves the gateway until shutdown.
//!
//! The seams are generic: the binary hands in the real NATS publisher/consumer
//! and the OIDC authenticator, while tests hand in fakes (or nothing at all,
//! which is a host with no broker).
//!
//! Known gap, stated rather than hidden: [`control::resume`] needs the original
//! genesis [`Bootstrap`] (its leader user id is not part of the tenant record),
//! so the host *reports* incomplete tenants instead of resuming them behind your
//! back. [`Host::resume_provisioning`] takes that bootstrap from an operator, and
//! storing it in the tenant record is the follow-up.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use loomery_core::id::Id;
use loomery_core::tenant::Replica;
use loomery_core::tenant::TenantState;
use loomery_genesis::Bootstrap;
use openraft::BasicNode;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::HostConfig;
use crate::control::Router;
use crate::control::incomplete;
use crate::control::provision;
use crate::control::resume;
use crate::gateway::Authenticator;
use crate::gateway::CommandPlane;
use crate::gateway::GroupRegistry;
use crate::gateway::router as gateway_router;
use crate::outbox::Publisher;
use crate::outbox::cursor::CursorStore;
use crate::outbox::worker::OutboxWorker;
use crate::outbox::worker::Reporter;
use crate::outbox::worker::WorkerEvent;
use crate::raft::RaftGroup;
use crate::saga::Consumer;
use crate::saga::InvitationAcceptance;
use crate::saga::SagaRunner;

/// How long boot waits for a leader before giving up.
pub const LEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the saga runner waits after a bus failure.
pub const SAGA_BACKOFF: Duration = Duration::from_millis(250);

/// The groups this host runs, keyed by group id.
///
/// A `DashMap` (not a frozen map) because provisioning adds groups while the
/// gateway is serving: [`GroupRegistry::group`] is called on every command.
#[derive(Default)]
pub struct GroupTable {
    groups: DashMap<String, RaftGroup>,
}

impl GroupTable {
    /// Registers a group, replacing any previous handle for that id.
    pub fn insert(&self, group_id: &str, group: RaftGroup) {
        self.groups.insert(group_id.to_owned(), group);
    }

    /// How many groups the host runs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// Whether the host runs no groups.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// The ids of the groups the host runs.
    #[must_use]
    pub fn ids(&self) -> Vec<String> {
        self.groups
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

impl GroupRegistry for GroupTable {
    fn group(&self, group_id: &str) -> Option<RaftGroup> {
        self.groups.get(group_id).map(|entry| entry.value().clone())
    }
}

/// A booted, served runtime host.
pub struct Host<A, P, C> {
    config: HostConfig,
    control: RaftGroup,
    groups: Arc<GroupTable>,
    router: Arc<Router>,
    plane: Arc<CommandPlane>,
    authenticator: Arc<A>,
    publisher: Option<Arc<P>>,
    consumer: Option<Arc<C>>,
    report: Reporter,
    shutdown: watch::Sender<bool>,
    workers: Vec<JoinHandle<()>>,
}

impl<A, P, C> Host<A, P, C>
where
    A: Authenticator + 'static,
    P: Publisher + 'static,
    C: Consumer + 'static,
{
    /// Boots the control group, the tenant groups and the command plane.
    ///
    /// `publisher` and `consumer` are `None` for a host with no broker: the
    /// gateway still serves, and no workers are started.
    ///
    /// # Errors
    ///
    /// An invalid configuration, an unopenable data directory or group, a
    /// control group that never elects a leader, or a tenant group that cannot be
    /// booted.
    pub async fn boot(
        config: HostConfig,
        authenticator: Arc<A>,
        publisher: Option<Arc<P>>,
        consumer: Option<Arc<C>>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        tokio::fs::create_dir_all(&config.data_dir).await?;

        let (control, router) = Self::boot_control(&config).await?;
        let groups = Arc::new(GroupTable::default());

        for (organization_id, tenant) in control.state_machine().tenants().await {
            if let Some(group_id) = tenant.group_id.as_deref()
                && tenant.is_active()
            {
                Self::boot_tenant(&config, &groups, group_id).await?;
            }
            router.apply(organization_id, &tenant);
        }

        let plane = Arc::new(CommandPlane::new(
            Arc::clone(&router),
            Arc::clone(&groups) as Arc<dyn GroupRegistry>,
            Arc::clone(&authenticator) as Arc<dyn Authenticator>,
            Duration::from_millis(config.http.ryw_hold_ms),
        ));

        let (shutdown, _) = watch::channel(false);
        let report: Reporter = Arc::new(|event: WorkerEvent| match event.detail {
            crate::outbox::worker::WorkerDetail::Published { messages, cursor } => {
                eprintln!(
                    "[outbox] {} published {messages} message(s) up to {}:{}",
                    event.group_id, cursor.log_index, cursor.position
                );
            }
            crate::outbox::worker::WorkerDetail::Failed { reason, backoff } => {
                eprintln!(
                    "[outbox] {} retrying in {:?}: {reason}",
                    event.group_id, backoff
                );
            }
        });

        Ok(Self {
            config,
            control,
            groups,
            router,
            plane,
            authenticator,
            publisher,
            consumer,
            report,
            shutdown,
            workers: Vec::new(),
        })
    }

    /// Boots (or recovers) the control group and projects its tenant records.
    async fn boot_control(config: &HostConfig) -> anyhow::Result<(RaftGroup, Arc<Router>)> {
        let directory = config.data_dir.join(&config.control_group);
        // A directory that does not exist yet is a new deployment: the group has
        // no log, so this node becomes its first voter. An existing directory is
        // recovery, and must *not* be initialized again.
        let fresh = !directory.exists();

        let control = RaftGroup::boot_persistent(
            config.node_id,
            config.control_group.clone(),
            &directory,
            config.group.clone(),
        )
        .await?;

        if fresh {
            control
                .raft()
                .initialize(BTreeMap::from([(config.node_id, BasicNode::default())]))
                .await?;
        }
        wait_for_leader(&control).await?;

        let router = Arc::new(Router::new());
        router.rebuild(&control.state_machine().tenants().await);

        Ok((control, router))
    }

    /// Boots (or recovers) one tenant group and registers it.
    async fn boot_tenant(
        config: &HostConfig,
        groups: &GroupTable,
        group_id: &str,
    ) -> anyhow::Result<RaftGroup> {
        if let Some(existing) = groups.group(group_id) {
            return Ok(existing);
        }

        let directory = config.data_dir.join(group_id);
        let fresh = !directory.exists();
        let group = RaftGroup::boot_persistent(
            config.node_id,
            group_id.to_owned(),
            &directory,
            config.group.clone(),
        )
        .await?;

        if fresh {
            group
                .raft()
                .initialize(BTreeMap::from([(config.node_id, BasicNode::default())]))
                .await?;
        }
        wait_for_leader(&group).await?;
        groups.insert(group_id, group.clone());

        Ok(group)
    }

    /// Provisions a new tenant: the placement record, genesis, activation and
    /// routing — then hosts its group.
    ///
    /// This is the operator path (`loomery-server` calls it for a new
    /// organization); [`Host::resume_provisioning`] finishes one whose genesis
    /// was interrupted.
    ///
    /// # Errors
    ///
    /// The group cannot be booted, or the control plane refuses the placement.
    pub async fn provision(
        &mut self,
        group_id: &str,
        replicas: &[Replica],
        bootstrap: &Bootstrap,
    ) -> anyhow::Result<()> {
        let mut group = Self::boot_tenant(&self.config, &self.groups, group_id).await?;
        provision(
            &mut self.control,
            &mut group,
            &self.router,
            group_id,
            replicas,
            bootstrap,
        )
        .await?;
        Ok(())
    }

    /// The gateway's router, ready to serve.
    pub fn router(&self) -> axum::Router {
        gateway_router(Arc::clone(&self.plane))
    }

    /// The groups this host runs.
    #[must_use]
    pub fn groups(&self) -> &GroupTable {
        &self.groups
    }

    /// The organizations the control group has recorded, and whether each is
    /// active.
    pub async fn tenants(&self) -> Vec<(Id, TenantState)> {
        self.control.state_machine().tenants().await
    }

    /// Tenants whose provisioning did not finish.
    ///
    /// Reported rather than resumed: the genesis bootstrap's leader user id is
    /// not part of the tenant record, so finishing one needs the operator's
    /// [`Bootstrap`] (see [`Host::resume_provisioning`]).
    pub async fn incomplete_tenants(&self) -> Vec<(Id, TenantState)> {
        incomplete(&self.control).await
    }

    /// Finishes one tenant's genesis from the operator's bootstrap, then routes
    /// it and boots its group.
    ///
    /// # Errors
    ///
    /// The tenant's group cannot be booted, or genesis/activation fails.
    pub async fn resume_provisioning(&mut self, bootstrap: &Bootstrap) -> anyhow::Result<()> {
        let group_id = self
            .control
            .state_machine()
            .tenants()
            .await
            .into_iter()
            .find(|(organization_id, _)| organization_id == &bootstrap.organization_id)
            .and_then(|(_, tenant)| tenant.group_id)
            .ok_or_else(|| anyhow::anyhow!("the organization has no tenant record"))?;

        let mut group = Self::boot_tenant(&self.config, &self.groups, &group_id).await?;
        resume(&mut self.control, &mut group, &self.router, bootstrap).await?;
        Ok(())
    }

    /// Spawns the workers: one outbox tailer per tenant group, plus the saga
    /// runner when a broker is configured.
    ///
    /// # Errors
    ///
    /// An outbox cursor that cannot be read, or a tenant group whose state
    /// machine is unavailable.
    pub async fn start_workers(&mut self) -> anyhow::Result<()> {
        let shutdown = self.shutdown.subscribe();

        if let Some(publisher) = self.publisher.clone() {
            for (organization_id, group_id) in self.active_groups().await {
                let directory = self.config.data_dir.join(&group_id);
                let group = self
                    .groups
                    .group(&group_id)
                    .ok_or_else(|| anyhow::anyhow!("group {group_id} is not hosted here"))?;
                let cursors = CursorStore::in_group_dir(&directory);
                let mut worker = OutboxWorker::resuming(
                    group_id.clone(),
                    organization_id,
                    group,
                    Arc::clone(&publisher),
                    cursors,
                )
                .await?;
                let report = Arc::clone(&self.report);
                let shutdown = shutdown.clone();
                self.workers.push(tokio::spawn(async move {
                    if let Err(error) = worker.run(shutdown, &report).await {
                        eprintln!("[outbox] {group_id} stopped: {error}");
                    }
                }));
            }
        }

        if let Some(consumer) = self.consumer.clone() {
            let runner = SagaRunner::new(
                Arc::clone(&consumer),
                InvitationAcceptance,
                Arc::clone(&self.groups) as Arc<dyn GroupRegistry>,
            );
            let mut shutdown = shutdown.clone();
            self.workers.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                return;
                            }
                        }
                        result = runner.run_once() => {
                            match result {
                                Ok(true) => {}
                                Ok(false) => tokio::time::sleep(SAGA_BACKOFF).await,
                                Err(error) => {
                                    eprintln!("[sagas] {error}");
                                    tokio::time::sleep(SAGA_BACKOFF).await;
                                }
                            }
                        }
                    }
                }
            }));
        }

        Ok(())
    }

    /// The `(organization_id, group_id)` of every active tenant this host runs.
    async fn active_groups(&self) -> Vec<(Id, String)> {
        self.control
            .state_machine()
            .tenants()
            .await
            .into_iter()
            .filter_map(|(organization_id, tenant)| match tenant.group_id {
                Some(group_id) if tenant.is_active() => Some((organization_id, group_id)),
                _ => None,
            })
            .collect()
    }

    /// Serves the gateway on the configured address until [`Host::shutdown`].
    ///
    /// # Errors
    ///
    /// The address cannot be bound, or the server fails.
    pub async fn serve(&self) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(&self.config.http.bind).await?;
        self.serve_on(listener).await
    }

    /// Serves the gateway on an already-bound listener.
    ///
    /// # Errors
    ///
    /// The server fails.
    pub async fn serve_on(&self, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
        let mut shutdown = self.shutdown.subscribe();

        axum::serve(listener, self.router())
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await?;

        Ok(())
    }

    /// Signals shutdown without waiting.
    ///
    /// What [`Host::serve`] and [`Host::serve_on`] return on: it is also what a
    /// test or an operator can use when it does not own the host.
    pub fn signal_shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Signals shutdown, waits for the workers, and closes every group.
    ///
    /// # Errors
    ///
    /// A group fails to shut down.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.signal_shutdown();

        for worker in self.workers {
            worker.abort();
        }
        for group_id in self.groups.ids() {
            if let Some(group) = self.groups.group(&group_id) {
                group.shutdown().await?;
            }
        }
        self.control.shutdown().await?;

        Ok(())
    }

    /// The authenticator the gateway uses.
    #[must_use]
    pub fn authenticator(&self) -> &Arc<A> {
        &self.authenticator
    }

    /// The router projection, for tests and operator tooling.
    #[must_use]
    pub fn routes(&self) -> &Arc<Router> {
        &self.router
    }

    /// A publisher-only host with no broker: the gateway serves, no workers run.
    ///
    /// # Errors
    ///
    /// The configuration is invalid or a group cannot be booted.
    pub async fn boot_without_broker(
        config: HostConfig,
        authenticator: Arc<A>,
    ) -> anyhow::Result<Self> {
        Self::boot(config, authenticator, None, None).await
    }
}

/// Builds the real NATS publisher and consumer for `config`.
///
/// The two halves share one stream definition: the publisher creates it, the
/// consumer adopts it and adds the durable subscription the saga runner pulls.
///
/// # Errors
///
/// The broker is unreachable, or the stream/consumer cannot be created.
#[cfg(feature = "nats")]
pub async fn connect_nats(
    config: &crate::config::NatsConfig,
) -> anyhow::Result<(
    Arc<crate::outbox::NatsPublisher>,
    Arc<crate::outbox::NatsConsumer>,
)> {
    let publisher = Arc::new(crate::outbox::NatsPublisher::connect_config(config).await?);
    let consumer = Arc::new(crate::outbox::NatsConsumer::connect(config).await?);
    Ok((publisher, consumer))
}

/// Waits for a group to have a leader.
async fn wait_for_leader(group: &RaftGroup) -> anyhow::Result<()> {
    group
        .raft()
        .wait(Some(LEADER_TIMEOUT))
        .metrics(|metrics| metrics.current_leader.is_some(), "a leader")
        .await?;
    Ok(())
}
