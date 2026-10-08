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
//! 3. it **reconciles**: a tenant whose provisioning was interrupted is finished
//!    from state alone, because the record carries the genesis leader;
//! 4. it warms the identity provider (the caller does that) and builds the
//!    [`CommandPlane`] — the only path from untrusted input to consensus.
//!
//! [`Host::start_workers`] then spawns one **outbox worker** per tenant group (a
//! persisted cursor per group, so a restart resumes instead of republishing), the
//! **saga runner**, and the periodic reconciliation sweep. [`Host::serve`] serves
//! the gateway, including the admin-only provisioning route, until shutdown.
//!
//! The host is shared (`Arc<Host>`) and therefore uses interior mutability: the
//! control group sits behind an async mutex and the worker set behind a plain
//! one, so provisioning can happen while the gateway is serving.
//!
//! The seams are generic: the binary hands in the real NATS publisher/consumer
//! and the OIDC authenticator, while tests hand in fakes (or nothing at all,
//! which is a host with no broker).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use dashmap::DashMap;
use loomery_core::id::Id;
use loomery_core::tenant::Replica;
use loomery_core::tenant::TenantState;
use loomery_core::timestamp::Timestamp;
use loomery_genesis::Bootstrap;
use openraft::BasicNode;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::HostConfig;
use crate::control::Router;
use crate::control::bootstrap_for;
use crate::control::incomplete;
use crate::control::provision;
use crate::control::resume;
use crate::gateway::Authenticator;
use crate::gateway::CommandPlane;
use crate::gateway::GroupRegistry;
use crate::gateway::ProvisionError;
use crate::gateway::ProvisionFuture;
use crate::gateway::ProvisionRequest;
use crate::gateway::Provisioned;
use crate::gateway::Provisioner;
use crate::gateway::router_with_provisioner;
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

/// How often the reconciliation sweep looks for interrupted tenants.
pub const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

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

/// What one reconciliation pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Tenants whose genesis this pass finished.
    pub resumed: Vec<Id>,
    /// Tenants it could not finish, with the reason.
    pub skipped: Vec<(Id, String)>,
}

impl ReconcileReport {
    /// Whether the pass found nothing to do.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.resumed.is_empty() && self.skipped.is_empty()
    }
}

/// A booted, served runtime host.
pub struct Host<A, P, C> {
    config: HostConfig,
    control: Mutex<RaftGroup>,
    groups: Arc<GroupTable>,
    router: Arc<Router>,
    plane: Arc<CommandPlane>,
    authenticator: Arc<A>,
    publisher: Option<Arc<P>>,
    consumer: Option<Arc<C>>,
    report: Reporter,
    shutdown: watch::Sender<bool>,
    workers: StdMutex<Vec<JoinHandle<()>>>,
    /// The groups that already have an outbox worker.
    tailed: StdMutex<BTreeSet<String>>,
}

impl<A, P, C> Host<A, P, C>
where
    A: Authenticator + 'static,
    P: Publisher + 'static,
    C: Consumer + 'static,
{
    /// Boots the control group, hosts the tenants, reconciles and builds the
    /// command plane.
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

        let host = Self {
            config,
            control: Mutex::new(control),
            groups,
            router,
            plane,
            authenticator,
            publisher,
            consumer,
            report,
            shutdown,
            workers: StdMutex::new(Vec::new()),
            tailed: StdMutex::new(BTreeSet::new()),
        };

        // A restart finishes what a crash interrupted, from state alone.
        host.reconcile().await?;

        Ok(host)
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

    /// Boots (or recovers) one tenant group and registers it with this host.
    async fn host_group(&self, group_id: &str) -> anyhow::Result<RaftGroup> {
        Self::boot_tenant(&self.config, &self.groups, group_id).await
    }

    /// The single-node placement this host writes for a tenant it hosts.
    fn placement(&self) -> Vec<Replica> {
        vec![Replica {
            node_id: self.config.node_id,
            address: self.config.node_address.clone(),
        }]
    }

    /// Provisions a tenant: the placement record, genesis, activation, routing —
    /// and the group is hosted here, with its outbox tailed.
    ///
    /// Idempotent: every key and id is derived from the business tuple (D12), so
    /// replaying the same request answers the same ids.
    ///
    /// # Errors
    ///
    /// [`ProvisionError`]: a refused request is permanent, the rest is retryable.
    pub async fn provision_with(
        &self,
        request: ProvisionRequest,
    ) -> Result<Provisioned, ProvisionError> {
        let group_id = request
            .group_id
            .clone()
            .unwrap_or_else(|| default_group_id(&request.organization_id));

        // A group id is a NATS subject segment (D11), and this host derives one
        // when the caller does not.
        if group_id.trim().is_empty() || group_id.contains('.') {
            return Err(ProvisionError::Refused(
                "the group id must be a non-empty NATS subject segment".to_owned(),
            ));
        }

        let bootstrap = Bootstrap {
            organization_id: request.organization_id.clone(),
            leader_user_id: request.leader_user_id.clone(),
            occurred_at: Timestamp::now(),
        };

        let mut group = self
            .host_group(&group_id)
            .await
            .map_err(ProvisionError::Failed)?;
        {
            let mut control = self.control.lock().await;
            provision(
                &mut control,
                &mut group,
                &self.router,
                &group_id,
                &self.placement(),
                &bootstrap,
            )
            .await
            .map_err(ProvisionError::Failed)?;
        }
        self.ensure_outbox_worker(&request.organization_id, &group_id)
            .await
            .map_err(ProvisionError::Failed)?;

        Ok(Provisioned {
            organization_id: request.organization_id,
            group_id,
        })
    }

    /// Finishes one tenant's genesis from an operator-supplied bootstrap, then
    /// routes it and hosts its group.
    ///
    /// # Errors
    ///
    /// The tenant has no record or no group, its group cannot be booted, or
    /// genesis/activation fails.
    pub async fn resume_provisioning(&self, bootstrap: &Bootstrap) -> anyhow::Result<()> {
        let group_id = {
            let control = self.control.lock().await;
            control
                .state_machine()
                .tenants()
                .await
                .into_iter()
                .find(|(organization_id, _)| organization_id == &bootstrap.organization_id)
                .and_then(|(_, tenant)| tenant.group_id)
                .ok_or_else(|| anyhow::anyhow!("the organization has no tenant record"))?
        };

        let mut group = self.host_group(&group_id).await?;
        {
            let mut control = self.control.lock().await;
            resume(&mut control, &mut group, &self.router, bootstrap).await?;
        }
        self.ensure_outbox_worker(&bootstrap.organization_id, &group_id)
            .await?;

        Ok(())
    }

    /// Finishes every tenant whose provisioning did not complete.
    ///
    /// The genesis bootstrap comes from the tenant record, so this needs no
    /// operator input; a record that predates the leader field (or names none) is
    /// reported in [`ReconcileReport::skipped`] instead of being guessed at.
    ///
    /// # Errors
    ///
    /// A tenant group could not be booted. Individual failures inside genesis are
    /// retried by the next pass, because the placement is already recorded and
    /// genesis is idempotent.
    pub async fn reconcile(&self) -> anyhow::Result<ReconcileReport> {
        let mut report = ReconcileReport::default();

        for (organization_id, tenant) in self.incomplete_tenants().await {
            let Some(group_id) = tenant.group_id.clone() else {
                report.skipped.push((
                    organization_id,
                    "the tenant record names no group".to_owned(),
                ));
                continue;
            };
            let Some(bootstrap) = bootstrap_for(&organization_id, &tenant) else {
                report.skipped.push((
                    organization_id,
                    "the tenant record names no genesis leader; resume it with a bootstrap"
                        .to_owned(),
                ));
                continue;
            };

            let mut group = self.host_group(&group_id).await?;
            {
                let mut control = self.control.lock().await;
                resume(&mut control, &mut group, &self.router, &bootstrap).await?;
            }
            self.ensure_outbox_worker(&organization_id, &group_id)
                .await?;
            report.resumed.push(organization_id);
        }

        Ok(report)
    }

    /// The gateway's router, without the provisioning route.
    pub fn router(&self) -> axum::Router {
        crate::gateway::router(Arc::clone(&self.plane))
    }

    /// The groups this host runs.
    #[must_use]
    pub fn groups(&self) -> &GroupTable {
        &self.groups
    }

    /// The organizations the control group has recorded, and whether each is
    /// active.
    pub async fn tenants(&self) -> Vec<(Id, TenantState)> {
        self.control.lock().await.state_machine().tenants().await
    }

    /// Tenants whose provisioning did not finish.
    pub async fn incomplete_tenants(&self) -> Vec<(Id, TenantState)> {
        let control = self.control.lock().await;
        incomplete(&control).await
    }

    /// Starts the outbox worker for `group_id` if it is hosted and not already
    /// tailed.
    async fn ensure_outbox_worker(
        &self,
        organization_id: &Id,
        group_id: &str,
    ) -> anyhow::Result<()> {
        let Some(publisher) = self.publisher.clone() else {
            return Ok(());
        };
        if self
            .tailed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(group_id)
        {
            return Ok(());
        }

        let group = self
            .groups
            .group(group_id)
            .ok_or_else(|| anyhow::anyhow!("group {group_id} is not hosted here"))?;
        let cursors = CursorStore::in_group_dir(&self.config.data_dir.join(group_id));
        let mut worker = OutboxWorker::resuming(
            group_id.to_owned(),
            organization_id.clone(),
            group,
            publisher,
            cursors,
        )
        .await?;

        let report = Arc::clone(&self.report);
        let shutdown = self.shutdown.subscribe();
        let owner = group_id.to_owned();
        let task = tokio::spawn(async move {
            if let Err(error) = worker.run(shutdown, &report).await {
                eprintln!("[outbox] {owner} stopped: {error}");
            }
        });

        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(task);
        self.tailed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(group_id.to_owned());

        Ok(())
    }

    /// The `(organization_id, group_id)` of every active tenant this host runs.
    async fn active_groups(&self) -> Vec<(Id, String)> {
        self.tenants()
            .await
            .into_iter()
            .filter_map(|(organization_id, tenant)| match tenant.group_id {
                Some(group_id) if tenant.is_active() => Some((organization_id, group_id)),
                _ => None,
            })
            .collect()
    }

    /// Spawns the workers: one outbox tailer per active tenant group, the saga
    /// runner when a broker is configured, and the reconciliation sweep.
    ///
    /// # Errors
    ///
    /// An outbox cursor that cannot be read, or a tenant group whose state
    /// machine is unavailable.
    pub async fn start_workers(self: &Arc<Self>) -> anyhow::Result<()> {
        for (organization_id, group_id) in self.active_groups().await {
            self.ensure_outbox_worker(&organization_id, &group_id)
                .await?;
        }

        if let Some(consumer) = self.consumer.clone() {
            let runner = SagaRunner::new(
                Arc::clone(&consumer),
                InvitationAcceptance,
                Arc::clone(&self.groups) as Arc<dyn GroupRegistry>,
            );
            let mut shutdown = self.shutdown.subscribe();
            self.workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(tokio::spawn(async move {
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

        // The sweep that finishes provisioning a crash interrupted.
        let host = Arc::clone(self);
        let mut shutdown = self.shutdown.subscribe();
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                return;
                            }
                        }
                        _ = ticker.tick() => match host.reconcile().await {
                            Ok(report) if !report.is_empty() => eprintln!(
                                "[reconcile] resumed {} tenant(s), skipped {}",
                                report.resumed.len(),
                                report.skipped.len()
                            ),
                            Ok(_) => {}
                            Err(error) => eprintln!("[reconcile] {error}"),
                        },
                    }
                }
            }));

        Ok(())
    }

    /// The gateway's router, with `POST /organizations` wired to this host.
    ///
    /// Takes `&Arc<Self>` because the route holds the host: provisioning reaches
    /// the control group and the group table while the gateway is serving.
    pub fn router_with_provisioning(self: &Arc<Self>) -> axum::Router
    where
        Self: Provisioner,
    {
        router_with_provisioner(
            Arc::clone(&self.plane),
            Some(Arc::clone(self) as Arc<dyn Provisioner>),
        )
    }

    /// Serves the gateway on the configured address until [`Host::shutdown`].
    ///
    /// # Errors
    ///
    /// The address cannot be bound, or the server fails.
    pub async fn serve(self: &Arc<Self>) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(&self.config.http.bind).await?;
        self.serve_on(listener).await
    }

    /// Serves the gateway on an already-bound listener.
    ///
    /// # Errors
    ///
    /// The server fails.
    pub async fn serve_on(self: &Arc<Self>, listener: tokio::net::TcpListener) -> anyhow::Result<()>
    where
        Self: Provisioner,
    {
        let mut shutdown = self.shutdown.subscribe();

        axum::serve(listener, self.router_with_provisioning())
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

    /// Signals shutdown, stops the workers, and closes every group.
    ///
    /// Takes `&Arc<Self>` like the other runtime methods: the sweep task holds a
    /// handle to the host, so a consuming `self` signature could not be reached
    /// from the callers that own one.
    ///
    /// # Errors
    ///
    /// A group fails to shut down.
    pub async fn shutdown(self: &Arc<Self>) -> anyhow::Result<()> {
        self.signal_shutdown();

        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for worker in workers {
            worker.abort();
        }
        for group_id in self.groups.ids() {
            if let Some(group) = self.groups.group(&group_id) {
                // What the batching actually did, not what it was configured to do.
                // A batch limit above the offered concurrency never binds, and the
                // only place that shows is in these counters — so they get a line
                // rather than living in an accessor nobody reads.
                eprintln!("[batching] {group_id}: {}", group.writer().batch_stats());
                group.shutdown().await?;
            }
        }
        self.control.lock().await.shutdown().await?;

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

    /// A host with no broker: the gateway serves, no outbox workers run.
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

/// Provisions tenants on this host, for the gateway's admin route.
impl<A, P, C> Provisioner for Host<A, P, C>
where
    A: Authenticator + 'static,
    P: Publisher + 'static,
    C: Consumer + 'static,
{
    fn provision(&self, request: ProvisionRequest) -> ProvisionFuture<'_> {
        Box::pin(self.provision_with(request))
    }
}

/// The group id a tenant gets when the caller does not name one.
///
/// A UUID is a valid subject segment (D11), so the id is usable as a NATS token
/// without escaping.
#[must_use]
pub fn default_group_id(organization_id: &Id) -> String {
    format!("tenant-{organization_id}")
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
