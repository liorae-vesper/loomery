// SPDX-License-Identifier: MPL-2.0

//! A stress harness for the `test-services` adapters.
//!
//! It drives the **real** Keycloak and NATS `JetStream` services through the
//! shell's own code paths, at a scale the integration suite does not:
//!
//! * `auth` — concurrent `userinfo` authentication and password-grant token
//!   acquisition, asserting that no identity ever crosses under load;
//! * `outbox` — Raft propose → apply → outbox → `JetStream`, then a **crash
//!   replay** (fresh outboxes, cursors at the start) that the broker's
//!   `Nats-Msg-Id` dedup window must absorb;
//! * `e2e` — the same, but every command goes through the command plane with a
//!   real Keycloak authentication per command, so the run also asserts that the
//!   per-user actor recorded in the log matches the token that submitted it.
//!
//! Every scenario asserts its invariants and exits non-zero when one fails; the
//! last stdout line is `JSON {...}` with the measurements.
//!
//! ```sh
//! mise run svc-up
//! LOOMERY_STRESS_SCENARIO=e2e \
//! LOOMERY_TEST_NATS_URL=nats://127.0.0.1:4222 \
//! LOOMERY_TEST_KEYCLOAK_URL=http://127.0.0.1:8080 \
//!   cargo run --release -p loomery-shell --features test-services --example services_stress
//! ```
//!
//! Method, knobs, measured results and limits: `workpad/benchmarks/services-stress.md`.
//! `scripts/bench-services-stress.py` (`mise run bench-services-stress`) runs the
//! scenarios in release mode and collects the JSON.
#![allow(clippy::cast_precision_loss)] // Presentation statistics only; raw samples stay in microseconds.

#[cfg(not(feature = "test-services"))]
fn main() {
    eprintln!(
        "services_stress needs the Keycloak and NATS adapters: \
         cargo run --features test-services --example services_stress \
         (see workpad/benchmarks/services-stress.md)"
    );
    std::process::exit(2);
}

#[cfg(feature = "test-services")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stress::run().await
}

#[cfg(feature = "test-services")]
mod stress {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::time::Instant;

    use loomery_core::Uuid;
    use loomery_core::actor::Actor;
    use loomery_core::envelope::Command;
    use loomery_core::envelope::Payload;
    use loomery_core::id::Id;
    use loomery_core::key::Key;
    use loomery_core::tenant::Replica;
    use loomery_core::tenant::TenantState;
    use loomery_core::tenant::TenantStatus;
    use loomery_core::timestamp::Timestamp;
    use loomery_shell::control::Router;
    use loomery_shell::gateway::AuthError;
    use loomery_shell::gateway::Authenticator;
    use loomery_shell::gateway::CommandPlane;
    use loomery_shell::gateway::CommandRequest;
    use loomery_shell::gateway::GroupRegistry;
    use loomery_shell::gateway::KeycloakAuthenticator;
    use loomery_shell::group::GroupOps;
    use loomery_shell::outbox::NatsPublisher;
    use loomery_shell::outbox::Outbox;
    use loomery_shell::raft::RaftGroup;
    use serde::Serialize;
    use serde_json::Value;
    use serde_json::json;

    /// The namespace every synthetic identity derives from.
    const NS: Uuid = Uuid::from_u128(0x5c1a_9f43_2e78_4b06_8d17_a2c3_b4d5_e6f7);

    /// The test realm's users, as `(username, password)`.
    const USERS: [(&str, &str); 2] = [("ada", "ada"), ("admin", "admin")];

    /// The command every scenario submits.
    const COMMAND: &str = "task.create";

    /// The event it produces.
    const EVENT: &str = "task.created";

    // ---------------------------------------------------------------- settings

    /// A required service setting.
    fn required(name: &str) -> anyhow::Result<String> {
        std::env::var(name).map_err(|_| {
            anyhow::anyhow!("{name} is not set: start the services with `mise run svc-up`")
        })
    }

    /// An optional setting with a default.
    fn text(name: &str, default: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| default.to_owned())
    }

    /// An optional integer setting with a default.
    fn count(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    // ------------------------------------------------------------- measurement

    /// Latency samples in microseconds.
    #[derive(Default)]
    struct Latencies {
        samples: Mutex<Vec<u64>>,
    }

    impl Latencies {
        /// Records one observation.
        fn record(&self, elapsed: Duration) {
            let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
            self.samples
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(micros);
        }

        /// The samples, sorted ascending.
        fn sorted(&self) -> Vec<u64> {
            let mut samples = self
                .samples
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            samples.sort_unstable();
            samples
        }
    }

    /// Nearest-rank percentile, so no sample is ever discarded.
    fn percentile(sorted: &[u64], percentage: usize) -> Option<u64> {
        let rank = sorted.len().checked_mul(percentage)?.div_ceil(100);
        sorted.get(rank.checked_sub(1)?).copied()
    }

    /// Throughput and percentiles for one measured batch.
    #[derive(Serialize)]
    struct Stats {
        ops: usize,
        seconds: f64,
        per_second: f64,
        min_us: Option<u64>,
        p50_us: Option<u64>,
        p95_us: Option<u64>,
        p99_us: Option<u64>,
        max_us: Option<u64>,
    }

    impl Stats {
        /// Summarizes `samples` collected over `elapsed`.
        fn new(samples: &[u64], elapsed: Duration) -> Self {
            let seconds = elapsed.as_secs_f64();
            Self {
                ops: samples.len(),
                seconds,
                per_second: if seconds == 0.0 {
                    0.0
                } else {
                    samples.len() as f64 / seconds
                },
                min_us: samples.first().copied(),
                p50_us: percentile(samples, 50),
                p95_us: percentile(samples, 95),
                p99_us: percentile(samples, 99),
                max_us: samples.last().copied(),
            }
        }
    }

    /// What one worker observed, merged across workers at the end.
    #[derive(Default)]
    struct Tally {
        classes: [usize; 3],
        mismatches: Vec<String>,
        failures: Vec<String>,
    }

    impl Tally {
        /// Folds another worker's tally in.
        fn merge(&mut self, other: Self) {
            for (slot, value) in self.classes.iter_mut().zip(other.classes) {
                *slot = slot.saturating_add(value);
            }
            self.mismatches.extend(other.mismatches);
            self.failures.extend(other.failures);
        }

        /// Adds one observation to a class.
        fn count(&mut self, class: usize) {
            if let Some(slot) = self.classes.get_mut(class) {
                *slot = slot.saturating_add(1);
            }
        }

        /// Fails the run if anything unexpected happened.
        fn check(&self) -> anyhow::Result<()> {
            anyhow::ensure!(
                self.failures.is_empty(),
                "{} call(s) failed, first: {}",
                self.failures.len(),
                self.failures.first().map_or("", String::as_str)
            );
            anyhow::ensure!(
                self.mismatches.is_empty(),
                "{} call(s) returned the wrong result, first: {}",
                self.mismatches.len(),
                self.mismatches.first().map_or("", String::as_str)
            );
            Ok(())
        }
    }

    // ------------------------------------------------------------------ shared

    /// The synthetic organization id of scenario group `index`.
    fn organization(index: usize) -> Id {
        Id::from(Key::new(
            &NS,
            &format!("stress-organization-{}", index.rem_euclid(1_000)),
        ))
    }

    /// A suffix unique to this process, so repeated runs never collide in the
    /// broker's dedup window (the outbox identity is `<group>:<log>:e<pos>`).
    fn run_tag() -> &'static str {
        static TAG: OnceLock<String> = OnceLock::new();
        TAG.get_or_init(|| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_nanos());
            format!("{}-{nanos}", std::process::id())
        })
    }

    /// The tenant group id of scenario group `index`.
    fn group_id(index: usize) -> String {
        format!("stress-{}-{}", run_tag(), index.rem_euclid(1_000))
    }

    /// The Raft node id of scenario group `index`.
    fn node_id(index: usize) -> u64 {
        u64::try_from(index).unwrap_or(0).saturating_add(1)
    }

    /// One intended replica, as a routing record needs.
    fn replica(index: usize) -> Replica {
        Replica {
            node_id: node_id(index),
            address: format!(
                "http://127.0.0.1:{}",
                7000_u64.saturating_add(node_id(index))
            ),
        }
    }

    /// Hosts every group the harness booted.
    struct Groups {
        groups: HashMap<String, RaftGroup>,
    }

    impl GroupRegistry for Groups {
        fn group(&self, group_id: &str) -> Option<RaftGroup> {
            self.groups.get(group_id).cloned()
        }
    }

    /// Assigns `user_id` to `organization_id`, as the invitation saga does: the
    /// stream id is derived from the business tuple (D12), so the gateway's
    /// membership check finds it.
    async fn assign_member(
        group: &mut RaftGroup,
        organization_id: &Id,
        user_id: &Id,
    ) -> anyhow::Result<()> {
        let command = Command {
            envelope_version: 1,
            id: Id::new(),
            aggregate_id: loomery_core::membership::organization_assignment_id(
                organization_id,
                user_id,
            ),
            organization_id: organization_id.clone(),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(&NS, &format!("assign:{organization_id}:{user_id}")),
            correlation_key: Key::new(&NS, "services-stress"),
            actor: Actor::System,
            command_type: loomery_core::membership::ASSIGN_MEMBER.to_owned(),
            payload: Payload {
                version: 1,
                data: format!(r#"{{"user_id":"{user_id}"}}"#),
            },
        };
        group.propose(command).await?;
        Ok(())
    }

    /// Gives `user_id` `role` in `workspace_id`, as genesis 3 and the invitation
    /// flow do.
    async fn join_workspace(
        group: &mut RaftGroup,
        organization_id: &Id,
        workspace_id: &Id,
        user_id: &Id,
        role: loomery_core::membership::Role,
    ) -> anyhow::Result<()> {
        let command = Command {
            envelope_version: 1,
            id: Id::new(),
            aggregate_id: loomery_core::membership::workspace_membership_id(
                organization_id,
                workspace_id,
                user_id,
            ),
            organization_id: organization_id.clone(),
            workspace_id: Some(workspace_id.clone()),
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(
                &NS,
                &format!("join:{organization_id}:{workspace_id}:{user_id}"),
            ),
            correlation_key: Key::new(&NS, "services-stress"),
            actor: Actor::System,
            command_type: loomery_core::membership::ADD_MEMBER.to_owned(),
            payload: Payload {
                version: 1,
                data: format!(
                    r#"{{"user_id":"{user_id}","role":"{}"}}"#,
                    match role {
                        loomery_core::membership::Role::Owner => "Owner",
                        loomery_core::membership::Role::Member => "Member",
                        loomery_core::membership::Role::Viewer => "Viewer",
                    }
                ),
            },
        };
        group.propose(command).await?;
        Ok(())
    }

    /// A `task.create` command for the outbox scenario.
    fn task_command(org: &Id, label: &str) -> Command {
        Command {
            envelope_version: 1,
            id: Id::new(),
            aggregate_id: Id::from(Key::new(&NS, &format!("task-{label}"))),
            organization_id: org.clone(),
            workspace_id: None,
            occurred_at: Timestamp::now(),
            causation_key: Key::new(&NS, &format!("cause-{label}")),
            correlation_key: Key::new(&NS, "services-stress"),
            actor: Actor::System,
            command_type: COMMAND.to_owned(),
            payload: Payload {
                version: 1,
                data: format!(r#"{{"title":"task {label}"}}"#),
            },
        }
    }

    // -------------------------------------------------------------------- run

    /// Runs the scenario selected by `LOOMERY_STRESS_SCENARIO`.
    pub async fn run() -> anyhow::Result<()> {
        let scenario = text("LOOMERY_STRESS_SCENARIO", "e2e");
        let report = match scenario.as_str() {
            "auth" => auth().await?,
            "outbox" => outbox().await?,
            "e2e" => end_to_end().await?,
            other => anyhow::bail!(
                "unknown LOOMERY_STRESS_SCENARIO {other:?}: expected auth, outbox or e2e"
            ),
        };

        println!("JSON {}", serde_json::to_string(&report)?);
        Ok(())
    }

    // ------------------------------------------------------------------- auth

    /// What one `authenticate` call is expected to return.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Expect {
        /// A token of user `slot` that must resolve to that identity.
        Valid(usize),
        /// A token the realm does not know.
        Invalid,
        /// No token at all.
        Missing,
    }

    /// The expectation for call `index`: three quarters valid, the rest split
    /// between a bad token and no token.
    fn expectation(index: usize) -> Expect {
        match index.rem_euclid(8) {
            0 => Expect::Missing,
            1 => Expect::Invalid,
            rest => Expect::Valid(rest.rem_euclid(USERS.len())),
        }
    }

    /// Concurrent token acquisition, then concurrent `userinfo` authentication.
    #[allow(clippy::too_many_lines)] // Two phases, a worker pool and the plan check: the sequence is the scenario.
    async fn auth() -> anyhow::Result<Value> {
        let base = required("LOOMERY_TEST_KEYCLOAK_URL")?;
        let realm = text("LOOMERY_TEST_KEYCLOAK_REALM", "loomery");
        let client = text("LOOMERY_TEST_KEYCLOAK_CLIENT", "loomery-gateway");
        let workers = count("LOOMERY_STRESS_WORKERS", 16);
        let requests = count("LOOMERY_STRESS_REQUESTS", 2_000);
        let grants = count("LOOMERY_STRESS_GRANTS", 200);
        anyhow::ensure!(
            workers > 0 && requests > 0 && grants > 0,
            "LOOMERY_STRESS_WORKERS, LOOMERY_STRESS_REQUESTS and LOOMERY_STRESS_GRANTS must be positive"
        );

        let authenticator = Arc::new(KeycloakAuthenticator::new(&base, &realm));

        // 1. the password grant under concurrency
        let granted = Arc::new(Mutex::new(Vec::<(String, usize)>::new()));
        let grant_latencies = Arc::new(Latencies::default());
        let grant_started = Instant::now();
        let next = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..workers {
            let (base, realm, client) = (base.clone(), realm.clone(), client.clone());
            let (next, granted, latencies) = (
                Arc::clone(&next),
                Arc::clone(&granted),
                Arc::clone(&grant_latencies),
            );
            handles.push(tokio::spawn(async move {
                let mut failures = Vec::new();
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= grants {
                        break;
                    }
                    let slot = index.rem_euclid(USERS.len());
                    let Some((name, password)) = USERS.get(slot) else {
                        break;
                    };
                    let started = Instant::now();
                    match KeycloakAuthenticator::password_token(
                        &base, &realm, &client, name, password,
                    )
                    .await
                    {
                        Ok(token) => {
                            latencies.record(started.elapsed());
                            if let Ok(mut tokens) = granted.lock() {
                                tokens.push((token, slot));
                            }
                        }
                        Err(error) => failures.push(format!("grant for {name}: {error}")),
                    }
                }
                failures
            }));
        }
        let mut grant_failures = Vec::new();
        for handle in handles {
            grant_failures.extend(handle.await?);
        }
        let grant_elapsed = grant_started.elapsed();
        let grant_samples = grant_latencies.sorted();
        anyhow::ensure!(
            grant_failures.is_empty(),
            "{} token grant(s) failed, first: {}",
            grant_failures.len(),
            grant_failures.first().map_or("", String::as_str)
        );

        // One token per user to reuse; the load is the userinfo call.
        let pool = granted
            .lock()
            .map(|tokens| tokens.clone())
            .unwrap_or_default();
        let mut sessions = Vec::new();
        let mut expected = Vec::new();
        for slot in 0..USERS.len() {
            let token = pool
                .iter()
                .find(|(_, granted_slot)| *granted_slot == slot)
                .map(|(token, _)| token.clone())
                .ok_or_else(|| anyhow::anyhow!("no granted token for user slot {slot}"))?;
            let identity = authenticator
                .authenticate(Some(&token))
                .await
                .map_err(|error| anyhow::anyhow!("the session token for slot {slot}: {error}"))?;
            sessions.push(token);
            expected.push(identity);
        }

        // 2. concurrent authentication, valid and invalid mixed
        let latencies = Arc::new(Latencies::default());
        let auth_started = Instant::now();
        let next = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..workers {
            let authenticator = Arc::clone(&authenticator);
            let sessions = sessions.clone();
            let expected = expected.clone();
            let (next, latencies) = (Arc::clone(&next), Arc::clone(&latencies));
            handles.push(tokio::spawn(async move {
                let mut tally = Tally::default();
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= requests {
                        break;
                    }
                    let expect = expectation(index);
                    let token = match expect {
                        Expect::Valid(slot) => sessions.get(slot).cloned(),
                        Expect::Invalid => Some("not-a-token".to_owned()),
                        Expect::Missing => None,
                    };
                    let started = Instant::now();
                    let outcome = authenticator.authenticate(token.as_deref()).await;
                    latencies.record(started.elapsed());

                    match (expect, outcome) {
                        (Expect::Valid(slot), Ok(identity)) => {
                            if expected.get(slot) == Some(&identity) {
                                tally.count(0);
                            } else {
                                tally.mismatches.push(format!(
                                    "slot {slot} returned {identity:?} instead of {:?}",
                                    expected.get(slot)
                                ));
                            }
                        }
                        (Expect::Valid(slot), Err(error)) => tally
                            .failures
                            .push(format!("the valid token of slot {slot}: {error}")),
                        (Expect::Invalid, Err(AuthError::Unknown)) => tally.count(1),
                        (Expect::Missing, Err(AuthError::Missing)) => tally.count(2),
                        (other, outcome) => tally.mismatches.push(format!(
                            "call {index} expected {other:?} but got {outcome:?}"
                        )),
                    }
                }
                tally
            }));
        }
        let mut tally = Tally::default();
        for handle in handles {
            tally.merge(handle.await?);
        }
        let auth_elapsed = auth_started.elapsed();
        let samples = latencies.sorted();

        // 3. the plan itself: every class must be observed exactly as intended
        let mut planned = Tally::default();
        for index in 0..requests {
            match expectation(index) {
                Expect::Valid(_) => planned.count(0),
                Expect::Invalid => planned.count(1),
                Expect::Missing => planned.count(2),
            }
        }
        tally.check()?;
        anyhow::ensure!(
            tally.classes == planned.classes,
            "observed classes {:?} do not match the plan {:?}",
            tally.classes,
            planned.classes
        );

        let stats = Stats::new(&samples, auth_elapsed);
        println!(
            "auth: {} valid, {} unknown, {} missing in {:.2}s = {:.1}/s (p50 {}us, p99 {}us)",
            tally.classes.first().copied().unwrap_or(0),
            tally.classes.get(1).copied().unwrap_or(0),
            tally.classes.get(2).copied().unwrap_or(0),
            auth_elapsed.as_secs_f64(),
            stats.per_second,
            stats.p50_us.unwrap_or(0),
            stats.p99_us.unwrap_or(0),
        );

        Ok(json!({
            "scenario": "auth",
            "workers": workers,
            "requests": requests,
            "grants": pool.len(),
            "grant_stats": Stats::new(&grant_samples, grant_elapsed),
            "authenticate_stats": stats,
            "valid": tally.classes.first().copied().unwrap_or(0),
            "invalid": tally.classes.get(1).copied().unwrap_or(0),
            "missing": tally.classes.get(2).copied().unwrap_or(0),
            "mismatches": tally.mismatches.len(),
            "invariants": "ok",
        }))
    }

    // ----------------------------------------------------------------- outbox

    /// A booted group and the organization routed to it.
    struct Tenant {
        group_id: String,
        organization_id: Id,
        /// The workspace the profile's commands name (the role check is scoped to
        /// it; a deployment reads it from the board).
        workspace_id: Id,
        group: RaftGroup,
    }

    /// Boots `groups` single-node groups and proposes `commands` commands into
    /// each, `workers` at a time.
    async fn apply_commands(
        groups: usize,
        commands: usize,
        workers: usize,
        latencies: &Arc<Latencies>,
        failures: &Arc<Mutex<Vec<String>>>,
    ) -> anyhow::Result<Vec<Tenant>> {
        let mut tenants = Vec::new();
        for index in 0..groups {
            let organization_id = organization(index);
            let group = RaftGroup::boot_single_node(node_id(index)).await?;
            let next = Arc::new(AtomicUsize::new(0));
            let mut handles = Vec::new();
            for worker in 0..workers {
                let mut group = group.clone();
                let organization_id = organization_id.clone();
                let (next, latencies, failures) = (
                    Arc::clone(&next),
                    Arc::clone(latencies),
                    Arc::clone(failures),
                );
                handles.push(tokio::spawn(async move {
                    loop {
                        let sequence = next.fetch_add(1, Ordering::Relaxed);
                        if sequence >= commands {
                            break;
                        }
                        let label = format!("{index}-{worker}-{sequence}");
                        let started = Instant::now();
                        match group.propose(task_command(&organization_id, &label)).await {
                            Ok(_) => latencies.record(started.elapsed()),
                            Err(error) => {
                                if let Ok(mut failures) = failures.lock() {
                                    failures.push(format!("propose {label}: {error}"));
                                }
                            }
                        }
                    }
                }));
            }
            for handle in handles {
                handle.await?;
            }

            let applied = group.state_machine().applied_events(&organization_id).await;
            anyhow::ensure!(
                applied.len() == commands,
                "group {index} applied {} events, expected {commands}",
                applied.len()
            );

            tenants.push(Tenant {
                group_id: group_id(index),
                organization_id,
                workspace_id: Id::from(Key::new(&NS, &format!("outbox-workspace-{index}"))),
                group,
            });
        }

        Ok(tenants)
    }

    /// Publishes every group's applied events, then replays them from scratch to
    /// prove the broker absorbs the duplicates.
    struct Published {
        /// Messages the first pass sent (one per distinct event).
        first: usize,
        /// Messages the crash replay re-sent (the very same set).
        replay: usize,
        /// How much the stream grew in the first pass.
        first_delta: u64,
        /// How much it grew in the replay: zero when dedup absorbed everything.
        replay_delta: u64,
        /// The broker's dedup window the replay had to fall inside.
        window: Duration,
    }

    /// Publishes every group's applied events, then replays them from scratch to
    /// prove the broker absorbs the duplicates.
    async fn publish_and_replay(
        publisher: &Arc<NatsPublisher>,
        tenants: &[Tenant],
    ) -> anyhow::Result<Published> {
        let before = publisher.stored_messages().await?;

        // 1. the first pass: one outbox per group, concurrently
        let mut handles = Vec::new();
        for tenant in tenants {
            let applied = tenant
                .group
                .state_machine()
                .applied_events(&tenant.organization_id)
                .await;
            let publisher = Arc::clone(publisher);
            let group_id = tenant.group_id.clone();
            handles.push(tokio::spawn(async move {
                Outbox::new(publisher).flush(&group_id, &applied).await
            }));
        }
        let mut sent: usize = 0;
        for handle in handles {
            sent = sent.saturating_add(handle.await??);
        }

        let after_first = publisher.stored_messages().await?;
        let first_delta = after_first.saturating_sub(before);
        anyhow::ensure!(
            first_delta == u64::try_from(sent).unwrap_or(u64::MAX),
            "the stream grew by {first_delta} for {sent} published messages"
        );

        // 2. the crash replay: fresh cursors, so everything is republished
        let window = publisher.duplicate_window().await?;
        let mut handles = Vec::new();
        for tenant in tenants {
            let applied = tenant
                .group
                .state_machine()
                .applied_events(&tenant.organization_id)
                .await;
            let publisher = Arc::clone(publisher);
            let group_id = tenant.group_id.clone();
            handles.push(tokio::spawn(async move {
                Outbox::new(publisher).flush(&group_id, &applied).await
            }));
        }
        let mut republished: usize = 0;
        for handle in handles {
            republished = republished.saturating_add(handle.await??);
        }

        let after_replay = publisher.stored_messages().await?;
        anyhow::ensure!(
            republished == sent,
            "the replay republished {republished} of {sent} messages"
        );
        anyhow::ensure!(
            after_replay == after_first,
            "the replay stored {} extra message(s); the dedup window is {window:?}",
            after_replay.saturating_sub(after_first)
        );

        Ok(Published {
            first: sent,
            replay: republished,
            first_delta,
            replay_delta: after_replay.saturating_sub(after_first),
            window,
        })
    }

    /// Raft → outbox → `JetStream`, with a crash-replay dedup pass.
    async fn outbox() -> anyhow::Result<Value> {
        let nats_url = required("LOOMERY_TEST_NATS_URL")?;
        let groups = count("LOOMERY_STRESS_GROUPS", 4);
        let commands = count("LOOMERY_STRESS_COMMANDS", 250);
        let workers = count("LOOMERY_STRESS_WORKERS", 4);
        anyhow::ensure!(
            groups > 0 && commands > 0 && workers > 0,
            "LOOMERY_STRESS_GROUPS, LOOMERY_STRESS_COMMANDS and LOOMERY_STRESS_WORKERS must be positive"
        );

        let publisher = Arc::new(NatsPublisher::connect(&nats_url).await?);
        let latencies = Arc::new(Latencies::default());
        let failures = Arc::new(Mutex::new(Vec::new()));

        let started = Instant::now();
        let tenants = apply_commands(groups, commands, workers, &latencies, &failures).await?;
        let apply_elapsed = started.elapsed();

        let failures = failures.lock().map(|f| f.clone()).unwrap_or_default();
        anyhow::ensure!(
            failures.is_empty(),
            "{} proposal(s) failed, first: {}",
            failures.len(),
            failures.first().map_or("", String::as_str)
        );

        let publish_started = Instant::now();
        let pass = publish_and_replay(&publisher, &tenants).await?;
        let publish_elapsed = publish_started.elapsed();

        let stats = Stats::new(&latencies.sorted(), apply_elapsed);
        println!(
            "outbox: {groups} groups, {} events in {:.2}s = {:.1}/s; published {} (p50 {}us) in {:.2}s, replayed {}, stream deltas first={} replay={}",
            stats.ops,
            apply_elapsed.as_secs_f64(),
            stats.per_second,
            pass.first,
            stats.p50_us.unwrap_or(0),
            publish_elapsed.as_secs_f64(),
            pass.replay,
            pass.first_delta,
            pass.replay_delta,
        );

        Ok(json!({
            "scenario": "outbox",
            "groups": groups,
            "commands_per_group": commands,
            "workers": workers,
            "events": stats.ops,
            "apply_stats": stats,
            "publish_seconds": publish_elapsed.as_secs_f64(),
            "published": pass.first,
            "republished": pass.replay,
            "stream_delta_first": pass.first_delta,
            "stream_delta_replay": pass.replay_delta,
            "duplicate_window_seconds": pass.window.as_secs_f64(),
            "invariants": "ok",
        }))
    }

    // -------------------------------------------------------------------- e2e

    /// The command plane under load: a real Keycloak authentication per command,
    /// then the outbox and the broker.
    #[allow(clippy::too_many_lines)] // Load, per-actor verification, publish and replay: the sequence is the scenario.
    async fn end_to_end() -> anyhow::Result<Value> {
        let nats_url = required("LOOMERY_TEST_NATS_URL")?;
        let base = required("LOOMERY_TEST_KEYCLOAK_URL")?;
        let realm = text("LOOMERY_TEST_KEYCLOAK_REALM", "loomery");
        let client = text("LOOMERY_TEST_KEYCLOAK_CLIENT", "loomery-gateway");
        let groups = count("LOOMERY_STRESS_GROUPS", 2);
        let commands = count("LOOMERY_STRESS_COMMANDS", 100);
        let workers = count("LOOMERY_STRESS_WORKERS", 8);
        anyhow::ensure!(
            groups > 0 && commands > 0 && workers > 0,
            "LOOMERY_STRESS_GROUPS, LOOMERY_STRESS_COMMANDS and LOOMERY_STRESS_WORKERS must be positive"
        );

        let authenticator = Arc::new(KeycloakAuthenticator::new(&base, &realm));
        let mut sessions = Vec::new();
        let mut expected = Vec::new();
        for (name, password) in USERS {
            let token =
                KeycloakAuthenticator::password_token(&base, &realm, &client, name, password)
                    .await
                    .map_err(|error| anyhow::anyhow!("the token for {name}: {error}"))?;
            let identity = authenticator
                .authenticate(Some(&token))
                .await
                .map_err(|error| anyhow::anyhow!("the identity of {name}: {error}"))?;
            sessions.push(token);
            expected.push(identity);
        }

        let mut registry = HashMap::new();
        let router = Router::new();
        let mut tenants = Vec::new();
        for index in 0..groups {
            let group = RaftGroup::boot_single_node(node_id(index)).await?;
            router.apply(
                organization(index),
                &TenantState {
                    group_id: Some(group_id(index)),
                    replicas: vec![replica(index)],
                    leader_user_id: None,
                    status: TenantStatus::Active,
                },
            );
            // The users this profile authenticates as must belong to the
            // organization they write to (a real deployment gets this from the
            // invitation flow).
            let mut group = group;
            let workspace_id = Id::from(Key::new(&NS, &format!("workspace-{index}")));
            for identity in &expected {
                assign_member(&mut group, &organization(index), &identity.user_id).await?;
                join_workspace(
                    &mut group,
                    &organization(index),
                    &workspace_id,
                    &identity.user_id,
                    loomery_core::membership::Role::Member,
                )
                .await?;
            }
            registry.insert(group_id(index), group.clone());
            tenants.push(Tenant {
                group_id: group_id(index),
                organization_id: organization(index),
                workspace_id,
                group,
            });
        }

        let plane = Arc::new(CommandPlane::new(
            Arc::new(router),
            Arc::new(Groups { groups: registry }),
            authenticator,
            Duration::from_millis(50),
        ));

        // Every command authenticates against Keycloak before it is proposed.
        let total = groups.saturating_mul(commands);
        let workspaces: Arc<Vec<Id>> = Arc::new(
            tenants
                .iter()
                .map(|tenant| tenant.workspace_id.clone())
                .collect(),
        );
        let latencies = Arc::new(Latencies::default());
        let failures = Arc::new(Mutex::new(Vec::new()));
        let next = Arc::new(AtomicUsize::new(0));
        let started = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..workers {
            let plane = Arc::clone(&plane);
            let sessions = sessions.clone();
            let workspaces = Arc::clone(&workspaces);
            let (next, latencies, failures) = (
                Arc::clone(&next),
                Arc::clone(&latencies),
                Arc::clone(&failures),
            );
            handles.push(tokio::spawn(async move {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= total {
                        break;
                    }
                    let slot = index.rem_euclid(USERS.len());
                    let tenant = index.div_euclid(commands);
                    let request = CommandRequest {
                        organization_id: organization(tenant),
                        aggregate_id: Id::from(Key::new(&NS, &format!("e2e-task-{index}"))),
                        workspace_id: workspaces.get(tenant).cloned(),
                        command_type: COMMAND.to_owned(),
                        payload: json!({ "title": format!("task {index}") }),
                        causation_id: None,
                        correlation_id: None,
                        token: sessions.get(slot).cloned(),
                    };
                    let started = Instant::now();
                    match plane.submit(request).await {
                        Ok(_) => latencies.record(started.elapsed()),
                        Err(error) => {
                            if let Ok(mut failures) = failures.lock() {
                                failures.push(format!("command {index}: {error}"));
                            }
                        }
                    }
                }
            }));
        }
        for handle in handles {
            handle.await?;
        }
        let submit_elapsed = started.elapsed();
        let samples = latencies.sorted();

        let failures = failures.lock().map(|f| f.clone()).unwrap_or_default();
        anyhow::ensure!(
            failures.is_empty(),
            "{} command(s) failed, first: {}",
            failures.len(),
            failures.first().map_or("", String::as_str)
        );

        // The plan: which user's token submitted how many commands per group.
        let mut planned: HashMap<(usize, usize), usize> = HashMap::new();
        for index in 0..total {
            let key = (index.div_euclid(commands), index.rem_euclid(USERS.len()));
            let entry = planned.entry(key).or_insert(0);
            *entry = entry.saturating_add(1);
        }

        // The invariant: the actor recorded for every event matches the token,
        // for every group and every user, under concurrency.
        let mut applied_total: usize = 0;
        for (index, tenant) in tenants.iter().enumerate() {
            let events = tenant
                .group
                .committed_events(&tenant.organization_id)
                .await?;
            let mut observed: HashMap<usize, usize> = HashMap::new();
            for event in events.iter().filter(|event| event.event_type == EVENT) {
                let actor = match &event.actor {
                    Actor::User { id } => id.clone(),
                    other => anyhow::bail!("an event was recorded for {other:?}, not a user"),
                };
                let Some(slot) = expected
                    .iter()
                    .position(|identity| identity.user_id == actor)
                else {
                    anyhow::bail!("group {index} recorded an event for an unknown actor {actor:?}");
                };
                let entry = observed.entry(slot).or_insert(0);
                *entry = entry.saturating_add(1);
            }
            for slot in 0..USERS.len() {
                let want = planned.get(&(index, slot)).copied().unwrap_or(0);
                let got = observed.get(&slot).copied().unwrap_or(0);
                anyhow::ensure!(
                    want == got,
                    "group {index} recorded {got} event(s) for user slot {slot}, expected {want}"
                );
            }
            applied_total = applied_total.saturating_add(
                tenant
                    .group
                    .state_machine()
                    .applied_events(&tenant.organization_id)
                    .await
                    .len(),
            );
        }

        let publisher = Arc::new(NatsPublisher::connect(&nats_url).await?);
        let publish_started = Instant::now();
        let pass = publish_and_replay(&publisher, &tenants).await?;
        let publish_elapsed = publish_started.elapsed();

        // Every command produced exactly one event, and every event went out.
        anyhow::ensure!(
            applied_total == total,
            "the groups applied {applied_total} event(s) for {total} command(s)"
        );
        anyhow::ensure!(
            pass.first == total,
            "the outbox published {} message(s) for {total} command(s)",
            pass.first
        );

        let stats = Stats::new(&samples, submit_elapsed);
        println!(
            "e2e: {groups} groups, {} commands with a real Keycloak auth each in {:.2}s = {:.1}/s (p50 {}us, p99 {}us)",
            stats.ops,
            submit_elapsed.as_secs_f64(),
            stats.per_second,
            stats.p50_us.unwrap_or(0),
            stats.p99_us.unwrap_or(0),
        );
        println!(
            "e2e: published {} distinct event(s) in {:.2}s, replayed {} with a stream delta of {}",
            pass.first,
            publish_elapsed.as_secs_f64(),
            pass.replay,
            pass.replay_delta,
        );

        Ok(json!({
            "scenario": "e2e",
            "groups": groups,
            "commands_per_group": commands,
            "workers": workers,
            "events": applied_total,
            "submit_stats": stats,
            "publish_seconds": publish_elapsed.as_secs_f64(),
            "published": pass.first,
            "republished": pass.replay,
            "stream_delta_first": pass.first_delta,
            "stream_delta_replay": pass.replay_delta,
            "duplicate_window_seconds": pass.window.as_secs_f64(),
            "invariants": "ok",
        }))
    }
}
