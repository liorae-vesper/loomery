// SPDX-License-Identifier: MPL-2.0

//! Integration tests against the local Keycloak and NATS `JetStream` services.
//!
//! Feature-gated and env-driven, so the default suite never needs a broker or an
//! `IdP`. Start the services and run these with:
//!
//! ```sh
//! mise run test-services
//! ```
#![cfg(feature = "test-services")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // a test crate

use std::env;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use loomery_core::Uuid;
use loomery_core::actor::Actor;
use loomery_core::envelope::Command;
use loomery_core::envelope::Event;
use loomery_core::envelope::Payload;
use loomery_core::id::Id;
use loomery_core::key::Key;
use loomery_core::tenant::Replica;
use loomery_core::tenant::TenantState;
use loomery_core::tenant::TenantStatus;
use loomery_core::timestamp::Timestamp;
use loomery_shell::config::HostConfig;
use loomery_shell::config::HttpConfig;
use loomery_shell::config::NatsConfig;
use loomery_shell::control::Router;
use loomery_shell::gateway::AuthError;
use loomery_shell::gateway::Authenticator;
use loomery_shell::gateway::CommandPlane;
use loomery_shell::gateway::CommandRequest;
use loomery_shell::gateway::GroupRegistry;
use loomery_shell::gateway::Identity;
use loomery_shell::gateway::OidcAuthenticator;
use loomery_shell::gateway::StaticAuthenticator;
use loomery_shell::gateway::keycloak::KeycloakAuthenticator;
use loomery_shell::group::GroupOps;
use loomery_shell::host::Host;
use loomery_shell::outbox::NatsConsumer;
use loomery_shell::outbox::Outbox;
use loomery_shell::outbox::OutboxMessage;
use loomery_shell::outbox::Publisher;
use loomery_shell::outbox::SUBJECT_PREFIX;
use loomery_shell::outbox::nats::NatsPublisher;
use loomery_shell::raft::RaftGroup;
use loomery_shell::saga::Consumer;
use serde_json::json;
use tower::ServiceExt;

/// A required service URL; fails loudly if the stack is not running.
fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is not set; run `mise run svc-up` first"))
}

/// An optional setting with a default.
fn optional(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// A per-run suffix so repeated runs do not collide in the stream.
fn run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("{}-{nanos}", std::process::id())
}

#[tokio::test]
async fn jetstream_stores_each_outbox_message_once_per_dedup_id() {
    let run = run_id();
    let publisher = NatsPublisher::connect_config(&nats_config(&required("LOOMERY_TEST_NATS_URL")))
        .await
        .expect("connect to NATS");
    let subject = format!("loomery.events.test.{run}");
    let message = |id: &str, payload: &str| OutboxMessage {
        subject: subject.clone(),
        message_id: format!("{run}:{id}"),
        payload: payload.as_bytes().to_vec(),
    };

    let before = publisher
        .stored_messages_for(&subject)
        .await
        .expect("stream info");

    publisher
        .publish(message("1", "first"))
        .await
        .expect("publish first");
    publisher
        .publish(message("1", "a retry with the same id"))
        .await
        .expect("publish duplicate id");
    publisher
        .publish(message("2", "second"))
        .await
        .expect("publish second");

    let after = publisher
        .stored_messages_for(&subject)
        .await
        .expect("stream info");
    assert_eq!(
        after - before,
        2,
        "two distinct Nats-Msg-Id values; the duplicate is absorbed"
    );
}

#[tokio::test]
async fn keycloak_tokens_become_identities_with_the_admin_claim() {
    let base = required("LOOMERY_TEST_KEYCLOAK_URL");
    let realm = optional("LOOMERY_TEST_KEYCLOAK_REALM", "loomery");
    let client = optional("LOOMERY_TEST_KEYCLOAK_CLIENT", "loomery-gateway");
    let authenticator = KeycloakAuthenticator::new(&base, &realm);

    let ada = KeycloakAuthenticator::password_token(&base, &realm, &client, "ada", "ada")
        .await
        .expect("ada's token");
    let identity = authenticator
        .authenticate(Some(&ada))
        .await
        .expect("ada authenticates");
    assert!(!identity.is_admin, "ada is not in the admins group");

    let admin = KeycloakAuthenticator::password_token(&base, &realm, &client, "admin", "admin")
        .await
        .expect("admin's token");
    let identity = authenticator
        .authenticate(Some(&admin))
        .await
        .expect("admin authenticates");
    assert!(identity.is_admin, "admin is in the admins group");

    assert_eq!(
        authenticator.authenticate(None).await,
        Err(AuthError::Missing)
    );
    assert_eq!(
        authenticator.authenticate(Some("not-a-token")).await,
        Err(AuthError::Unknown)
    );
}

/// A real TLS handshake through the `test-services` stack.
///
/// Opt-in (`LOOMERY_TEST_HTTPS_PROBE`), so CI does not depend on a public host:
///
/// ```sh
/// LOOMERY_TEST_HTTPS_PROBE=https://example.com \
///   cargo test -p loomery-shell --features test-services --test test_services https_probe
/// ```
#[tokio::test]
async fn https_probe_completes_a_tls_handshake() {
    let Ok(url) = env::var("LOOMERY_TEST_HTTPS_PROBE") else {
        return; // opted out
    };

    // `rustls-no-provider` requires a provider before a Client is built.
    loomery_shell::gateway::install_tls_provider();

    let response = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("the TLS request should complete");
    assert!(
        response.status().is_success() || response.status().is_redirection(),
        "unexpected status {}",
        response.status()
    );
}

// ---------------------------------------------------------------------------
// Light stress profiles
//
// The heavy profiles live in `examples/services_stress.rs`
// (`mise run bench-services-stress`); these run the same invariants at a size
// that belongs in CI.
// ---------------------------------------------------------------------------

/// The namespace the stress profiles derive their synthetic identities from.
const STRESS_NS: Uuid = Uuid::from_u128(0x5c1a_9f43_2e78_4b06_8d17_a2c3_b4d5_e6f7);

/// The test realm's users, as `(username, password)`.
const USERS: [(&str, &str); 2] = [("ada", "ada"), ("admin", "admin")];

/// A synthetic organization id for profile `index`.
fn organization(index: usize) -> Id {
    Id::from(Key::new(&STRESS_NS, &format!("test-organization-{index}")))
}

/// A synthetic tenant group id for profile `index`, unique to this run.
fn tenant(group: &str, index: usize) -> String {
    format!("{group}-{index}")
}

/// How many messages the broker stores for one group's subjects.
///
/// Scoped to the group: one stream carries every group's events (D11), so a
/// stream-wide count also moves when another test publishes concurrently.
async fn stored_for_group(publisher: &NatsPublisher, group_id: &str) -> u64 {
    publisher
        .stored_messages_for(&format!("{SUBJECT_PREFIX}{group_id}.>"))
        .await
        .expect("stream info")
}

/// [`stored_for_group`] summed over several groups.
async fn stored_for_groups(publisher: &NatsPublisher, group_ids: &[String]) -> u64 {
    let mut total = 0u64;
    for group_id in group_ids {
        total = total.saturating_add(stored_for_group(publisher, group_id).await);
    }
    total
}

/// Assigns `user_id` to `organization_id`, as the invitation saga does: the
/// stream id is derived from the business tuple (D12), so the gateway's
/// membership check finds it.
async fn assign_member(group: &mut RaftGroup, organization_id: &Id, user_id: &Id) {
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
        causation_key: Key::new(&STRESS_NS, &format!("assign:{organization_id}:{user_id}")),
        correlation_key: Key::new(&STRESS_NS, "test-services"),
        actor: Actor::System,
        command_type: loomery_core::membership::ASSIGN_MEMBER.to_owned(),
        payload: Payload {
            version: 1,
            data: format!(r#"{{"user_id":"{user_id}"}}"#),
        },
    };
    group.propose(command).await.expect("assign the member");
}

/// Gives `user_id` `role` in `workspace_id`, as genesis ③ and the invitation
/// flow do.
async fn join_workspace(
    group: &mut RaftGroup,
    organization_id: &Id,
    workspace_id: &Id,
    user_id: &Id,
    role: loomery_core::membership::Role,
) {
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
            &STRESS_NS,
            &format!("join:{organization_id}:{workspace_id}:{user_id}"),
        ),
        correlation_key: Key::new(&STRESS_NS, "test-services"),
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
    group.propose(command).await.expect("join the workspace");
}

/// Boots a single-node group and returns it with its organization.
async fn boot(index: usize) -> (Id, RaftGroup) {
    let organization = organization(index);
    let group = RaftGroup::boot_single_node(u64::try_from(index).unwrap_or(0).saturating_add(1))
        .await
        .expect("boot a group");
    (organization, group)
}

/// A `task.create` command with a unique causation key.
fn task_command(organization: &Id, label: &str) -> Command {
    Command {
        envelope_version: 1,
        id: Id::new(),
        aggregate_id: Id::from(Key::new(&STRESS_NS, &format!("task-{label}"))),
        organization_id: organization.clone(),
        workspace_id: None,
        occurred_at: Timestamp::now(),
        causation_key: Key::new(&STRESS_NS, &format!("cause-{label}")),
        correlation_key: Key::new(&STRESS_NS, "test-services-stress"),
        actor: Actor::System,
        command_type: "task.create".to_owned(),
        payload: Payload {
            version: 1,
            data: format!(r#"{{"title":"task {label}"}}"#),
        },
    }
}

/// Hosts the groups a profile booted.
struct Hosting {
    groups: std::collections::HashMap<String, RaftGroup>,
}

impl GroupRegistry for Hosting {
    fn group(&self, group_id: &str) -> Option<RaftGroup> {
        self.groups.get(group_id).cloned()
    }
}

/// One token per realm user, with the identity it must resolve to.
async fn sessions(
    base: &str,
    realm: &str,
    client: &str,
    authenticator: &KeycloakAuthenticator,
) -> (Vec<String>, Vec<Identity>) {
    let mut tokens = Vec::new();
    let mut identities = Vec::new();
    for (name, password) in USERS {
        let token = KeycloakAuthenticator::password_token(base, realm, client, name, password)
            .await
            .expect("a token");
        let identity = authenticator
            .authenticate(Some(&token))
            .await
            .expect("the token authenticates");
        tokens.push(token);
        identities.push(identity);
    }
    (tokens, identities)
}

#[tokio::test]
async fn concurrent_authentication_keeps_identities_apart() {
    let base = required("LOOMERY_TEST_KEYCLOAK_URL");
    let realm = optional("LOOMERY_TEST_KEYCLOAK_REALM", "loomery");
    let client = optional("LOOMERY_TEST_KEYCLOAK_CLIENT", "loomery-gateway");
    let authenticator = Arc::new(KeycloakAuthenticator::new(&base, &realm));
    let (tokens, identities) = sessions(&base, &realm, &client, &authenticator).await;

    let workers = 6usize;
    let rounds = 20usize;
    let mut tasks = Vec::new();
    for worker in 0..workers {
        let authenticator = Arc::clone(&authenticator);
        let (tokens, identities) = (tokens.clone(), identities.clone());
        tasks.push(tokio::spawn(async move {
            for round in 0..rounds {
                let slot = worker.saturating_add(round).rem_euclid(USERS.len());
                let token = tokens.get(slot).cloned();
                let identity = authenticator
                    .authenticate(token.as_deref())
                    .await
                    .expect("a valid token authenticates");
                assert_eq!(
                    Some(&identity),
                    identities.get(slot),
                    "worker {worker} round {round} resolved to the wrong identity"
                );
            }
        }));
    }
    for task in tasks {
        task.await.expect("the worker finished");
    }

    // The negative paths stay distinct from the valid ones under load.
    let invalid = authenticator.authenticate(Some("not-a-token")).await;
    assert_eq!(invalid, Err(AuthError::Unknown));
    assert_eq!(
        authenticator.authenticate(None).await,
        Err(AuthError::Missing)
    );
    assert!(workers.saturating_mul(rounds) > 0);
}

#[tokio::test]
async fn a_replayed_outbox_batch_is_absorbed_by_the_broker() {
    let run = run_id();
    let publisher = Arc::new(
        NatsPublisher::connect_config(&nats_config(&required("LOOMERY_TEST_NATS_URL")))
            .await
            .expect("connect to NATS"),
    );
    let (organization_id, mut group) = boot(0).await;
    let group_id = tenant(&format!("outbox-{run}"), 0);
    let events = 25usize;

    for index in 0..events {
        group
            .propose(task_command(
                &organization_id,
                &format!("{group_id}-{index}"),
            ))
            .await
            .expect("propose");
    }
    let applied = group.state_machine().applied_events(&organization_id).await;
    assert_eq!(applied.len(), events, "one event per command");

    let before = stored_for_group(&publisher, &group_id).await;
    let first = Outbox::new(Arc::clone(&publisher))
        .flush(&group_id, &applied)
        .await
        .expect("the first flush");
    assert_eq!(first, events);
    let after_first = stored_for_group(&publisher, &group_id).await;
    assert_eq!(
        after_first - before,
        events as u64,
        "each distinct message is stored exactly once"
    );

    // A crash replay starts from a fresh cursor and republishes everything; the
    // broker's dedup window must absorb it, leaving the stream untouched.
    let replayed = Outbox::new(Arc::clone(&publisher))
        .flush(&group_id, &applied)
        .await
        .expect("the replay");
    assert_eq!(replayed, events);
    let after_replay = stored_for_group(&publisher, &group_id).await;
    assert_eq!(
        after_replay, after_first,
        "the replayed batch must not add messages"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Boot, load, per-actor verification and replay: the sequence is the test.
async fn commands_through_the_plane_hold_up_with_real_auth_and_the_broker() {
    let nats_url = required("LOOMERY_TEST_NATS_URL");
    let base = required("LOOMERY_TEST_KEYCLOAK_URL");
    let realm = optional("LOOMERY_TEST_KEYCLOAK_REALM", "loomery");
    let client = optional("LOOMERY_TEST_KEYCLOAK_CLIENT", "loomery-gateway");
    let run = run_id();

    let authenticator = Arc::new(KeycloakAuthenticator::new(&base, &realm));
    let (tokens, identities) = sessions(&base, &realm, &client, &authenticator).await;

    // Two tenants, each hosted locally and routed to its own group.
    let router = Arc::new(Router::new());
    let mut hosted = std::collections::HashMap::new();
    let mut tenants = Vec::new();
    for index in 0..2usize {
        let (organization_id, group) = boot(index).await;
        let group_id = tenant(&format!("plane-{run}"), index);
        router.apply(
            organization_id.clone(),
            &TenantState {
                group_id: Some(group_id.clone()),
                replicas: vec![Replica {
                    node_id: u64::try_from(index).unwrap_or(0).saturating_add(1),
                    address: format!("http://127.0.0.1:70{index}1"),
                }],
                leader_user_id: None,
                status: TenantStatus::Active,
            },
        );
        hosted.insert(group_id.clone(), group.clone());
        tenants.push((group_id, organization_id, group));
    }
    // Both authenticated users are members of both tenants, so the writes are
    // authorized (the membership check is what a real deployment gets from the
    // invitation flow).
    let workspace_id = Id::from(Key::new(&STRESS_NS, "plane-workspace"));
    for (_, organization_id, group) in &mut tenants {
        for identity in &identities {
            assign_member(group, organization_id, &identity.user_id).await;
            join_workspace(
                group,
                organization_id,
                &workspace_id,
                &identity.user_id,
                loomery_core::membership::Role::Member,
            )
            .await;
        }
    }

    let plane = Arc::new(CommandPlane::new(
        router,
        Arc::new(Hosting { groups: hosted }),
        authenticator,
        std::time::Duration::from_millis(50),
    ));

    // Every command authenticates against Keycloak before it is proposed.
    let per_tenant = 12usize;
    let workers = 4usize;
    let total = tenants.len().saturating_mul(per_tenant);
    let mut plan = std::collections::HashMap::new();
    for index in 0..total {
        let key = (index.div_euclid(per_tenant), index.rem_euclid(USERS.len()));
        let entry = plan.entry(key).or_insert(0usize);
        *entry = entry.saturating_add(1);
    }

    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..workers {
        let plane = Arc::clone(&plane);
        let tokens = tokens.clone();
        let next = Arc::clone(&next);
        let run = run.clone();
        let workspace_id = workspace_id.clone();
        tasks.push(tokio::spawn(async move {
            let mut accepted = 0usize;
            loop {
                let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if index >= total {
                    break;
                }
                let slot = index.rem_euclid(USERS.len());
                let request = CommandRequest {
                    organization_id: organization(index.div_euclid(per_tenant)),
                    aggregate_id: Id::from(Key::new(
                        &STRESS_NS,
                        &format!("plane-task-{run}-{index}"),
                    )),
                    workspace_id: Some(workspace_id.clone()),
                    command_type: "task.create".to_owned(),
                    payload: json!({ "title": format!("task {index}") }),
                    causation_id: None,
                    correlation_id: None,
                    token: tokens.get(slot).cloned(),
                };
                plane
                    .submit(request)
                    .await
                    .expect("the command is accepted");
                accepted += 1;
            }
            accepted
        }));
    }
    let mut accepted = 0usize;
    for task in tasks {
        accepted += task.await.expect("the worker finished");
    }
    assert_eq!(accepted, total);

    // The identity never crossed: the actor of every event is the token's user.
    let publisher = Arc::new(
        NatsPublisher::connect_config(&nats_config(&nats_url))
            .await
            .expect("connect to NATS"),
    );
    let group_ids: Vec<String> = tenants
        .iter()
        .map(|(group_id, ..)| group_id.clone())
        .collect();
    let before = stored_for_groups(&publisher, &group_ids).await;
    let mut flushed = 0usize;
    for (index, (group_id, organization_id, group)) in tenants.iter().enumerate() {
        let events = group
            .committed_events(organization_id)
            .await
            .expect("events");
        let mut observed: std::collections::HashMap<usize, usize> =
            std::collections::HashMap::new();
        for event in events
            .iter()
            .filter(|event| event.event_type == "task.created")
        {
            let actor = match &event.actor {
                Actor::User { id } => id.clone(),
                other => panic!("recorded for {other:?}, not a user"),
            };
            let slot = identities
                .iter()
                .position(|identity| identity.user_id == actor)
                .expect("a known user");
            let entry = observed.entry(slot).or_insert(0usize);
            *entry = entry.saturating_add(1);
        }
        for slot in 0..USERS.len() {
            assert_eq!(
                observed.get(&slot).copied().unwrap_or(0),
                plan.get(&(index, slot)).copied().unwrap_or(0),
                "tenant {index} recorded the wrong number of events for user slot {slot}"
            );
        }

        let applied = group.state_machine().applied_events(organization_id).await;
        flushed = flushed.saturating_add(
            Outbox::new(Arc::clone(&publisher))
                .flush(group_id, &applied)
                .await
                .expect("the outbox flush"),
        );
    }
    // One published message per command, plus one per assignment and one per
    // workspace join made above.
    let granted = USERS.len() * tenants.len() * 2;
    assert_eq!(
        flushed,
        total + granted,
        "one published message per command, assignment and workspace join"
    );

    let after_first = stored_for_groups(&publisher, &group_ids).await;
    assert_eq!(
        after_first - before,
        flushed as u64,
        "every published message reached the stream once"
    );

    // And the replay is still absorbed, with the plane's events this time.
    for (group_id, organization_id, group) in &tenants {
        let applied = group.state_machine().applied_events(organization_id).await;
        Outbox::new(Arc::clone(&publisher))
            .flush(group_id, &applied)
            .await
            .expect("the replay");
    }
    assert_eq!(
        stored_for_groups(&publisher, &group_ids).await,
        after_first,
        "the replayed batch must not add messages"
    );
}

// ---------------------------------------------------------------------------
// The runtime adapters against the real services
// ---------------------------------------------------------------------------

/// The broker configuration the runtime tests use.
///
/// The stream is the production one: `JetStream` rejects two streams whose
/// subjects overlap, and the tests isolate themselves through per-run *group
/// ids* (so their subjects differ) and per-run consumers instead.
fn nats_config(url: &str) -> NatsConfig {
    NatsConfig {
        url: url.to_owned(),
        ..NatsConfig::default()
    }
}

/// A canonical event, as the outbox would publish it.
fn event_for(organization_id: &Id) -> Event {
    Event {
        envelope_version: 1,
        id: Id::from(Key::new(&STRESS_NS, "runtime-live-event")),
        aggregate_id: Id::from(Key::new(&STRESS_NS, "runtime-live-task")),
        organization_id: organization_id.clone(),
        workspace_id: None,
        occurred_at: Timestamp::from(1_700_000_000_000),
        causation_key: Key::new(&STRESS_NS, "runtime-live-cause"),
        correlation_key: Key::new(&STRESS_NS, "runtime-live"),
        actor: Actor::System,
        event_type: "task.created".to_owned(),
        payload: Payload {
            version: 1,
            data: r#"{"title":"live"}"#.to_owned(),
        },
    }
}

#[tokio::test]
async fn a_real_keycloak_token_validates_through_the_oidc_adapter() {
    let base = required("LOOMERY_TEST_KEYCLOAK_URL");
    let realm = optional("LOOMERY_TEST_KEYCLOAK_REALM", "loomery");
    let client = optional("LOOMERY_TEST_KEYCLOAK_CLIENT", "loomery-gateway");

    // Nothing provider-specific: the issuer drives discovery and the key set.
    let authenticator = OidcAuthenticator::new(loomery_shell::config::OidcConfig {
        issuer: format!("{base}/realms/{realm}"),
        ..loomery_shell::config::OidcConfig::default()
    })
    .expect("a configured adapter");
    authenticator.warm().await.expect("discovery and JWKS");

    let admin_token =
        KeycloakAuthenticator::password_token(&base, &realm, &client, "admin", "admin")
            .await
            .expect("admin's token");
    let admin = authenticator
        .authenticate(Some(&admin_token))
        .await
        .expect("a locally validated token");
    assert!(admin.is_admin, "the realm puts `admin` in the admins group");

    let ada_token = KeycloakAuthenticator::password_token(&base, &realm, &client, "ada", "ada")
        .await
        .expect("ada's token");
    let ada = authenticator
        .authenticate(Some(&ada_token))
        .await
        .expect("a locally validated token");
    assert!(!ada.is_admin);
    assert_eq!(
        ada.email.as_deref(),
        Some("ada@example.com"),
        "the verified email claim is what an invitation binds to"
    );
    assert_ne!(
        admin.user_id, ada.user_id,
        "distinct subjects, distinct ids"
    );

    let truncated: String = admin_token.chars().take(10).collect();
    assert_eq!(
        authenticator.authenticate(Some(&truncated)).await,
        Err(AuthError::Unknown),
        "a truncated signature is refused"
    );
}

#[tokio::test]
async fn the_nats_consumer_peeks_until_acked() {
    let url = required("LOOMERY_TEST_NATS_URL");
    let run = run_id();
    let group = format!("consumer-{run}");
    let organization_id = Id::from(Key::new(&STRESS_NS, &group));

    // A durable consumer filtered to this run's group, so other runs' events
    // cannot appear.
    let config = NatsConfig {
        // The stream still holds every event; only this consumer is narrowed, so
        // it cannot see another run's messages.
        filter_subject: Some(format!("loomery.events.{group}.>")),
        durable: format!("saga-{run}"),
        ..nats_config(&url)
    };
    let publisher = NatsPublisher::connect_config(&config)
        .await
        .expect("connect to NATS");
    let consumer = NatsConsumer::connect(&config).await.expect("a consumer");

    let message = OutboxMessage {
        subject: format!("loomery.events.{group}.task.created"),
        message_id: format!("{group}:7:e0"),
        payload: serde_json::to_vec(&event_for(&organization_id)).unwrap(),
    };
    publisher.publish(message).await.expect("publish");

    let first = consumer
        .next()
        .await
        .expect("no consume error")
        .expect("the published message");
    assert_eq!(first.group_id, group);
    assert_eq!(first.log_index, 7, "the log index is rebuilt from the id");
    assert_eq!(first.event.event_type, "task.created");

    // Peek, not consume: the message stays until it is acked.
    let again = consumer
        .next()
        .await
        .expect("no consume error")
        .expect("still");
    assert_eq!(again.message_id, first.message_id);

    consumer.ack(&first).await.expect("ack");
    assert!(
        consumer.next().await.expect("no consume error").is_none(),
        "nothing is left once the message is acked"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Provision, write over the gateway, publish, restart: the sequence is the test.
async fn a_host_publishes_committed_events_to_the_real_broker() {
    let url = required("LOOMERY_TEST_NATS_URL");
    let root = tempfile::tempdir().expect("a temporary data directory");
    let run = run_id();
    let group = format!("host-{run}");
    let organization_id = Id::from(Key::new(&STRESS_NS, &group));

    // The host takes the real publisher; the identity provider is faked because
    // this test is about the outbox, not about OIDC (which the test above covers).
    let member_id = Id::from(Key::new(&STRESS_NS, &format!("{run}-member")));
    let config = HostConfig {
        data_dir: root.path().to_path_buf(),
        http: HttpConfig {
            bind: "127.0.0.1:0".to_owned(),
            ryw_hold_ms: 50,
        },
        ..HostConfig::default()
    };
    let publisher = Arc::new(
        NatsPublisher::connect_config(&nats_config(&url))
            .await
            .expect("connect to NATS"),
    );
    let observer = NatsPublisher::connect_config(&nats_config(&url))
        .await
        .expect("connect to NATS");
    let before = stored_for_group(&observer, &group).await;

    let authenticator = Arc::new(StaticAuthenticator::new().with_token(
        "member",
        Identity {
            user_id: member_id.clone(),
            is_admin: false,
            email: None,
        },
    ));
    let host = Arc::new(
        Host::boot(
            config,
            authenticator,
            Some(Arc::clone(&publisher)),
            None::<Arc<NatsConsumer>>,
        )
        .await
        .expect("boot the host"),
    );

    host.provision_with(loomery_shell::gateway::ProvisionRequest {
        organization_id: organization_id.clone(),
        // The owner *is* the caller: genesis ③ makes them a member, so the write
        // below is authorized.
        leader_user_id: member_id.clone(),
        group_id: Some(group.clone()),
    })
    .await
    .expect("provision a tenant");
    host.start_workers().await.expect("start the workers");

    // A real write through the gateway, then a real publish to JetStream. The
    // command names the workspace genesis created (a client reads it from the
    // board), which is what the role check is scoped to.
    let workspace_id = host
        .groups()
        .group(&group)
        .expect("the tenant is hosted")
        .state_machine()
        .committed_events(&organization_id)
        .await
        .into_iter()
        .find(|event| event.event_type == "workspace.created")
        .map(|event| event.aggregate_id.to_string())
        .expect("genesis creates the workspace");
    let request = axum::http::Request::builder()
        .method("POST")
        .uri(format!("/organizations/{organization_id}/commands"))
        .header("authorization", "Bearer member")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({
                "workspace_id": workspace_id,
                "aggregate_id": Id::from(Key::new(&STRESS_NS, &format!("{run}-task"))).to_string(),
                "command_type": "task.create",
                "payload": { "title": "from the host" },
            })
            .to_string(),
        ))
        .expect("a request");
    assert_eq!(
        host.router()
            .oneshot(request)
            .await
            .expect("the gateway answers")
            .status(),
        axum::http::StatusCode::OK
    );

    let mut stored = before;
    for _ in 0..200 {
        stored = stored_for_group(&observer, &group).await;
        if stored > before {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        stored > before,
        "the host's outbox worker published the committed event to JetStream"
    );

    // The cursor the worker persisted is what a restart resumes from.
    let cursor =
        loomery_shell::outbox::cursor::CursorStore::in_group_dir(&root.path().join(&group))
            .load()
            .await
            .expect("the persisted cursor");
    assert!(cursor.log_index > 0);

    host.shutdown().await.expect("a clean shutdown");
}
