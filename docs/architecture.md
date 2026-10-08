# Architecture at a glance

How the pieces built so far fit together, in diagrams. Everything here is
implemented and covered by the gates; the [design tracker](design.md) says what is
planned, and [host.md](host.md) documents the runtime gaps that remain.

Every diagram here is parsed by `mise run docs-mermaid` (the same parser a
renderer uses), so a broken one fails CI instead of a reader. To look at them,
`mise run docs-mermaid-render` writes SVGs to `tools/mermaid-check/out/`.

- [Crates and dependencies](#crates-and-dependencies)
- [The write path](#the-write-path)
- [The read path](#the-read-path)
- [Authorization](#authorization)
- [Onboarding and provisioning](#onboarding-and-provisioning)
- [Inside a group](#inside-a-group)
- [The runtime host](#the-runtime-host)
- [Outbox and sagas](#outbox-and-sagas)
- [Deployment](#deployment)
- [What is not built yet](#what-is-not-built-yet)

## Crates and dependencies

Four crates, each with one job. Dependencies point *downward* only: the domain
never learns about Raft, HTTP or NATS.

```mermaid
graph BT
    core["loomery-core<br/>pure domain, no I/O, no clock, no randomness<br/>aggregates, plans, DomainError, Key, dedup"]
    genesis["loomery-genesis<br/>deterministic onboarding script<br/>Bootstrap, Progress, AddOwner"]
    shell["loomery-shell<br/>the runtime<br/>raft, control plane, gateway, outbox, saga, host"]
    server["loomery-server<br/>the binary<br/>config load, boot, serve"]

    genesis --> core
    shell --> core
    shell --> genesis
    server --> shell
```

| Crate | Owns |
|---|---|
| `loomery-core` | Command/event vocabulary and the `AggregatePlan` that decides what a command means. Injected timestamps and ids (D5/D10/D12): same input, same output, no ambient state. |
| `loomery-genesis` | The onboarding script (`AssignLeader`, `CreateWorkspace`, `AddOwner`) as a resumable `Progress` state machine, and the derived ids that make it replayable. |
| `loomery-shell` | Everything that touches the outside world: the Raft group and its storage/transport, the control plane, the gateway, the outbox, the saga runner, and the `Host` that wires them. |
| `loomery-server` | A thin binary: read configuration, install the TLS provider, build the authenticator, boot the host, serve until a signal. |

## The write path

One route, one sequence of gates. Nothing untrusted reaches consensus, and
nothing *pure* runs after it.

```mermaid
flowchart TD
    client["client"]
    http["gateway::http<br/>POST /organizations/{id}/commands<br/>extract bearer token"]
    auth["Authenticator<br/>OidcAuthenticator (JWKS, cached)<br/>KeycloakAuthenticator (test stack)<br/>StaticAuthenticator (tests)"]
    identity["Identity<br/>user_id, is_admin, verified email"]
    group_for["CommandPlane::group_for<br/>control::Router: organization to group"]
    authorize["CommandPlane::authorize<br/>organization membership<br/>plus required workspace role"]
    precompute["edge pre-compute<br/>hash_password, attribute_acceptance"]
    mint["build_command<br/>derive Key from the business tuple<br/>actor = the authenticated user"]
    propose["RaftGroup::propose<br/>write through consensus, wait for apply"]
    consensus["OpenRaft<br/>leader appends, replicates, commits"]
    apply["MemStateMachine::apply"]
    plan["loomery-core<br/>AggregatePlan::process"]
    reject["rejection<br/>409 conflict, 400 for a bad payload<br/>stable code in the body"]
    fold["fold the events<br/>aggregate state, dedup registry,<br/>membership index, applied events"]
    outcome["CommandOutcome::Applied or Replayed"]

    client --> http --> auth --> identity --> group_for --> authorize --> precompute --> mint --> propose --> consensus --> apply --> plan
    plan -->|"Err(DomainError)"| reject
    plan -->|"Ok(events)"| fold --> outcome
    reject --> http
    outcome --> http
```

Two properties are worth naming, because they are what makes retries safe:

- **Idempotence is derived, not negotiated.** The command's `Key` is a UUIDv5 of
  the business tuple, so re-submitting an identical command is a replay, and the
  dedup registry — *applied* state, not the log tail — answers
  `Applied::Replayed`. A replay whose fingerprint differs is a conflict (D12).
- **Unknown is not failure.** A consensus error means the outcome is *unknown*;
  the caller re-reads rather than assuming nothing happened.

## The read path

Reads are scoped, and a read may be answered by the leader's own state or
forwarded — the `X-Min-Index` gate is what makes read-your-writes hold.

```mermaid
flowchart TD
    client["client"]
    ws_route["GET /organizations/{id}/workspaces/{ws}/events"]
    org_route["GET /organizations/{id}/events"]
    auth["authenticate: bearer token to Identity"]
    ws_authz{"workspace_role<br/>in that workspace?"}
    org_authz{"is_admin or<br/>owns a workspace<br/>of the organization?"}
    ryw{"X-Min-Index present?"}
    gate["ensure_min_index<br/>wait up to ryw_hold for the applied index"]
    recent["Recent: answer locally"]
    forward["ForwardToLeader: 307 to the leader"]
    unavailable["Unavailable: 503"]
    filter["state machine filter<br/>events of this workspace:<br/>workspace_id == ws or aggregate_id == ws"]
    log["committed_events<br/>the organization's applied log"]

    client --> ws_route --> auth
    client --> org_route --> auth
    auth --> ws_authz
    auth --> org_authz
    ws_authz -->|"yes"| ryw
    ws_authz -->|"no"| forbid["403 Forbidden"]
    org_authz -->|"yes"| ryw
    org_authz -->|"no"| forbid
    ryw -->|"yes"| gate
    ryw -->|"no"| filter
    gate --> recent --> filter
    gate --> forward
    gate --> unavailable
    filter --> answer_ws["200: this workspace's events"]
    log --> answer_org["200: the whole log"]
    recent -.-> log
```

## Authorization

Every write is checked against the role it needs; reads are checked against a
role in the workspace they name. `is_admin` bypasses all of it, and exactly one
command is exempt.

```mermaid
flowchart TD
    caller["caller"] --> valid{"valid token?"}
    valid -->|"no"| unauth["401 Unauthorized"]
    valid -->|"yes"| admin{"is_admin?"}
    admin -->|"yes"| allow["allowed"]
    admin -->|"no"| exempt{"command_type is<br/>invitation.accept?"}
    exempt -->|"yes"| allow
    exempt -->|"no"| member{"organization member?"}
    member -->|"no"| forbid["403 Forbidden"]
    member -->|"yes"| scoped{"scoped command:<br/>does it name a workspace?"}
    scoped -->|"no"| forbid
    scoped -->|"yes"| role{"holds the required role?<br/>Viewer / Member / Owner"}
    role -->|"no"| forbid
    role -->|"yes"| allow
```

Roles are ordered by authority (`Viewer < Member < Owner`), and "organization
administrator" means *owns at least one workspace of the organization*. The
membership index that answers these questions is folded from applied events, so
it cannot disagree with the log.

The acceptance is exempt because the invitee is a stranger until it lands — which
is exactly why the gateway, not the client, supplies its attribution:

```mermaid
flowchart LR
    body["client body:<br/>user_id, email"] -.->|"discarded"| bin["never trusted"]
    token["authenticated token"] --> claim["verified email claim<br/>require_verified_email"]
    token --> subject["subject: the caller"]
    claim --> payload["payload:<br/>user_id = caller,<br/>email = verified address"]
    subject --> payload
    payload --> plan{"plan checks:<br/>actor == user_id?<br/>address == invited address?"}
    plan -->|"no"| mismatch["409 EmailMismatch<br/>or NotTheInvitee"]
    plan -->|"yes"| accepted["invitation.accepted<br/>carrying the invitation's address"]
```

## Onboarding and provisioning

Provisioning is the control plane's job; the genesis script is the tenant's. A
crash between them is recovered from state alone.

```mermaid
sequenceDiagram
    autonumber
    participant Admin as admin client
    participant HTTP as gateway
    participant Control as control group (Raft)
    participant Host as Host
    participant Tenant as tenant group (Raft)
    participant Gen as genesis script

    Admin->>HTTP: POST /organizations {organization_id, leader_user_id}
    HTTP->>HTTP: admin claim required
    HTTP->>Control: propose tenant.register (placement record)
    Control-->>HTTP: tenant.registered {group_id, replicas, leader_user_id}
    HTTP->>Host: host the group
    Host->>Tenant: boot_persistent, initialize, wait for leader
    HTTP->>Control: propose tenant.activate
    HTTP->>Control: propose organization.assign_leader
    HTTP->>Gen: bootstrap_for(tenant) then resume_provisioning
    Gen->>Tenant: workspace.create, membership.add_owner (derived ids)
    Tenant-->>Gen: Progress {assigned_leader, created_workspace, added_owner}
    HTTP-->>Admin: 201 Created

    Note over Host,Control: On boot and every 30s, Host::reconcile<br/>re-reads control state and finishes any<br/>incomplete bootstrap: crash-resumable, idempotent
```

## Inside a group

One `RaftGroup` per tenant, plus one for the control plane. The state machine is
where the pure core runs, and everything a read needs is applied state.

```mermaid
graph TD
    subgraph raft_group["RaftGroup (one per tenant, one for control)"]
        raft["OpenRaft 0.9<br/>leader election, replication, snapshots"]
        transport["tonic transport<br/>optional TLS/mTLS"]
        log_store["RocksLogStore<br/>replicated log + vote, persistent"]
        sm["MemStateMachine<br/>apply"]
    end

    subgraph applied["applied state (inside the state machine)"]
        events["applied events, in log order"]
        agg["per-aggregate state<br/>fold over events"]
        dedup["dedup registry<br/>replay vs conflict"]
        members["membership index<br/>roles by workspace"]
        watch["applied index watch channel<br/>drives the outbox and RYW"]
    end

    raft --> sm
    transport --> raft
    log_store --> raft
    sm --> events
    sm --> agg
    sm --> dedup
    sm --> members
    sm --> watch

    core["loomery-core<br/>AggregatePlan::process + fold"] --> sm
    snapshot["snapshots<br/>dedup mirror + aggregate state"] -.-> sm
    sm -.-> snapshot
```

## The runtime host

`Host` is the wiring: two kinds of group, one projected router, one command plane,
and the background tasks that keep the log moving outward.

```mermaid
flowchart TB
    gateway["axum router<br/>4 routes"]
    config["HostConfig<br/>data_dir, node_id, node_address<br/>http, nats, oidc, group tuning<br/>JSON file + LOOMERY_* overrides"]

    subgraph host["Host (loomery-server)"]
        direction TB
        control["control RaftGroup<br/>tenant placement records"]
        router["control::Router<br/>organization to group_id"]
        groups["GroupTable<br/>tenant RaftGroups"]
        plane["CommandPlane<br/>authenticate, authorize, route"]
        auth["Authenticator"]
        provisioner["Provisioner<br/>POST /organizations"]
        control --> router
        router --> plane
        control --> groups
        auth --> plane
        provisioner --> control
        provisioner --> groups
    end

    subgraph workers["background workers (started by the host)"]
        direction TB
        outbox_worker["OutboxWorker per tenant group<br/>poll 5s, backoff 100ms to 5s"]
        saga_runner["SagaRunner<br/>InvitationAcceptance"]
        reconcile["reconcile sweep<br/>every 30s"]
    end

    gateway --> plane
    gateway --> provisioner
    config --> host
    groups --> outbox_worker
    groups --> saga_runner
    control --> reconcile
    reconcile --> groups
```

## Outbox and sagas

The log is the source of truth; everything else is derived from it. Publishing is
at-least-once with a persisted cursor, and the saga is the consumer that closes
the onboarding loop.

```mermaid
flowchart LR
    apply["event applied"] --> watch["applied watch channel"]
    watch --> worker["OutboxWorker"]
    worker --> outbox["Outbox<br/>per-tenant cursor"]
    outbox --> publisher["Publisher"]
    publisher --> nats["NatsPublisher<br/>JetStream stream LOOMERY_OUTBOX<br/>subject loomery.events.{group}"]
    cursor_file["CursorStore<br/>outbox-cursor.json<br/>survives restart"] -.-> worker
    nats --> consumer["NatsConsumer<br/>peek, ack; poison message acked<br/>and counted as dropped"]
    consumer --> saga["SagaRunner"]
    saga --> handler["InvitationAcceptance<br/>is_acceptance(subject)"]
    handler --> proposal["propose membership commands<br/>back into the tenant group"]
    proposal --> apply
```

Delivery is **at-least-once**: the cursor advances only after a publish is
acknowledged, so a crash re-publishes rather than losing an event. The saga is
therefore idempotent by the same derived-key rule as everything else.

## Deployment

What `loomery-server` talks to, and what is on disk.

```mermaid
graph LR
    client["clients"] -->|"HTTPS"| server["loomery-server<br/>axum gateway"]
    server -->|"OIDC discovery + JWKS<br/>cached, refresh cooldown 1s"| keycloak["identity provider<br/>Keycloak in the test stack"]
    server -->|"publish events<br/>consume saga messages"| nats["NATS JetStream<br/>stream LOOMERY_OUTBOX"]
    server --> data["data_dir"]
    data --> ctrl_dir["control group directory<br/>log, vote, snapshots"]
    data --> tenant_dir["one directory per tenant group"]
    data --> cursor["outbox-cursor.json"]
```

The default test suite needs none of this: it runs against the trait boundaries
with in-process fakes. Keycloak and NATS are exercised by `mise run
test-services` (see [testing-services.md](testing-services.md)).

## What is not built yet

The diagrams above are the current system, not the target one. Not drawn, because
not implemented:

| Gap | What it means today |
|---|---|
| Multi-node groups and placements | Every group is single-node (`initialize` with one voter); the placement record has a `replicas` list, but nothing joins it. |
| Read models | Reads filter the applied log — `O(history)` per read, and a workspace's own events are matched by aggregate id. |
| Tracing | No OpenTelemetry; the host reports through `eprintln!`. |
| Snapshot-recovery hardening | The heavy restart paths are covered by tests, but interrupted snapshot build/purge injection is not. |
| Phases 2–7 | Work core (projects, comments, dependencies, follow), read APIs with keyset pagination, search, notifications. See the [design tracker](design.md). |
