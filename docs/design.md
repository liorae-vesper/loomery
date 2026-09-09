# Trellis — Design

**Status: DRAFT** (Rust workspace scaffolded; pure core in progress —
`crates/core` owns `Id` + `Event`; shell planned on Tokio + OpenRaft)

This is the design and planning document for **Trellis**, an event-sourced
backend for team collaboration built in **Rust** on the **Tokio** async
runtime, with **OpenRaft** providing consensus. It defines the architecture
(pure functional core + imperative shell), the workspace/derive planned crate
layout, the phased build plan, and the open decisions.

The domain model and architecture are specified across this document and the
research notes in [`docs/research/`](research/). Development guardrails live in
[`guardrails.md`](guardrails.md).

---

## 1. What Trellis is

Trellis is an **event-sourced backend for team collaboration**: organizations,
workspaces, projects, tasks, documentation, and AI-assisted workflows. Its
non-negotiable principles:

1. **Append-only.** Never mutate; corrections are compensating events.
2. **Pure core.** `prepare`/`apply` are deterministic; all I/O in the shell.
3. **One consensus group per organization** + a control group (users, orgs,
   router).
4. **Genesis bootstrap.** Tenant groups are born with their first three events
   committed — no cross-group provisioning sagas.
5. **Read-Your-Writes.** Post-write reads carry `X-Min-Index` session tokens.
6. **WAL-tailing outbox.** No direct network calls from the consensus loop.
7. **Edge pre-computation.** Blocking-but-pure work (bcrypt/argon2) at the
   gateway.
8. **Frozen payloads + upcast + writer gating** — old events decode forever.
9. **DedupIndex** — the only idempotency store, folded into group state.
10. **No 2PC.** Cross-group coordination is choreography via NATS + sagas.

---

## 2. Architecture

Trellis follows Gary Bernhardt's **Functional Core, Imperative Shell**. All
business logic lives in pure, deterministic functions; side effects (storage,
consensus, networking, integration) are pushed to the outer shell.

### 2.1 The core (pure)

```
execute(state, command) -> Result<Execution, DomainError>
apply(state, event)     -> State
```

- `prepare` validates a command against state and produces the events to append
  (optionally paired with outbox integration events), or returns an error. It is
  **pure** — no I/O, no wall clock, no randomness, no message passing.
- `apply` folds an event into state; given the same state and event it always
  yields the same state (the replay primitive).
- **Determinism is preserved by injecting IDs and timestamps through the
  command envelope** — the core never reads the clock or generates IDs.
- This makes the algebraic guarantee hold:
  `fold(fold(state, e1), e2) == fold(state, e1 ++ e2)` — the property every
  Raft replica depends on (identical commands, identical applies).

The core knows nothing about: OpenRaft, Tokio, NATS, storage engines,
serialization details, or which node is the leader.

### 2.2 The shell (imperative, Tokio)

```
[HTTP gateway (axum)] -> [edge pre-compute] -> [router] -> [Raft::client_write]
                                                             │  propose → log → quorum
                                                             v
                               execute/apply (pure core) → in-memory state
                                                             │  (+ async snapshot)
                                                             v
                              WAL tailer → NATS JetStream (outbox)
```

The shell is a Tokio application: one task per Raft group, `mpsc` channels for
command/response flow, background tasks for snapshots, the outbox tailer, and
saga runners. The shell owns:

- **Transport** — the gateway (axum) plus the OpenRaft `RaftNetwork`
  implementation (tonic gRPC is the default recommendation, see [D1](#d1-consensus)).
- **Edge pre-computation** — password hashing, token minting, before commands
  enter consensus.
- **Routing** — `organization_id → group` lookup (see §2.3).
- **Consensus** — OpenRaft: one `Raft` instance per organization plus one
  control group (see [D1](#d1-consensus)).
- **Persistence** — the `RaftLogStorage` (log) and `RaftStateMachine` (state +
  snapshots) implementations (see [D2](#d2-storage-engine)).
- **Outbox** — a tailer task streaming committed log entries and publishing
  domain events to NATS JetStream via `async-nats` ([D8](#d8-sagas-bus)).
- **Recovery** — startup replay: load latest snapshot, apply committed-but-
  unapplied log after it, rebuild read models.

### 2.3 Consistency: Eventual + Read-Your-Writes

Reads are served from any replica's in-memory state (zero-I/O — a
`DashMap`/`Arc<RwLock<HashMap>>` read model projected by the local state
machine). A client that wrote at log-index *N* includes `X-Min-Index: N` on
subsequent reads; a lagging replica holds (≤50 ms) until it catches up, then
serves the read — otherwise the gateway forwards the request to the leader.

- The committed index is exposed via OpenRaft's `RaftMetrics` (and the local
  state machine's applied index); the RYW hold is a small async wait on those.
- Stale-after-write is bounded to replication lag; plain eventual reads remain
  available to other clients.

---

## 3. Crate layout (pure core)

The core is a set of structs, traits, and pure functions living in
`crates/core`, mirroring the domain model. The package is named
**`trellis-core`** — a crate literally named `core` would shadow the standard
library's `core` in rustdoc/doctests and in any downstream build using proc
macros (their generated code refers to `::core` paths). Keep it that way.

| Item (crate path) | Purpose |
|---|---|
| `trellis_core::id::Id` | UUIDv7 wrapper (canonical string form) — see [D4](#d4-uuid-representation) |
| `trellis_core::envelope::EventEnvelope` | The wrapper for every committed event (see §6) |
| `trellis_core::envelope::Command` | Command form: `prepare` input, carries injected `occurred_at` + ids |
| `trellis_core::actor::Actor` | `{ kind: User \| System \| Saga, user_id, saga_name }` — who performed an action |
| `trellis_core::timestamp::Timestamp` | ms-since-epoch UTC wrapper (injected; the core never reads the clock) |
| `trellis_core::error::DomainError<C>` | Generic-over-code domain error: `{ code, message, cause }` — each domain area brings its own code enum; `cause: Option<anyhow::Error>` chains shell-side errors |
| `trellis_core::versioning` | Upcast contract: per-aggregate **static chains** (closed `KnownPayload` enum + exhaustive match), `UpcastCode`, `Upcaster` fn alias (P2) |
| `trellis_core::aggregate::AggregatePlan` | The trait: `prepare`, `apply`, plus shared `process` and `fold` helpers |
| `trellis_core::aggregate::Execution` (+ `OutboundEvent`, `ContentType`) | Result of a command: domain events to commit + optional outbound integration events (D8/D11) |
| `trellis_core::dedup::DedupIndex` | The idempotency window (P3) folded into group state |
| `trellis_core::org::Organization` | Organization aggregate |
| `trellis_core::user::User` | User aggregate |
| `trellis_core::workspace::Workspace` | Workspace aggregate |
| `trellis_core::task::Task` | Task aggregate |
| `trellis_core::membership::OrganizationAssignment` | Organization ↔ User membership |
| `trellis_core::membership::WorkspaceMembership` | Workspace ↔ User membership with role |
| `trellis_core::test_helpers` / `proptest` support | Deterministic envelope builder for tests |

### 3.1 Core contracts

```rust
// Static associated functions — the plan is its *type* (module-style, like
// Elixir's `mod.execute/2`); there is no instance to borrow.
trait AggregatePlan<State, ErrorCode> {
    fn prepare(state: State, command: Command)
        -> Result<Execution, DomainError<ErrorCode>>;

    fn apply(state: State, event: Event) -> State;
}

// shared: dedup hit -> Replayed; miss -> A::prepare
fn process::<State, ErrorCode, A: AggregatePlan<State, ErrorCode>>(
    state, registry, command) -> Processed<ErrorCode>;
// fold = sequential apply
fn fold::<State, ErrorCode, A: AggregatePlan<State, ErrorCode>>(
    state, events: &[Event]) -> State;
```

Critical-section contract: processing a command does **not** record the dedup
entry; the shell records it *after* the events are durably appended *and*
applied (inside the state machine), so a failed append needs no rollback.

**Note on the current scaffold:** `EventEnvelope::occurred_at` is the
injected [`Timestamp`] (`crate::timestamp`, i64 ms since epoch, D5) — the
`Instant` placeholder was replaced during the scaffold. `Timestamp::now()`
and `Id::new()` exist but are documented as **shell-side conveniences**; the
core receives values through the command envelope. Tracked in
`CONTINUE.md`.

---

## 4. Shell layout — Rust/Tokio/OpenRaft counterparts

| Concept | Rust counterpart |
|---|---|
| Runtime | **Tokio** (multi-threaded, `flavor = "multi_thread"`); one task per Raft group |
| Consensus (one group per org + control group) | **OpenRaft** — async-native Raft in Rust; `Raft<TypeConfig>` per group, each with its own `RaftStorage`. See [D1](#d1-consensus) |
| Storage (log / state / snapshots) | `RaftLogStorage` + `RaftStateMachine` impls — sled, rocksdb, or hand-rolled segment engine (see [D2](#d2-storage-engine) + research notes) |
| Transport for Raft RPCs | `RaftNetwork` impl over **tonic** (gRPC) — `append_entries`, `vote`, `full_snapshot` |
| WAL-tailing outbox → NATS JetStream | A Tokio task streaming committed entries, publishing via **async-nats**; dedup by `(group_id, log_index)` ([D8](#d8-sagas-bus), [D11](#d11-outbox-subjects-stream-naming-and-dedup-identity)) |
| Router (`organization_id` → group) | `DashMap` read model projected by the control group's state machine |
| Read models / projections | `DashMap`/`Arc<RwLock<HashMap>>` tables fed by projections applied inside the state machine |
| Worker lifecycle / idle groups | OpenRaft groups idle silently (no heartbeat traffic when idle); snapshotting is async and backgrounded |
| Gateway, edge pre-compute (argon2) | **axum** HTTP server; hash before the command enters consensus |
| Saga runner | Tokio tasks consuming NATS JetStream with retry classification; timers via `tokio::time` |
| Bootstrap worker | Genesis script ①②③, emits as `actor = Saga { user_id: None, name: "control-plane:Bootstrap" }`, deterministic `uuid_v5`-style causation |
| Read-Your-Writes (`X-Min-Index`, 50 ms hold) | axum middleware; follower catch-up wait on `RaftMetrics`, then leader redirection |
| OIDC auto-provision, system-admin claim | axum middleware; `ADMIN_GROUP` env |
| Observability | `tracing` + `tracing-opentelemetry` + OTLP exporter (Phase 7) |

---

## 5. Phase plan

### Phase 0 — Pure core foundation
`Id`, `Timestamp`, `Actor`, `Error`/`ErrorCode`, `Event`/`Command`
(serde_json), versioning/upcast machinery, the `AggregatePlan` trait, `DedupIndex`,
and the first aggregates (Organization, User, Workspace, Task,
OrganizationAssignment, WorkspaceMembership) with `proptest` property tests.

**Gate:** envelope round-trip + upcast tests green; `DedupIndex` eviction +
dedup-hit tests green; transition-matrix property tests green for all six
aggregates.

### Phase 1 — Control plane, genesis & onboarding
Control group on OpenRaft; router read model; genesis bootstrap worker;
axum gateway with `causation_id` minting, RYW `X-Min-Index` hold, edge
pre-compute hooks; OIDC auto-provision + system-admin claim; Invitation domain
+ acceptance saga; NATS outbox tailer first slice via `async-nats`; saga-runner
seed.

**Gate (E2E onboarding):** register org → genesis ①②③ → workspace + Owner;
invite by email → accept → provisioned → can log in and read the board;
crash mid-provisioning resumes with no duplicate genesis; duplicate
`causation_id` dedup-hits with RYW honored; admin-only commands enforced.

### Phase 2 — Work core
Task/Project/ProjectTask/Comment/TaskDependency/TaskFollow aggregates; read
models; query API with keyset pagination; guards (membership, circular
dependencies, completion); CompletionSaga + TaskCompletedDerivation +
BlockedStatusProjector; replay-equivalence test (same log → identical state on
N replicas).

### Phase 3 — Notifications & delivery
Notification + NotificationPreference aggregates; fan-out sagas; InApp
(WebSocket), Email, Slack gateways; scheduler (digests, invitation expiry);
retention caps + replay.

### Phase 4 — Knowledge base & RAG
Documentation aggregate; document processing pipeline; **tantivy** FTS engine
([D6](#d6-fts-engine)); embedding sidecar (checkpointed, never re-call the LLM);
tenant-scale brute-force KNN (or `pgvector`, [D7](#d7-vector-store)).

### Phase 5 — Automation & integrations
WorkflowRule aggregate + rule engine (500 ms budget, fail-open) +
ActionExecutor (retry/backoff/circuit/dedup); Connection domain with
gateway-side encryption + key rotation; health monitors; system_sync actions.

### Phase 6 — Agent (Capture)
Capture aggregate + ActionDispatcher; tool ACL; agent context view (RAG +
conversation); destructive-action confirmation; failure notifications.

### Phase 7 — Hardening & compliance
`tracing`/OpenTelemetry observability; GDPR erasure (crypto-shredding runbook);
durable archive; deployment runbooks + rolling upgrade drill (writer gating);
perf budget load tests.

---

## 6. The event envelope

Every committed event is wrapped in an envelope (`Event`, §3). Fields:
`envelope_version`, `id`, `aggregate_id`, `organization_id` (= group id),
`workspace_id` (optional), `actor`, `occurred_at` (injected), `causation_key`
(dedup key), `correlation_id` (trace), `event_type` (string name), `payload` (a versioned
`Payload { version, data }`, `data` being the event value as a JSON string),
`actor` (the [`Actor`] that emitted it — user, system, or saga).

The envelope is the one canonical, string-typed format for published domain
events (used by the outbox + all saga/projector consumers). The OpenRaft log
stores machine structures opaquely (log entries are `serde_json` values/D3);
the envelope is the published form, version-guarded by `envelope_version` +
`payload_version`.

- **Versioning (P2):** never mutate a shipped payload struct; a change is a new
  frozen version (`TaskCreatedV2`). Each aggregate owns its chains as a closed
  `KnownPayload` enum + exhaustive `upcast` match (`versioning` docs) — adding
  a version is a compile-time forcing function: new variant ⇒ missing arm ≠
  builds. `apply` upcasts at fold time; stored bytes are never rewritten
  (append-only). Snapshots are versioned too; stale snapshots are upcast on
  load.
- **Writer gating:** new event types are written only once all replicas run a
  supporting version (cluster capability flag on the control group). Decode
  first, write second.

---

## 7. Decisions register

Open decisions with recommendations. Flip status to **DECIDED** as they land.

### D1 — Consensus
**Recommendation: OpenRaft.** OpenRaft is the de-facto production-grade Raft
library in Rust: async-native (Tokio), explicit `RaftStorage`/`RaftNetwork`
interfaces we control, snapshot + membership APIs we need for the per-tenant
model, and years of production use in **databend** (`databend-meta`) among
others. An opinionated application server (APIs -> `Raft::client_write` ->
log replication -> `StateMachine::apply`) maps 1:1 onto our command flow
(see the `raft-kv-memstore` example). Hand-rolling Raft in Rust duplicates
battle-tested machinery for no benefit at our cluster size (3–5 nodes).
**Status: DECIDED (target).** First slice (Phase 1 control group) lands on
OpenRaft 0.9.x; the transport is `RaftNetwork` over **tonic gRPC** (matching
OpenRaft's `RaftNetworkV2` RPC shape — append_entries / vote / full_snapshot,
plus the optional `stream_append` pipelining). Findings so far:
- OpenRaft 0.9 splits storage into **`RaftLogStorage`** (log) +
  **`RaftStateMachine`** (apply/snapshot) with an `Adapter` bridging the old
  combined `RaftStorage`; log and state-machine operations run in parallel.
- Async traits use OpenRaft's `#[openraft-macros::add_async_trait]`; network
  methods take an `RPCOption` (hard/soft TTL) — see
  `docs/research/openraft-storage.md` and the upgrade guides in the crate.
- Multiple co-resident groups per process is the intended deployment (each
  tenant group + the control group = independent `Raft` tasks); idle groups
  send no heartbeats, matching Trellis's silent-tenant property.
- A Raft **group** in Trellis = one `organization_id` (or the control group);
  group membership changes (add/remove node) go through `change_membership`.

### D2 — Storage engine
**Recommendation: start with `sled` or the hand-rolled segment engine** (see
`docs/research/storage-engine-alternatives.md` + `indexed-segment-file-format.md`)
behind the `RaftLogStorage`/`RaftStateMachine` traits. RocksDB (`rocksdb` crate)
is the escape hatch if raw throughput or a queryable store is needed —
OpenRaft ships RocksStore/SledStore/MemStore examples we can crib.
**Status: OPEN** — needs a spike against OpenRaft 0.9's storage calls
(`try_append_entry`, `get_log_state`, `save_committed`, snapshot builder).

### D3 — Envelope/payload encoding
Options: `bincode` (compact, fast, not human-readable) vs **serde_json**
(stable, human-readable, versioned via `payload_version` + upcast).
**Recommendation: serde_json for the envelope/payload wire format**; `bincode`
allowed for log entries / snapshots behind the storage traits (log bytes are
opaque to replicas' `apply`, and snapshots are best-effort derived data).
**Status: RECOMMENDED**

### D4 — UUID representation
**Recommendation: `uuid` crate, `Uuid::now_v7()` strings** via the `Id` newtype
in `crates/core` (matches the JSON API; debuggable; already in Cargo.lock).
Compact 16-byte binary form is a later optimization only if log size demands it.
**Status: DECIDED (scaffold — `uuid 1.26`, feature `v7`)**

### D5 — Timestamps
**Recommendation: integer ms wrapper `Timestamp`** (injected through the command
envelope; mirrors the injected-timestamp rule), compact and deterministic.
Convert to `chrono::DateTime`-style types at API boundaries.
**Status: DECIDED (scaffold — `crate::timestamp`, i64 ms; `now()` is
shell-side)**

### D6 — FTS engine (Phase 4)
Options: **tantivy** (pure-Rust, mature, the standard choice) vs SQLite FTS via
`rusqlite` vs a hand-rolled index. Needs a spike.
**Status: OPEN (Phase 4)**

### D7 — Vector store (Phase 4)
**Options:** tenant-scale brute force with `ndarray`/`half` (simple, no deps)
vs `hnswlib-rs` (ANN) vs `sqlite-vec` via rusqlite vs `pgvector` (on
Postgres). Platform-wide ANN is explicitly out of v1.
**Status: OPEN (Phase 4)**

### D8 — Sagas bus
**Recommendation: `async-nats` over NATS JetStream**, matching the choreography
model; dedup by `(group_id, log_index)`.
**Status: RECOMMENDED.** Publishing convention: `Nats-Msg-Id =
<group_id>:<log_index>:e<pos>|i<pos>`; the JetStream dedup window absorbs
re-issued publishes from leader failover or restart ([D11](#d11-outbox-subjects-stream-naming-and-dedup-identity)).

### D9 — Storage of "cold" read-model state
ETS/Sled-equivalent question: in Rust, `sled`/`redb`/SQLite (`rusqlite`)
for durable read models (retention, archives), or a consensus-backed
projection store. Phase 3 concern.
**Status: OPEN (Phase 3)**

### D10 — Data-shape validation
**Recommendation: serde derive + [`validator`](https://crates.io/crates/validator)
(or `garde`) for shape/value validation at the envelope boundary.** Follows from
the zoi-era lesson: validate untrusted input against bounded, compile-time
schemas *before* it enters `execute`. In Rust the analogue of the OOM
atom-table lesson is: never build unbounded types/enums from user strings —
parse into `serde_json::Value`, validate against a bounded schema, then
convert. Group names derived from UUIDs are additionally shape-gated (uuid
format) and capped.
**Status: RECOMMENDED**

### D11 — Outbox subjects, stream naming, and dedup identity
**Decision (carried from the original design) — the NATS subject/stream
naming for the first outbox slice:**
- Stream: one `TRELLIS_OUTBOX` JetStream stream per deployment, capturing
  `trellis.>`.
- Domain events publish to `trellis.events.<group_id>.<event_type>` (e.g.
  `trellis.events.org:01hx….invitation_accepted`); integration events keep
  their own subject verbatim (e.g. `trellis.email.invitation`).
- Dedup: `Nats-Msg-Id = <group_id>:<log_index>:e<pos>` for domain events,
  `…:i<pos>` for integration events of the same log entry — one log entry
  yields distinct per-message ids while the `(group_id, log_index)` pair stays
  in the headers/cursor for saga-level dedup.
- Outbox envelope fields per event follow `Event` (deterministic ids —
  re-publishes are byte-identical).
**Status: DECIDED (carried over)**

---

## 8. Progress tracker

### Scaffold
- [x] Cargo workspace (`crates/*`, edition 2024, resolver 2), toolchain
      `rust 1.98.1` via mise
- [x] `rustfmt.toml` (max_width 100, tabs 4, newline Unix)
- [x] `deny.toml` license allowlist + advisories + bans (no openssl, md-5, sha1;
      crates.io only)
- [x] hk hooks (commit-msg: cog verify + fmt/deny/clippy gates; WIP escape
      hatch; linear history) — see `guardrails.md`
- [x] `mise run verify` — check + clippy -D warnings + fmt + deny + package

### Pure core (Phase 0)
- [x] `trellis_core::id::Id` (UUIDv7 wrapper, serde round-trip + shape tests)
- [x] `trellis_core::envelope::EventEnvelope` + `Payload` (serde round-trip +
      exact wire-format snapshot test)
- [x] `trellis_core::timestamp::Timestamp` (injected i64 ms, D5; `now()` shell-side)
- [x] `trellis_core::envelope::Command` (injected carrier, mirrors `Event`; round-trip test)
- [x] `trellis_core::actor::Actor` (`User \| System \| Saga`, serde round-trip + equality tests)
- [x] `trellis_core::error::DomainError<C>` (generic over code enum; `cause` chains via `anyhow`; display/equality/source tests)
- [x] `trellis_core::versioning` (per-aggregate static chain contract:
      `UpcastCode` + `Upcaster` + documented pattern; chains land with each
      aggregate)
- [x] `trellis_core::aggregate::AggregatePlan` (`prepare`/`apply` trait +
      `process`/`fold` free fns; dedup-on-`causation_key`; property + unit
      tests)
- [x] `trellis_core::aggregate::Execution` — real result type: `events` to
      commit + `outbound_events` (subject/content-type/payload); carried by
      `Executed(Execution)`; covered by unit test (35 total)
- [x] `trellis_core::dedup::DedupIndex` — bounded FIFO idempotency window
      (`dashmap` + `VecDeque`, deterministic eviction; unit + **proptest**
      model-based and metadata-retention properties)
- [ ] `trellis_core::org::Organization`
- [ ] `trellis_core::user::User`
- [ ] `trellis_core::workspace::Workspace`
- [ ] `trellis_core::task::Task`
- [ ] `trellis_core::membership` (OrganizationAssignment / WorkspaceMembership)
- [ ] Property tests (`proptest`: replay determinism, fold associativity,
      random-commands-never-crash) *(dedup window ✓ — model-based eviction
      + re-insert-never-refreshes)*

**Phase 0 gate ⏳** — envelope round-trip ✓ (24 tests incl. proptest);
DedupIndex eviction + dedup-hit property tests ✓; transition-matrix property
tests green for all six aggregates; coverage floor met.

### Shell (Phases 1–7)
- [ ] Phase 1 control plane *(next — OpenRaft 0.9 spike)*
  - [ ] Control group on OpenRaft (`RaftLogStorage`/`RaftStateMachine` +
        `RaftNetwork` over tonic): `shell::control`
  - [ ] Router read model + RYW `X-Min-Index` hold
  - [ ] Genesis bootstrap worker (deterministic ①②③, uuid_v5-style causation,
        crash-resume idempotency)
  - [ ] Outbox slice: `shell::outbox` publisher + `async-nats` transport (D8/D11)
  - [ ] Saga-runner seed + `InvitationSaga` (acceptance → assignment + membership)
  - [ ] axum gateway: command plane, causation minting, edge pre-compute
        (argon2), admin claim enforcement, RYW middleware
  - [ ] E2E gate candidates green in tests: register → genesis → workspace +
        Owner; invite → accept → provisioned → login; crash-resume
        no-duplicate; dedup-hit with RYW; admin enforcement
- [ ] Phase 2 work core
- [ ] Phase 3 notifications
- [ ] Phase 4 knowledge base & RAG
- [ ] Phase 5 automation & integrations
- [ ] Phase 6 agent (Capture)
- [ ] Phase 7 hardening & compliance

---

## 9. Provenance

`docs/research/` holds design research notes for the consensus, storage, and
integration layers:

| Note | Covers |
|---|---|
| `openraft-vs-alternatives.md` | OpenRaft vs hand-rolled Raft, external coordination (etcd/consul), DB replication; why per-tenant Raft groups |
| `storage-engine-alternatives.md` | sled/redb/RocksDB/SQLite vs hand-rolled segment files; vector store constraint; backup anchored to event index |
| `indexed-segment-file-format.md` | An embedded-index segment-file format adapted from the log-storage research |
| `openraft-storage.md` | OpenRaft 0.9 storage interfaces — `RaftLogStorage`/`RaftStateMachine` split, snapshot builder, gotchas |

---

*Last updated: scaffold era — Rust workspace initialized; `crates/core` holds
`Id` + `Event`; Phase 0 (pure core) is next.*