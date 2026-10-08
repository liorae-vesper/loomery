# Loomery — Design

**Status: IN PROGRESS** — the pure core, the shell (OpenRaft, tonic networking,
RocksDB storage, TLS, batching), the control plane, the gateway and the outbox +
sagas are implemented. OIDC and NATS bindings, deployment, observability and
Phases 2–7 remain.

This is the design and planning document for **Loomery**, an event-sourced
backend for team collaboration built in **Rust** on the **Tokio** async
runtime, with **OpenRaft** providing consensus. It defines the architecture
(pure functional core + imperative shell), the workspace/derive planned crate
layout, the phased build plan, and the open decisions.

The domain model and architecture are specified across this document and the
research notes in [`docs/research/`](research/). Development guardrails live in
[`guardrails.md`](guardrails.md).

---

## 1. What Loomery is

Loomery is an **event-sourced backend for team collaboration**: organizations,
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
11. **"Find anything" within the tenant.** Search is a first-class read path, not
    a report: any entity — and, where it matters, the history that produced it —
    is findable by free text *and* by its fields, and a result a caller may not
    read is a result they do not get. The tenant (one organization, one consensus
    group) is the search boundary, so an index lives beside its group and is
    rebuilt from the record ([D13](#d13--history-is-append-only-checkpoints-carry-state)).

---

## 2. Architecture

For a picture of how the built pieces fit together — crate graph, the write and
read paths, authorization, onboarding, group internals, host wiring and
deployment — see [architecture.md](architecture.md).

Loomery follows Gary Bernhardt's **Functional Core, Imperative Shell**. All
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
                                                             │  (checkpoint before reply)
                                                             v
                              WAL tailer → NATS JetStream (outbox)
```

The shell is a Tokio application: one task per Raft group, `mpsc` channels for
command/response flow, background tasks for snapshots, the outbox tailer, and
saga runners. The shell owns:

- **Transport** — the gateway (axum) plus the OpenRaft `RaftNetwork`
  implementation over tonic gRPC, with opt-in TLS/mTLS (see [D1](#d1--consensus)).
- **Edge pre-computation** — password hashing, token minting, before commands
  enter consensus.
- **Routing** — `organization_id → group` lookup (see §2.3).
- **Consensus** — OpenRaft: one `Raft` instance per organization plus one
  control group (see [D1](#d1--consensus)).
- **Persistence** — the `RaftLogStorage` (log) and `RaftStateMachine` (state +
  snapshots) implementations (see [D2](#d2--storage-engine)).
- **Outbox** — a tailer task streaming committed log entries and publishing
  domain events to NATS JetStream via `async-nats` ([D8](#d8--sagas-bus)).
- **Recovery** — restore the durable applied-state checkpoint and the separately
  stored Raft snapshot; OpenRaft replays the committed suffix after the restored
  applied index. Snapshots and log retention follow [D2](#d2--storage-engine).

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
**`loomery-core`** — a crate literally named `core` would shadow the standard
library's `core` in rustdoc/doctests and in any downstream build using proc
macros (their generated code refers to `::core` paths). Keep it that way.

| Item (crate path) | Purpose |
|---|---|
| `loomery_core::id::Id` | Canonical UUID id — minted `UUIDv7` or derived `UUIDv5` (canonical string form) — see [D4](#d4--uuid-representation), [D12](#d12--identity-minted-intents-and-derived-entities) |
| `loomery_core::key::Key` | Derived identity (`UUIDv5`): causation/dedup keys, event ids, derived entity ids (`Id::from(key)`) — see [D12](#d12--identity-minted-intents-and-derived-entities) |
| `loomery_core::envelope::Event` | The wrapper for every committed event (see §6) |
| `loomery_core::envelope::Command` | Command form: `prepare` input, carries injected `occurred_at` + ids |
| `loomery_core::actor::Actor` | `{ kind: User \| System \| Saga, user_id, saga_name }` — who performed an action |
| `loomery_core::timestamp::Timestamp` | ms-since-epoch UTC wrapper (injected; the core never reads the clock) |
| `loomery_core::error::DomainError<C>` | Generic-over-code domain error: `{ code, message, cause }` — each domain area brings its own code enum; `cause: Option<anyhow::Error>` chains shell-side errors |
| `loomery_core::versioning` | Upcast contract: per-aggregate **static chains** (closed `KnownPayload` enum + exhaustive match), `UpcastCode`, `Upcaster` fn alias (P2) |
| `loomery_core::aggregate::AggregatePlan` | The trait: `prepare`, `apply`, plus shared `process` and `fold` helpers |
| `loomery_core::aggregate::Execution` (+ `OutboundEvent`, `ContentType`) | Result of a command: domain events to commit + optional outbound integration events (D8/D11) |
| `loomery_core::dedup::Registry` | The idempotency window (P3) folded into group state; entries carry the intent fingerprint |
| `loomery_core::org::Organization` | Organization aggregate |
| `loomery_core::user::User` | User aggregate |
| `loomery_core::workspace::Workspace` | Workspace aggregate |
| `loomery_core::task::Task` | Task aggregate |
| `loomery_core::membership::OrganizationAssignment` | Organization ↔ User membership |
| `loomery_core::membership::WorkspaceMembership` | Workspace ↔ User membership with role |
| `loomery_core::test_helpers` / `proptest` support | Deterministic envelope builder for tests |

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

The built shell (port, genesis worker, networked persistent Raft groups) is documented in
[`shell.md`](shell.md); the table below is the planned full layout.

| Concept | Rust counterpart |
|---|---|
| Runtime | **Tokio** (multi-threaded, `flavor = "multi_thread"`); one task per Raft group |
| Consensus (one group per org + control group) | **OpenRaft** — async-native Raft in Rust; `Raft<TypeConfig>` per group, each with its own `RaftLogStorage`/`RaftStateMachine`. See [D1](#d1--consensus) |
| Storage (log / state / snapshots) | `RaftLogStorage` + `RaftStateMachine` over RocksDB; awaited state checkpoints and scheduled Raft snapshots (see [D2](#d2--storage-engine)) |
| Transport for Raft RPCs | `RaftNetworkV2` over **tonic** gRPC — `append_entries`, `vote`, transport-fragmented `full_snapshot`; opt-in TLS/mTLS |
| WAL-tailing outbox → NATS JetStream | A Tokio task streaming committed entries, publishing via **async-nats**; dedup by `(group_id, log_index)` ([D8](#d8--sagas-bus), [D11](#d11--outbox-subjects-stream-naming-and-dedup-identity)) |
| Router (`organization_id` → group) | `DashMap` read model projected by the control group's state machine |
| Read models / projections | `DashMap`/`Arc<RwLock<HashMap>>` tables fed by projections applied inside the state machine |
| Worker lifecycle / idle groups | Heartbeat/election behavior follows OpenRaft configuration; scheduled snapshots are separate from awaited state checkpoints |
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

The read-model half is shaped by [storage-layout.md](storage-layout.md) (the
`projections` family) and [search.md](search.md) (the index beside it), both
rebuilt from the append-only record.

### Phase 3 — Notifications & delivery
Notification + NotificationPreference aggregates; fan-out sagas; InApp
(WebSocket), Email, Slack gateways; scheduler (digests, invitation expiry);
retention caps + replay.

### Phase 4 — Knowledge base & RAG
Documentation aggregate; document processing pipeline; **tantivy** FTS engine
([D6](#d6--fts-engine)); embedding sidecar (checkpointed, never re-call the LLM);
tenant-scale brute-force KNN (or `pgvector`, [D7](#d7--vector-store-phase-4)).

The FTS engine is no longer a Phase-4 question: [D6](#d6--fts-engine) is decided
(tantivy, one index per tenant) and its design is [search.md](search.md), because
"find anything" is [principle 11](#1-what-loomery-is) rather than a knowledge-base
feature.

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
(dedup key), `correlation_key` (saga/workflow correlation), `event_type`
(string name), `payload` (a versioned
`Payload { version, data }`, `data` being the event value as a JSON string),
`actor` (the [`Actor`] that emitted it — user, system, or saga).

The envelope is the one canonical, string-typed format for published domain
events (used by the outbox + all saga/projector consumers). The OpenRaft log
stores machine structures opaquely (log entries are `serde_json` values/D3);
the envelope is the published form, version-guarded by `envelope_version` +
`payload_version`.

**Identity (D12).** Envelope identity is never minted inside the core: the
committed command is re-`prepare`d on *every* replica, so anything random would
diverge. `occurred_at`, `actor` and the keys arrive injected; `Event::id` is
**derived** from the command's `causation_key` and the event's index
(`Command::event_id`), which is also what keeps a rebuilt retry proposing the
same events. `Command::fingerprint` hashes the intent (type, payload, scope —
not the per-attempt fields) and is recorded beside the dedup entry so a reused
idempotency key on a different request is answered with a conflict rather than
replayed.

- **Versioning (P2):** never mutate a shipped payload struct; a change is a new
  frozen version (`TaskCreatedV2`). Each aggregate owns its chains as a closed
  `KnownPayload` enum + exhaustive `upcast` match (`versioning` docs) — adding
  a version is a compile-time forcing function: new variant ⇒ missing arm ≠
  builds. `apply` upcasts at fold time; stored bytes are never rewritten
  (append-only). Snapshots carry a format version. The current implementation
  accepts v1 (including legacy snapshots without an explicit version) and rejects
  unsupported versions; snapshot upcast chains remain future work.
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
**Status: DECIDED and implemented (migrated to OpenRaft 0.10).** OpenRaft
`RaftNetworkV2` over **tonic gRPC** for `append_entries`, `vote` and
`full_snapshot`, with the transport owning snapshot fragmentation and
reassembly. `stream_append` currently uses openraft's default sequential
implementation; a bidirectional `StreamAppend` RPC is the next step. See
[openraft-010-migration.md](research/openraft-010-migration.md) for what the
migration changed.
A versioned protobuf envelope routes each request by group id and carries the
pinned OpenRaft JSON request/Result types. One listener serves many groups.

TLS is opt-in through `GroupConfig.transport.server_tls` and `client_tls`.
The server loads a PEM certificate/key pair; clients verify a configured CA and
the peer URI host (or an explicit `server_name`). A server-side client CA bundle
requires client certificates, enabling mTLS with configured client identities.
Configured TLS requires `https://` membership addresses; plaintext requires
`http://`. Invalid material or a scheme mismatch fails without plaintext fallback.
Server configuration belongs to the shared listener, and client configuration
to each group's peer connections. Rotation requires recreating the listener and
channels; automatic certificate reload is not implemented.

**Leader command batching:** opt-in `GroupConfig.proposals` limits a shared
proposal queue by count, command bytes, collection delay and channel capacity.
Concurrent producers clone `RaftGroup::writer()`; `GroupOps::propose` uses the
same writer. Several commands can share one Raft entry and index, with ordered
application and individual dedup/rejection responses after quorum and apply.
Successful siblings are not rolled back when another command is rejected.
This preserves synchronized log writes and the configured apply durability;
it does not change how appends are scheduled or flushed: each append is still one
synchronized WAL batch, and since the 0.10 migration several appends may be in
flight at once.
Batching defaults to disabled. Upgrade every replica before enabling the new
batch entry format; old-binary downgrade after batched logs is unsupported.
Snapshot/retention settings count entries, so their command coverage increases
with batching. See [configuration](raft-configuration.md#opt-in-command-batching)
and [paired measurements](benchmarks/batching.md).

Findings:
- OpenRaft splits storage into **`RaftLogStorage`** (log) +
  **`RaftStateMachine`** (apply/snapshot); log and state-machine operations run in
  parallel. 0.10 moved the log's vote read to `RaftLogReader` and the state
  machine's `apply` onto a stream of per-entry responders.
- Async traits use OpenRaft's `#[openraft-macros::add_async_trait]`; network
  methods take an `RPCOption` (hard/soft TTL) — see
  `docs/research/openraft-storage.md` and the upgrade guides in the crate.
- Multiple co-resident groups per process is the intended deployment (each
  tenant group + the control group = independent `Raft` tasks). Heartbeats and
  elections are enabled by default. Silent tenant groups require a separate
  lifecycle design; they are not guaranteed by the current implementation.
- A Raft **group** in Loomery = one `organization_id` (or the control group);
  group membership changes (add/remove node) go through `change_membership`.

### D2 — Storage engine
**Status: DECIDED — RocksDB with a persistent state machine.** Each replica
opens a distinct database per group. Log/vote/commit writes use the WAL with
`sync=true`; log purge and its floor marker are atomic. `apply()` awaits a
complete checkpoint containing aggregate state, applied events, dedup, membership
and applied index before returning success. Recovery loads this checkpoint and
replays committed logs after its applied index. Storage errors stop the Raft node.

Raft snapshots are separate records, built by OpenRaft according to
`GroupConfig.raft.snapshot_policy` (default: `LogsSinceLast(5000)`). Snapshot
creation persists the record before publishing it to OpenRaft; installation
atomically saves both checkpoint and snapshot before replacing live state.
OpenRaft controls log purge using snapshot coverage and configured retention.
An apply checkpoint is not itself a trigger for snapshot publication or log purge.

**Scheduling policy:** keep awaited state checkpoints for this first persistent
slice; use background scheduling for Raft snapshots, while synchronizing their
completed storage writes. Background execution and durable synchronization are
independent. Database calls run on Tokio's blocking pool and are awaited;
full-state serialization currently runs on the calling task. Awaited checkpoint
cost grows with history. Incremental persistence or snapshot-backed recovery is
a future optimization, requiring crash/purge/replay tests and latency/recovery
benchmarks before changing the contract. The controlled multi-process tonic/
RocksDB harness in [benchmark guide](benchmarks/README.md) measures the current path. Do not replace awaited checkpoints
with unchecked background writes or disable WAL synchronization.

An experimental `GroupConfig.storage.state_persistence = "snapshot"` mode
implements in-memory apply with durable scheduled snapshots and committed-log
replay. The default remains `"checkpoint"`; each database records its mode and
rejects changes. See the [paired checkpoint spike](benchmarks/checkpoint-spike.md)
for measurements and remaining validation.

**Amendment — column families (decided).** One database per group stays, and the
tenant *is* the namespace: the database is the tenant, so every key inside it is
already tenant-scoped and no `tenant:` prefix is needed (an escape hatch if
per-database overhead ever hurts: one database per node with prefixed keys and a
shared `Env`/block cache — isolation then becomes a naming convention, which is
why it is not the default). Inside that database, the kinds of data live in
**column families** rather than sharing one key space:

| Column family | Holds |
|---|---|
| `default` | format marker and persistence mode (openers fail closed on unknown) |
| `raft_log` | Raft entries, vote, committed pointer, purge floor |
| `state` | aggregate state — written as **deltas**, only the aggregates a batch touched — plus the dedup window and the applied index |
| `events` | **the append-only record**: the events of each apply, in order |
| `projections` | materialized read models, droppable and rebuildable at will (D9) |

Two consequences, both verified against `rocksdb` 0.24.0 rather than assumed:

- **A `WriteBatch` spans column families atomically** (`put_cf`/`delete_cf`/`delete_range_cf`),
  so "the state moved and these events happened" is one durable fact, and so is
  "these Raft entries are purged and the record already has their events".
- **Every opener must list every existing column family** — RocksDB refuses to
  open otherwise (`Column families not opened: …`). The list lives in one place,
  guarded by the format marker, and an unknown family fails closed.

Because the `events` family is written at apply time — always before Raft purges
the entries carrying those events — purging the Raft log cannot lose history, and
the separate archive-on-purge step this design first described is unnecessary. The
per-apply cost also stops growing with the history: a batch writes its own state
deltas and its own events, not the whole state ([D13](#d13--history-is-append-only-checkpoints-carry-state)).

The search index is **not** a column family. It is a tantivy directory beside the
database (`<group>/index/`), because the index wants real files it can mmap — see
[D6](#d6--fts-engine) — and it is derived, so it is rebuilt from `events` whenever
it is wrong.

The full layout — families, key formats, the atomic batch, recovery, snapshots and
purge, the record/derived split, and the tests each step owes — is
[storage-layout.md](storage-layout.md).

The reasoning and upstream contracts are recorded in
[checkpoint-policy.md](research/checkpoint-policy.md). Configuration and startup
examples are in [raft-configuration.md](raft-configuration.md).

### D3 — Envelope/payload encoding
Options: `bincode` (compact, fast, not human-readable) vs **serde_json**
(stable, human-readable, versioned via `payload_version` + upcast).
**Recommendation: serde_json for the envelope/payload wire format**; `bincode`
allowed for log entries / snapshots behind the storage traits (log bytes are
opaque to replicas' `apply`; snapshots are derived state but must meet the
selected durability contract before they cover purged logs).
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

### D6 — FTS engine
The search half of ["find anything"](#1-what-loomery-is) (principle 11), which
makes it the twin of [D9](#d9--storage-of-cold-read-model-state): whichever store
holds the projections has to hold, or sit beside, the index.

Options as recorded: **tantivy** (pure-Rust, mature, the standard choice) vs
SQLite FTS5 via `rusqlite` vs a hand-rolled index. Current facts (2026-10):
tantivy is at 0.26.x, MIT-licensed, actively maintained, and gives BM25 scoring,
stemming for 17 languages, phrase and prefix queries, Levenshtein `FuzzyTermQuery`,
fast fields (Lucene doc-values equivalent), facets and aggregations — so it can
filter and rank in one pass, which is what permission-scoped search needs.
SQLite's FTS5 is in the amalgamation and enabled by default there (a
non-amalgamation build must define `SQLITE_ENABLE_FTS5`), but it offers less:
no stemming, no facets or aggregations.

The comparison, including how each option scopes results to the caller's
permissions, is in
[read-model-store-options.md](research/read-model-store-options.md#search-is-the-cornerstone).
The decided design — what is findable (entities, not history), the schema sketch,
where the scope filter sits, the outbox-driven update with `as_of` disclosed, how
the index is rebuilt, and what stays out of scope — is [search.md](search.md).
**Status: DECIDED — tantivy, one index per tenant beside the database, indexed
from the outbox asynchronously**

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
re-issued publishes from leader failover or restart ([D11](#d11--outbox-subjects-stream-naming-and-dedup-identity)).

### D9 — Storage of "cold" read-model state

ETS/Sled-equivalent question: in Rust, `sled`/`redb`/SQLite (`rusqlite`)
for durable read models (retention, archives), or a consensus-backed
projection store. Phase 3 concern.
The candidates, with what each implies for reads, rebuilds, licences and the
build, are compared in
[read-model-store-options.md](research/read-model-store-options.md). Because the
cornerstone is ["find anything"](#1-what-loomery-is) (principle 11), the store
question has a search half, and because
[D2's amendment](#d2--storage-engine) puts the durable data in RocksDB column
families, the answer is: **the `projections` family for ordered and structured
reads, tantivy beside the database for search** — both per group, both rebuilt
from the append-only record. That supersedes the note's earlier `redb`
recommendation, which predates the column-family layout and would add a second
engine where the first one now suffices.
**Status: DECIDED — durable projections live in the per-group `projections` column
family; search lives in a tantivy directory beside the database**

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
- Stream: one `LOOMERY_OUTBOX` JetStream stream per deployment, capturing
  `loomery.>`.
- Domain events publish to `loomery.events.<group_id>.<event_type>` (e.g.
  `loomery.events.org:01hx….invitation_accepted`); integration events keep
  their own subject verbatim (e.g. `loomery.email.invitation`).
- Dedup: `Nats-Msg-Id = <group_id>:<log_index>:e<pos>` for domain events,
  `…:i<pos>` for integration events of the same log entry — one log entry
  yields distinct per-message ids while the `(group_id, log_index)` pair stays
  in the headers/cursor for saga-level dedup.
- Outbox envelope fields per event follow `Event` (deterministic ids —
  re-publishes are byte-identical).
**Status: DECIDED (carried over)**

### D12 — Identity: minted intents and derived entities
**Decision — one source of nondeterminism per intent; derive everything else:**
- **Mint once per intent, never per attempt.** The shell mints the
  `causation_key` (or validates a client-supplied one) together with the
  per-attempt envelope `id`, and persists them in the envelope, so a retry
  re-uses them instead of inventing a second command.
- **Derive what must be reproducible.** `Key` (`UUIDv5`, RFC 4122 §4.3) covers
  causation/dedup keys, *event ids* (`Command::event_id(index)` =
  `v5(causation_key, index)`) and entity ids whose identity is a function of
  the intent — genesis: "the default workspace of org X" =
  `Id::from(Key::new(GENESIS_NAMESPACE_V1, …))`.
- **Event ids derive from `causation_key`, not from `Command::id`:** the id of
  an *attempt* changes when a crashed shell rebuilds its envelope, whereas the
  id of the *intent* does not.
- **The window this closes:** the shell records the dedup entry *after* durable
  append + apply. A crash in between leaves a committed event with no dedup
  entry, so the resume re-runs `prepare`; with a freshly minted entity id that
  retry is a *different* creation (a duplicate workspace, or a retry that can
  never succeed), while a derived id is recognizable from the state itself.
- **Two keys, two jobs.** `causation_key` is the *intent*: one per command,
  so a retry is a replay. `correlation_key` is the *workflow*: one per saga
  instance, shared by every command and event it produces and derived from the
  workflow's business identity (an invitation id, an organization) — never
  from per-attempt state, so a resumed runner re-derives it and consumers group
  a workflow's events by it. Per-attempt tracing is observability's job, not
  the envelope's.
- **Idempotency-key reuse is a conflict, not a replay.**
  `Command::fingerprint` hashes the intent; the shell records it with the dedup
  entry and answers `409` when a `causation_key` returns with a different
  fingerprint (a client reusing a key for another request).
- **Untrusted strings are parsed, not adopted.** `Id::parse` (canonical UUID,
  any version) and `Key::try_from` (canonical **`UUIDv5`** only) are the entry
  points for outside data; `Id::from(&str)` stays for ids already known
  canonical. `uuid` is re-exported as `loomery_core::Uuid` so namespace
  constants cannot drift into a second `uuid` version.
- **Naming follows the rule.** `*_id` holds an `Id` — an *entity* identity,
  minted or derived-then-adopted (`Id::from(key)`); `*_key` holds a `Key` — a
  deterministic derivation (causation, intent fingerprint, workflow
  correlation). The `Key` invariant is enforced on **every** entry point,
  including `serde` deserialization, so a `UUIDv7` cannot be smuggled into a
  `_key` field through JSON. `Id` stays permissive on purpose: production ids
  are canonical UUIDs, while fixtures and injected test ids may be
  human-readable strings (`Id::parse` is what rejects those at the boundary).
- **Costs accepted.** Derived ids are not time-ordered (no `v7` index
  locality) and are *guessable* by anyone who knows the tuple: derive from a
  server-known scope (the organization), never from a value the caller picks,
  and never treat a `Key` as a capability.
- **The derivation tuple is a contract.** Include the tenant, exclude mutable
  data (names, status), and version *generations in the namespace* — a script
  version inside the hashed data would make a binary upgraded mid-provisioning
  derive different ids and duplicate its own work.
- **Where randomness stays:** secrets and capabilities (invitation tokens,
  share links, sessions) are not ids — they are random or HMAC'd values.
**Status: DECIDED (scaffold — `id::Id::parse`, `key::Key`, `Command::event_id`,
`Command::fingerprint`, `dedup::Entry::fingerprint`, genesis derivation)**

---

### D13 — History is append-only; checkpoints carry state

**Status: DECIDED (target shape; amends [D2](#d2--storage-engine), which is the
current implementation).** The record of what happened is **append-only**: the
replicated log, plus an archive of any segment that is purged once a snapshot
covers it. The state machine keeps **current state** — the fold — and checkpoints
persist *that*, not a copy of the event list they were folded from. Two read
paths follow from it: **query reads come from read models**, and **history reads
(the event log of an organization or workspace) come from the append-only
record**, never from an in-memory list that happens to be one replica's copy.

D2's checkpoint carries aggregate state *and* applied events on every apply. That
keeps the whole history durable — the union of the current checkpoint and the log
tail is complete — but it rewrites the entire history on every committed batch,
so its cost is quadratic in the history: the opt-in soak measures 162 → 34 → 17
commands per second at 200 → 1,000 → 2,000 commands, while snapshot mode (which
does not persist per apply) degrades far more gently
([persistence hardening](benchmarks/persistence-hardening.md)). The in-memory
event list grows the same way and is never trimmed, so every replica of every
group holds its group's whole history in RAM.

**Order of work** (revised: the `events` column family in
[D2's amendment](#d2--storage-engine) makes the record durable per apply, which
replaces the archive-on-purge step this decision first described):

1. **Column families and the `events` family.** Split the database into families
   and append each apply's events to `events`. Additive to the contract: the
   checkpoint still carries what it did yesterday, and the record is now complete
   without an archive.
2. **Deltas and state-only recovery.** `state` is written per aggregate, so an
   apply writes only what it changed; the checkpoint drops the event list; reads
   stop needing an in-memory list of every event. This is the step that changes
   the D2 contract, so it needs its crash/purge/replay tests and latency/recovery
   benchmarks first, exactly as D2 requires of any change to that contract.
3. **Read models** ([D9](#d9--storage-of-cold-read-model-state)): status and query
   reads answer from `projections` and from the search index, history reads from
   `events`.

History reads move to the append-only record as part of 2 and 3; the
`X-Min-Index` gate keeps reading the applied index from state, which both steps
keep.

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
- [x] `loomery_core::id::Id` (UUIDv7 wrapper, serde round-trip + shape tests)
- [x] `loomery_core::envelope::EventEnvelope` + `Payload` (serde round-trip +
      exact wire-format snapshot test)
- [x] `loomery_core::timestamp::Timestamp` (injected i64 ms, D5; `now()` shell-side)
- [x] `loomery_core::envelope::Command` (injected carrier, mirrors `Event`; round-trip test)
- [x] `loomery_core::actor::Actor` (`User \| System \| Saga`, serde round-trip + equality tests)
- [x] `loomery_core::error::DomainError<C>` (generic over code enum; `cause` chains via `anyhow`; display/equality/source tests)
- [x] `loomery_core::versioning` (per-aggregate static chain contract:
      `UpcastCode` + `Upcaster` + documented pattern; chains land with each
      aggregate)
- [x] `loomery_core::aggregate::AggregatePlan` (`prepare`/`apply` trait +
      `process`/`fold` free fns; dedup-on-`causation_key`; property + unit
      tests)
- [x] `loomery_core::aggregate::Execution` — real result type: `events` to
      commit + `outbound_events` (subject/content-type/payload); carried by
      `Executed(Execution)`; covered by unit test (35 total)
- [x] `loomery_core::dedup::DedupIndex` — bounded FIFO idempotency window
      (`dashmap` + `VecDeque`, deterministic eviction; unit + **proptest**
      model-based and metadata-retention properties)
- [x] `loomery_core::org::Organization` — `assign_leader` (①), `rename`, `archive`
- [x] `loomery_core::user::User` — `provision`, `update_profile`, `deactivate`
- [x] `loomery_core::workspace::Workspace` — `create` (②), `rename`, `archive`
- [x] `loomery_core::task::Task` — `create`, `rename`, `complete`, `reopen` (Phase-0 slice)
- [x] `loomery_core::membership` — `WorkspaceMembership` (`add_owner` ③,
      `add_member`, `change_role`, `remove_member`) and `OrganizationAssignment`
      (`assign_member`, `remove_member`)
- [x] Property tests (`proptest`: transition matrix, replay determinism,
      random-commands-never-crash, fold associativity) for all six aggregates,
      plus the dedup window

**Phase 0 gate ✅** — envelope round-trip and dedup properties pass, and the
six aggregates carry unit, transition-matrix, invariant and replay property
tests (`cargo test -p loomery-core`, 143 tests). The coverage/CRAP floor is
enforced by `mise run crap` in CI. The frozen command/event taxonomy is
recorded in [`domain-model.md`](domain-model.md).

### Shell (Phases 1–7)
- [x] Phase 1 control plane *(orchestration, router, RYW, gateway, outbox and sagas landed; OIDC and NATS have runtime adapters, and `loomery-server` wires them)*
  - [x] Control group on OpenRaft (`RaftLogStorage`/`RaftStateMachine` +
        `RaftNetworkV2` over tonic): `shell::control`
    - [x] levels 1–2 of the spike: in-memory `RaftLogStorage` +
          `RaftStateMachine` (`crates/shell/src/raft`) and the `RaftGroup`
          `GroupOps` adapter; passes OpenRaft's `testing::Suite` and drives
          genesis end-to-end in process
    - [x] tonic `RaftNetworkV2` + multi-node membership through `RaftGroup::raft`,
          RocksDB durability and configurable transport/storage/consensus tuning
    - [x] tenant placement: `loomery_core::tenant` records dispatched by the
          group state machine; `RaftGroup::boot_persistent` boots the control
          group like any other
  - [x] Router read model (`shell::control::Router`): the control group's tenant
        records projected into `organization_id → group`
  - [x] Tenant lifecycle + reconciliation: `shell::control::{provision, resume,
        incomplete}` — register → genesis → activate → route, idempotent by
        derived keys, with the route fenced behind genesis
  - [x] RYW `X-Min-Index` hold (gateway)
  - [x] Genesis script (`crates/genesis`): the deterministic ①②③ plan —
        derived keys/ids, commands, and progress read back from the committed
        events by causation key
  - [x] Genesis bootstrap worker: the async loop around the script
        (`crates/shell/src/bootstrap.rs`) over `GroupOps`, with crash-resume
        and replay tests; the control-plane controller and `resume` call it,
        and `incomplete()` is the reconciliation work list
  - [x] Outbox slice: `shell::outbox` tailer + `Publisher` seam (D8/D11); the
        NATS binding is deployment wiring
  - [x] Saga-runner seed + `InvitationAcceptance` (acceptance → assignment +
        membership), replay-safe by derived identity
  - [x] axum gateway: command plane, causation minting, edge pre-compute
        (argon2), admin claim enforcement, RYW middleware
  - [x] persistence hardening validation: 300-command history survives restart
        in both modes; see `docs/benchmarks/persistence-hardening.md`
  - [x] E2E gate candidates green in tests: register → genesis → workspace +
        Owner; invite → accept → provisioned → login; crash-resume
        no-duplicate; dedup-hit with RYW; admin enforcement
  - [x] Runtime host: `shell::host::Host` (control group, tenant groups, router,
        command plane, outbox workers with a persisted cursor per group, saga
        runner, graceful shutdown) behind one `HostConfig`, with
        `loomery-server` as the entry point — see [`host.md`](host.md)
  - [x] Provider-agnostic OIDC: discovery, JWKS caching and **local** JWT
        validation (ring-backed `jsonwebtoken`), configurable subject/groups/admin
        claims, exercised offline against a throwaway provider and live against
        Keycloak
  - [x] NATS runtime: the outbox publisher, a durable pull consumer with peek/ack
        semantics, and an outbox worker that resumes from a persisted cursor
        instead of relying on the broker's dedup window
  - [x] Provisioning is an API, not a library call: `POST /organizations`
        (admin only) through the gateway's `Provisioner` seam
  - [x] Crash-resumable provisioning: the placement records the genesis leader, so
        the host reconciles an interrupted onboarding from state (at boot and
        every 30 s) instead of needing the original caller
  - [x] Scoped reads: a workspace read answers that workspace (its own events and
        the work inside it), and the organization-wide log needs ownership —
        previously any member could read everything
  - [x] Onboarding attribution: the gateway overwrites an invitation acceptance's
        `user_id` and `email` from the authenticated caller (the *verified* email
        claim), so an acceptance binds to the address the invitation was issued to
        and a forged payload cannot accept on someone else's behalf
  - [x] Authorization: reads and writes require organization membership (or the
        admin claim), with `invitation.accept` as the single onboarding exemption;
        workspace-scoped commands are checked against the caller's **role** in the
        workspace they name (`Viewer` reads, `Member` works, `Owner` manages), and
        `invitation.create` requires owning a workspace of the organization. The
        membership index those checks read is derived from the applied events, so
        they are map lookups rather than scans
- [x] Column families and the `events` family (D2 amendment, D13 step 1): split
      the database into `default`/`raft_log`/`state`/`events`/`projections`, append
      each apply's events to `events`, fail closed on an unknown format marker —
      additive, so the checkpoint keeps carrying what it does today (`Disk` opens
      the layout, the record is written in the same batch as the checkpoint, and
      the layout marker refuses anything else)
- [x] Deltas and state-only recovery (D13 step 2): `state` keyed per aggregate so
      an apply writes only what it changed, and the checkpoint no longer carries the
      event list — landed with its tests and the soak, which went from 17 to ~1,190
      commands per second at 2,000 commands. The rate still falls with history,
      identically in both modes: that residue is above storage and is open. The
      in-memory copy of the record is what step 3 removes
- [ ] Projections and the search index (D9, D6): the `projections` family for
      boards and lookups, a tantivy directory per group for "find anything", both
      rebuilt from `events`
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
| `checkpoint-policy.md` | Checkpoint scheduling versus durability, current recovery contract and future optimization criteria |
| `openraft-storage.md` | OpenRaft storage interfaces — `RaftLogStorage`/`RaftStateMachine` split, snapshot builder, gotchas |
| `openraft-010-migration.md` | The 0.9.25 → 0.10.0-alpha.36 migration: measured error counts, surface inventory, and the three behaviour changes it forced |

---

*Last updated: 2026-10-06 — runtime host and binary, provider-agnostic OIDC, NATS
runtime adapters, provisioning, scoped reads and role-checked authorization at the
gateway, service-backed stress profiles and a license bundle.*
