---

> **Research note for the Loomery project.** Informs decision D1 (consensus).

# OpenRaft vs the Alternatives — Research

> Decision D1 has landed: OpenRaft over tonic gRPC. This note preserves the original comparison; current interfaces and limitations are documented in [the shell reference](../../docs/shell.md) and [design D1](../design.md#d1--consensus).

Loomery is a multi-tenant event-sourced backend: **one consensus group per
organization + a control group**, running inside a single Rust process per
node. This note answers: **which consensus/coordination approach should drive
those groups?** OpenRaft wins the recommendation; here is the reasoning.

---

## 0. Framing — it's not "Raft instead of networking"

OpenRaft is **consensus**: quorum writes, one leader per term, a totally
ordered replicated log. That's the semantics an event store needs. The question
is which vehicle provides it: a Raft library (OpenRaft), a hand-rolled Raft, an
external coordination service (etcd/consul), or a database's built-in
replication (Postgres logical replication).

---

## 1. The landscape in Rust

| Option | What it gives you | Why not the default |
|---|---|---|
| **OpenRaft** (databend) | Async Raft library on Tokio: `RaftLogStorage`/`RaftStateMachine` interfaces, `RaftNetwork` trait, snapshots, membership changes, learners, metrics | — |
| Hand-rolled Raft | Full control | Reimplements a subtle, Jepsen-grade protocol for no benefit at 3–5 nodes |
| etcd / consul | External CP stores, well-understood | An op per command is an extra network hop + a hard external dependency; no per-tenant log semantics, no custom state machine apply |
| DB replication (Postgres logical replication) | Strong, proven | One global stream, not per-tenant groups; replay is at SQL granularity, not our event JSON; couples consensus to a specific engine (D2) |
| NATS JetStream streams | Durable pub/sub, dedup, replay | AP-ish delivery semantics, not linearizable consensus; no leader election/quorum |

## 2. Why OpenRaft specifically

- **Async-native Tokio**: `Raft<TypeConfig>` is a spawnable set of tasks;
  `RaftNetwork` and the storage traits are async traits (`#[openraft-macros::add_async_trait]`).
  This matches Loomery's Tokio shell exactly — no FFI, no threads-per-group.
- **We implement storage.** `RaftLogStorage` (log) and `RaftStateMachine`
  (apply + snapshots) are interfaces we own — so D2 (segment files vs sled vs
  RocksDB) stays an implementation detail behind the traits.
- **The application-server pattern maps 1:1** (from the `raft-kv-memstore`
  example): `client -> app::write() -> Raft::client_write() -> log replication
  -> StateMachine::apply() -> state update`. Our `execute`/`apply` (pure core)
  slot into `apply`; the WAL tailer for the outbox reads committed entries.
- **Production track record**: openraft powers `databend-meta`; it is designed
  for embedded, long-running, strongly-consistent metadata clusters.
- **Feature set we need historically** (v0.9): log/state-machine split for
  parallel access, `save_committed` for startup apply, snapshot builder +
  chunked snapshot transfer (`full_snapshot` / `Chunked::send_snapshot`),
  pipelined replication via `stream_append`, joint/learner membership changes,
  `RPCOption` hard/soft TTLs, `RaftMetrics` (used for the RYW hold).

## 3. The per-tenant multi-group model

- One `Raft<TypeConfig>` **task per organization** + one control group per
  node. OpenRaft scales to many co-resident groups (databend-meta runs one
  large group; the pattern extends to many small ones).
- **Silent idle groups**: no heartbeat traffic when there's nothing to
  replicate — matches Loomery's hibernation-friendly tenant model.
- **Group membership**: a tenant group is created via `initialize` (bootstrap),
  nodes join/leave via `change_membership`; the genesis worker drives this.

## 4. Lessons from elsewhere (why we care about determinism)

- Raft's correctness requires every replica apply identical commands
  identically. Khepri (RabbitMQ) went so far as to extract and verify the
  bytecode of transaction functions to guarantee purity. Loomery sidesteps the
  whole class of problems the Khepri/Horus way but simpler: **commands and
  events are plain data** (`serde_json`), and `apply` is a pure function —
  determinism is structural, not verified.
- Raft replicas replay the log from a snapshot; the **snapshot + log must be
  consistent** — OpenRaft's `get_log_state`/`save_committed` exist precisely to
  answer "what is applied vs not applied after restart."
- Membership changes are where hand-rolled Rafts die; OpenRaft's
  `ChangeMembership` (joint consensus / learner promotion) is battle-tested.

## 5. Decision taxonomy — when which tool?

| Need | Right tool | Why |
|---|---|---|
| 3–10 nodes, strong per-entity consistency | **OpenRaft per group** | Quorum, no split-brain, fast failover, log = event store |
| External coordination (leader election, config) | etcd/consul | Only when we don't want to run our own raft groups |
| Durable pub/sub, fan-out, replay | NATS JetStream (outbox, under Raft commit) | AP delivery; *not* a replacement for consensus |
| SQL query layer | Postgres etc. | Read models only; the Raft log *is* the durability layer (D9) |
| CRDT/AP data with high churn | Not Loomery's model | Loomery is CP per tenant; Raft is the home turf |

**For Loomery specifically:** cluster size is 3–5 nodes, so external registry
mesh limits are irrelevant; what matters is (a) per-tenant linearizability,
(b) partition predictability, and (c) an append-only replicated log that *is*
the event store — all three are OpenRaft's home turf.

## 6. Use cases that justify "Raft in the app, not in a service"

1. **Per-tenant SaaS** — one Raft group per tenant in-process (Loomery's model).
2. **Replicated metadata stores** — databend-meta, Khepri-class workloads.
3. **Event-sourced systems** — a committed Raft log *is* an event store
   (immutable, totally ordered, persistent); the unified-log pattern.
4. **Coordination primitives** — leader election, config replication with
   custom state machines.
5. **Anything where partition behavior must be a known answer** — audits,
   compliance, enterprise ops.

## Sources

- OpenRaft repo + getting-started: https://github.com/databendlabs/openraft
- `raft-kv-memstore` example (write flow): https://github.com/databendlabs/openraft/tree/main/examples/raft-kv-memstore
- OpenRaft v0.8→v0.9 upgrade guide (storage split, RaftNetwork methods,
  add_async_trait): https://github.com/databendlabs/openraft/blob/main/openraft/src/docs/upgrade_guide/upgrade-v08-v09.md
- OpenRaft change log: https://github.com/databendlabs/openraft/blob/main/change-log.md
- Khepri overview (why replace Mnesia, partition predictability):
  https://rabbitmq.github.io/khepri/overview-summary.html
- RabbitMQ quorum queues / streams (per-entity Raft groups in production):
  https://www.rabbitmq.com/docs/quorum-queues

*Compiled: 2026-08 (reshaped for the Rust/Tokio/OpenRaft stack).*