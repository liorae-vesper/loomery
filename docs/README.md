# Documentation

Start with [design.md](design.md) for the architecture and decisions, or
[CONTINUE.md](CONTINUE.md) for the current implementation and next work.

## Reference

| Document | Purpose |
|---|---|
| [Design](design.md) | Architecture, decisions register and phased roadmap; distinguishes implemented and planned features |
| [Architecture at a glance](architecture.md) | Mermaid diagrams: crate graph, write/read paths, authorization, onboarding, group internals, host wiring, deployment |
| [Domain model](domain-model.md) | The six Phase-0 aggregates: commands, events, payloads, state and legal transitions |
| [Implementation guide](implementation.md) | Build pipeline, Raft startup, storage/network wiring and tenant group lifecycle |
| [Shell](shell.md) | Current group API, genesis worker, transport and storage behavior |
| [Control plane](control-plane.md) | Tenant placement records, router projection, provisioning and reconciliation |
| [Gateway](gateway.md) | Identity, admin claim, edge pre-compute, command plane and read-your-writes |
| [Outbox and sagas](outbox-and-sagas.md) | Committed-event publishing, cursors, retry classification and the invitation saga |
| [Runtime host](host.md) | `loomery-server`: configuration, wiring, boot sequence and known gaps |
| [Storage layout](storage-layout.md) | The decided column-family layout: keys, the atomic apply batch, recovery, purge, the record/derived split |
| [Search](search.md) | "Find anything" within a tenant: the index beside the database, scoping, rebuild, staleness |
| [Test services](testing-services.md) | Keycloak and NATS JetStream for the opt-in integration suite |
| [Third-party licenses](third-party-licenses.md) | The bundled dependency license texts, how they are generated and checked |
| [Raft configuration](raft-configuration.md) | Persistent replica startup, tuning, TLS/mTLS and immutable persistence modes |
| [Development guardrails](guardrails.md) | Toolchain, verification tasks, hooks and CI |
| [Continuation](CONTINUE.md) | Concise handoff; consult the design tracker for the full backlog |

## Tutorials

Read these in order for the core-to-shell walkthrough. The OpenRaft tutorial
builds the original in-memory baseline; use the reference above for persistent
networked deployment.

1. [Group port](tutorials/shell-group.md) — commands, reads and unknown outcomes.
2. [Genesis worker](tutorials/genesis-worker.md) — deterministic bootstrap and retry behavior.
3. [OpenRaft spike](tutorials/openraft-spike.md) — storage traits and the Raft adapter.

## Benchmarks

- [Controlled consensus benchmark](benchmarks/README.md) — workload, phases, configuration and measurement limits.
- [Checkpoint comparison](benchmarks/checkpoint-spike.md) — paired persistence experiment and measured results.
- [Command batching](benchmarks/batching.md) — bounded proposal batches, semantics and paired throughput results.
- [Batch-size matrix](benchmarks/batch-matrix.md) — configurable count/concurrency sweeps, randomized repeats and observed batch distributions.
- [Failure injection](benchmarks/failure-injection.md) — crashes during writes, quorum loss, lost replies and flush-callback failures.
- [Persistence hardening](benchmarks/persistence-hardening.md) — long-history restart validation in both modes, and the gaps left open.
- [Deployment path at scale](benchmarks/deployment-scale.md) — three nodes at 2k → 20k events: the flat curve, what limits it, and the caveats.
- [Multi-group probe](benchmarks/multigroup.md) — co-resident group read/write capacity, methodology and limits.
- [Services stress profiles](benchmarks/services-stress.md) — the Keycloak and NATS adapters under load, the invariants asserted and the measured results.
- [Example workload](benchmarks/consensus.json) — configuration for the release harness.

The paired runner is executable tooling in
[../scripts/bench-persistence.py](../scripts/bench-persistence.py), invoked by
`mise run bench-persistence`. Command batching has a separate
[runner](../scripts/bench-batching.py), invoked by `mise run bench-batching`.

## Research

Research records the reasoning and alternatives behind decisions. Earlier
recommendations are retained as provenance; the design register and current
configuration reference describe what was selected and implemented.

| Note | Topic |
|---|---|
| [OpenRaft versus alternatives](research/openraft-vs-alternatives.md) | Consensus and per-tenant groups |
| [Storage engine alternatives](research/storage-engine-alternatives.md) | Log storage, projections, backups and vector storage |
| [Read-model store options](research/read-model-store-options.md) | The D9 candidates for durable projections, with sources |
| [Indexed segment format](research/indexed-segment-file-format.md) | Alternative log format; not the implemented RocksDB backend |
| [OpenRaft storage](research/openraft-storage.md) | Storage contracts and version-specific integration notes |
| [OpenRaft 0.10 migration](research/openraft-010-migration.md) | The 0.9.25 → 0.10.0-alpha.36 migration: measured error counts, surface inventory and behaviour changes |
| [Consensus storage performance](research/consensus-storage-performance.md) | Serialized syncs, batching, blocking-pool timing and database closure investigation |
| [Checkpoint policy](research/checkpoint-policy.md) | Scheduling versus durability and the two recovery modes |

Keep current behavior in reference docs, implementation walkthroughs in
`tutorials/`, measurements in `benchmarks/`, and decision research in
`research/`. Link to detailed sources instead of appending duplicate session
summaries. Keep runtime scripts outside this directory.
