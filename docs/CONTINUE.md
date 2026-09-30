# Loomery — continuation

Read [design.md](design.md) for the architecture, decisions and full progress
tracker. [README.md](README.md) maps the documentation. The project is a Rust
workspace with a deterministic core and an imperative Tokio/OpenRaft shell.

## Implemented

- Core envelope, identity, timestamp, actor, errors, aggregate execution,
  versioning contracts and the bounded dedup registry. Organization, workspace
  and membership implement the genesis slice; full domain coverage is pending.
- Deterministic genesis script and async bootstrap worker. The worker re-reads
  applied events before retrying an unknown outcome, using derived causation
  keys to avoid duplicate provisioning.
- In-memory single-node Raft baseline and persistent multi-node groups over
  tonic gRPC and RocksDB. A shared listener multiplexes group IDs. Membership
  is explicitly initialized and changed through `RaftGroup::raft()`.
- Optional TLS/mTLS with peer verification and no plaintext fallback.
- Two persistence modes: `checkpoint` (default, full durable state per apply
  batch) and `snapshot` (experimental, in-memory apply plus durable snapshots
  and committed-log replay). Both keep synchronized Raft log writes and quorum
  acknowledgements. Each database durably pins its mode; mismatches and invalid
  markers fail startup. Nonempty unmarked databases require checkpoint mode.
- Controlled multi-process benchmarks, leader failover and database restart
  checks. The paired spike measured approximately 174 versus 550 writes/s on
  one host; this is a workload-specific result, not deployment capacity.

See [shell.md](shell.md), [raft-configuration.md](raft-configuration.md),
[checkpoint policy](research/checkpoint-policy.md) and
[benchmark results](benchmarks/checkpoint-spike.md) for details.

## Next work

1. Finish Phase 0 aggregates and their transition-matrix/replay property tests.
2. Build control-plane tenant lifecycle and router projections. Wire startup
   reconciliation and retry sweeping into the existing bootstrap worker.
3. Implement the gateway and read-your-writes `X-Min-Index` wait/leader fallback.
   Local reads exist; gateway session-token enforcement is still planned.
4. Add the committed-log outbox, NATS delivery, invitation choreography and saga
   runner according to design D8/D11.
5. Extend persistence validation to large histories, interrupted snapshot
   creation/installation/purge and storage failures before changing the default.
   Multi-group mixed read/write capacity has not been benchmarked.

D1 (OpenRaft/tonic) and D2 (RocksDB) are selected. FTS and vector-store decisions
remain Phase 4 work. Preserve the original research when revisiting decisions.

## Working conventions

- Use the pinned mise environment for all Rust commands.
- Keep the core deterministic: inject IDs and timestamps, derive reproducible
  identities, and put I/O in the shell.
- Record dedup only after committed application. Errors from proposal mean an
  unknown outcome; re-read before deciding whether to retry.
- Frozen event names/payloads, versioning and writer gating follow design D12
  and §6. Unsupported snapshot versions currently fail; upcast chains are future work.
- Keep linear Git history and conventional commits. Do not bypass checks for
  ordinary work. [guardrails.md](guardrails.md) documents hooks and CI.

## Verification and benchmarks

```sh
mise run verify
mise exec -- cargo test --workspace --all-targets
mise exec -- cargo test --workspace --doc
mise run coverage
mise run bench-consensus
mise run bench-persistence -- --output benchmark-results/persistence-comparison
```

Both storage modes pass OpenRaft's suite. Tests also cover replication, snapshot
transfer, restart/dedup, TLS trust/identity and persistence-mode protection.
Benchmarks retain raw samples and databases in ignored `benchmark-results/`.
