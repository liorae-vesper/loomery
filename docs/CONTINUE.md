# Loomery — continuation

Read [design.md](design.md) for the architecture, decisions and full progress
tracker. [README.md](README.md) maps the documentation. The project is a Rust
workspace with a deterministic core and an imperative Tokio/OpenRaft shell.

## Implemented

- Core envelope, identity, timestamp, actor, errors, aggregate execution,
  versioning contracts and the bounded dedup registry. The six Phase-0
  aggregates are implemented (organization, user, workspace, membership +
  organization assignment, task) with transition-matrix, invariant and replay
  property tests, plus the invitation and tenant-placement aggregates; see
  [domain-model.md](domain-model.md).
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
- Opt-in shared proposal batching, bounded by command count, bytes, collection
  delay and channel capacity. Commands share one Raft index, apply in order and
  retain individual responses/dedup/rejections. Defaults preserve single-command
  entries; every replica must support batch entries before enabling the option.
  New paired three-trial medians were 172→431 checkpoint and 537→1781 snapshot
  writes/s at concurrency/batch size 8. See [batching results](benchmarks/batching.md).
- Configurable batch/concurrency matrix with seeded, interleaved repeats and
  observed batch-size histograms. At fixed concurrency 128, limits 1/8/16/32/64/128
  reached their targets; count 128 measured 1612 checkpoint and 3842 snapshot
  writes/s. Small checkpoint batches regressed relative to the same-load
  baseline. All 36 trials passed crash/recovery checks. See [matrix results](benchmarks/batch-matrix.md).
- Controlled failures during write phases: leader/follower SIGKILL, loss of both
  followers and discarded replies, with retry/index/dedup and full event
  convergence checks after recovery. Append tests inject a flush callback error
  and require Raft to stop without acknowledging or applying the batch.
  See [failure injection](benchmarks/failure-injection.md).
- Control plane: tenant placement records, the `organization_id → group` router
  projection, and a tenant-creation controller with startup/retry reconciliation
  ([control-plane.md](control-plane.md)).
- Gateway: identity + admin claim, argon2 edge pre-compute, a command plane with
  causation minting and `409` on key reuse, and the `X-Min-Index`
  read-your-writes gate over axum ([gateway.md](gateway.md)).
- Outbox and sagas: committed events publish with the D11 dedup identity and a
  resumable cursor; `SagaRunner` consumes with ack/retry classification and the
  invitation acceptance saga provisions assignment + membership replay-safely
  ([outbox-and-sagas.md](outbox-and-sagas.md)).
- Phase-1 end-to-end acceptance in the default suite: onboarding, invitation and
  consistency (dedup replay, key reuse conflict, RYW, admin claim).
- Persistence hardening: a 300-command history survives restart in both modes
  ([persistence-hardening.md](benchmarks/persistence-hardening.md)); a
  co-resident multi-group probe records read/write capacity
  ([multigroup.md](benchmarks/multigroup.md)).

See [shell.md](shell.md), [raft-configuration.md](raft-configuration.md),
[checkpoint policy](research/checkpoint-policy.md) and
[benchmark results](benchmarks/checkpoint-spike.md) for details.

### Runtime host

- `shell::host::Host` boots the control group (initializing a fresh data
  directory), hosts one persistent group per active tenant, warms the identity
  provider, builds the command plane and serves the gateway; it starts one outbox
  worker per group (cursor persisted beside the group's database) and the saga
  runner, and shuts down gracefully. `crates/server` is the binary
  (`mise run server -- --config host.json`).
- Identity is provider-agnostic: discovery, JWKS caching and local JWT validation
  with configurable claim names. The broker runtime is `async-nats` behind the
  `nats` feature: publisher, durable pull consumer and the tailer worker.
- Provisioning is an admin-only gateway route (`POST /organizations`) wired to a
  `Provisioner` seam; the placement records the genesis leader, so an interrupted
  onboarding is finished from state at boot and by a 30 s sweep. Reads and writes
  require organization membership (or the admin claim); workspace-scoped
  commands are checked against the caller's role there (`Viewer` reads, `Member`
  works, `Owner` manages), and `invitation.accept` is the single onboarding
  exemption. Reads are scoped: a workspace read answers one workspace, the
  organization log needs ownership. An invitation acceptance carries the caller's
  *verified* email into consensus, so only the invited address can accept it.
- The runtime lives behind `nats`/`oidc` features (`test-services` enables both),
  so the default suite stays self-contained; the adapters are covered offline
  (a throwaway provider and fakes) and live (`mise run test-services`).

## Next work

1. ~~Finish Phase 0 aggregates and their transition-matrix/replay property
   tests.~~ **Done:** all six, plus the invitation and tenant-placement
   aggregates, each with unit, transition-matrix, invariant and replay
   property tests.
2. ~~Build control-plane tenant lifecycle and router projections.~~ **Done:**
   `shell::control` holds the tenant router projection, `provision`
   (register → genesis → activate → route) and `incomplete`/`resume` for the
   startup and retry sweeps. The periodic sweep loop itself is host wiring.
3. ~~Implement the gateway and read-your-writes `X-Min-Index` wait/leader
   fallback.~~ **Done:** `shell::gateway` authenticates (behind an
   `Authenticator`), enforces the admin claim, hashes passwords with argon2 at
   the edge, mints/validates the causation key, answers `409` on key reuse,
   routes to the active tenant and applies the `X-Min-Index` gate. OIDC is the
   remaining deployment wiring.
4. ~~Add the committed-log outbox, NATS delivery, invitation choreography and
   saga runner according to design D8/D11.~~ **Done:** the outbox publishes
   applied events with the D11 identity and a resumable cursor, `SagaRunner`
   consumes with ack/retry classification, and `InvitationAcceptance` provisions
   assignment + membership replay-safely. The NATS JetStream binding is
   deployment wiring behind `LOOMERY_NATS_URL`.
0. Runtime follow-ups: multi-node control groups and placements (the runbook
   work), and a read model behind the (now scoped) reads — today they filter the
   applied log, which is `O(history)` per read. The decided shape of that work is
   recorded: [storage-layout.md](storage-layout.md) (column families, the `events`
   family, deltas and state-only recovery) and [search.md](search.md) (the tantivy
   index beside the database). Steps 1 and 2 have landed: the families and the
   append-only `events` family behind a layout marker that fails closed, and the
   state deltas — an apply now writes only what it changed, which removed the
   quadratic cost. The path was then measured properly — in a release build, after
   finding that the fold mirrored the whole dedup window on every command — and it
   is flat at ~11–15k commands per second per group, linear to 8,000 commands. Step 3
   is open: reads still answer from an in-memory copy of the record rather than from
   `events`, and neither the projections nor the search index exists yet. Each step's tests are named in [D13](design.md#d13--history-is-append-only-checkpoints-carry-state).
5. ~~Extend persistence validation to large histories, interrupted snapshot
   creation/installation/purge and storage failures before changing the default.~~
   **Done:** a 300-command history survives restart in both modes; interrupted
   snapshot builds and purges are injected through real storage rejections
   (`raft/interruption_tests.rs`); and the opt-in soak measures a long history —
   which shows checkpoint mode is quadratic in it (17/s at 2,000 commands, ~78×
   slower than snapshot mode) and leaves snapshot mode the one that scales. The
   default is unchanged; capacity remains a separate probe. See
   [benchmarks/persistence-hardening.md](benchmarks/persistence-hardening.md).

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
