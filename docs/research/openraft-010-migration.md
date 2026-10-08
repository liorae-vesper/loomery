# Migrating to openraft 0.10 for pipelined append

Status: **scoping, not started.** Branch `feature/openraft-pipelined-append`. The dependency
bump below was applied and then reverted so the branch is only this document; the compile
state of the migration has **not** been verified, and the error count from the earlier sizing
(490) is stale and must be re-taken.

## Why, and what we expect to get

The goal is pipelined append: stop waiting for a response per entry. Our own measurements say
where that can and cannot pay:

* The **unbatched** path pays a fixed round trip per command — 1.29 ms/command on three nodes,
  3.4 ms/entry on one. Pipelining attacks exactly this.
* The **batched** path has already amortized it: ~0.048 ms/command at 256 commands per entry,
  and the remaining cost is per-command work, not the network.

So the honest expectation is a large win unbatched, and little or none batched. Both must be
measured before and after; the deployment harness (`mise run bench-deployment-scale`) does both.

## The bump, and the trap in it

`cargo add` **fails (exit 101)** against our manifest:

```
error: unrecognized feature for crate openraft: storage-v2
```

It validates the *existing* feature list against the new version before editing, and 0.10 has
no `storage-v2` — the v2 API is now the only storage API. Edit the manifest by hand:

```toml
openraft = { version = "0.10.0-alpha.36", features = ["serde", "single-threaded", "compat"] }
```

then `cargo update -p openraft --precise 0.10.0-alpha.36`. (In practice this also dropped
`winnow v1.0.4` from the lock.)

Version 0.10.0-alpha.36 is an **alpha**.

### Features as of 0.10.0-alpha.36

* default: `clap`, `serde`, `tokio-rt`
* available: `adapt-network-v1`, `anyhow`, `bench`, `bt`, `compat`, `loosen-follower-log-revert`,
  `metrics-logids`, `runtime-stats`, `serde_json`, `single-term-leader`, `single-threaded`,
  `singlethreaded`, `tracing-log`, `type-alias`
* removed: `storage-v2`; `singlethreaded` → `single-threaded`

`adapt-network-v1` is the escape hatch for an old-style network; `compat` is **not** a trait
adapter despite the name — it is `Upgrade`/`Compat<From, To>`, for deserializing data written
by an older version. That matters here because we persist `LogId`/`Vote`/`Entry`, not because
it saves us the trait migration.

## The network side

`RaftNetworkV2` gains `type SnapshotData` (moved off `RaftTypeConfig`) and supplies
`stream_append` **with a default sequential implementation** (`stream_append_sequential`: send
one request, wait for it, send the next). That splits the work in two, which is the point of
staging it:

1. Migrate and land with the default — no protocol change, no pipelining yet.
2. Implement real pipelining over a **bidirectional streaming** RPC in
   `crates/shell/proto/raft.proto`, with the receiving side calling `Raft::stream_append`.

The trait's contract to respect: responses must be yielded **in the same order as the input
requests**, and the implementation enforces `option.soft_ttl()` (a long-lived stream uses
`soft_ttl` for setup/idle policy, not `hard_ttl`). One HTTP/2 stream per follower keeps order
without sequence numbers; a multiplexed design would have to reorder by sequence number.

## Surface to migrate

| File | What changes |
|---|---|
| `crates/shell/src/raft/mod.rs` | `declare_raft_types!` — needs `LeaderId`/`Responder`; `SnapshotData` moves out |
| `crates/shell/src/raft/log_store.rs` | `RaftLogReader`/`RaftLogStorage` splits; `StorageError` becomes a struct; `LogFlushed` callback shape |
| `crates/shell/src/raft/rocks_log_store.rs` | same, on the real store |
| `crates/shell/src/raft/state_machine.rs` | `RaftStateMachine` trait split; `SnapshotMeta` loses `snapshot_id` |
| `crates/shell/src/raft/network.rs` | `RaftNetwork` → `RaftNetworkV2` + granular `Net*` sub-traits (`NetAppend`, `NetVote`, `NetSnapshot`, ...) |
| `crates/shell/src/raft/transport.rs` | the tonic client implementation, and the bidi RPC in leg 2 |
| `crates/shell/src/raft/port.rs`, `proposal.rs` | `Raft<TypeConfig>` call sites |
| `crates/shell/proto/raft.proto` | new bidirectional `StreamAppend` RPC (leg 2) |
| `*_tests.rs`, `suite.rs` | the suite is the correctness oracle for the whole migration |

## Plan

1. Bump and inventory: hand-edit the manifest, `cargo update --precise`, `cargo check
   --all-features` into a file, categorize the errors by code and file. **Do not commit a
   broken build to `main` under any circumstance.**
2. `TypeConfig` + storage traits, keeping batching behaviour identical.
3. Network to `RaftNetworkV2`, still on the default sequential `stream_append`.
4. Get the full suite green (`mise run test`, including the hardening and interruption tests)
   and `mise run verify`, and **commit that** — a working 0.10 migration with no behaviour
   change.
5. Only then the bidi RPC and true pipelining, with tests for ordering and for `soft_ttl`.
6. Measure: deployment harness unbatched and batched, single node and three, against the
   recorded 0.9 numbers in `docs/benchmarks/results/deployment-path.json`.

Steps 1–4 are a migration; step 5 is the feature; step 6 decides whether step 5 was worth it.

## Risks

* **Alpha dependency.** Pinning an alpha for a performance feature is a real cost to weigh.
* `single-term-leader` and `loosen-follower-log-revert` are gone; if we relied on either, the
  behaviour changes rather than failing at compile time — worth checking before leg 2.
* Persisted formats: `SnapshotMeta` losing `snapshot_id` and `StorageError` becoming a struct
  both touch data we write, so the `compat` types are worth a look in leg 2 even though they do
  not help the trait work.
* The reward is expected to be concentrated in the unbatched path, which is not the
  configuration we would deploy.
