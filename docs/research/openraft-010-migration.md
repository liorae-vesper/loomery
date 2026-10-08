# Migrating to openraft 0.10 for pipelined append

Status: **inventory taken, migration not started.** Branch `feature/openraft-pipelined-append`.

The bump was applied and checked: **511 errors**, which is not as bad as it sounds, because
they collapse into about ten root causes — the largest three are mechanical. This is the
measured list, not a guess.

```
 174  the trait bound `u64: RaftTypeConfig` is not satisfied        (142 + 32)
  71  the trait bound `BasicNode: std::error::Error` is not satisfied
  65  (): RaftStateMachine<TypeConfig> is not satisfied              (50 + 15)
  60  cannot be sent between threads safely                          (dyn Any, 13 futures, ...)
  26  the trait bound `u64: RaftLeaderId` is not satisfied
  27  E0107: generic arity changed (struct/enum takes a different number of arguments)
  11  E0407: method not a member of trait — `vote` (RaftNetwork),
      `truncate` / `read_vote` (RaftLogStorage)
  10  `BasicNode: Hash` / `Ord` not satisfied
   4  `BasicNode: NodeId` not satisfied
   3  `raft::AppData` doesn't implement `Display`
   6  E0308, 6 E0053, 6 E0046, 5 E0271, 1 E0560
```

By file the errors cluster where expected: `state_machine.rs` 187, `transport.rs` 186,
`mod.rs` 182, `port.rs` 112, `host.rs` 77, `rocks_log_store.rs` 57, `network.rs` 44,
`log_store.rs` 42, `group.rs` 40.

## Reading the list

* **`StorageError<u64>` is the single biggest cause.** 0.10 makes `StorageError` a struct, so
  every `StorageError<u64>` in the log stores and the state machine is now being read as
  "a `StorageError` parameterised by the config `u64`" — hence "`u64: RaftTypeConfig` is not
  satisfied". Deleting those generic arguments is mechanical and should clear ~174 errors.
  The same de-genericizing applies to the 71 `BasicNode: std::error::Error` errors, which are
  error types still being handed a type parameter they no longer take.
* **`single-threaded` may be the wrong feature for us.** It sets `OptionalSend = ()`, which
  makes the boxed futures and streams non-`Send` — and we run a multi-threaded tokio runtime
  with a tonic transport, which is where the ~60 "cannot be sent between threads safely"
  errors come from. Worth testing `["serde", "compat"]` without it before chasing those.
* **The genuine trait work is smaller than the count suggests**: `(): RaftStateMachine` (65),
  `u64: RaftLeaderId` (26, the `LeaderId` associated type), the E0107 arity changes, and the
  E0407 trait splits (`vote` off `RaftNetwork`; `truncate`/`read_vote` off `RaftLogStorage`).
* **`raft::AppData` needs `Display`** — 3 errors, one small impl.
* Removing `SnapshotData` from `declare_raft_types!` alone changed nothing (511 → 510), which
  is consistent with the cascade being rooted in the error types rather than in the macro.

## Measured bump recipe (verified)

The dependency bump used to be described here as untested; it is now done and confirmed in
both `Cargo.toml` and `Cargo.lock`.

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
