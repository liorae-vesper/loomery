# Migrating to openraft 0.10

Status: **all six legs done — migrated, measured, and the one thing that did not pay was
removed.** Branch `feat/openraft-010-migration` (renamed from
`feature/openraft-pipelined-append` once the pipelining was gone).

`cargo check -p loomery-shell --all-features` is clean, `mise run test` (179 shell lib tests,
including `testing::log::Suite`, the hardening and interruption suites) and `mise run verify`
are green, and `main` is untouched.

**The measured verdict: the migration paid, the pipelining did not show up — so the
`StreamAppend` RPC was removed.** The deployment path is 2.3–2.7× faster at the default config
and 11.9–15.7× at concurrency 128 unbatched, and the 0.9 single-node anomaly (294 writes/s
against 777 on three nodes) is gone. But the same build with `stream_append` on openraft's
sequential default matches the pipelined one within a few percent in *both* directions, so
**the win is 0.10's removal of the per-flush append serialization, not the bidirectional RPC**.
Numbers, attribution arms, the latency A/B/A/B and the wedge the first sweep hit are in
[deployment-scale.md § After](../benchmarks/deployment-scale.md#after-openraft-010);
the design record of what was built and why it went is
[Pipelined append](#pipelined-append-leg-5-built-measured-removed).

## What the migration cost, measured

The inventory below was taken at the bumped-but-broken commit: **514 errors** from
`cargo check -p loomery-shell --all-features` (the packet said 511; the counting convention
differs by a few). Every step was measured:

| Step | Change | Errors after |
|---|---|---|
| 0 | bump applied, nothing else | 514 |
| 1 | error types re-parameterised: `StorageError<u64>` → `StorageError<TypeConfig>`, `RPCError<u64, BasicNode, E>` → `RPCError<TypeConfig, E>`, `RaftError<u64>` → `RaftError<TypeConfig>`, … | 334 |
| 2 | `features = ["serde", "compat"]`, dropping `single-threaded` | 314 |
| 3 | `declare_raft_types!` (default `LeaderId`, no `Entry`/`SnapshotData`) + `Display for AppData` | 313 |
| 4 | storage traits split, `Raft<TypeConfig, SM>`, `io::Error` returns, `SnapshotMeta` without `snapshot_id` | 214 |
| 5 | `RaftNetworkV2` + the `Net*` blanket sub-traits, and the fragmenting `full_snapshot` | 0 |

Two of the packet's guesses were wrong in a useful way:

* **`StorageError` is still generic in 0.10** — it is parameterised by the *type config*, not by
  nothing. The mechanical fix is `StorageError<TypeConfig>`, not bare `StorageError`; the same
  applies to `RPCError` (config + error, the node type is gone), `RaftError`,
  `ClientWriteError`, `RemoteError` and `Timeout`.
* **Dropping `single-threaded` is worth more than the ~83 `Send` errors it removes.** It also
  un-masks ~64 `(): RaftStateMachine<TypeConfig>` errors, for a net −20 — and it is the feature
  set a multi-threaded Tokio runtime with a tonic transport needs, so it is what we ship.

## What 0.10 changed under us

Three changes are not mechanical. They change behaviour or the shape of the code, and they are
the things to re-check before the benchmark:

1. **Local appends are no longer serialized behind the previous flush.** 0.9's core waited for
   the log store's flush callback before issuing the next append; 0.10 tracks IO completion with
   a watermark (`IOFlushed`/`IOId`) and lets appends overlap. This is the pipelining the branch
   exists for, and it is a real behaviour change: the hardening test
   `pinned_raft_core_waits_for_flush_before_appending_the_next_client_write` became
   `a_second_append_is_not_serialized_behind_the_first_flush` and now asserts the invariants that
   survive — an entry is readable as soon as `append` returns, and a client is answered only once
   a flush covering its entry has completed — instead of the 0.9 serialization.
2. **Snapshots are fragmented by the network, not the core.** 0.10 removed the chunked
   `InstallSnapshotRequest` from the crate (`openraft-legacy` keeps it) and hands
   `RaftNetworkV2::full_snapshot` the whole snapshot, so the transport owns fragmentation
   (`option.snapshot_chunk_size()`, 3 MiB by default) *and* reassembly. The migration landed
   this as a unary JSON fragment per RPC; leg 5 replaced it with **one client-streamed
   `InstallSnapshot` RPC** (proto change), which is the version to reason about:

   * `SnapshotChunk { group_id, opening, data, done }` — routing key and JSON `{vote, meta}`
     only on the first fragment, raw `bytes` for the payload. Raw bytes matter: the unary cut
     put frames inside an `Envelope`'s JSON, which inflates binary data ~4x and so needed
     `snapshot_max_chunk_size * 4 <= max_message_bytes` to hold (now validated in
     `GroupConfig::validate` as `chunk * 2 <= max_message_bytes`).
   * The follower's reassembly buffer is a local in `install_snapshot`, so a sender that dies
     mid-transfer leaves nothing behind and two transfers cannot collide. The unary cut kept
     that state in a shared map keyed by `(group, leader)`, with no eviction — the single
     piece of state that was not scoped to a call.
   * **No whole-RPC deadline.** Openraft passes `Config::install_snapshot_timeout` (200 ms by
     default) as `hard_ttl`, which 0.9 applied *per chunk*; on one stream it would abort every
     non-trivial transfer and openraft would restart it. The bounds are openraft's `cancel`
     future, the configured TCP keepalive, and `max_message_bytes` per fragment. A true idle
     policy needs per-fragment progress, which the request side does not expose; the append
     stream has that signal and uses it (see
     [Pipelined append](#pipelined-append-leg-5-built-measured-removed)).

   **Peak follower memory is unchanged** by all of this: `Raft::install_full_snapshot` takes
   the whole snapshot and `SnapshotData` is in-memory, so streaming removes the round trips and
   the leaked shared buffer, not the peak. The fragmenter, the framing helper and the
   reassembler are unit-tested in `transport.rs`, and
   `snapshots_cross_tonic_and_unknown_groups_are_rejected` now covers a real multi-fragment
   transfer and a truncated stream over tonic.
3. **`SnapshotMeta` lost `snapshot_id`.** A snapshot is identified by the position it covers,
   and the transfer id lives on the wire (in `openraft-legacy`'s v1 metadata). The stored
   snapshot format still round-trips 0.9 data — the third field is written empty and ignored on
   read — so no data migration is needed. The interruption test that asserted on the stored id
   now asserts on the stored *bytes*.

Smaller shapes worth knowing:

* `Raft<C, SM>` is generic over the state machine: every handle in the shell is
  `raft::RaftHandle` = `Raft<TypeConfig, Arc<MemStateMachine>>`.
* Storage methods return `io::Error`; openraft builds the structured `StorageError` around it.
  `read_vote` moved to `RaftLogReader`, `truncate` became `truncate_after(Option<..>)`
  (keep-inclusive, where 0.9's `truncate` was remove-inclusive), and `append` takes `IOFlushed`.
* `RPCError<C>` fixes its error parameter to `Infallible`, so a remote Raft error has no
  `RemoteError` to live in and is reported as `Unreachable` — the mapping openraft's own example
  network uses. `StreamingError` has no remote variant either.
* Metrics travel over the runtime-agnostic watch channel: `metrics().borrow_watched()`.
* `AppData` needs `Display`; ours names the command types and never the payload.

## The pre-migration inventory (bumped, broken, 514 errors)

The bump was applied and checked: **514 errors**, which is not as bad as it sounds, because
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
openraft = { version = "0.10.0-alpha.36", features = ["serde", "compat"] }
```

(`single-threaded` was in the bump as first applied and is deliberately not in the manifest:
see [What the migration cost, measured](#what-the-migration-cost-measured).)

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
requests**. One HTTP/2 stream per follower keeps order without sequence numbers; a multiplexed
design would have to reorder by sequence number. The trait also asks that the implementation
use `option.soft_ttl()` for a stream's setup or idle policy rather than `hard_ttl` — that
guidance was followed first and had to be abandoned, see
[Pipelined append](#pipelined-append-leg-5-built-measured-removed).

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

1. ~~Bump and inventory: hand-edit the manifest, `cargo update --precise`, `cargo check
   --all-features` into a file, categorize the errors by code and file. **Do not commit a
   broken build to `main` under any circumstance.**~~ **Done** (the bump is its own `wip`
   commit; the pre-commit hook's `wip` marker is the sanctioned escape hatch for a
   deliberately non-compiling tree).
2. ~~`TypeConfig` + storage traits, keeping batching behaviour identical.~~ **Done.**
3. ~~Network to `RaftNetworkV2`, still on the default sequential `stream_append`.~~ **Done.**
4. ~~Get the full suite green (`mise run test`, including the hardening and interruption tests)
   and `mise run verify`, and **commit that** — a working 0.10 migration with no behaviour
   change.~~ **Done, with the three documented behaviour changes above.**
5. ~~Only then the bidi RPC and true pipelining, with tests for ordering and for `soft_ttl`.~~
   **Done**: the snapshot stream and the bidirectional `StreamAppend`, below.
6. ~~Measure: deployment harness unbatched and batched, single node and three, against the
   recorded 0.9 numbers in `docs/benchmarks/results/deployment-path.json`.~~ **Done**: five
   configs, three trials each, 39/39 passing, plus the sequential-arm attribution runs.

Steps 1–4 are a migration; step 5 is the feature; step 6 was to decide whether step 5 was worth
it. **It says no, measurably.** The migration should ship. The append half of leg 5 is neutral
performance and carries a wedge-prone failure mode that we already had to fix once, so the
honest options were (a) revert the `StreamAppend` RPC and keep the migration plus the streamed
snapshot transfer, or (b) keep it as groundwork for a deployment where RTT dominates. **The
decision was (a)**: the unpipelined build measured level to a few percent in each direction, so
the RPC, its stall knob and its failure mode bought nothing that the same numbers could not be
had without.

## Pipelined append (leg 5): built, measured, removed

This section is the design record of the feature the migration existed for — it was implemented
and measured, and then deleted in `b82a2b1`. It is kept because the two hard-won details below
cost real time and would cost it again.

`StreamAppend` was a bidirectional RPC in `crates/shell/proto/raft.proto`:

* The leader streams `AppendEntries` requests on one `Envelope` stream; the follower relays
  them into `Raft::stream_append` and streams its results back on the response direction.
  **One HTTP/2 stream preserves order in each direction**, so the trait's "responses in input
  order" contract needs no sequence numbers and the transport never reorders anything.
* Requests and results each get a 64-slot channel, matched to openraft's own pipeline depth
  (`PIPELINE_BUFFER_SIZE`) so the transport is not the tighter bottleneck, and bounded so a
  leader cannot run unboundedly ahead of a slow follower.
* Opening the stream carries the configured RPC deadline (`request_timeout_ms`), because a
  peer that never sends response headers would otherwise hang replication. A failure *there*
  surfaces from `stream_append` itself, which openraft answers by backing off and retrying, so
  it cannot corrupt progress.
* A stream that produces nothing is closed after **`transport.stream_stall_timeout_ms`**
  (10 s by default). This is where the trait's `soft_ttl` guidance had to be abandoned, and
  the reason is worth keeping: `soft_ttl` is three quarters of `hard_ttl`, and on the
  replication path `hard_ttl` is the *heartbeat interval* — 100 ms in the benchmark, so a
  75 ms bound. A 75 ms gap is ordinary under load, so the stream was torn down mid-burst with
  requests outstanding; openraft's progress then keeps a `matching` index ahead of what the
  follower has, and the conflict that would repair it is *discarded* (`update_conflicting`
  ignores a conflict at or above `searching_end`, and one carrying a stale inflight id leaves
  the entry untouched). Its own docs note replication then "cannot make progress", so the
  follower never catches up. A stall detector is the bound this needs to be, not a latency
  budget, and no TTL openraft passes is usable for it.
* The follower stops feeding its `Raft` the moment the caller drops the results stream, so an
  abandoned stream cannot apply requests behind the leader's back.
* `Conflict` and `HigherVote` are protocol *answers*, not transport failures: they travel as
  `Ok(..)` and end the stream after delivery, matching `stream_append_sequential`. Only a
  remote `Fatal` becomes an `RPCError` (as `Unreachable` — `RPCError<C>` cannot carry a
  `RemoteError`, exactly as on the unary path).

Tests: `append_results_come_back_in_request_order` drives three distinct requests (each ack
carries the index it matched, so a reordering would show) through the raw bidi client and
asserts the acks come back in order; `a_stalled_append_stream_is_closed` serves a peer that
accepts the stream and never answers, and asserts the stream is reported closed — attributed
by the error message rather than a wall clock, because timing assertions here are flaky under
a fully parallel suite. The three-replica tonic test now replicates over this path, and
heartbeats do too.

**What it bought: nothing measurable, and it was removed.** See
[the measurement](../benchmarks/deployment-scale.md#after-openraft-010) and its
[latency comparison](../benchmarks/deployment-scale.md#latency). Comparing the two arms trial by
trial, the unpipelined build is level to ~8% ahead at concurrency 128 and up to ~9% *behind* at
concurrency 8, where a per-entry round trip is genuinely on the critical path — i.e. a few
percent either way, against a spread of ±10% between repeats of the same arm. The large wins over
0.9 come from the migration itself: openraft 0.10 no longer serializes local appends behind the
previous flush. Removing the RPC cost the branch ~200 lines of transport, one config knob
(`stream_stall_timeout_ms`) and two tests, and removed the failure mode described above. It also
removed the branch's entire premise, which is the honest result: the premise was wrong, and the
migration was still worth doing.

**The lesson to keep.** Do not put a per-response deadline on a replication stream. Openraft's
`hard_ttl` on that path is the heartbeat interval and its `soft_ttl` is derived from it, so the
natural reading of "honour `soft_ttl` for idle policy" produces a 75 ms bound that tears the
stream down mid-burst under load — and openraft cannot repair the progress that leaves behind,
because `update_conflicting` discards the conflict that would (see the wedge note in the
benchmark doc).

## Risks

* **Alpha dependency.** The pin was taken for a performance feature that did not pay, so it is
  worth saying why it is still the right call: the *migration* to that alpha is worth 2.3–31.8×
  over 0.9 on its own (0.10 stopped serializing local appends behind the previous flush). The
  cost is the alpha's churn, not the missing pipelining benefit — and an alpha is churn.
* `single-term-leader` and `loosen-follower-log-revert` are gone; if we relied on either, the
  behaviour changes rather than failing at compile time — worth checking before leg 2.
* Persisted formats: `SnapshotMeta` losing `snapshot_id` and `StorageError` becoming a struct
  both touch data we write, so the `compat` types are worth a look in leg 2 even though they do
  not help the trait work.
* The reward is expected to be concentrated in the unbatched path, which is not the
  configuration we would deploy.
