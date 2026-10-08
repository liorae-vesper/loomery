# Handoff: openraft 0.10 migration for pipelined append

This is a self-contained work packet. Everything needed to continue is here or in the two
files it names; the previous session's context is not required.

**All six legs are complete.** `crates/shell` is on openraft **0.10.0-alpha.36** with
`cargo check -p loomery-shell --all-features` clean and `mise run test` / `mise run verify`
green on branch `feature/openraft-pipelined-append`. `main` is untouched, and the first commit
that may merge is the migration one.

**The measured verdict: the migration paid, the pipelining did not show up.** 2.4–2.7× at the
default deployment config, 11.8–15.5× unbatched at concurrency 128, and 0.9's single-node
anomaly gone — but a build with `stream_append` reverted to openraft's sequential default
matches the pipelined one on throughput and is weakly ahead on p50, so the win is 0.10's core,
not the bidirectional RPC.
The numbers and the attribution arms are in
[deployment-scale.md](../benchmarks/deployment-scale.md#after-openraft-010-and-pipelined-append).

**One decision is outstanding, and it is not a measurement:** whether to keep or revert the
append half of leg 5 (`StreamAppend`). It is performance-neutral here and it introduced a
silent wedge that had to be fixed once (see the sweep note in the benchmark doc and
`docs/raft-configuration.md` on `stream_stall_timeout_ms`). The streamed snapshot transfer is
independent of it and is a robustness improvement either way.

Read `docs/research/openraft-010-migration.md` for the surface inventory, the measured error
counts per step, the `StreamAppend` design, and **the behaviour changes the migration had to
accept** — two of those move the write path on their own, which is why the benchmark needed the
sequential-arm runs to say anything.

## The job

Migrate `crates/shell` from openraft **0.9.25** to **0.10.0-alpha.36**, then implement
**pipelined append** so a leader does not wait for a per-entry response, and measure whether it
was worth it.

Work happens on branch **`feature/openraft-pipelined-append`** in
`/home/john/Workspace/Liorae/loomery`. `main` is untouched and must stay that way until the
branch is green.

Read first: `docs/research/openraft-010-migration.md` (plan, surface inventory, measured
counts, behaviour changes) and `docs/benchmarks/deployment-scale.md` (what the numbers
currently are).

## What is already done

**Leg 1 — the bump (complete).** `openraft = { version = "0.10.0-alpha.36", features =
["serde", "compat"] }` in `crates/shell/Cargo.toml`, verified in `Cargo.lock`. The
`single-threaded` feature was in the bump as first applied and is deliberately gone: it sets
`OptionalSend = ()`, which conflicts with our multi-threaded Tokio runtime and tonic
transport, and dropping it is net −20 errors (it un-masks `RaftStateMachine` errors that were
hiding behind the `Send` failures).

**Leg 2 — the migration (complete, one commit).** Measured error counts, step by step:
514 (bumped) → 334 (error types re-parameterised) → 314 (features) → 313 (`TypeConfig`) →
214 (storage traits, `Raft<C, SM>`) → 0 (`RaftNetworkV2` + fragmenting `full_snapshot`).
What landed:

* **Error types:** `StorageError<TypeConfig>` (still generic in 0.10 — it takes the *type
  config*), `RPCError<TypeConfig, E>` (the node type is gone), `RaftError<TypeConfig>`,
  `ClientWriteError<TypeConfig>`. `Unreachable<TypeConfig>` is built from an error.
* **`TypeConfig`:** `declare_raft_types!` with `D`/`R`/`NodeId`/`Node`/`AsyncRuntime` only;
  `Entry`, `LeaderId`, `Vote`, `Payload`, `Responder` and `Batch` come from the macro defaults,
  and `SnapshotData` left the type config entirely. `Display for AppData` names the command
  types and never the payload. `raft::alias` binds the openraft aliases to our config, and
  `raft::RaftHandle` = `Raft<TypeConfig, Arc<MemStateMachine>>`.
* **Storage:** the reader/writer split (`read_vote` on `RaftLogReader`), `truncate` →
  `truncate_after(Option<LogIdOf<C>>)` (keep-inclusive), `io::Error` return types, `IOFlushed`
  instead of the deprecated `LogFlushed`, and `RaftStateMachine::apply` consuming a stream of
  `EntryResponder`s. The existing batching/dedup/persistence logic is unchanged and lives in
  `state_machine::apply_batch`, which the tests drive directly.
* **Network:** `NoopNetwork` and `TonicNetwork` implement `RaftNetworkV2` (the `Net*`
  sub-traits come from openraft's blanket impls). `stream_append` is **overridden** with a
  bidirectional `StreamAppend` RPC: the leader streams requests, the follower streams results
  back through `Raft::stream_append`, and HTTP/2 ordering supplies the in-order contract.
  `full_snapshot` is a client-streamed `InstallSnapshot` RPC: the sender fragments
  (`SnapshotChunk`, raw bytes, routing key and JSON `{vote, meta}` on the first fragment) and
  the follower reassembles inside the handler frame, aborting a stream that ends early.
* **Tests:** the whole suite is green, including `openraft::testing::log::Suite` (note the new
  path), the hardening and interruption suites. Two tests changed for 0.10 semantics rather
  than because of a bug; both are documented in the migration doc: the append-serialization
  hardening test, and the interruption test that asserted on the retired `snapshot_id`.

## Next actions, in order

Nothing is measured-outstanding. What remains is one decision and, if it goes the other way,
one experiment:

1. **Keep or revert the append half of leg 5 (`StreamAppend`).** Reverting is deleting the
   override in `RaftNetworkV2 for TonicNetwork` plus the `StreamAppend` RPC in
   `crates/shell/proto/raft.proto`; openraft's `stream_append_sequential` takes over and the
   measured numbers do not change (see the sequential arms in
   [the benchmark](../benchmarks/deployment-scale.md#the-win-is-the-migration-not-the-pipelining)).
   Keeping it costs the bidi plumbing, the stall knob and the failure mode that produced the
   first wedged sweep. The snapshot streaming is independent and stays either way.
2. **If it is kept, the open question is where the remaining time goes.** The batched ceiling
   is unmoved at ~13,000 writes/s (batch 128, concurrency 128) and the unbatched path is now
   ~9,000–12,000, so the next lever is per-entry cost rather than the exchange — which is what
   `docs/benchmarks/deployment-scale.md#where-the-per-entry-cost-actually-is` already says.
   Verifying whether the stall bound or the 64-slot channel is the limiter is a two-constant
   change in `transport.rs`; both are `TransportConfig`-visible now except the channel size.

## Constraints that are not negotiable

* **Git:** one branch per part, linear history, fast-forward only into `main`, no merge
  commits, and only merge when the CI-equivalent check passes. The default test suite must
  stay self-contained (fakes / in-process); real NATS, OIDC and services stay behind
  `nats`, `oidc`, `test-services` features.
* **Never commit a non-compiling build to `main`.** On this branch it is already the case, and
  the branch must not merge until `mise run test` and `mise run verify` are green.
* **The events record is sacred.** Destructive format changes are fine pre-release, but the
  `events` family must never be lost; everything derived may be rebuilt.
* **Lints:** no `unwrap`/`expect`/panicking indexing/`string_slice`/`arithmetic_side_effects`
  in production code.
* **Performance probes and soaks run `--release`** — a debug build measures the compiler.
* **CI is Buildkite now.** The local equivalent is
  `docker run --rm -v "$PWD:/workdir" -w /workdir -v loomery-ci-target:/target -e CARGO_TARGET_DIR=/target loomery-ci sh -c "mise run verify && mise run test"`.
* **Documentation in markdown is part of done.**
* **Interrupt and ask** if a design decision is not specified, a new dependency is needed, or a
  gate fails for environmental reasons.

## Traps that cost the previous session real time

* **`cargo add openraft@0.10.0-alpha.36` fails with exit 101** — `unrecognized feature for
  crate openraft: storage-v2`. Cargo validates the existing feature list against the new
  version before editing. Hand-edit the manifest, then `cargo update -p openraft --precise`.
* **`compat` is not a trait adapter** despite the name. It is `Upgrade`/`Compat<From, To>` for
  reading data written by an older version — useful for persisted `LogId`/`Vote`/`Entry`, and
  no help with the trait migration.
* **The pre-commit hook refuses to commit a non-compiling tree** (it runs `cargo fmt --check &&
  cargo deny check && cargo clippy -- -D warnings`). It has a sanctioned escape hatch: a commit
  message matching `wip` skips those gates. Use it only for genuinely in-progress commits, and
  say so in the message.
* **Do not pipe a command into `tail` and read success from it.** The previous session reported
  a dependency bump as done when `cargo add` had exited 101 — the truncation hid the error.
  Verify from the files (`grep` the manifest and the lock) instead.
* **Benchmark knobs:** `max_batch_bytes` silently caps the batch (~2 KB per command, so the
  256 KB default stops at ~127 commands); `--snapshot-points ""` disables snapshot points; the
  harness rejects a `name_bytes` above `MAX_NAME_BYTES` (200).
* **`act -j` accepts exactly one job id** — repeating it runs only the last one.

## Traps leg 2 found (0.10 API details that cost time)

* **`StorageError` is generic.** `StorageError<C>` takes the type config, so the fix is
  `StorageError<TypeConfig>`. `RPCError` lost the node type: `RPCError<C, E>`. The same
  re-parameterisation applies to `RaftError`, `ClientWriteError`, `RemoteError` and `Timeout`.
* **`Raft<C, SM>` is generic over the state machine.** Every handle needs
  `Raft<TypeConfig, Arc<MemStateMachine>>`; `raft::RaftHandle` names it once.
* **The RPC types lost their `NodeId` parameter:** `AppendEntriesResponse<C>`, `VoteRequest<C>`,
  `VoteResponse<C>` — not `<u64>`.
* **`apply` takes a stream of `EntryResponder`s and sends responses per entry.** Keep the batch
  logic in a separate function so it stays testable without an `OpenRaft` responder, and send
  each response only after the durable write — otherwise a failed write tells a writer it
  committed.
* **`futures_util` is not a direct dependency.** `tokio_stream::{Stream, StreamExt}` is, and
  `tokio_stream::Stream` *is* `futures_core::Stream`, so the trait bounds line up.
* **`RPCOption::snapshot_chunk_size` is `pub(crate)`**, so a test cannot force a multi-fragment
  transfer through `full_snapshot` itself. The transport tests therefore drive the framed
  stream through the raw tonic client with the same helpers the client uses
  (`fragments`/`opening_json`/`pump_fragments` in `transport.rs`), which covers reassembly over
  a real stream.
* **Do not put openraft's `hard_ttl` on a stream.** For snapshots it is
  `install_snapshot_timeout`, 200 ms by default, which 0.9 applied per chunk; a whole-stream
  deadline aborts every non-trivial transfer and openraft restarts it.
* **`openraft::testing::{StoreBuilder, Suite}` moved** to `openraft::testing::log`, and
  `Suite::test_all` is now `async`.
* **Metrics moved to the runtime-agnostic watch channel:** `metrics().borrow_watched()` (bring
  `openraft::type_config::async_runtime::WatchReceiver` into scope), not tokio's `borrow()`.
* **An explicit `Entry = openraft::Entry<TypeConfig>` line must go:** in 0.10 `Entry` is
  `Entry<CLID, Payload>`, and the macro's default links it to the configured `Payload`.
* **`RaftNetwork` in 0.10 is an empty deprecated stub** (the v1 trait moved to the separate
  `openraft-legacy` crate), so `impl RaftNetwork<C> for X { fn vote … }` produces E0407. Only
  implement the `Net*` traits or `RaftNetworkV2`.
* **A remote `RaftError` cannot be carried by `RPCError<C>`** (its error parameter is
  `Infallible`). Report it as `Unreachable`, which is what openraft's own example network does.
* **openraft's first log index is 0**, so `prev_log_id: None` only matches a request whose first
  entry is index 0. A hand-built `AppendEntriesRequest` with index 1 trips an assertion inside
  openraft's following handler (`first_ent.index() == prev_log_id.next_index()`) and panics a
  core task — the test sees a `Panicked` error rather than a clear message.
* **`StreamAppendResult` is `Result<Option<LogId>, StreamAppendError>`.** `Conflict` and
  `HigherVote` are *answers*, not RPC failures: they ride as `Ok(..)` and both the core and
  `stream_append_sequential` end the stream right after delivering one. Mapping them to an
  `RPCError` would lose the leader's conflict recovery.
* **The server's `stream_append` output drains accepted requests after the input ends** (it
  queues a oneshot per request before yielding). That is why a one-request heartbeat stream
  still gets its answer, and why closing the request half is safe.
* **`tokio_stream::pending()` plus a stub `Transport` impl** is a clean way to test a peer that
  accepts a stream and never answers; `Status::unimplemented` covers the methods the stub does
  not care about without tripping the `unimplemented!` lint.

## What this is expected to buy, honestly

Pipelining attacks the per-entry round trip. Batching has already amortized that: at 256
commands per entry the path spends ~0.048 ms per command on work, against 1.29 ms per command
unbatched (three nodes) and ~3.4 ms per entry on a single node. So the expected win is large
**unbatched** and small **batched** — and the unbatched path is not what we would deploy. There
is also an open oddity this work does not explain: a single node unbatched is *slower* than
three nodes (295 vs 777 writes/s), which is not elections (term stays 1) and not heartbeats
(100 ms vs 10 ms changes nothing), and disappears entirely under batching. Pin an alpha
dependency only with that trade-off in view.
