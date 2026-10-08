# Handoff: openraft 0.10 migration for pipelined append

This is a self-contained work packet. Everything needed to continue is here or in the two
files it names; the previous session's context is not required.

## The job

Migrate `crates/shell` from openraft **0.9.25** to **0.10.0-alpha.36**, then implement
**pipelined append** so a leader does not wait for a per-entry response, and measure whether it
was worth it.

Work happens on branch **`feature/openraft-pipelined-append`** in
`/home/john/Workspace/Liorae/loomery`. It is currently at a deliberately **non-compiling**
commit; `main` is untouched and must stay that way until the branch is green.

Read first: `docs/research/openraft-010-migration.md` (plan, surface inventory, feature list)
and `docs/benchmarks/deployment-scale.md` (what the numbers currently are).

## What is already done (leg 1, complete)

* Bump applied and verified in both files:
  `openraft = { version = "0.10.0-alpha.36", features = ["serde", "single-threaded", "compat"] }`
  in `crates/shell/Cargo.toml`, and `cargo update -p openraft --precise 0.10.0-alpha.36`.
* `cargo check -p loomery-shell --all-features` → **511 errors**, inventoried in the migration
  doc as a root-cause map. The count is a cascade; there are about ten real causes:

  | Count | Cause | Kind |
  |---|---|---|
  | 174 | `u64: RaftTypeConfig is not satisfied` — `StorageError<u64>`, and 0.10 made `StorageError` a struct | mechanical |
  | 71 | `BasicNode: std::error::Error is not satisfied` — error types still taking a type parameter | mechanical |
  | ~60 | `cannot be sent between threads safely` — likely the `single-threaded` feature | feature trial |
  | 65 | `(): RaftStateMachine<TypeConfig>` — the trait split | real work |
  | 26 | `u64: RaftLeaderId` — the `LeaderId` associated type | real work |
  | 27 | E0107 — generic arity changed | mechanical |
  | 11 | E0407 — `vote` off `RaftNetwork`; `truncate`/`read_vote` off `RaftLogStorage` | real work |
  | 17 | `BasicNode: Hash/Ord/NodeId`; `AppData` needs `Display` | small |

  Errors cluster in `state_machine.rs` (187), `transport.rs` (186), `mod.rs` (182),
  `port.rs` (112), `host.rs` (77), `rocks_log_store.rs` (57), `network.rs` (44),
  `log_store.rs` (42), `group.rs` (40).

## Next actions, in order

1. **Mechanical first, because it should halve the count.** Delete the type parameters from
   error types: `StorageError<u64>` → `StorageError`, and the same for the error types behind
   the 71 `BasicNode: std::error::Error` errors. Then re-run the check and **record the new
   count** — that number decides whether this is a short migration or a long one.
2. **Test the feature set.** `single-threaded` sets `OptionalSend = ()`, making boxed futures
   and streams non-`Send`; we run a multi-threaded tokio runtime with a tonic transport.
   Try `features = ["serde", "compat"]` (no `single-threaded`) and compare the error count
   before doing any work on the ~60 `Send` errors.
3. **`TypeConfig`.** Add the `LeaderId` associated type; give `AppData` a `Display` impl.
   Note that removing `SnapshotData` from `declare_raft_types!` changed nothing on its own
   (511 → 510) — `SnapshotData` moved to `RaftNetworkV2`, so it belongs in the network impl.
4. **Storage traits.** `log_store.rs`, `rocks_log_store.rs`, `state_machine.rs`: the reader /
   writer split, `read_vote`/`truncate` relocation, `SnapshotMeta` losing `snapshot_id`.
   Behaviour must not change: batching, dedup and the `events` family writes all stay as they
   are. Use `crates/shell/src/raft/suite.rs` plus the hardening and interruption tests as the
   correctness oracle.
5. **Network.** `network.rs` and `transport.rs` to `RaftNetworkV2` + the granular `Net*`
   sub-traits, using the **default sequential `stream_append`** — no protocol change yet.
6. **Get it green and commit.** `mise run test` (including the raft hardening and interruption
   tests) and `mise run verify`. This commit is the milestone: a working 0.10 with no
   behaviour change. **This is the first commit that may merge.**
7. **Then, and only then, real pipelining.** A bidirectional `StreamAppend` RPC in
   `crates/shell/proto/raft.proto`, the receiving side calling `Raft::stream_append`, responses
   yielded in input order (one ordered HTTP/2 stream per follower avoids needing sequence
   numbers), honouring `option.soft_ttl()` for idle policy rather than `hard_ttl`. Test
   ordering and TTL.
8. **Measure.** `mise run bench-deployment-scale` on the unbatched *and* batched paths, one
   node and three, against the recorded 0.9 numbers in
   `docs/benchmarks/results/deployment-path.json`. This decides whether step 7 was worth it.

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

## What this is expected to buy, honestly

Pipelining attacks the per-entry round trip. Batching has already amortized that: at 256
commands per entry the path spends ~0.048 ms per command on work, against 1.29 ms per command
unbatched (three nodes) and ~3.4 ms per entry on a single node. So the expected win is large
**unbatched** and small **batched** — and the unbatched path is not what we would deploy. There
is also an open oddity this work does not explain: a single node unbatched is *slower* than
three nodes (295 vs 777 writes/s), which is not elections (term stays 1) and not heartbeats
(100 ms vs 10 ms changes nothing), and disappears entirely under batching. Pin an alpha
dependency only with that trade-off in view.
