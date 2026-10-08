# Persistence hardening — what is verified, and what is not

This note records the persistence validation behind Phase 1's
"extend persistence validation" item. It maps each claim to the test or
experiment that supports it, and is explicit about the gaps.

## What is verified

| Property | Evidence |
|---|---|
| A **long history** survives a restart in both persistence modes (300 commands, checkpoint and snapshot) | `crates/shell/src/raft/hardening_tests.rs` — applies 300 `task.create` commands, shuts down, reopens the same database and checks every event is back |
| The **dedup window** survives a restart (a re-proposed command replays instead of duplicating) | same test: re-proposing the last command yields `ProposeOutcome::Replayed` and adds no event |
| The **applied index** never goes backwards across a restart | same test |
| **Storage contracts** (append, truncate, purge, snapshot build + install, re-applying committed entries, vote round-trip) | `crates/shell/src/raft/suite.rs` (in-memory) and `crates/shell/src/raft/persistent_tests.rs` (RocksDB, both modes) run OpenRaft's `testing::Suite` |
| **Snapshot transfer** between replicas, and recovery from a purged prefix | `persistent_tests::snapshots_cross_tonic_and_unknown_groups_are_rejected`, `persistent_tests::snapshot_mode_recovers_log_only_and_snapshot_with_purged_prefix` |
| **Storage failure injection** — a log flush callback that fails must stop Raft without acknowledging or applying | `crates/shell/src/raft/append_tests.rs` |
| **Mode protection** — a database's persistence mode is pinned; mismatches and invalid markers fail startup | `persistent_tests::persistence_mode_is_fixed_across_restarts_in_both_directions`, `unmarked_nonempty_database_is_pinned_to_legacy_checkpoint_mode`, `invalid_persistence_marker_fails_closed` |
| **Interrupted snapshot creation** — a build whose persist fails installs nothing, keeps the previous snapshot, and is retryable | `crates/shell/src/raft/interruption_tests.rs` — builds against a read-only database (a real `RocksDB` write rejection, not a mocked seam), then checks the installed snapshot and the stored bytes are unchanged, that recovery still works, and that the next build covers the newer log |
| **Interrupted purge** — a rejected purge batch moves neither the covered entries nor the purge floor | same file: `purge` against a read-only database, then both invariants, then the positive control (a committed purge moves both, and the floor survives a reopen on its own) |
| **Restart/failover** in the controlled benchmark harness | `mise run bench-consensus`, `mise run test-consensus-failures` |
| **A long history** (up to 10,000 commands) with a mid-history snapshot, a restart and an intact dedup window | `crates/shell/src/raft/hardening_tests.rs` — the opt-in soak below |
| **State-only recovery** — the fold and the dedup window answer with the Raft log purged away entirely | `hardening_tests::checkpoint_state_answers_without_its_log` |
| The **dedup window** answers "how big" and "who is next" without reading itself back | `loomery_core::dedup` — `len_and_oldest_answer_without_reading_the_window` |

Run the whole set with:

```sh
mise run test
```

The soak is opt-in, and runs a release build (see above) because it is a
performance probe:

```sh
mise run soak                                  # 2,000 commands, both modes
mise run soak-checkpoint                       # 8,000 commands, checkpoint mode
mise run soak-snapshot                         # 10,000 commands, snapshot mode
LOOMERY_TEST_SOAK=1 LOOMERY_TEST_SOAK_COMMANDS=50000 mise run test
```

## The soak: what a long history costs

Measured with the opt-in soak on one machine (2026-10-07, `rustc` 1.98.1, release
of nothing — a `test` profile build), applying `task.create` commands to one
single-node group:

| Mode | Commands | Apply | Rate | Snapshot build | Restart | On disk |
|---|---|---|---|---|---|---|
| checkpoint | 200 | 1.23 s | ~162/s | 5.8 ms | 157 ms | 39.6 MB |
| checkpoint | 1,000 | 28.9 s | ~34/s | 28.8 ms | 230 ms | 38.0 MB |
| checkpoint | 2,000 | 115.8 s | ~17/s | 56.3 ms | 335 ms | 41.3 MB |
| snapshot | 200 | 58 ms | ~3,434/s | 5.8 ms | 30 ms | 0.36 MB |
| snapshot | 1,000 | 468 ms | ~2,134/s | 29.1 ms | 256 ms | 1.63 MB |
| snapshot | 2,000 | 1.52 s | ~1,319/s | 55.9 ms | 803 ms | 3.23 MB |
| snapshot | 10,000 | 18.1 s | ~553/s | 276 ms | 1.53 s | 43.7 MB |

What the numbers say:

- **Checkpoint mode is quadratic in the history.** Each apply serializes the whole
  state, so throughput halves as the history doubles (162 → 34 → 17 per second at
  200 → 1,000 → 2,000 commands). At 2,000 commands snapshot mode is ~78× faster
  end to end.
- **Snapshot mode degrades gently**: 3,434 → 2,134 → 1,319 → 553 per second over
  200 → 10,000 commands. It is the mode that scales, and it is still the
  experimental one.
- **Recovery is cheap in both modes** and linear in the history: 30 ms at 200
  commands to 1.53 s at 10,000, from a snapshot plus a log tail.
- **The on-disk figures are file sizes, not live data**: `RocksDB` compacts in the
  background, so a directory measured right after a run holds whatever levels
  existed then. Checkpoint's footprint looks flat because every apply *overwrites*
  the single state key; snapshot's grows with the history, as its snapshots do.

The default stays checkpoint: it is the mode whose recovery does not depend on a
snapshot being present, and its cost is acceptable at the history sizes the system
serves today. Choosing snapshot mode by default needs a decision and a benchmark of
its own.

## After step 1: the families

Splitting the database into column families and appending every apply's events to
an `events` family ([storage-layout.md](../storage-layout.md)) is deliberately
additive: it makes the record durable on its own account — so purging the Raft log
can no longer lose history — without changing how state is written. The same soak,
1,000 commands on one machine, before and after:

| Mode | Apply, before | Apply, after | Restart, before | Restart, after | On disk, before | On disk, after |
|---|---|---|---|---|---|---|
| checkpoint | 28.9 s (~34/s) | 29.5 s (~33/s) | 230 ms | 2.50 s | 38.0 MB | 981 MB |
| snapshot | 468 ms (~2,134/s) | 590 ms (~1,694/s) | 256 ms | 287 ms | 1.63 MB | 2.15 MB |

What this says:

- **The apply rate is unchanged, as designed.** The record is written alongside the
  checkpoint, and the checkpoint is what dominates — step 1 was not expected to fix
  the quadratic cost, and did not. Snapshot mode, which writes no state per apply,
  stays fast.
- **Checkpoint mode's on-disk size and restart time are *not* explained by this
  step.** They are file sizes and WAL replay, not live data, and they moved far more
  than the events added: the readings were taken on a loaded machine right after a
  full suite with the database still un-compacted. They are recorded here rather
  than smoothed over, and step 2's benchmarks (the ones
  [D2](../design.md#d2--storage-engine) requires before a contract change) have to
  account for them: an unexplained 25× in file size is exactly the kind of thing
  that should not be inherited by the next step.

## Why the soak read slow, and what it actually costs

The first version of the numbers below was measured in an **unoptimised test
build** and with a cost I had not looked for. Both are corrected here, because the
difference matters: it is the difference between "the storage is a bottleneck" and
"the storage is not".

**1. The soak measured a debug build.** `cargo test` compiles without
optimisation, and a durable write path spends its time in code the optimiser
matters for. The same 2,000-command soak:

| Build | Checkpoint | Snapshot |
|---|---|---|
| `cargo test` (debug) | ~2,760/s | — |
| `cargo test --release` | **~13,134/s** | **~15,258/s** |

`mise run soak` and its variants therefore run `--release`: a performance probe in
an unoptimised build measures the compiler.

**2. The apply path cloned the whole dedup window on every command.** The window is
FIFO and bounded (4,096 entries), and the state machine used to mirror it after
every insert by asking the registry for *all* of its entries — 4,096 tuples per
command once the window was full, and a window that *grows* with the history while
it fills, which made the cost grow with the history too. It is the same fold code
in both persistence modes, which is exactly why the slope was identical in both:
the problem was never the storage engine. `Registry::len()`/`Registry::oldest()`
answer the two questions the mirror actually needed, and the mirror is gone.

With that fixed, the curve is **flat** — the per-command cost no longer depends on
the history at all:

| Commands | Checkpoint (release) | Snapshot (release) | Restart | On disk |
|---|---|---|---|---|
| 2,000 | ~13,134/s (152 ms) | ~15,258/s (131 ms) | 35 ms | 5.0 MB |
| 8,000 | ~11,381/s (703 ms) | — | 128 ms | 29.7 MB |

**What this does and does not say.** It says the persistence path is not the
bottleneck: ~11–15k commands per second per group, linear to 8,000 commands, with a
synchronous WAL write for every append and every apply. It does **not** say what a
deployment does — these are single-node, in-process measurements with no network
between replicas and no fsync pressure from a shared disk; the controlled
multi-process harness ([consensus benchmarks](README.md)) is where end-to-end
numbers with batching and tonics belong. It also does not say a relational database
would be slower or faster: it says the storage layer is not what was limiting this,
and that the two things that were limiting it are now named.

## After step 2: the deltas

Step 2 ([storage-layout.md](../storage-layout.md)) makes an apply persist **only
what it changed** — the aggregate states its events touch, the dedup entries the
window added or evicted, and the applied index — in the same synchronous batch as
its events, and makes recovery read that state back per aggregate instead of from a
whole-state record. The same soak, one machine, before and after, at 2,000 commands:

| Mode | Apply, before | Apply, after | Restart, before | Restart, after | On disk, before | On disk, after |
|---|---|---|---|---|---|---|
| checkpoint | 113.2 s (**~17/s**) | 1.68 s (**~1,190/s**) | 393 ms | 200 ms | 33.7 MB | 5.0 MB |
| snapshot | 1.34 s (~1,497/s) | 1.59 s (~1,255/s) | 365 ms | 805 ms | 5.83 MB | 4.22 MB |

That is a **67× improvement** in the mode that had the quadratic cost, and it also
accounts for the unexplained checkpoint figures recorded after step 1 (981 MB on
disk, 2.50 s restart): they were the whole-state rewrite's churn, and the rewrite is
gone.

The curve for checkpoint mode after the change, and the same shape in snapshot mode:

| Commands | Apply (checkpoint) | Rate | Restart | On disk | Rate (snapshot) |
|---|---|---|---|---|---|
| 1,000 | 578 ms | ~1,730/s | 105 ms | 2.57 MB | — |
| 2,000 | 1.68 s | ~1,190/s | 200 ms | 5.03 MB | ~1,255/s |
| 4,000 | 4.85 s | ~824/s | 387 ms | 9.99 MB | ~849/s |

**That table was measured in a debug build, before the dedup-window cost was
found.** Both are corrected above: the slope was the per-command mirror of the dedup
window (a fold cost, shared by both modes), and the absolute numbers were a debug
build. Read the section above for what the path actually costs; the table here is
kept as the before-and-after of step 2 alone, in the same build.

## What is *not* verified

- **A crash inside the purge batch.** OpenRaft drives `purge`, and the store
  performs the covered-log deletion and the purge-floor write in one synchronous
  WAL batch, so there is no window between them to crash into — which is the
  mitigation, not a gap. What *is* injected is a rejected batch, where neither
  side may move; the atomicity claim itself rests on the single batch.
- **A process kill mid-serialization.** The interruption tests inject a storage
  write rejection. A `SIGKILL` during serialization would leave the same
  observable state (nothing persisted, the previous snapshot intact) because the
  persist is a single key write, but it is not exercised as a kill.
- **Capacity under deployment load**, which is a separate probe:
  [`multigroup.md`](multigroup.md).

## Defaults

No default was changed by this pass: checkpoint remains the default mode, and
switching a database's mode is still an explicit, pinned decision. Any change
to that needs its own decision and benchmark.
