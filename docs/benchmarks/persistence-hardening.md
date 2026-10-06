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
| **Restart/failover** in the controlled benchmark harness | `mise run bench-consensus`, `mise run test-consensus-failures` |

Run the whole set with:

```sh
mise run test
```

## What is *not* verified

- **Interrupted snapshot creation.** A snapshot build that is abandoned midway
  (process kill during serialization/persist) is not injected. The suite covers
  a *completed* build and install; the abandoned-build path relies on the
  builder being idempotent and keyed by `last_applied_index`, which is the
  documented contract (`docs/research/checkpoint-policy.md`) but not exercised
  under injection.
- **Interrupted purge.** OpenRaft drives `purge`; a crash exactly between the
  covered-log deletion and the purge-floor write is not injected. The RocksDB
  store performs both in one synchronous WAL batch, which is the mitigation.
- **Very large histories.** 300 commands is a correctness probe, not a soak
  test. Checkpoint cost grows with history size; see the measured tradeoffs in
  [`checkpoint-spike.md`](checkpoint-spike.md) and the storage investigation in
  [`../research/consensus-storage-performance.md`](../research/consensus-storage-performance.md).
- **Capacity**, which is a separate probe:
  [`multigroup.md`](multigroup.md).

## Defaults

No default was changed by this pass: checkpoint remains the default mode, and
switching a database's mode is still an explicit, pinned decision. Any change
to that needs its own decision and benchmark.
