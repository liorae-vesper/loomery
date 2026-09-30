# Checkpoint scheduling and durability

Reviewed against OpenRaft 0.9.25 and RocksDB on 2026-09-30.

Two independent choices are often called “sync versus async”: whether command
application waits for a checkpoint, and whether a completed storage write has
been synchronized to durable media. Moving a write to a background task does
not decide its durability.

[OpenRaft's storage contract](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/storage/v2.rs)
allows two recovery models: a persistent state machine saves state before
`apply` returns; a snapshot-backed state machine may apply in memory, but must
persist snapshots and restore from them at startup. In both models the log
retains the suffix needed for recovery.

[RocksDB's synchronous-write documentation](https://github.com/facebook/rocksdb/wiki/Basic-Operations#synchronous-writes)
explains that `sync=true` waits for durable storage, while unsynchronized
writes may be lost on a machine crash. Its atomic write batches keep related
updates together. This is independent of Rust async execution.

| Model | Cost paid by writes | Recovery | Requirements |
|---|---|---|---|
| Awaited state checkpoint per apply batch (current) | Full state serialization and synchronized checkpoint write | Load checkpoint, replay committed suffix | State, membership, applied index and dedup persisted together before apply completes |
| Background periodic snapshots with in-memory apply | Snapshot CPU/I/O outside command completion; state-copy/lock contention remains | Load durable snapshot, replay longer committed suffix | Persist snapshot before publishing it; retain logs until coverage is durable; bounded builder concurrency; ordered installation and error handling |

For Loomery's intended larger histories, background periodic snapshots or
incremental state persistence should reduce write-path work compared with
rewriting every historical event per batch. This is an architectural inference,
not a measured throughput claim. The choice also depends on recovery-time
budgets, state size, write rate and acceptable snapshot contention.

The current decision is to keep awaited durable state checkpoints and separate
OpenRaft-scheduled snapshots. OpenRaft's default snapshot policy is
`LogsSinceLast(5000)`; users can tune `GroupConfig.raft.snapshot_policy`.
Database calls already run on Tokio's blocking pool, but apply awaits their
completion. Full state serialization currently runs on the calling task.

A future migration to snapshot-backed recovery must be deliberate: change the
recovery contract, retain the committed log suffix, serialize an immutable state
view off the runtime, synchronize completed snapshots, and test crashes during
snapshot creation, installation and log purge. Simply spawning the current
checkpoint write and returning from apply would violate the selected persistent
state-machine contract. Disabling WAL synchronization is not the proposed
optimization.

Before changing the policy, measure command latency (including p95/p99), state
copy/serialization time, bytes written per command, blocking-pool queueing and
restart replay time at expected tenant sizes. Incremental state writes remain
another candidate because they can retain the current recovery contract while
avoiding full historical-state rewrites.

## Experimental comparison

The default remains `group.storage.state_persistence = "checkpoint"`. An
opt-in `"snapshot"` spike implements the second recovery model above: apply in
memory, persist scheduled snapshots before publication, and restore the last
durable snapshot plus the committed log suffix. Log/vote/commit writes still
use synchronized WAL writes; acknowledgement still follows quorum and apply.
The selected mode is recorded durably and cannot change for an existing database.
Snapshot installation remains durable before replacing live state.

The paired runner and measured results are in
[checkpoint-spike.md](../benchmarks/checkpoint-spike.md). This experiment does
not promote snapshot-backed recovery to the production default. Snapshot state
copying and serialization still hold a state read lock; serialization runs on
the blocking pool. Large-state contention, interrupted snapshot writes and
machine/power failures need further validation before adopting the mode.
