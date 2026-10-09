# Consensus storage performance investigation

Investigated on 2026-10-01 against the workspace's then-pinned OpenRaft 0.9.25
and rust-rocksdb 0.24.0. This note explains the earlier single-group benchmark
results of approximately 170 checkpoint-mode and 550 snapshot-mode writes/s.
It does not change the production durability or recovery policy.

> **Since migrated to OpenRaft 0.10.0-alpha.36**
> ([openraft-010-migration.md](openraft-010-migration.md)). One finding below no
> longer holds: 0.10 does *not* wait for the first append's flush callback before
> issuing the next append — it tracks IO completion with a watermark and lets
> appends overlap. That is the lever this note says is missing, and it is where
> the win came from: the deployment path is 2.3–2.7× faster at the default config
> and up to 15.7× unbatched at concurrency 128, *without* the network-side
> pipelining this note's next steps anticipated (that was built, measured within a
> few percent, and removed). See
> [deployment-scale.md](../benchmarks/deployment-scale.md#after-openraft-010).

## Findings from the pinned code

The strongest candidates are serialized durable log writes and poor leader-side
batching, followed by the extra full-history work in checkpoint mode.

- OpenRaft's `append_to_log` awaits `log_store.append` **and then the flush
  callback**. Its core therefore waits for durability even if the store returns
  early. The command loop executes engine work after individual API messages.
  See the [pinned Raft core](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/core/raft_core.rs).
- Its commit command separately awaits `save_committed` before scheduling
  state-machine application. Our [log store](../../crates/shell/src/raft/rocks_log_store.rs)
  synchronizes both append batches and committed-pointer updates. Commit
  updates may cover multiple commands; do not assume two sync calls per command.
- `spawn_blocking` in [disk.rs](../../crates/shell/src/raft/disk.rs) keeps native
  RocksDB work off Tokio executor workers. Awaiting the result still leaves
  the caller waiting. Replacing this with synchronous I/O inside async methods
  would additionally block the executor. [Tokio's guidance](https://docs.rs/tokio/1.53.1/tokio/task/fn.spawn_blocking.html)
  supports blocking-pool use for finite blocking operations and notes that
  started tasks cannot be cancelled merely by dropping/aborting their awaiter.
- Checkpoint apply clones and serializes the complete aggregate state, event
  history and dedup window. It also serializes the inner snapshot bytes as a JSON
  byte array in `StoredSnapshot`, amplifying bytes and CPU work. It holds a state
  read lock across checkpoint persistence. Snapshot builders similarly retain
  a read lock during copying/serialization. These costs grow with tenant history.
- Apply reads committed entries back through the log reader before invoking
  the state machine. JSON decoding, range reads and thread-pool handoffs add work
  beyond the synchronized append itself.

An [upstream discussion](https://github.com/databendlabs/openraft/discussions/1170)
describes slow durable stores, serialized 0.9 append I/O, shared heartbeat/log
connections, and the importance of callback durability. Its high-throughput
numbers include different storage/synchronization conditions and are not a
valid target comparison. Simply detaching our append callback cannot pipeline
past the pinned core's explicit wait. Waiting for 100 appends before flushing
could deadlock a single group when its core is waiting for the first callback.

## Append contract, callback tests and grouped WAL sync

Our synchronous append is conservative, not a callback correctness violation.
The [0.9.25 storage contract](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/storage/v2.rs)
requires entries to be readable when append returns and durable when the
success callback fires. It explicitly permits the callback **before or after**
append returns. Our batch has WAL enabled and `sync=true`; the blocking-pool
write finishes before we signal success. Write failures are reported through
both the append result and the callback. Vote/log/truncate/purge operations
remain ordered because the core awaits each operation; merely spawning their
write closures and returning would lose that guarantee.

Both checkpoint and snapshot RocksDB modes run OpenRaft's
[`Suite::test_all`](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/testing/suite.rs).
Its append helper waits for append return and then the flush callback before
subsequent assertions. This exercises callback completion alongside log,
vote, truncation, purge and snapshot behavior, but does not independently prove
visibility before a deferred callback, inject sync failures, or emulate power
loss. It is necessary coverage, not proof of physical durability.

Additional [append tests](../../crates/shell/src/raft/append_tests.rs) check a
real RocksDB append through an existing reader, successful callback completion
and close/reopen; a read-only RocksDB write rejection must fail both return and
callback without adding a log entry. A controlled in-memory store also returns
with entries visible but deliberately withholds the flush callback. Two
concurrent `client_write` requests confirm that the pinned core cannot issue
the second append until the first callback is released. Both requests then
finish after their respective callbacks are released. That test isolates the
core's scheduling constraint from RocksDB and device speed.

Async WAL writes plus grouped synchronization are a valid architecture:

1. An ordered writer completes each `WriteBatch` with WAL enabled and
   `sync=false`. Append may return only after those entries become readable.
2. Capture a high-water mark of completed writes and their pending callbacks
   **before** starting the WAL sync. A successful sync completes only callbacks
   covered by that mark; writes arriving during the sync need a later one.
3. Use a bounded maximum delay and/or batch threshold, with a blocking worker
   performing the native sync. A count threshold alone must never leave a lone
   request waiting forever. Fail covered callbacks on sync error and stop the
   writer from acknowledging subsequent operations; drain or fail pending work
   during shutdown.
4. Keep votes, log writes, truncation, purge and commit metadata in their required
   order. `save_vote` still must wait for durability before returning. Persisted
   snapshots and checkpoint-mode apply retain their own durability requirements.

The pinned rust-rocksdb API exposes `DB::flush_wal(true)`, which flushes internal
WAL buffers and synchronizes them. With `manual_wal_flush=true`, `SyncWAL()`
alone does not drain those internal buffers; native RocksDB recommends
`FlushWAL(true)`. These are WAL operations, not memtable-to-SST flushes. See
the [native API](https://github.com/facebook/rocksdb/blob/v10.4.2/include/rocksdb/db.h)
and [WAL performance guidance](https://github.com/facebook/rocksdb/wiki/WAL-Performance).

The missing ingredient for **this single-group implementation** is multiple
outstanding appends to the same WAL. OpenRaft 0.9.25 waits for the first callback
before providing the next append. (0.10 removed that wait: the core now permits
overlapping appends and tracks their completion with an IO watermark, so this
particular obstacle is gone — see
[openraft-010-migration.md](openraft-010-migration.md).) A periodic worker would usually have one
leader append to sync per interval, adding delay rather than grouping writes.
Each tenant currently opens a separate database/WAL, so different tenant
callbacks cannot share one database's WAL sync either. Useful grouping requires
verified pipelined Raft I/O, explicit command batching, or a deliberate shared
database layout. The latter changes isolation, tuning and recovery and is not
an incidental storage flag. The later
[command-batching implementation](../../docs/raft-configuration.md#opt-in-command-batching)
chooses explicit batching above Raft: several commands share one entry, retaining
the synchronous append/callback path. It introduces no grouped WAL worker or
weakened acknowledgment policy. See [the paired results](../benchmarks/batching.md).

## Storage-only probe

A temporary release diagnostic used the same RocksDB resource limits/LZ4 as
production, an 800-byte repeated payload, 400 sequential iterations per arm,
and `sync=true` with WAL enabled. It ran on the workspace's btrfs mount,
`/dev/nvme0n1p2`, not `/tmp`. Queue time was measured from submission until the
blocking closure began; service time covered native writes, not just the sync
syscall. These are one-run diagnostics, not the full consensus workload.

| Arm | Records/s | Service p50 | Blocking queue p50 | Total p50 |
|---|---:|---:|---:|---:|
| One blocking closure, two sequential sync writes per iteration | 517 | 1965 µs | n/a | 1965 µs |
| One `spawn_blocking` per iteration, two sync writes | 484 | 1988 µs | 3 µs | 1996 µs |
| One `spawn_blocking` per iteration, one sync write | 964 | 993 µs | 3 µs | 1002 µs |
| Eight records per single synchronized batch | 7235 | 1077 µs | 3 µs | 1085 µs per batch |

The eight-record probe also placed its synthetic committed marker in the same
batch. This is a raw storage comparison, **not** a proposal to mark uncommitted
Raft logs committed during append. Consensus controls when the real marker may
advance. Direct versus spawn arms have different scheduling and run order;
their difference is not a precise causal estimate of pool overhead.

Synchronization cost dwarfed observed queue delay. This supports the serialized
sync/batching hypothesis; it does not quantify every stage of consensus or
prove the pool cannot saturate with many tenant groups. RocksDB
[group commit](https://github.com/facebook/rocksdb/wiki/WAL-Performance) combines
eligible concurrent writes to the same database, but does not proactively wait
to enlarge batches. One outstanding append per group offers little opportunity.
Separate tenant databases cannot share a RocksDB write group.

The preliminary `/tmp` probe was on tmpfs and much faster. It is excluded from
the storage comparison: tmpfs does not represent persistent-device sync cost.
This also explains why earlier tiny smoke runs under `/tmp` cannot be compared
with the btrfs benchmark results. Source and output are retained locally in
ignored `benchmark-results/storage-probe-20261001/{probe.rs,report.json}`.

## Actual consensus batch trace

A temporary trace recorded append entry counts and committed-pointer calls for
a three-voter, snapshot-mode run: concurrency 8, 100 writes, no warmup, no final
snapshot, all replicas killed/reopened. It measured 534 writes/s with zero
failures. Counts include startup/membership work and belong to the original
replica PIDs, excluding reopened processes:

| Replica | Append entry counts | Committed-pointer writes |
|---|---|---:|
| Bootstrap leader | 105 batches of one entry | 31 |
| Follower 2 | 15 × one, 12 × seven, 2 × three | 26 |
| Follower 3 | 15 × one, 12 × seven, 1 × four, 1 × three | 25 |

The leader fails to batch queued client commands into larger local appends,
while replication batches entries for followers. RPC batch limits therefore
do not necessarily increase leader append batch size. The benchmark concurrency
of eight means roughly eight commands can queue behind this sequential path;
its ~14 ms per-command median is not ~14 ms of native write service per command.
Reports and the trace remain in `benchmark-results/sync-diagnostic-20261001/`.
Temporary instrumentation was removed after measurement.

## Closing RocksDB and WAL recovery

`RaftGroup::shutdown` stops Raft; it does not consume the group or explicitly
close its database. `Disk` holds an `Arc<DB>`, and state-machine/read/task clones
can prolong its lifetime. The Rust wrapper calls `rocksdb_close` when the
database's final owned handle drops. Depending on retained tasks, dropping one
group handle need not reach that point immediately.

A new test in [persistent_tests.rs](../../crates/shell/src/raft/persistent_tests.rs)
boots and applies genesis in each mode, stops/drops the group while retaining
its state-machine handle, verifies that a second open fails with a lock error,
then releases that handle and successfully reopens/replays. Re-running genesis
does not duplicate events. This reproduced a lifetime/lock pitfall, not corrupted
WAL replay.

RocksDB distinguishes [database close and sync durability](https://github.com/facebook/rocksdb/wiki/Basic-Operations).
Replay of unflushed memtable data from WAL on restart is normal. Synchronized
WAL writes protect acknowledged data without depending on a graceful close;
flushing memtables to SST files on every command is not necessary. With default
WAL buffering, a process crash differs from a machine/power failure. Our prior
SIGKILL tests exercise the former, not the latter.

[WAL recovery modes](https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes)
can truncate recovery at corruption or skip damaged records. Do not silently
select a permissive mode or repair a replicated log merely to make startup
succeed: holes or reverted persisted votes/log pointers can violate Raft's
storage assumptions. An actual failure needs the RocksDB error/LOG, retained
files and a coordinated replica-rebuild policy. No WAL corruption has been
reproduced in this investigation.

## Next experiments, preserving durability

1. ~~Measure larger application batches as an explicit API/response semantics
   change, or spike a verified OpenRaft version with truly pipelined I/O.~~ Done:
   command batching is implemented and measured (2.50× checkpoint, 3.31×
   snapshot), and 0.10 was spiked — the win was in its *core*, not in the
   network-side pipelining, which measured within a few percent and was removed.
   An
   ordered writer must preserve vote/log/truncation ordering, read visibility
   when append returns, callback durability and error propagation. A dedicated
   writer thread alone would not have bypassed 0.9.25's callback wait; 0.10
   removes that wait, so this is now a question of what the extra concurrency
   buys rather than whether it is possible.
2. Instrument append/commit/apply batch sizes, queue/service times, serialized
   bytes and RPC latency in a repeatable harness. Compare concurrency 1/8/32
   and one versus three voters on the same persistent filesystem.
3. Reduce checkpoint serialization/write amplification with incremental state
   or a compatible storage encoding; test replay and snapshot installation.
   Releasing snapshot locks earlier requires preserving publication ordering.
4. Verify lifecycle under in-flight snapshot/write tasks and repeated graceful
   close/reopen. Define a consuming/draining host lifecycle before promising
   that `shutdown` alone closes the database.
5. Collect RocksDB stall/compaction/WAL statistics before changing background
   jobs or pipelined-write knobs. [Write stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls)
   are a separate mechanism; this investigation did not measure stall counters.

Disabling WAL or synchronization would change the tested guarantee and is not
the optimization proposed here. Preserve the [immutable recovery mode](../../docs/raft-configuration.md)
for existing deployments while comparing implementations.
