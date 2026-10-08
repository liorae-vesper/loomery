# Controlled consensus benchmark

The `consensus_bench` example runs Loomery's production tonic transport,
RocksDB storage and pure-core apply path. Each replica is a separate child
process with its own Tokio runtime, real loopback TCP listener, database
and WAL. The coordinator submits valid `workspace.create` commands through
the leader's shared `ProposalWriter`, which calls `Raft::client_write` directly
when batching is disabled and groups commands when enabled. Peer replication uses tonic gRPC.
There is no HTTP gateway or client-facing gRPC write endpoint in this benchmark.

Run from the repository root with the pinned toolchain:

```bash
mise exec -- cargo build --release -p loomery-shell --example consensus_bench
mise exec -- cargo run --release -p loomery-shell --example consensus_bench -- \
  --config docs/benchmarks/consensus.json \
  --output benchmark-results/baseline-001
```

`mise run bench-consensus` runs the same configuration with a new timestamped
output directory. `--help` lists CLI options. Without a config file, the runner
uses the same workload defaults with OpenRaft's normal defaults. An output
directory must be new; the runner never resets or reuses an existing database.
Choose an output path on the filesystem/device you intend to measure.

## Workload and phases

Every trial has fresh databases and unique deterministic causation/entity keys.
The warmup creates real state; measured operations continue growing that same
state. Keys cannot accidentally turn into dedup hits. Payload generation and
control-pipe traffic are outside the per-command latency interval.

1. Boot independent replicas, initialize node 1, admit learners and promote all
   nodes to voters. Record initialization time separately from database boot.
2. Run warmup, then wait for every replica to apply its last log and verify its
   event count before starting measurement.
3. Maintain at most `concurrency` in-flight writes. Record each latency from
   `client_write` invocation until the committed-and-applied response, including
   the awaited durable state checkpoint. Record raw samples, successful log
   indexes, errors and exact encoded command JSON sizes.
4. Wait for every follower's applied index and verify event counts. This is a
   post-batch catch-up barrier, not a per-entry propagation-latency measurement.
5. Trigger a leader snapshot and wait for snapshot coverage to reach the last
   measured log. Mark whether an automatic snapshot already covered that index,
   so an already-completed snapshot is not mistaken for new snapshot-build cost.
6. When `failover` is true and there is more than one voter, kill the benchmark's
   leader process, observe a new leader and commit a fresh probe write. Leader
   observation is polled every 10 ms and includes coordinator overhead. This is
   an idle-leader crash after the write phase, not failure during an in-flight write.
7. Stop surviving replicas, reopen all databases on the same membership ports,
   wait for leadership and verify every replica's applied index and event count.
   Record restart-to-verified-recovery time. The formerly killed leader must catch
   up with the post-failover probe too. Stop all processes and retain databases.

All log/state/snapshot writes keep the production synchronous WAL durability
settings. No benchmark-only storage shortcuts are used. The benchmark therefore
includes today's full-state checkpoint cost, which grows with history.

## Parameters

Config files may override any of the example's fields and any `GroupConfig`
setting. The runner merges partial objects with serialized defaults, validates
configuration and rejects typos. Effective configuration is saved in full.

| Field | Meaning |
|---|---|
| `nodes` | 1, 3 or 5 voters; 1 is a RocksDB/apply baseline without peer RPCs |
| `trials` | Independent fresh-database trials; default 3 |
| `warmup` / `operations` | Warmup and measured command counts; defaults 100 / 1000 |
| `concurrency` | Bounded closed-loop in-flight writes; default 8 |
| `name_bytes` | Workspace name length, not total RPC size; default 256 |
| `payload_pattern` | `pseudorandom` printable ASCII (default), or compressible `repeated` characters |
| `worker_threads` | Tokio runtime workers per replica and coordinator; default 2 |
| `operation_timeout_ms` | Deadline per proposal; timeout means unknown commit outcome; default 10000 |
| `phase_timeout_ms` | Whole control-phase deadline; default 120000 |
| `failover` | Crash the benchmark-owned leader after measurement; default true |
| `group.raft` | Heartbeat/election timeouts, replication batch limits, snapshot and retention policy |
| `group.transport` | gRPC timeouts/windows/message limits and optional TLS settings |
| `group.storage` | RocksDB cache, memtable, background-job and file limits |

For TLS or mTLS, supply the `server_tls` and `client_tls` settings documented in
[raft-configuration.md](../raft-configuration.md). The local benchmark requires
both, or neither. It advertises loopback IP addresses: either issue certificates
with a `127.0.0.1` IP SAN, or set `client_tls.server_name` to a verified DNS SAN
such as `localhost`. Certificate files must be accessible to every replica.

The load generator is closed-loop: the next write is sent when a slot becomes
free. It measures service latency under bounded load, not latency under an
independent arrival rate. Slowdowns also reduce the offered request rate;
there is no coordinated-omission correction. Percentiles describe successful
writes only, while failures are always counted and preserved. No automatic
retries hide leadership/timeout problems.

## Artifacts and comparisons

Each output directory contains:

- `config.json`: complete effective configuration.
- `environment.json`: timestamp, debug/release profile, CPU/memory/available CPU
  count, OS, data filesystem, Rust compiler and Git revision/dirty status.
- `Cargo.lock`: exact dependency resolution for the run.
- `trial-NNN/report.json`: boot times/PIDs/addresses, warmup and measured p50,
  p95, p99, min/max, throughput, raw samples, follower barriers, snapshot time,
  crash election/probe, restart verification and replica term/log/apply/snapshot
  metrics.
- `trial-NNN/warmup-samples.json` and `write-samples.json`: returned batch results
  saved before subsequent correctness checks, including failures.
- `trial-NNN/error.json`: failure reason if a trial cannot complete. A phase
  timeout can terminate the child before its raw samples are returned; treat
  that run as invalid and commit outcomes as unknown.
- `trial-NNN/<node-id>/`: retained RocksDB databases for inspection.

Times use microseconds and local monotonic clocks. Replica absolute clocks are
never subtracted. Throughput is successes divided by the measured batch wall
time; setup, warmup and recovery are excluded. Percentiles use exact nearest-rank
selection over individual successful samples. Compare the per-trial results,
not an average of percentiles. Tiny smoke runs cannot establish meaningful p99s.

For a controlled comparison, use release builds from a recorded commit, the
same host/filesystem, workload sizes and payload pattern. Keep the host otherwise
idle, record any CPU affinity/governor settings, and change one parameter at a
time. Start with 1 versus 3 voters and concurrency 1 versus 8, then vary payload
size, heartbeat/election intervals and snapshot thresholds. Increase operation
counts until the tails and trial-to-trial variation stabilize; account for the
checkpoint cost increasing as history grows. Compare TLS separately.

Separate processes isolate runtimes, but still share this host's CPU, disk,
page cache and loopback network. There is no injected network delay/loss or
cross-host clock comparison. These are host-specific end-to-end measurements,
not isolated RPC/fsync timings or multi-machine deployment predictions.
[OpenRaft's minimal benchmark](https://github.com/databendlabs/openraft) similarly
cautions against treating framework benchmarks as application performance;
[RocksDB's benchmark methodology](https://github.com/facebook/rocksdb/wiki/performance-benchmarks)
records storage and workload conditions. Use profiler/I/O tools if a run needs
attribution to serialization, log fsync, checkpoint fsync or replication.

## Comparing state persistence

Run the [paired spike](checkpoint-spike.md) with
`mise run bench-persistence -- --output benchmark-results/persistence-comparison`.
The runner uses identical workloads for both modes, keeps synchronous Raft log
writes, tests snapshot thresholds of 5000 and 200 entries, skips a final forced
snapshot, kills all replicas, then checks recovered indices and event counts.

The harness also accepts `snapshot_before_restart` (default true) and
`crash_restart` (default false). With a skipped snapshot, `snapshot_us` and
`snapshot_already_current` are null in report schema 2. `crash_restart` kills the
owned replica processes rather than asking them to shut down; this tests process
crashes, not host power loss. Automatic snapshots may still occur.

## Comparing command batching

Run `mise run bench-batching -- --output benchmark-results/batching-comparison`.
The [paired runner](../../scripts/bench-batching.py) compares count 1 against
count 8 in each persistence mode, with the same workload, synchronized WAL,
quorum and after-apply replies. It verifies failover and SIGKILL/restart recovery
without forcing a final snapshot. Pass `--batch-commands N` to change the count;
request concurrency limits the available batch size.

Report schema 3 adds `distinct_log_indices` and `commands_per_log_index` to
write summaries. These are observed indices for successful fresh workspace
commands, not configured targets or counts of sync syscalls. Queue and collection
waiting are included in individual latency samples. See [batching.md](batching.md).

For a configurable sweep of 8/16/32/64/128-command limits, use
`mise run bench-batch-matrix -- --output benchmark-results/batch-matrix`.
The [matrix guide](batch-matrix.md) explains fixed-concurrency controls,
randomized repeat order, CSV/JSON tables and observed batch-size distributions.

## The deployment path at growing sizes

Use `mise run bench-deployment-scale` to drive this harness at 2,000 / 5,000 /
10,000 / 20,000 events on three nodes (checkpoint mode, plus snapshot mode at the
largest size), with batching and failover off so the curve is about scale. The
[scale note](deployment-scale.md) records the results, the probes that move
concurrency and batching, and what the numbers do and do not say.

> **Payload note:** `name_bytes` is bounded by the domain —
> `workspace::MAX_NAME_BYTES`, 200 bytes — and the harness now refuses a config
> that asks for a longer name instead of producing a run in which every command is
> rejected. Recorded runs from before this change used 256-byte names and are not
> payload-comparable with anything measured after it.

## Failure injection during writes

Use `mise run test-consensus-failures` to inject leader/follower crashes, quorum
loss and deliberately lost replies during write phases, then verify retries and
whole-cluster SIGKILL recovery. The [fault experiment guide](failure-injection.md)
documents configurable batch sizes, exact event checks, storage callback failure
tests and the limits of process-crash testing.
