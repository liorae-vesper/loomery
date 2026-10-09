# Checkpoint versus snapshot-backed apply

> **Measured before the openraft 0.10 migration.** The numbers below come from 0.9.25, and the
> migration moved the write path — 2.3–31.8× on the
> [deployment path](deployment-scale.md#after-openraft-010). Read them as the record of the
> decision they informed, not as current throughput.

Measured on 2026-09-30 using the experimental modes in this workspace. The
snapshot-backed implementation improves single-group write throughput by about
3.1× on this host. It remains opt-in; the default is still checkpoint recovery.

Both arms use three separate replica processes, tonic over loopback, RocksDB
with synchronized WAL writes, quorum commit, and responses after application.
The only configured difference within each pair is
`group.storage.state_persistence`: `"checkpoint"` or `"snapshot"`.
Checkpoint mode synchronizes the complete applied state per apply batch.
Snapshot mode applies in memory, restores the last durable snapshot at startup,
and lets OpenRaft replay the committed log suffix. Snapshots are durable before
publication, so they can safely cover logs selected for purge.

## Reproduce

```sh
mise run bench-persistence -- --output benchmark-results/persistence-comparison
```

The [paired runner](../../scripts/bench-persistence.py) builds the release example once, then runs
four arms sequentially: both modes with snapshot thresholds of 5000 and 200
entries. Each arm uses the base workload in [consensus.json](consensus.json):
three trials, 100 warmup writes, 1000 measured writes, concurrency 8,
256-byte pseudorandom workspace names, and two Tokio workers per process.
Pass `--config path.json` to change the workload, including larger histories.
Use a new output directory for each comparison. Results include raw samples,
environment/configuration records, retained databases and `comparison.json`.

No final snapshot is forced. After measurement, the runner kills the leader,
waits for election, writes one probe command, verifies all surviving replicas,
kills the remaining replicas, and reopens every original database. Recovery
checks each replica's applied index and event count. These are process crashes,
not power-loss tests. The 5000-entry arm stays below its snapshot threshold;
the 200-entry arm exercises automatic snapshots during writes.

## Results

Values below are medians across three trials, including the median of each
trial's latency percentile; they are not pooled percentiles.

| Snapshot threshold | Mode | Writes/s | p50 (ms) | p99 (ms) | Restart to verified recovery (ms) |
|---|---|---:|---:|---:|---:|
| 5000 | Checkpoint | 174.3 | 44.60 | 81.54 | 513.9 |
| 5000 | Snapshot | 550.1 | 14.53 | 19.38 | 374.4 |
| 200 | Checkpoint | 170.3 | 45.51 | 109.13 | 471.3 |
| 200 | Snapshot | 529.5 | 14.78 | 26.20 | 409.1 |

Throughput ratios are 3.16× at threshold 5000 and 3.11× at threshold 200.
All 12 trials had zero write failures. All three replicas recovered 1101 events
in every trial, including the post-failover probe. Snapshot-backed recovery also
passes OpenRaft's storage suite and a targeted restart test covering log-only
recovery, durable snapshot plus purged prefix and retained suffix, and dedup
after recovery. Databases persist their recovery mode and reject mode changes.

The host was an AMD Ryzen 9 7900 with 24 available logical CPUs, approximately
32 GB RAM and btrfs on `/dev/nvme1n1p2`; Rust 1.98.1 through mise. Measurements
used an uncommitted workspace based on `0fe11e7`. The matching configurations,
environment and reports are retained locally under
`benchmark-results/{checkpoint,snapshot}-crash-{5000,200}/`; these artifacts are
ignored by Git. `benchmark-results/comparison.json` contains the summary.

## Interpretation and remaining work

Removing repeated full-history checkpoints materially reduces write-path cost
without removing quorum or synchronized log durability. Frequent snapshots add
some latency to the snapshot-backed arm. Checkpoint throughput ranged from
129–174 writes/s in the 200-entry arm, so more repeated runs are needed to
characterize its variance.

This is a short, single-group, closed-loop, one-host experiment. It does not
measure tenant-level parallelism, read traffic or independent-machine network
latency. Restart time includes boot, election and catch-up; the faster snapshot
restart here does not establish that replay will be faster for large histories.
Snapshot copying and serialization still hold a state read lock, although
serialization runs on the blocking pool. Before changing the default, test
larger histories, crashes during snapshot writes/installations/purge,
storage failures, bounded snapshot contention, and recovery-time budgets.
