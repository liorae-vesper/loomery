# Configurable command batch matrix

> **Measured before the openraft 0.10 migration.** The numbers below come from 0.9.25, and the
> migration moved the write path — 2.3–31.8× on the
> [deployment path](deployment-scale.md#after-openraft-010). Read them as the record of the
> decision they informed, not as current throughput.

The [matrix runner](../../scripts/bench-batch-matrix.py) compares batch limits
1, 8, 16, 32, 64 and 128 in checkpoint and snapshot-backed persistence modes.
Batch limit 1 is always included as the baseline for each mode/concurrency pair.
This measures the production proposal writer, tonic replication and synchronized
RocksDB log path, including quorum and application before each successful reply.

## Run and configure

```sh
mise run bench-batch-matrix -- --output benchmark-results/batch-matrix
```

The default [workload](batch-matrix.json) uses three replicas, concurrency 128,
three trials per arm, 256 warmup commands and 4096 measured commands. Keeping
concurrency fixed means increasing the batch limit is the only configured
difference within a mode/concurrency pair. The largest selected batch can fill
without increasing offered concurrency between arms. The runner records actual
commands per Raft index and a batch-size histogram; the configured limit alone
does not prove batches fill.

Each round executes every arm once in a seeded shuffled order. Every execution
starts fresh replica processes/databases and runs one trial. This interleaves
the three repeats rather than measuring every baseline repeat first. The seed
and complete execution order are retained in `manifest.json`. The default seed
is 20261001. Set `--seed` to vary it.

```sh
# Select sizes; baseline 1 is included automatically.
mise run bench-batch-matrix -- --batch-sizes 8 16 32 64 128 \
  --concurrencies 128 --trials 3 --operations 4096 --warmup 256 \
  --output benchmark-results/batch-matrix-custom

# Add concurrency as a second dimension. Low-concurrency arms can underfill.
mise run bench-batch-matrix -- --batch-sizes 8 32 128 \
  --concurrencies 8 32 128 --modes snapshot \
  --output benchmark-results/batch-matrix-load

# Review the complete plan without building, opening listeners or writing results.
mise run bench-batch-matrix -- --output benchmark-results/planned --dry-run

# Recalculate tables from existing evidence without rerunning trials.
mise run bench-batch-matrix -- --output benchmark-results/batch-matrix --summarize
```

Use `--config` for another consensus workload. CLI overrides include
`--max-delay-ms`, `--max-batch-bytes`, `--queue-capacity`, trial count,
operations and warmup. Changing these modifies all arms equally. The selected
concurrencies and sizes form a Cartesian product with the selected persistence
modes. The workload's concurrency is used when `--concurrencies` is omitted.

Default collection delay is 1 ms, byte budget 256 KiB and channel capacity 1024.
The 5000-entry snapshot threshold keeps the default workload below automatic
snapshots in every arm. No final snapshot is forced. Each trial kills the
leader, checks a post-election write, then kills/reopens all three replicas and
verifies applied indices and event counts. These are process crashes, not
power-loss tests. Custom workloads can cross the snapshot threshold; because it
counts entries rather than commands, batching then changes snapshot frequency.

## Evidence and interpretation

Each arm stores independent repeats under
`<mode>-c<concurrency>-b<limit>/repeat-NNN/`. Every repeat contains the usual
effective configuration, environment, raw command samples, replica databases,
failover and recovery checks. `repeat-NNN.json` records its input configuration.

The runner generates `matrix.json`, `matrix.csv` and `matrix.md`. Summary rows
include median/min/max trial throughput, median per-trial p50/p95/p99, the
throughput ratio to the matching baseline, observed average commands/index,
the pooled batch-size histogram and all recovered event counts. Percentiles
are not pooled across trials. Missing/failed trials and recovery mismatches
prevent a valid summary; they are not silently excluded. A failed harness run
stops the matrix and retains its evidence.

Queue and collection waits are included in command latency. A closed-loop
concurrency of 128 supplies more outstanding work than the earlier
[concurrency-8 comparison](batching.md), and 4096 commands grow checkpoint
history further than its 1000-command workload. Their absolute rates are not
directly comparable. These experiments share one host's CPU, persistent disk
and loopback network; they do not establish multi-machine deployment capacity.

The writer currently waits for each batch's after-apply response before
submitting its next batch. Checkpoint mode serializes full state/history for
each apply batch. A larger configured command batch can reduce that cost, but
small explicit batches also constrain the worker's outstanding Raft submissions.
The unbatched path submits concurrent single-command requests directly, allowing
consensus and state-machine work to overlap; committed ranges can contain
multiple commands. This is a plausible explanation of checkpoint-mode
differences, not a measurement of apply-batch sizes or a causal attribution.
Do not assume every intermediate batch size must outperform the baseline.

## Recorded results

The 2026-10-01 experiment uses the default matrix and retains all evidence in
ignored `benchmark-results/batch-matrix-20261001/`. All 36 independent trials
completed. The table uses medians across three trials, including the median of
each trial's latency percentile, rather than pooling percentiles.

| Mode | Batch limit | Writes/s | Gain vs same-mode baseline | Trial range (writes/s) | p50 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| Checkpoint | 1 | 440.6 | 1.00× | 436.3–445.3 | 275.70 | 475.78 |
| Checkpoint | 8 | 142.3 | 0.32× | 142.0–144.4 | 877.79 | 1618.21 |
| Checkpoint | 16 | 279.7 | 0.63× | 279.4–281.0 | 449.17 | 860.39 |
| Checkpoint | 32 | 531.8 | 1.21× | 530.1–534.6 | 226.84 | 469.56 |
| Checkpoint | 64 | 969.7 | 2.20× | 964.7–981.1 | 124.78 | 263.69 |
| Checkpoint | 128 | 1611.6 | 3.66× | 1605.2–1622.4 | 73.47 | 156.44 |
| Snapshot | 1 | 752.2 | 1.00× | 748.0–755.3 | 171.66 | 200.57 |
| Snapshot | 8 | 1269.9 | 1.69× | 911.6–1384.3 | 100.58 | 134.48 |
| Snapshot | 16 | 2143.5 | 2.85× | 1810.8–2156.8 | 58.88 | 84.48 |
| Snapshot | 32 | 2837.7 | 3.77× | 2686.4–2884.0 | 43.95 | 69.15 |
| Snapshot | 64 | 3451.9 | 4.59× | 1519.9–3456.1 | 35.47 | 61.53 |
| Snapshot | 128 | 3841.8 | 5.11× | 3793.4–3845.1 | 33.15 | 56.49 |

Concurrency is **128 for every row**. Every measured batch filled its configured
limit in every trial. Pooled batch counts per mode across the three repeats were
12288, 1536, 768, 384, 192 and 96 for limits 1, 8, 16, 32, 64 and 128 respectively.
There were zero write failures across 147456 measured commands. All three
replicas recovered 4353 events in every trial: 256 warmup, 4096 measured and
one post-failover command. No run was discarded.

The host was AMD Ryzen 9 7900 with 24 available logical CPUs, approximately
32 GB RAM and btrfs on `/dev/nvme0n1p2`, using Rust 1.98.1 via mise. Measurements
used an uncommitted workspace based on `0665b64`; each repeat records its build
profile, revision/dirty status, environment and effective configuration.

Size 128 had the highest throughput of the tested limits in both modes under
this workload. Snapshot throughput increased with the limit in these medians,
but its size-8 and size-64 ranges show material variability. The narrow size-128
range does not establish sustained production capacity. Checkpoint sizes 8 and
16 were slower than their matching unbatched baseline; the serial batch-writer
and growing full-history checkpoint path described above provide a hypothesis,
not measured attribution of those costs. The experiment changed neither the
production writer nor its defaults to favor these measurements.

Higher limits, different concurrency/arrival patterns, checkpoint-history size,
collection delay and sustained compaction need separate comparisons. In
particular, a count of 128 at concurrency 8 cannot reproduce these full batches.
