# Command batching on OpenRaft 0.9.25

Measured on 2026-10-01 with the production shared proposal writer. Batching
eight commands per Raft entry improved median checkpoint throughput by 2.50×
and snapshot-backed throughput by 3.31× compared with new unbatched runs on
the same host. Raft log WAL synchronization, quorum and after-apply responses
remain enabled in both arms.

## Reproduce

```sh
mise run bench-batching -- --output benchmark-results/batching-comparison
```

The [paired runner](../../scripts/bench-batching.py) runs both batch counts
in each persistence mode. Only `group.proposals.max_batch_commands` differs
within a pair. Defaults: count 1 versus 8, three trials per arm, three replica
processes over tonic loopback, 100 warmup and 1000 measured workspace commands,
concurrency 8, 256-byte pseudorandom names and two Tokio workers per replica.
Collection delay is 1 ms, the command-byte budget is 256 KiB and the proposal
channel holds 1024 commands. The script alternates pair order between the
two modes; it is not a randomized trial-order experiment.

All trials use a 5000-entry snapshot threshold, skip the final forced snapshot,
kill the leader, verify a successful command after re-election, then kill and
reopen every replica and check applied indices and event counts. They remain
below the automatic snapshot threshold. These are process crash tests, not
power-loss tests. The persistence mode is fixed per database as before.

## Results

Each number is the median of three per-trial measurements, including the median
of per-trial percentiles. Percentiles are not pooled across trials.

| Persistence | Commands per entry | Writes/s | Gain | p50 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|
| Checkpoint | 1 | 172.3 | baseline | 45.43 | 80.49 |
| Checkpoint | 8 | 431.0 | 2.50× | 17.81 | 30.25 |
| Snapshot | 1 | 537.2 | baseline | 14.33 | 18.32 |
| Snapshot | 8 | 1780.6 | 3.31× | 4.40 | 5.70 |

Raw batched checkpoint throughputs were 443.7, 416.1 and 431.0 writes/s;
batched snapshot throughputs were 1780.6, 871.7 and 1839.1 writes/s. The snapshot
spread is material; the median is not a sustained-capacity guarantee. The
unbatched checkpoint runs ranged from 155.9 to 173.4 writes/s, and unbatched
snapshot runs from 497.9 to 557.7 writes/s. No trial was discarded.

Every arm recorded the expected observed batch size: 1000 distinct measured
Raft indices unbatched versus 125 batched. All 12 trials had zero write failures,
and all three replicas recovered 1101 applied events in every trial, including
the post-failover command. Queue/collection wait is included in command latency;
payload preparation and setup/recovery are outside the write interval.

The environment/configuration, raw samples, reports and retained databases are
in ignored `benchmark-results/batching-20261001/`, with `comparison.json` holding
the summary. The host uses AMD Ryzen 9 7900, approximately 32 GB RAM and btrfs
on `/dev/nvme0n1p2`; Rust 1.98.1 via mise. Runs used an uncommitted workspace
based on `0665b64`. New baselines avoid comparing different harness versions
or filesystems with the earlier 170/550 results.

## Semantics and limits

This batches **commands into one Raft entry**, rather than changing OpenRaft's
serialized append/callback scheduling or introducing asynchronous WAL sync.
The writer retains enqueue order, and state-machine application keeps one
dedup/rejection outcome per command. A rejection does not undo successful
siblings. Checkpoint mode persists once for the apply batch, retaining the
same acknowledgment guarantee.

Commands share a Raft index; their identities remain distinct command/event
IDs. Snapshot thresholds count entries, so their coverage in commands changes
with batching. Sequential callers will usually produce single-command entries
and may incur collection delay. The measured concurrency of eight supplies
the eight-command batches; larger configured limits alone do not create more
concurrency. These results do not measure multiple tenant groups, reads, remote
machines, large histories or sustained compaction/stall behavior.

Batching defaults to disabled. Upgrade every replica before enabling it;
older binaries cannot decode the batch entry variant. See
[configuration and writer usage](../raft-configuration.md#opt-in-command-batching).

For larger batch limits at fixed concurrency, see the
[configurable 8/16/32/64/128 matrix](batch-matrix.md).
