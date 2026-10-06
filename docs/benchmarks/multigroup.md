# Multi-group mixed read/write probe

A reproducible in-process probe for **co-resident groups**: one process hosts
several independent Raft groups (one per tenant) and drives mixed reads and
writes against all of them at once.

```sh
GROUP_COUNT=4 WRITERS=2 READERS=2 DURATION_SECS=3 \
  cargo run --release --example multigroup_bench
```

Source: [`crates/shell/examples/multigroup_bench.rs`](../../crates/shell/examples/multigroup_bench.rs).

## What it measures

| Knob | Meaning |
|---|---|
| `GROUP_COUNT` | independent single-node, in-memory groups in one process |
| `WRITERS` | concurrent proposers per group |
| `READERS` | concurrent readers per group |
| `DURATION_SECS` | wall-clock duration of the run |

Writers propose distinct `task.create` commands and count acknowledgements;
readers call `committed_events(organization)` (the shell's current read path,
which returns the group's applied-event log) and count operations. The probe
reports writes and reads per second across all groups and per group.

## Measured (one host, one release build, 3-second runs)

| Config | Writes/s | Reads/s | Per-group writes/s |
|---|---|---|---|
| 4 groups, 2 writers, 2 readers | 4059.5 | 8407.9 | 1014.9 |
| 4 groups, 2 writers, 2 readers | 4013.5 | 8088.9 | 1003.4 |
| 4 groups, 2 writers, 2 readers | 3994.4 | 8074.7 | 998.6 |
| 8 groups, 2 writers, 2 readers | 6631.6 | 13828.9 | 828.9 |
| 2 groups, 4 writers, 0 readers | 6696.6 | — | 3348.3 |

Raw output for the last row of each config is reproduced by the command above.

## What the numbers say (and do not say)

- **Total throughput plateaus, per-group throughput falls.** 8 groups × 2
  writers (16 proposers) and 2 groups × 4 writers (8 proposers) both land near
  6.6–6.7 k writes/s, so the ceiling is the **process** (one Tokio runtime, one
  in-memory log per group, shared allocator), not the group count. Adding groups
  spreads the same budget: per-group writes fall from ~1015/s (4 groups) to
  ~829/s (8 groups).
- **Reads are cheaper than writes but scale with history.** Each read clones
  the group's whole applied-event log, so read cost grows with the number of
  events written — the probe's read rate is a *log-copy* rate, not a
  projection-read rate. A real read model (projections) removes that cost.
- **These are not deployment capacity numbers.** The probe is in-memory,
  single-host and single-process, with no disk, no network replication and no
  TLS. It measures the shell's per-group overhead and how co-residency behaves;
  disk and network numbers come from
  [`checkpoint-spike.md`](checkpoint-spike.md) and
  [`batching.md`](batching.md).

## Reproducing

1. `cargo build --release --example multigroup_bench` (one-time; ~minutes with
   RocksDB in the dependency graph).
2. Run the command above, varying the knobs.
3. Compare medians of at least three 3-second runs; single runs vary by a few
   percent (`4059.5` → `3994.4` across three identical runs).
