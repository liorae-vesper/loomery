# Deployment path at scale — three nodes, 2k → 20k events

What the *deployment* path costs, as opposed to the in-process path the
[persistence soak](persistence-hardening.md) measures. The
[controlled consensus harness](README.md) runs each replica as a separate process
with a real loopback TCP listener, the production tonic transport, RocksDB and the
production apply path; this note is that harness driven at four growing sizes.

Everything here is measured, and every number is traceable to a named file under
one output directory — nothing is smoothed, retried away or shrunk. Note that
`benchmark-results/` is gitignored, so that raw output lives on the machine that ran
the sweep: the paths below say exactly what to look for, and `mise run
bench-deployment-scale` regenerates the whole thing.

## How it was run

```sh
mise run bench-deployment-scale            # the sweep below
python3 scripts/bench-deployment-scale.py --dry-run     # configs and plan, nothing run
python3 scripts/bench-deployment-scale.py --points 2000 --trials 1 --tag smoke   # one point
python3 scripts/bench-deployment-scale.py --extra '{"concurrency": 64}'         # one knob probed
```

Held fixed for the whole curve: **3 nodes**, 3 trials per point, 100 warmup
commands, **concurrency 8**, 200-byte payload names, `LogsSinceLast(5000)`
snapshots, **batching off**, **failover off** (it belongs to
`mise run test-consensus-failures`, and in a scale curve it would add an election's
variance to every point). Only the operation count — and the persistence mode for
the last point — differ between configs; `configs/*.json` in the output directory
are the exact configs used.

Machine: AMD Ryzen 9 7900 (12 cores, 24 CPUs available), 32 GB RAM, btrfs on NVMe,
`rustc` 1.98.1, release profile. **It is a development machine, not an idle
benchmark host** — the sweep ran alongside other work, so these numbers are
conservative rather than best-case.

## The curve

Medians across three trials, checkpoint mode unless noted, from
`benchmark-results/20261008013400-deployment-scale/summary.json` (each median
computed from the `trial-00N/report.json` files beside it):

| Mode | Events | Writes/s | p50 | p95 | p99 | Verified | Restart | Init | On disk | Wall |
|---|---|---|---|---|---|---|---|---|---|---|
| checkpoint | 2,000 | 365 | 19.6 ms | 25.7 ms | 32.9 ms | 2,100 ×3 | 197 ms | 27 ms | 34.1 MB | 18 s |
| checkpoint | 5,000 | 379 | 19.5 ms | 24.8 ms | 39.5 ms | 5,100 ×3 | 598 ms | 28 ms | 104.9 MB | 43 s |
| checkpoint | 10,000 | 363 | 19.6 ms | 25.6 ms | 65.2 ms | 10,100 ×3 | 1.19 s | 27 ms | 196.1 MB | 89 s |
| checkpoint | 20,000 | 367 | 19.6 ms | 25.4 ms | 59.2 ms | 20,100 ×3 | 2.49 s | 27 ms | 559.7 MB | 172 s |
| snapshot | 20,000 | 384 | 19.5 ms | 24.2 ms | 58.1 ms | 20,100 ×3 | 2.23 s | 27 ms | 547.2 MB | 166 s |

"Verified" is the harness's own check: **every replica** confirms the applied event
count it was told to expect (operations + warmup), before measurement and again
after the restart — so these are not numbers from a leader that could not replicate.
Zero failed trials across all five points, and zero failed commands inside them
(`successes: 20000, failures: 0, distinct_log_indices: 20000` at the largest point).

**Throughput and the common percentiles are flat.** Writes/s, p50 and p95 are the
same at 2,000 events as at 20,000 (p50 within 1 %, p95 within 6 %); only p99 drifts
upward (33 → 59 ms), which is what a tail does as more work shares the same window.
Two things *do* grow, both linearly and both expected: the on-disk footprint
(~28 KB per event across three replicas — Raft log plus snapshots, 560 MB at 20,000
events) and the restart (197 ms at 2,000 to 2.49 s at 20,000, which is replaying a
longer log and installing a snapshot on three replicas). Neither is a degradation of
the write path: the write path's per-command cost does not change with scale, so
what limits it is a per-write cost, not a scale effect.

## What limits it: latency, not throughput

The numbers are consistent with throughput = concurrency ÷ latency: 8 ÷ 19.5 ms ≈
410 writes/s, against 365–384 measured. The harness keeps eight writes in flight and
each is awaited — so the path is **latency-bound**, and the way to move it is more
concurrency or fewer round trips, not more machines.

Four probes at 5,000 events, one trial each, changing one knob at a time (output
directories `benchmark-results/*-probe-*`):

| Probe | Writes/s | p50 | vs baseline |
|---|---|---|---|
| baseline: concurrency 8, unbatched | 379 | 19.5 ms | — |
| `group.proposals.max_batch_commands = 8` | 569 | 13.8 ms | 1.50× |
| concurrency 64 | 734 | 86.4 ms | 1.94× |
| both | **1,119** | 57.0 ms | 2.95× |

Both levers already exist and are opt-in: command batching groups concurrent
commands into one Raft entry (fewer entries ⇒ fewer round trips and fewer applies),
and concurrency keeps more writes in flight. Neither is on in the curve above,
deliberately: the curve is about *scale*, and batching has its own
[controlled matrix](batch-matrix.md).

### On the transport: `RaftNetworkV2`, pipelining, awaiting

- **`RaftNetworkV2` is not available to us.** openraft **0.9.25** — the version this
  workspace pins — has no such trait (verified: the symbol does not exist in the
  crate; it arrives in 0.10). Our `TonicNetwork` implements the legacy
  `RaftNetwork` over tonic, one unary RPC per call.
- **We do await each write.** `Raft::client_write` is awaited per command, and the
  apply awaits its durable write (D2's contract). What keeps the path busy is the
  harness's in-flight window, which is exactly what the concurrency probe moves.
- **Batching is implemented and off by default.** `group.proposals.max_batch_commands`
  (1 disables it) groups commands per entry; at 8 it is worth 1.5× here. The
  batching matrix is where its semantics and its own measurements live.
- What is *not* established: **where the 19.5 ms goes.** It is not the storage (the
  in-process soak is ~13,000 writes/s, roughly 34× this) and not the payload
  (constant across sizes). Splitting it between the tonic round trip, openraft's
  replication bookkeeping and the two synchronous writes per command is the next
  measurement, and it is open.

## A finding: the harness could not run at all

The first attempt failed with **100 % rejected commands**:

```
the state machine rejected the command: the workspace name must be non-empty
and within the length bound (InvalidName)
```

The harness generated 256-byte workspace names, while the domain bounds them at
`workspace::MAX_NAME_BYTES = 200` — and the harness validated its own knob only
against a 1 MiB ceiling, so nothing caught the drift. Every command in every phase
was rejected, which means `mise run bench-consensus`, and the persistence and
failure-injection scripts that inherit `docs/benchmarks/consensus.json`, were broken
by a domain rule that landed after they were recorded.

Fixed here: the harness defaults to the domain's bound and **refuses** a config that
asks for a name the state machine would reject, and the shared config uses 200
bytes. **Recorded baselines measured with the 256-byte payload are therefore not
directly comparable** to anything measured after this change — the payload size is
part of the workload.

## What this does and does not say

- It says what this path costs at these sizes **on this machine**: flat ~365–384
  writes/s, latency-bound, replicas verified, ~28 KB per event on disk.
- It does **not** say what a real deployment does: three processes on **loopback**
  on one host, no real network, no shared-disk fsync pressure from other tenants, no
  failover phase, no batching, and a machine that was not idle.
- It says the in-process path (~13,000 writes/s) and this path (~380) differ by
  ~34×: the consensus round trip and replication dominate, and the storage layer —
  the part the [persistence work](persistence-hardening.md) made linear — is not
  what limits a deployment.

## See also

- [Controlled consensus benchmark](README.md) — the harness, its phases and its own
  recorded runs
- [Persistence hardening](persistence-hardening.md) — the in-process path, and the
  quadratic cost the storage layout removed
- [Command batching](batching.md) and [batch-size matrix](batch-matrix.md) — the
  batching lever measured properly
- [Multi-group probe](multigroup.md) — co-resident groups on one node
