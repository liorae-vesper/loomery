# Deployment path at scale — three nodes, 2k → 20k events

What the *deployment* path costs, as opposed to the in-process path the
[persistence soak](persistence-hardening.md) measures. The
[controlled consensus harness](README.md) runs each replica as a separate process
with a real loopback TCP listener, the production tonic transport, RocksDB and the
production apply path; this note is that harness driven at four growing sizes.

Everything here is measured, and every number is traceable to a named file under
one output directory — nothing is smoothed, retried away or shrunk. Note that
`benchmark-results/` is gitignored, so the raw output lives on the machine that ran
the sweep: the paths below say exactly what to look for, and `mise run
bench-deployment-scale` regenerates the whole thing. The **medians every table here
quotes are committed** in
[results/deployment-path.json](results/deployment-path.json), which
`python3 scripts/bench-collect-results.py` regenerates from the runs — with each run's
config overrides, trial counts, verification counts and the machine it ran on, so a
number can be traced without the raw databases.

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

Four probes at 5,000 events, changing one knob at a time:

| Probe | Trials | Writes/s | p50 | vs baseline |
|---|---|---|---|---|
| baseline: concurrency 8, unbatched | 3 | 379 | 19.5 ms | — |
| unbatched, concurrency 64 | 1 | 756 | 84.7 ms | 1.99× |
| batching 8, `max_delay_ms: 1`, concurrency 8 | 1 | 1,110 | 7.0 ms | 2.93× |
| batching 8, `max_delay_ms: 1`, concurrency 64 | 1 | **1,129** | 56.8 ms | 2.98× |

One correction worth recording: an earlier version of this table reported 569 writes/s
for batching 8 at concurrency 8. That run used `max_delay_ms: 0`, which collects only
commands already queued — at 8 in flight a batch of 8 rarely fills, so it measured a
batch closer to 4 while claiming 8. The collection delay is load-bearing: the same
batch with `max_delay_ms: 1` is worth 2.93× rather than 1.50×. The matrix below holds
it at 1 for every row.

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

## The cheap path, measured: batching and concurrency

Before reaching for a newer consensus library, the two levers already in the tree
were measured *at scale*. Command batching groups concurrent commands into one Raft
entry (`group.proposals`), and concurrency decides how many writes are in flight.
Three trials per point, same harness, same machine as the curve above — only the
knobs differ:

| Point | 2,000 | 5,000 | 10,000 | 20,000 | p50 at 20,000 | On disk at 20,000 |
|---|---|---|---|---|---|---|
| baseline (concurrency 8, unbatched) | 365 | 379 | 363 | 367 | 19.6 ms | 559.7 MB |
| batching 8, concurrency 64 | 1,120 | 1,093 | 1,111 | 1,079 | 57.1 ms | 286.6 MB |
| batching 8, concurrency 8 | — | — | — | 1,080 | 7.1 ms (p99 8.8 ms) | 286.5 MB |
| **batching 32, concurrency 64** | **4,518** | — | — | **4,335** | 14.7 ms (p99 18.5 ms) | 293.6 MB |

The middle sizes were measured for the baseline and the first batched config and told
the same story (flat), so the ladder above reports the endpoints — `mise run
bench-deployment-scale` now sweeps 2,000 and 20,000 by default, with `--points` for
anything else.

**Throughput scales with the batch, not with concurrency.** Going from 8 commands per
entry to 32 — same concurrency — took 20,000 events from 1,079 to **4,335 writes/s**,
a 11.8× improvement on the unbatched baseline and 4× the batch-of-8 config, still
flat between the endpoints. The harness's own counters confirm the batches really
filled: **32.0 commands per entry** (20,000 commands in 625 entries), three replicas
verified at 20,100 events, zero failures. Little's law still holds (64 ÷ 14.7 ms ≈
4,354/s against 4,335 measured).

That says the unit cost is the **entry**, not the command: four times the commands per
entry bought four times the throughput. What that per-entry cost *is* — the tonic
round trip, openraft's replication bookkeeping, or the durable writes an entry
triggers — is not established here, and it is the measurement that would decide
whether pipelined replication (which attacks exactly that) is worth its cost.

What this says:

- **Batching is worth ~2.9×**, and it holds at every size: 365–379 writes/s becomes
  1,079–1,120, flat from 2,000 to 20,000 events.
- **Concurrency adds nothing once batching is on.** Batching 8 with 8 in flight and
  with 64 in flight both land at ~1,080 writes/s — but at 8 in flight the p50 is
  **7.1 ms** instead of 57 ms. More concurrency here buys queueing, not throughput.
- **Latency and throughput trade exactly as queueing predicts.** Every point matches
  Little's law — 8 ÷ 19.5 ms ≈ 410/s at baseline, 8 ÷ 7.1 ms ≈ 1,127/s batched,
  64 ÷ 57.1 ms ≈ 1,121/s — so the percentiles are queueing delay, not service time:
  a command waits behind the batch it belongs to. Read them as such.
- **Batching also halves the log.** 286.6 MB at 20,000 events against 559.7 MB
  unbatched, because eight commands share one entry (log entries, snapshots and
  their replay all shrink with it).
- One caveat inside the numbers: the 5,000-event batched point showed a p99 of
  229 ms against a 56 ms p50 — a tail worth watching, and the reason the note reports
  p99 rather than hiding it behind a mean.

So the bar a consensus upgrade has to clear is **~1,080 writes/s** as measured here,
and the recorded [batch-size matrix](batch-matrix.md) already reaches ~1,600 at a
batch limit of 128 — all with code that exists today, before any pipelining. That is
the comparison this note hands to anyone deciding whether a newer Raft API is worth
its cost.

## Batch-size matrix (endpoints, concurrency held at 128)

A point of "20,000" is 20,000 *measured commands*, each producing exactly one event,
plus 100 warmup commands — verified as 20,100 events on every replica. Batching
changes how many **entries** those commands occupy, never how many events they
produce.

Eight batches, three trials per size, checkpoint mode, concurrency fixed at 128 so
that every batch up to 128 can fill, endpoints only. "Observed" is the harness's own
`commands_per_log_index`: the batch size that actually happened.

| Batch | Observed | Commands/s @2k | @20k | p50 @20k | p99 @20k | Entries @20k | Entries/s @20k | On disk |
|---|---|---|---|---|---|---|---|---|
| 1 (unbatched) | 1.00 | 823 | 777 | 156.7 ms | 355.6 ms | 20,000 | **777** | 471.1 MB |
| 2 | 2.00 | 278 | 270 | 455.7 ms | 1004.7 ms | 10,000 | 135 | 451.0 MB |
| 4 | 4.00 | 559 | 528 | 230.6 ms | 545.2 ms | 5,000 | 132 | 456.2 MB |
| 8 | 8.00 | 1,129 | 1,067 | 114.4 ms | 263.4 ms | 2,500 | 133 | 286.7 MB |
| 16 | 16.00 | 2,197 | 2,043 | 59.1 ms | 78.0 ms | 1,250 | 128 | 291.6 MB |
| 32 | 32.00 | 4,520 | 4,262 | 29.9 ms | 44.3 ms | 625 | 133 | 293.6 MB |
| 64 | 63.90 | 8,372 | 8,045 | 15.4 ms | 26.2 ms | 313 | 126 | 292.5 MB |
| **128** | 127.39 | **14,226** | **13,781** | 9.2 ms | 11.0 ms | 157 | 108 | 291.9 MB |

Every point verified 20,100 events on all three replicas with zero failures.

What the matrix says:

- **Commands per second is proportional to the batch**: doubling the batch doubles
  the throughput, because the *entry* rate is flat at ~110–135 entries/s across every
  batched row. Throughput here is bought with entry size, not with parallelism.
- **The unbatched row is the odd one, and it matters**: 777 entries/s, because
  `max_batch_commands = 1` bypasses the collection queue and lets the harness's
  concurrency drive entries directly. So batches of 2 and 4 are *worse than no
  batching* — the batched path's entry ceiling sits below what the unbatched path
  already does per command — and batching only pays from about 8 commands per entry.
- **p50 falls as the batch grows** (156 ms → 9.2 ms) because the same 128 in-flight
  writes are spread over fewer, larger units: queueing again, and Little's law holds
  (128 ÷ 9.2 ms ≈ 13,900/s against 13,781 measured).
- **The log consolidates**: ~450–470 MB at batches 1–4 against ~290 MB from 8 upward.
- **Observed batch sizes match the configured ones**, the last two marginally under
  (63.9 and 127.4) because the tail of a run cannot fill a batch.

**Where that leaves the consensus question.** The bar is now 13,781 writes/s at batch
128 — and more usefully, the batched path's flat ~130 entries/s is *below* the 777
entries/s the unbatched path manages, so what we are hitting is something the batched
path adds per entry, not Raft's ability to accept entries. Whether the ~7.7 ms per
batch is the tonic round trip, replication acknowledgement, the durable writes, or our
writer awaiting one batch before collecting the next is **not established** — and that
split is what decides whether pipelined replication would help, or whether letting the
writer keep more than one batch in flight would help more.

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
