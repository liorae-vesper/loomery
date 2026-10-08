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

- **Pipelining has landed, so the curve below is now the *before* picture.**
  The migration to openraft **0.10.0-alpha.36**
  ([openraft-010-migration.md](../research/openraft-010-migration.md)) and the
  bidirectional `StreamAppend` RPC are both in: the leader streams AppendEntries
  requests to a follower and the follower streams results back on one ordered
  HTTP/2 stream, without waiting for a per-entry response. **Everything in this
  file was measured before that**, on the 0.9 sequential path, so its numbers are
  the baseline the pipelined path has to beat rather than current behaviour. Note
  also that openraft 0.10 no longer serializes local appends behind the previous
  flush, which changes the per-entry cost even where batching already amortized
  the round trip.
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

## After: openraft 0.10 and pipelined append

Everything above was measured on **0.9.25**, before the migration. This section is the
same harness, same machine, same configs, re-run on **0.10.0-alpha.36 with pipelined
append** — branch `feature/openraft-pipelined-append`, commit `ee0bb08` onward (the code
under test was committed before the runs; each run's `environment.json` records the
commit and whether the tree was dirty).

Three trials per point, release, medians. Raw runs are collected in
[`results/deployment-path.json`](results/deployment-path.json); the table names the run
directories so each number is traceable.

| Config | Point | 0.9.25 | 0.10 + pipelining | × | p50 before → after |
|---|---|---|---|---|---|
| 3 nodes, c=8, unbatched | checkpoint 2k | 365 | 983 | 2.7× | 19.6 → 8.0 ms |
| | checkpoint 5k | 379 | 926 | 2.4× | 19.5 → 8.3 ms |
| | checkpoint 10k | 363 | 964 | 2.7× | 19.6 → 8.0 ms |
| | checkpoint 20k | 367 | 924 | 2.5× | 19.6 → 8.1 ms |
| | snapshot 20k | 384 | 959 | 2.5× | 19.5 → 7.7 ms |
| 3 nodes, c=128, batch 1 | checkpoint 2k | 823 | 12,768 | **15.5×** | 155 → 9.7 ms |
| | checkpoint 20k | 777 | 9,134 | **11.8×** | 157 → 11.2 ms |
| 3 nodes, c=128, batch 8 | checkpoint 2k | 1,129 | 1,108 | 0.98× | 113 → 110 ms |
| | checkpoint 20k | 1,067 | 1,139 | 1.07× | 114 → 112 ms |
| 3 nodes, c=128, batch 128 | checkpoint 2k | 14,226 | 14,307 | 1.01× | 8.5 → 8.6 ms |
| | checkpoint 20k | 13,781 | 13,160 | 0.95× | 9.2 → 9.3 ms |
| **1 node**, c=128, batch 1 | checkpoint 20k | 294 | 8,634 | **29.3×** | 420 → 11.7 ms |

Before: `20261008013400-deployment-scale`, `…022806-matrix-batch1`,
`…023612-matrix-batch8`, `…023836-matrix-batch128`, `…025912-split-single-node-unbatched`.
After: `20261008153608-after-deployment-scale`, `…153935-after-matrix-batch1`,
`…153950-after-matrix-batch8`, `…154057-after-matrix-batch128`,
`…154109-after-single-node-unbatched`. Zero failed trials in all 39.

### The win is the migration, not the pipelining

The branch exists for pipelining, so the obvious reading of that table is wrong. Disabling
**only** the bidirectional `stream_append` override — one `#[cfg(any())]`, leaving openraft's
`stream_append_sequential` to send one request and wait for each response — gives:

| Config | Point | 0.9.25 | 0.10, sequential | 0.10, pipelined |
|---|---|---|---|---|
| 3 nodes, c=8, batch 1 | checkpoint 5k | 379 | **939** | 926 |
| 3 nodes, c=128, batch 1 | checkpoint 2k | 823 | **12,463** | 12,768 |
| 3 nodes, c=128, batch 1 | checkpoint 20k | 777 | **9,380** | 9,134 |
| 1 node, c=128, batch 1 | checkpoint 20k | 294 | **9,689** | 8,634 |

Sequential runs: `20261008152851-bisect-sequential`, `…154144-seq-arm-matrix-batch1`,
`…154159-seq-arm-single-node` (all measured with the override disabled in the working tree,
which those runs' `environment.json` record as a dirty tree).

So **pipelining is inside the noise band in every configuration measured**, and the 2.4–29×
gains come from openraft 0.10 itself: its core no longer waits for the previous append's flush
before issuing the next one, which was the serialization this benchmark was actually hitting.
The single-node anomaly from 0.9 disappears in *both* 0.10 arms, which is more evidence that it
lived in the 0.9 core's unbatched entry path rather than in the network round trip.

How wide is "noise"? The same config was re-run as a check: batch 1 c=128 20k gave 9,134,
9,401, and (sequential) 9,380; single node 20k gave 8,634 and 9,912 where sequential gave
9,689. Three-node comparisons are tight (~3%); the single-node, high-concurrency points spread
~15%, which is wider than the pipelined-versus-sequential gap in either direction.

Two caveats on that conclusion. It is **loopback**: pipelining's theoretical advantage is
removing a round trip per exchange, and here every round trip is microseconds, while openraft's
`LogsSince` payload accumulation already packs many entries into each unary request. On a link
where RTT dominates, the sequential arm would pay that RTT per request and the pipelined arm
would not — this benchmark cannot show that, and does not. And the batched rows are unmoved
from 0.9 in both arms, which is consistent with everything above: once batching amortizes the
exchange, neither the flush-wait nor the round trip is what limits the path.

### The sweep that wedged, and why

The first attempt at this sweep failed rather than producing numbers: a follower stopped at log
index 5017 while the cluster committed 5105, and never recovered — the trial hit the 900 s
barrier deadline, twice. The cause was in the pipelined transport, and it is worth recording
because the failure mode is silent and permanent:

- `stream_append` used `soft_ttl` — three quarters of `hard_ttl`, which on the replication path
  is `heartbeat_interval`, so **75 ms** in this config — as a per-response read timeout.
- A 75 ms gap is ordinary here (a durable fsync with 128 writes in flight), so the stream was
  torn down mid-burst with requests outstanding.
- Openraft's progress bookkeeping then holds a `matching` index ahead of what the follower
  actually has, and the conflict response that would repair it is *discarded*: a conflict at or
  above `searching_end` "carries no new information", and one carrying a stale inflight id
  leaves the progress entry untouched. Openraft's own docs note that in this state "log
  replication cannot make progress". So the follower is not retried into catching up; it is
  left behind until something else restarts replication.

The fix is `transport.stream_stall_timeout_ms` (default 10 s): a stall detector rather than a
latency budget, plus opening the stream under the configured `request_timeout_ms` (a failure
there returns from `stream_append` itself, which openraft retries safely) and having the
follower stop feeding its `Raft` the moment the caller drops the results stream. The numbers
above are from the fixed tree; `20261008152723-repro-5000-mine` is the failing run, kept in the
results file.

**What this does not say.** These are medians of three trials on one machine, and the
attribution arms are single runs per config rather than an interleaved, order-randomized
experiment — enough to rule *out* a large pipelining win, not enough to resolve a 3% one. The
recommendation that follows is a [revert-or-keep decision](../research/openraft-010-migration.md#pipelined-append-leg-5)
for the append half of leg 5, not a performance claim.

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

## Where the per-entry cost actually is

The matrix raised a question it could not answer: the batched path's entry rate looked
flat at ~130/s, which would mean a fixed cost per entry. Feeding the medians through a
per-command view says something else — and two of my own readings turned out wrong:

| Config (20,000 events) | Observed batch | Commands/s | Entries/s | ms per command |
|---|---|---|---|---|
| unbatched, concurrency 128 | 1.0 | 777 | **777** | 1.29 |
| batch 8 | 8.0 | 1,067 | 133 | 0.94 |
| batch 32 | 32.0 | 4,262 | 133 | 0.23 |
| batch 128 | 127.4 | 13,781 | 108 | **0.073** |
| batch 256, byte cap lifted | 253.2 | **20,870** | 82 | **0.048** |

- **The entry rate does not stay flat — it falls** (777 → 133 → 133 → 108 → 82/s) while
  commands per second rises 27-fold. The per-entry cost is linear in the commands an
  entry carries, so the unbatched path is paying a fixed round trip per *command*
  (1.29 ms), and batching amortizes it away. At the top of the ladder the path spends
  its time on per-command work — about 0.048 ms per command — not on round trips.
- **Correction: the batch-size matrix was capped by bytes, not by a ceiling.** Every
  row used `max_batch_bytes: 262144`, which at ~2 KB per command stops a batch at
  ~127 commands — so "batch 256" and "batch 512" silently measured batch 128, and the
  apparent plateau at ~13,000 was that cap. With the cap lifted to 8 MB, a batch of
  256 reaches **20,870 writes/s**. An earlier reading of mine ("the apply ceiling is
  ~13,000/s") was wrong for the same reason.
- **Correction: the small-batch penalty is batches not filling.** At batch 8 with
  concurrency 8: `max_delay_ms: 0` → observed batch 4.0 → 537 writes/s; 1 ms → batch
  8.0 → 1,067; 5 ms → batch 8.0 → 1,076. The collection delay is what lets a batch
  form; there is no hidden per-batch overhead beyond that.
- **A batch of 1,024 timed out** (recorded as a failure, three trials, not retried
  into looking better), so the ladder measured here ends at 256.
- **One thing this did not explain: a single node is *slower*.** With `nodes: 1` and
  concurrency 128, the unbatched path managed 295 writes/s at a p50 of 420 ms against
  777 writes/s on three nodes — and lowering `heartbeat_interval` from 100 ms to 10 ms
  did not move it (301 writes/s, p50 418 ms), so it is not heartbeat-paced commits.
  That is unexplained and flagged rather than filed under "expected".

**What it means for a consensus upgrade.** The lever a newer Raft API offers is
pipelined replication, which attacks the *per-entry round trip* — and that is exactly
the cost batching has already amortized: at 256 commands per entry the path is
spending ~0.048 ms per command on work, not waiting on replication. So pipelining has
little left to win here, and the next lever is the per-command work itself (apply and
the durable write), not the network.

## The single node that was slower than three

The matrix left an oddity: a single node at concurrency 128 unbatched managed 295
writes/s at a p50 of 420 ms, against 777 writes/s and 157 ms on three nodes. Two
hypotheses died on the evidence: the leader never left **term 1** in any of those runs
(so it was not re-electing), and lowering `heartbeat_interval` from 100 ms to 10 ms
changed nothing (301 writes/s, p50 418 ms).

Batching settles it — the stall is per *entry*, not per command:

| Config (20,000 events, single node) | Writes/s | p50 | Observed batch |
|---|---|---|---|
| unbatched, concurrency 128 | 295 | 420.1 ms | 1.0 |
| batching 128, concurrency 128 | **23,450** | **5.2 ms** | 127.4 |

With batching, one node is not merely as fast as three — it is *faster* (23,450 against
20,870 at batch 256 and 13,781 at batch 128), and its p50 is the best measured anywhere
(5.2 ms against 9.2 ms on three nodes), which is what having no replicas to wait for
should look like.

So the single-voter penalty lives in the *unbatched* entry path: roughly 3.4 ms per
entry against 1.29 ms with two followers, which is backwards and stays unexplained —
it is not elections, not heartbeats, and it disappears once commands share an entry.
What it changes is the practical advice: **on a single node, leave batching on.**

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
