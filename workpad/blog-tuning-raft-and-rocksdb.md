# 170 events per second: tuning Raft and RocksDB

We run one Raft group per tenant: [openraft](https://github.com/databendlabs/openraft) for
consensus, tonic for the wire, RocksDB for the log and the state. A benchmark harness drives
the real path — three replica processes, real sockets, real databases, no mocks — and measures
commands per second and per-command latency through the whole stack.

On 2026-09-30 that path measured **~170 writes/s** at the default configuration: three
replicas, concurrency 8, one command per entry, checkpoint recovery. (174.3 in the
wider-threshold arm, 170.3 in the tighter one; snapshot-backed recovery measured ~550.)
The same configuration today measures **1,225 writes/s** — 7× that.

Both numbers describe the same shape of configuration, but not the same trial: the early
ones come from a 1,000-write harness and the later ones from a 20,000-event one. Read the
path between them as a sequence of decisions, and compare any two figures only where they
come from the same run — which, for every before-and-after in this post, they do.

This is the path between those two numbers, in the order we walked it — including the parts
where we were wrong, and the two beliefs the measurements refused.

## 1. The first investigation, and what it got right

The suspects, on 2026-10-01, were a short list:

- **Serialized durable log writes.** OpenRaft's `append_to_log` awaited the store's append
  *and then the flush callback*, so its core waited for durability even when the store
  returned early.
- **Poor leader-side batching.** Followers received batches; the leader did not form them.
- **Checkpoint mode's full-history work.** Every apply serialized the entire applied state,
  the event history and the dedup window.

A storage-only probe — same RocksDB limits, 800-byte payloads, `sync=true`, one run per arm —
put the first one in proportion:

| Arm | Records/s |
|---|---:|
| One blocking closure, two sequential sync writes per iteration | 517 |
| One `spawn_blocking` per iteration, two sync writes | 484 |
| One `spawn_blocking` per iteration, one sync write | 964 |
| **Eight records per single synchronized batch** | **7,235** |

Synchronization cost dwarfed queue delay, and grouping eight records into one synchronized
batch was worth 15× against the arm that paid two syncs per record. The second suspect was confirmed by a trace of the real append path: on a three-voter
run the **bootstrap leader appended 105 batches of one entry**, while the followers received
15 single-entry batches and 12 of seven. The leader was not batching the commands queued
behind it, so RPC batch limits could not have helped: the batch had to be formed *above* Raft.

That is the origin of the batching knob, and the first thing we learned about it is in
section 6: the number you configure is a ceiling, not a promise.

## 2. The soak made the picture worse

Then we stopped measuring the write path and measured a *history*. Applying `task.create`
commands to one single-node group, 200 → 10,000 commands, both persistence modes
([persistence hardening](../docs/benchmarks/persistence-hardening.md)):

| Mode | 200 | 1,000 | 2,000 | 10,000 |
|---|---:|---:|---:|---:|
| checkpoint | ~162/s | ~34/s | **~17/s** | — |
| snapshot | ~3,434/s | ~2,134/s | ~1,319/s | ~553/s |

Checkpoint mode halved as the history doubled: quadratic, and by construction. Each apply
serialized the whole state, so an apply cost the size of everything the group knew rather than
the size of what it did. At 2,000 commands snapshot mode was ~78× faster end to end.

Two other things were wrong with those numbers, and finding out why took another pass.

## 3. Column families: the record on its own account

The layout change ([storage layout](../docs/storage-layout.md)) split one database per group
into five column families — `default` (markers), `RaftLog` (entries, vote, committed, purge
floor), `State` (the fold, membership, dedup, snapshot), `Events` (every applied event, in
order) and `Projections` — with one `WriteBatch` spanning families atomically, and every
opener listing every family, because RocksDB refuses to open a database with an unlisted one.

The point of it was not throughput. It was **durability of the record**: with events in a
family of their own, purging the Raft log can no longer lose history, because the record no
longer depends on the log that carried it.

So the apply rate was unchanged, by design:

| Mode | Apply before | Apply after | Restart before | Restart after | On disk before | On disk after |
|---|---|---|---|---|---|---|
| checkpoint | 28.9 s (~34/s) | 29.5 s (~33/s) | 230 ms | 2.50 s | 38.0 MB | 981 MB |
| snapshot | 468 ms (~2,134/s) | 590 ms (~1,694/s) | 256 ms | 287 ms | 1.63 MB | 2.15 MB |

And here is a thing worth recording rather than smoothing over: **checkpoint's on-disk size
went from 38 MB to 981 MB and its restart from 230 ms to 2.50 s.** That is 25× in file size
from a step that added one column family and no state write. We wrote it down as unexplained,
and said so in the note, because an unexplained 25× is exactly what the next step must not
inherit. (It turned out to be the whole-state rewrite's churn. Section 5.)

## 4. The culprits were not where we were looking

The soak's absolute numbers were wrong for a reason that had nothing to do with storage:

| Build | Checkpoint, 2,000 commands |
|---|---|
| `cargo test` (debug) | ~2,760/s |
| `cargo test --release` | **~13,134/s** |

A durable write path spends its time in code the optimiser matters for, and we had been
measuring the compiler. Every performance probe since runs `--release`.

The *slope* was wrong for a second reason, and it was not the storage engine either. The state
machine mirrored the dedup window after every command by asking the registry for *all* of its
entries: once the window filled, **4,096 tuples per command**, with a window that grew with
the history while it filled. That is the same fold code in both persistence modes — which is
exactly why the slope had been identical in both: the problem was never the storage engine.
Checkpoint mode's extra penalty was its own, the whole-state write; the fold was charging
both, and the diagnosis "checkpoint mode is quadratic" was half right.

`Registry::len()` and `Registry::oldest()` answer the two questions the mirror actually
needed, and the mirror is gone. With both fixed, the curve is flat — per-command cost no
longer depends on the history at all:

| Commands | Checkpoint | Snapshot | Restart | On disk |
|---|---:|---:|---:|---:|
| 2,000 | ~13,134/s | ~15,258/s | 35 ms | 5.0 MB |
| 8,000 | ~11,381/s | — | 128 ms | 29.7 MB |

## 5. Writing only what changed

The quadratic checkpoint was still there. The fix was to make an apply persist **only what it
changed** — the aggregate states its events touch, the dedup entries the window added or
evicted, the applied index — in the same synchronous batch as its events, and to make recovery
read state back per aggregate instead of from a whole-state record. Same soak, same machine,
2,000 commands:

| Mode | Apply before | Apply after | |
|---|---:|---:|---|
| checkpoint | 113.2 s (**~17/s**) | 1.68 s (**~1,190/s**) | **67×** |
| snapshot | 1.34 s (~1,497/s) | 1.59 s (~1,255/s) | — |

It also accounted for section 3's unexplained figures: the 981 MB and the 2.50 s restart were
the whole-state rewrite's churn, and the rewrite was gone. The curve afterwards, both modes
now comparable: ~1,730/s at 1,000 commands, ~1,190/s at 2,000, ~824/s at 4,000.

**An apply should cost what it did, not what it knows.** That is the same sentence as the
dedup-window bug and the same sentence as the fsyncs at the end.

## 6. The knobs, and the one that is not a promise

With the per-command cost understood, the lever from section 1 was worth building: batch
several commands into one Raft entry, above Raft, keeping the synchronous append path and the
acknowledgement policy unchanged
([command batching](../docs/raft-configuration.md),
[measured](../docs/benchmarks/batching.md)). Eight commands per entry against fresh
unbatched runs:

| Persistence | Unbatched | Batches of 8 | Gain |
|---|---:|---:|---:|
| checkpoint | 155.9–173.4 w/s | **431.0 w/s** | **2.50×** |
| snapshot | 497.9–557.7 w/s | **1,780.6 w/s** | **3.31×** |

Then we wrote up a batch-size matrix that "measured" batches of 256 and 512 — and it had
silently measured 128 every time, because that was the concurrency. The batch a writer
actually forms is:

```
batch = min(commands in flight, max_batch_commands, max_batch_bytes / frame_size)
```

plus whatever a small collection delay lets accumulate. **A count limit above your concurrency
is dead configuration.** We then blamed the *byte* budget, did the arithmetic wrong (we assumed
~2 KB frames; the real median is 741 bytes, so the default 256 KiB allows ~354 commands and
never bound), and only found it by raising concurrency alone.

The fix is not more arithmetic. The writer now publishes what it did — entries, commands, mean
and largest batch, **which limit bound each batch**, and how many of the largest command the
byte budget holds — so an inert limit is visible instead of assumed.

## 7. The migration, which paid for a different reason than we did it

We moved from openraft 0.9.25 to 0.10.0-alpha.36 to get pipelined append: 0.9 waits for each
`AppendEntries` response, and pipelining should remove that round trip. So we built the
bidirectional streaming RPC, measured it, and removed it — level to a few percent either way
(up to ~9% behind at low concurrency, level to ~8% ahead at high) against a ~±4% spread
between repeats of the same arm.

The upgrade itself was worth 2.3–31.8× on the same configuration family:

| 3 nodes, 20k events | 0.9.25 | 0.10 |
|---|---:|---:|
| default config (8 in flight, unbatched) | 367 w/s | **846 w/s** |
| 128 in flight, unbatched | 777 w/s | **9,272 w/s** |
| 1 node, 128 in flight | 294 w/s | **9,337 w/s** |
| 128 in flight, batches of 128 | 13,781 w/s | 13,574 w/s |

The reason was in the core, not the network: **0.9 serialized local appends behind the previous
flush; 0.10 tracks IO completion with a watermark and lets them overlap.** That is a per-entry
cost, and batching had already amortised the round trip we were trying to remove — which is why
the last row is a wash. We had the lever backwards.

It also had a failure mode worth knowing about. The streaming implementation needed a bound on
how long a stream could go quiet, and the obvious one was openraft's own `hard_ttl` — which on
the replication path is the *heartbeat interval*, so a per-response bound of tens of
milliseconds. Under load that tears the stream down mid-burst, and then the leader's progress
bookkeeping holds a `matching` index ahead of what the follower actually has, while the
conflict response that would repair it is **discarded**. Openraft's own documentation notes
that in this state "log replication cannot make progress". We watched a follower stop at log
5017 while the cluster committed 5105 and never catch up, into a 900-second timeout. A stall
detector fixed it; deleting the feature fixed it permanently.

## 8. Two fsyncs per apply, in the same database

Finally, the per-command work. We instrumented the apply path in phases — there is no `perf` in
our environments, `perf_event_paranoid` refuses it — and at 253 commands per entry the split
per command was:

| phase | µs/command |
|---|---:|
| state write (RocksDB batch + **fsync**) | 13.5 |
| log write (RocksDB batch + **fsync**) | 6.5 |
| core apply | 4.5 |
| parse the log entry | 4.3 (leader), 1.3 (follower) |
| serialise the state delta | 2.4 |
| encode the log entry | 0.8 |

Two synced writes, and they go to the **same RocksDB instance** — different column families,
one database. So every apply batch paid two fsyncs. Batching hides that: 253 commands share
them. Our *default* deployment path does not batch, so it paid both **per command** — and the
per-command view says what that means: unbatched at concurrency 128 a command costs 1.29 ms,
against 0.94 ms at eight commands per entry.

Dropping the sync on the apply batch (the Raft log keeps its own) is worth:

| 3 nodes, 20k events | synced | dropped | |
|---|---:|---:|---|
| default path (8 in flight, unbatched) | 913 w/s | **1,225 w/s** | **+34%** |
| same, snapshot-backed recovery | 888 w/s | 1,212 w/s | +36% |
| 256 in flight, batches of 253 | 21,250 w/s | 21,638 w/s | +1.8% |

The third row is the same finding from the other side: where the fsyncs are amortised over 253
commands there was nothing to win. And the first row is a control arm — the same code, minutes
later, one line changed back.

**Is it safe?** The Raft log is synced *before* the entry it carries is committed and applied,
so the log — with snapshots behind it — is the durability boundary. The batch that follows is
one atomic `WriteBatch` containing the record, the changed state and the applied marker, so a
power cut can lose the last window of applies but cannot leave a torn one: the marker moves
exactly as far as the record. Recovery replays the difference. Snapshot mode has always worked
this way, persisting no per-apply state at all.

What that costs is replay time after a crash — one fsync-window of applies instead of a shorter
catch-up. What it does *not* cost is correctness, and we pinned that with two tests: a lost
apply is repaired by replaying the log, and a store whose record has no marker is refused
rather than guessed at (both available guesses corrupt the record: keeping it duplicates every
event on replay, dropping it loses events no log may still hold).

**One honest caveat.** `SIGKILL` does not test durability — the page cache survives it. Only a
power cut exercises the loss. The argument is the ordering, and the tests cover the repair, not
the loss. We say that in the docs rather than implying the trade is "tested".

### The coda: batching does not amortise a shared device

If two fsyncs per batch are bad, more commands per batch must be better. At 8 in flight it is
not: turning batching on gave **1,231 w/s against 1,225** — a wash.

The reason is in the per-command view. Unbatched at concurrency 128 a command costs 1.29 ms;
at eight commands per entry, 0.94 ms — a 27% saving, not a factor of eight. Most of the
unbatched cost is a round trip per *command* rather than per entry, and the path is
latency-bound: throughput tracks concurrency ÷ latency (8 in flight over 19.5 ms is ~410
writes/s). So at low concurrency there is little round trip left to amortise — with three
replica processes on one machine sharing the device serving it. The fsync was the last
per-command lever, not the last possible one.

## What was not the bottleneck

- **JSON.** Encode 0.8 µs, parse 1.3–4.3 µs per command at 741 bytes. A binary codec would save
  a few µs, not the prize.
- **The core apply.** 4.5 µs/command. Real, but a third of the state write.
- **Batching, at low concurrency.** A wash, above.
- **RocksDB.** Once the debug build and the dedup-window fold were out of the way, the same
  engine did ~11–15k commands/s on one group with a synchronous WAL write per append and per
  apply. The storage layer was not what was limiting this.

What moved throughput was **concurrency**: 1,225 w/s at 8 in flight, 9,272 at 128, 21,638 at
256 with batches of 253. That is a caller-side choice, not a server knob — which is worth
knowing before you spend a week tuning the server.

## Two things we had believed that the measurements refused

- **"Batching by default would amortise the log fsync."** We said this, then measured it: a wash
  at the default concurrency. The inference was reasonable and wrong.
- **"+9.5% from dropping the sync."** Our first A/B was a single trial per arm and read +9.5% at
  the batched configuration. Three trials put it at +1.8%, inside the spread. The real effect
  was at the *unbatched* configuration, which we had not measured yet — and it was +34%.

One trial is not a measurement, and a control arm is cheaper than being wrong. Both of these
went into the docs as corrections rather than quietly disappearing.

## Appendix: a bug that existed only in a build nobody ran

Not a tuning finding, but the same disease. Our shell crate did not compile on its own —
`cargo check -p loomery-shell` failed on a missing `OsRng` — while `cargo check --workspace` was
green for months. Feature unification: a workspace build enabled `rand_core/getrandom` through
another member (`jsonwebtoken`), so the crate was only ever compiled with a feature it never
declared. The gate that catches it is a per-package build; `--workspace` cannot see it by
construction, and `--no-default-features` does not help either, because the masking feature is
requested *explicitly* by the neighbour. "It compiles" is a claim about the invocation, not the
crate.

## If we were starting again

1. Measure the configuration you deploy, at the concurrency you deploy it with, in a release
   build.
2. Instrument the phases before optimising any of them — four counters beat a week of inference.
3. When something is a ceiling, publish what actually bound.
4. Count your fsyncs. Ours were two, in the same database, and nobody had noticed.
5. Record the numbers you cannot explain instead of smoothing them. The 981 MB was a clue.

## Where the numbers come from

Every figure above is from a committed note, in the order the post uses them:
[checkpoint versus snapshot](../docs/benchmarks/checkpoint-spike.md) →
[the storage investigation](../docs/research/consensus-storage-performance.md) →
[persistence hardening](../docs/benchmarks/persistence-hardening.md) →
[storage layout](../docs/storage-layout.md) →
[command batching](../docs/benchmarks/batching.md) →
[deployment path at scale](../docs/benchmarks/deployment-scale.md#after-openraft-010).
