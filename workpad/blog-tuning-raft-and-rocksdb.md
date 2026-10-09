# Two fsyncs per write: notes from tuning Raft and RocksDB

We run one Raft group per tenant: [openraft](https://github.com/databendlabs/openraft) for
consensus, tonic for the wire, RocksDB for the log and the state. A benchmark harness
drives the real path — three replica processes, real sockets, real databases, no mocks —
and measures commands per second and per-command latency through the whole stack.

This is what we found while tuning it. Three of the findings contradicted something we
believed, and one of them was worth 34%.

## 1. The upgrade paid, but not for the reason we did it

We moved from openraft 0.9.25 to 0.10.0-alpha.36 to get `RaftNetworkV2` and pipelined
append: 0.9 waits for each `AppendEntries` response, and pipelining should remove that
round trip. So we built the bidirectional streaming RPC, measured it, and removed it —
it came out level to a few percent either way (up to ~9% *behind* at low concurrency,
level to ~8% ahead at high), against a ±4% spread between repeats of the same arm.

The upgrade itself was worth 2.3–31.8×:

| 3 nodes, 20k events | 0.9.25 | 0.10 |
|---|---|---|
| default config (8 in flight, unbatched) | 367 w/s | 846 w/s |
| 128 in flight, unbatched | 777 w/s | 9,272 w/s |
| 1 node, 128 in flight | 294 w/s | 9,337 w/s |
| 128 in flight, batches of 128 | 13,781 w/s | 13,574 w/s |

The reason was in the core, not the network: 0.9 serialized local appends behind the
previous flush; 0.10 tracks IO completion with a watermark and lets them overlap. That
is a per-entry cost, and batching had already amortised the round trip we were trying to
remove — which is why the last row is a wash. We had the lever backwards.

It also had a failure mode worth knowing about. The streaming implementation needed a
bound on how long a stream could go quiet, and the obvious one was openraft's own
`hard_ttl` — which on the replication path is the *heartbeat interval*, so a per-response
bound of tens of milliseconds. Under load that tears the stream down mid-burst, and then
the leader's progress bookkeeping holds a `matching` index ahead of what the follower
actually has — while the conflict response that would repair it is **discarded** (a
conflict at or above `searching_end`, or one carrying a stale inflight id, leaves the
progress entry untouched). Openraft's own documentation notes that in this state "log
replication cannot make progress". We watched a follower stop at log 5017 while the
cluster committed 5105 and never catch up, into a 900-second timeout. A stall detector
fixed it; deleting the feature fixed it permanently.

**Lesson.** Measure the configuration you deploy. A performance feature aimed at a cost
that something else already hides will look neutral, and its absence will look like a
regression you cannot find. And never put a deadline derived from the heartbeat interval
on a replication stream.

## 2. The batch is not the limit you set

Our batching knob is `max_batch_commands`. It is a ceiling, not a promise. The batch a
writer actually forms is

```
batch = min(commands in flight, max_batch_commands, max_batch_bytes / frame_size)
```

plus whatever a small collection delay lets accumulate. Only the first term reflects the
deployment; the rest is policy. Two consequences bit us:

- **A count limit above your concurrency is dead configuration.** With 8 concurrent
  callers, batches are 8 whatever the limit says. We had written up a batch-size matrix
  that "measured" batch 256 and 512 — and it had silently measured 128 every time,
  because that was the concurrency. We then blamed the *byte* budget, did the arithmetic
  wrong (we assumed ~2 KB frames; the real median is 741 bytes, so the default 256 KiB
  allows ~354 commands and never bound), and only found it by raising concurrency alone.
- **The fix is observability, not arithmetic.** The writer now publishes what it did:
  entries, commands, mean and largest batch, which limit bound each batch, and how many
  of the largest command the byte budget holds. An inert limit becomes visible instead of
  assumed.

**Lesson.** If a limit can be unreachable, publish which limit actually bound, or you
will tune a number that is never read.

## 3. Two fsyncs per apply, in the same database

This was the finding that paid. We instrumented the apply path in phases (there is no
`perf` in our environments — `perf_event_paranoid` refuses it), and at 253 commands per
entry the split per command was:

| phase | µs/command |
|---|---|
| state write (RocksDB batch + **fsync**) | 13.5 |
| log write (RocksDB batch + **fsync**) | 6.5 |
| core apply | 4.5 |
| parse the log entry | 4.3 (leader), 1.3 (follower) |
| serialise the state delta | 2.4 |
| encode the log entry | 0.8 |

Two synced writes, and they go to the **same RocksDB instance** — different column
families, one database. So every apply batch paid two fsyncs. Batching hides that: 253
commands share them. Our *default* deployment path does not batch, so it paid both **per
command** — and an entry carrying one command costs ~375 µs to write, where the same
entry carrying eight costs ~1,160 µs.

Dropping the sync on the apply batch (the Raft log keeps its own) is worth:

| 3 nodes, 20k events | synced | dropped |
|---|---|---|
| default path (8 in flight, unbatched) | 913 w/s | **1,225 w/s (+34%)** |
| same, snapshot-backed recovery | 888 w/s | 1,212 w/s (+36%) |
| 256 in flight, batches of 253 | 21,250 w/s | 21,638 w/s (+1.8%) |

The third row is the same finding from the other side: where the fsyncs are amortised
over 253 commands there was nothing to win. And the first row is a control arm — the same
code, minutes later, one line changed back.

**Is it safe?** The Raft log is synced *before* the entry it carries is committed and
applied, so the log — with snapshots behind it — is the durability boundary. The batch
that follows is one atomic `WriteBatch` containing the record, the changed state and the
applied marker, so a power cut can lose the last window of applies but cannot leave a
torn one: the marker moves exactly as far as the record. Recovery replays the difference.
Snapshot mode has always worked this way, persisting no per-apply state at all.

What that costs is replay time after a crash — one fsync-window of applies instead of a
shorter catch-up. What it does *not* cost is correctness, and we pinned that with two
tests: a lost apply is repaired by replaying the log, and a store whose record has no
marker is refused rather than guessed at (both available guesses corrupt the record:
keeping it duplicates every event on replay, dropping it loses events no log may still
hold).

**One honest caveat.** `SIGKILL` does not test durability — the page cache survives it.
Only a power cut exercises the loss. The argument is the ordering, and the tests cover
the repair, not the loss. We say that in the docs rather than implying the trade is
"tested".

## 4. Batching does not amortise a shared device

If two fsyncs per batch are bad, more commands per batch must be better. At 8 in flight
it is not: turning batching on gave **1,231 w/s against 1,225** — a wash.

The reason is in the numbers above. Eight commands in one entry cost ~1,160 µs to write
where one command costs ~375 µs, so the amortisation is ~2.6×, not 8×. And the real
constraint at low concurrency is not per-command work at all: it is three replicas
fsyncing on one disk. Each process's own attributed work (~175 µs/command) sits far below
the 812 µs/command of wall time, because the device is the queue.

**Lesson.** Amortising a syscall helps only while the syscall is what you are waiting on.
On a shared device, the queue moves to somewhere you cannot amortise.

## 5. Checkpointing the whole state per apply was quadratic

Before the fsync story there was a worse one, and it is the reason our recovery modes are
not symmetrical. Checkpoint mode originally wrote the *entire* applied state on every
apply batch. That makes an apply cost the size of the history, not the size of the batch,
and the soak showed exactly what you would expect: at 2,000 commands, checkpoint mode
managed **~17 commands/s** and 335 ms p99, against snapshot mode's ~1,190/s. A 78×
penalty that grows with history.

The fix was to write only what changed — the aggregates the batch's events are about, the
dedup entries it added or evicted, the applied marker — in the same synchronous batch as
the events. Same mode, same machine, 2,000 commands: **~17/s → ~1,190/s**, a 67×
improvement, and the curve stopped being a function of history.

The general lesson is the one this whole post keeps repeating: an apply should cost what
it *did*, not what it *knows*.

## 6. Things that were not the bottleneck

- **JSON.** Encode 0.8 µs, parse 1.3–4.3 µs per command at 743 bytes. A binary codec
  would save a few µs, not the prize.
- **The core apply.** 4.5 µs/command. Real, but a third of the state write.
- **Batching, at low concurrency.** See above.

What moved throughput was **concurrency**: 1,225 w/s at 8 in flight, 9,272 at 128,
21,638 at 256 with batches of 253. That is a caller-side choice, not a server knob — which
is worth knowing before you spend a week tuning the server.

## 7. Two things we had believed that the measurements refused

- **"Batching by default would amortise the log fsync."** We said this, then measured it:
  a wash at the default concurrency. The inference was reasonable and wrong.
- **"+9.5% from dropping the sync."** Our first A/B was a single trial per arm and read
  +9.5% at the batched configuration. Three trials put it at +1.8%, inside the spread.
  The real effect was at the *unbatched* configuration, which we had not measured yet —
  and it was +34%.

**Lesson.** One trial is not a measurement, and a control arm is cheaper than being
wrong. Both of these went into the docs as corrections rather than quietly disappearing.

## Appendix: a bug that existed only in a build nobody ran

Not a tuning finding, but the same disease. Our shell crate did not compile on its own —
`cargo check -p loomery-shell` failed on a missing `OsRng` — while `cargo check
--workspace` was green for months. Feature unification: a workspace build enabled
`rand_core/getrandom` through another member (`jsonwebtoken`), so the crate was only ever
compiled with a feature it never declared. The gate that catches it is a per-package
build; `--workspace` cannot see it by construction, and `--no-default-features` does not
help either, because the masking feature is requested *explicitly* by the neighbour.

**Lesson.** "It compiles" is a claim about the invocation, not the crate.

## If we were starting again

1. Measure the configuration you deploy, at the concurrency you deploy it with.
2. Instrument the phases before optimising any of them — four counters beat a week of
   inference.
3. When something is a ceiling, publish what actually bound.
4. Count your fsyncs. Ours were two, in the same database, and nobody had noticed.
5. Keep a control arm, and do not trust a single trial.
6. Say out loud which parts of a durability trade your tests *cannot* cover.

The measurements, the caveats and the run-by-run evidence are in
[`docs/benchmarks/deployment-scale.md`](../docs/benchmarks/deployment-scale.md) and
[`docs/raft-configuration.md`](../docs/raft-configuration.md).
