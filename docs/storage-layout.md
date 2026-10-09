# Storage layout

**Status: steps 1 and 2 implemented, step 3 not.** This is the layout
D2 (`workpad/design.md#d2--storage-engine`)'s amendment and
D13 (`workpad/design.md#d13--history-is-append-only-checkpoints-carry-state`) describe.

**Step 1** — the families and the append-only `events` family — is in the code:
every database is opened as `default`/`raft_log`/`state`/`events`/`projections`,
the layout marker is written and checked, the Raft log lives in its own family, and
every apply appends its events to `events`. The record is durable on its own
account, which is what makes purging the Raft log safe.

**Step 2** — state deltas and state-only recovery — is in the code as well: an
apply persists **only what it changed** (the aggregate states its events touch, the
dedup entries the window added or evicted, and the applied index), in the same
synchronous batch as its events, and recovery reads that state back per aggregate
instead of from a whole-state record. The quadratic cost that motivated all of this
is gone: the soak went from 17 to ~1,190 commands per second at 2,000 commands
(measured (`workpad/benchmarks/persistence-hardening.md#after-step-2-the-deltas`)).

**Step 3** — reads answering from the record and the projections instead of the
in-memory list of every event — is not, so the history list is still kept in RAM.
Where the code differs from the target, the difference is marked **today**.

The two steps that build this are in
D13's order of work (`workpad/design.md#d13--history-is-append-only-checkpoints-carry-state`):
families and the `events` family first, then deltas and state-only recovery.

## One database per group

```
<data_dir>/
  <control_group>/          one RocksDB: users, organizations, placements, router
    …column families…
  <group_id>/               one RocksDB per tenant (one organization)
    …column families…
    index/                  the tantivy directory — beside the database, not in it
```

**The tenant is the database.** That is what makes the rest simple: no key needs a
tenant prefix, no query needs a tenant filter to be *safe*, and the search index is
per tenant by construction — "find anything within the tenant" is answered by that
tenant's own directory. It also means a tenant can be backed up, restored or
deleted as a directory, and one tenant's compaction cannot read another's data.

The alternative — one database per node with `tenant:`-prefixed keys — exists only
as an escape hatch if per-database overhead (open file handles, background jobs,
memory) ever hurts at high tenant counts. It would need a shared `Env` with
bounded background threads and a shared block cache, and it would turn isolation
into a naming convention. Not the default.

## Column families

| Family | Holds | Key layout |
|---|---|---|
| `default` | nothing but markers | `format` → the format version; `state_persistence` → the pinned mode |
| `raft_log` | the Raft log and its bookkeeping | `l{index:016x}` big-endian → `Entry`; `vote`; `committed`; `purged` (the purge floor) |
| `state` | the fold, and the small indexes the fold maintains | `agg:{aggregate_id}` → `(id, aggregate state)`; `meta:applied` → applied `LogId`; `meta:membership` → last applied membership; `dedup:{causation_key}` → `(key, fingerprint, first log index)`; `snapshot` → the latest Raft snapshot record |
| `events` | **the append-only record** | `e:{log_index:016x}:{pos:02x}` → `Event` (a batched entry holds several events, hence `pos`) |
| `projections` | read models (D9 (`workpad/design.md#d9--storage-of-cold-read-model-state`)) | projection-specific; dropped and rebuilt at will |

Big-endian keys are what make a lexicographic range scan equal a numeric one: `l`
and `e` both iterate in log order, which is what history reads and Raft recovery
need.

The aggregate id travels **in the value**, not in the key: an id has to survive a
round trip, and not every id is a canonical UUID, so deriving one back out of a key
would mean parsing it. The key exists to place the aggregate in the family.

The membership index is **derived from the record** rather than persisted: recovery
loads the events anyway (step 3 removes that), and rebuilding the small map from
them is cheaper than keeping a second copy honest. Persisting it per member is part
of step 3, when the record stops being read at boot.

## One apply, one atomic batch

Every apply writes **one synchronous batch** (`sync=true`), and a batch may span
families:

| Written | When |
|---|---|
| `agg:{aggregate_id}` for each aggregate the batch's events are about | checkpoint mode |
| `meta:applied`, `meta:membership` | checkpoint mode |
| `dedup:{causation_key}` for each command the batch recorded, and a delete for each key the window evicted | checkpoint mode |
| `e:{log_index}:{pos}` for each event the batch produced | **always** |
| `l{index}` (Raft entries) | at append, before apply — Raft owns this |
| deletion of purged `l{index}` + `purged` | when OpenRaft purges, after a snapshot covers them |

A stream changes only by an event, so the events of a batch name exactly the
aggregate states to write — the delta needs no bookkeeping of its own. The dedup
window is bounded and FIFO, so the only entry an insert can evict is the one at the
front, which keeps that bookkeeping O(1) per command rather than a diff of the
window.

Snapshot mode writes no state per apply: recovery replays the log into the state a
snapshot carried, so its durable state *is* the snapshot.

Because the batch is atomic across families, **"the state moved and these events
happened" is one durable fact**: a crash leaves both or neither. And because
`events` is written at apply time — always before OpenRaft purges the entries
carrying those events — purging the Raft log cannot lose history. That is why this
layout has no archive-on-purge step.

That is what removed the quadratic cost the layout was designed around: the same
soak that measured 17 commands per second at 2,000 (because each apply rewrote the
whole history) now measures ~1,190, and checkpoint mode matches snapshot mode at
every size (measurements (`workpad/benchmarks/persistence-hardening.md`)).

## Recovery

1. Open the database, listing **every** family. RocksDB refuses to open otherwise
   (`Column families not opened: …`).
2. Check the `format` marker and the pinned `state_persistence` mode; refuse on
   anything unknown or inconsistent.
3. Recover the fold:
   - **checkpoint mode**: read `meta:applied` and `meta:membership`, every
     `agg:{aggregate_id}`, and the dedup window from its `dedup:*` entries (ordered
     by the log index each was recorded at);
   - **snapshot mode**: install the `snapshot` record, which carries the fold, the
     dedup window and the history up to its index.
4. Load the history from `events` — in checkpoint mode, where the fold is complete
   without the log. Snapshot mode lets the log tail extend the snapshot's history
   instead, so nothing is counted twice.
5. Replay `raft_log` entries after the recovered applied index, which is what
   advances a snapshot-mode fold.
6. Serve.

Nothing in recovery walks the history: state comes from `state`, and the log tail
is bounded by snapshot policy (`LogsSinceLast(5000)`).

**today:** recovery loads the `state` blob (which carries the event list) and
replays the committed log after its applied index; the state machine keeps that
event list in memory forever, and it is what reads filter.

## Snapshots and purge

- A **Raft snapshot** is a `state`-family record: `SnapshotMeta` plus the fold at
  that point — aggregate state, the membership index, the dedup window, the applied
  index. No event list: the events are already durable in `events`.
- **Purge** deletes the Raft entries a snapshot covers and writes the new floor in
  the same synchronous batch, so "purged" and "the floor moved" cannot disagree.
  The record is unaffected.
- Snapshot scheduling stays OpenRaft's (`GroupConfig.raft.snapshot_policy`), and an
  apply checkpoint is still not itself a purge trigger.

## The record, and what may be thrown away

A hard rule, because "destructive is fine pre-release" has a boundary:

| Data | Class | May be deleted? |
|---|---|---|
| `raft_log` entries above the floor | needed for Raft, not for history | yes, by purge |
| `events` | **the record** | **no** — deleting it loses history |
| `state` | derived (fold of `events`) | yes — rebuilt by replay |
| `snapshot` record | derived | yes — rebuilt |
| `projections` | derived | yes — rebuilt |
| `index/` | derived | yes — rebuilt |

So a format bump may wipe anything derived, and must fail closed rather than
half-open. `raft_log` may be truncated only by Raft's purge (history lives in
`events`); `events` is only lost if the whole tenant is discarded.

## Adopting it, and backups

Pre-release there are no deployments and no data to preserve, so adopting this
layout is: stop the host, discard `<data_dir>`, start it again and re-onboard. No
migration is owed. That stops being true at the first release: from then on a
layout change is either a migration or an explicit, announced discard, and the
format marker is what makes the second one impossible to do by accident.

A backup of a tenant is its database directory **plus** `events`' completeness —
which the database already is, since `events` is a family — so `index/` may be
skipped entirely and rebuilt after a restore. Derived data is never worth backing
up; the record is the only thing that must survive.

## Options

Per family: compression and bloom/table settings; the block cache is **shared**
across families (and across databases, if the escape hatch is ever needed) so the
per-database memory set in `StorageConfig` (write buffers, background jobs, open
files, block cache) is not multiplied by family count. `events` and `raft_log`
compress well (LZ4, as today); `state` is small JSON values and needs little.

## What stays in memory

- **The membership index** (`m:{user_id}`): small, read on every authorization
  decision, so it stays resident and is persisted so that recovery does not replay
  the history to rebuild it.
- **Aggregate state**: loaded per aggregate from `state`, with a bounded cache in
  front of it. Folding a command needs that aggregate's state, not the whole map,
  so the working set is what the hot path touches — not the tenant's history.
- **The applied-event list**: **today** still kept, for reads. Step 3 moves history
  reads onto `events` and deletes it; the record is already complete, ordered and
  durable, so the list is a cache of it rather than the truth.
- The applied-index watch channel (u64) stays: it drives the outbox and the
  read-your-writes gate.

## Reads

| Read | Serves it |
|---|---|
| history of a workspace or organization | a range scan over `events` (today: a filter over the in-memory list) |
| read-your-writes (`X-Min-Index`) | `meta:applied` from `state`, as today |
| board/list queries | `projections` (D9 (`workpad/design.md#d9--storage-of-cold-read-model-state`)) |
| "find anything" | the tantivy index — see [search.md](search.md) |
| authorization | the in-memory membership index, folded from events |

## Tests this layout owes

Each step lands with the evidence its contract change requires:

1. **Families and `events`.** The format marker fails closed on unknown layouts;
   every apply's events are present exactly once and in order; a purge leaves the
   record untouched; the interruption tests
   (`raft/interruption_tests.rs`) and the soak
   (`mise run soak`) still pass.
2. **Deltas and state-only recovery.** Crash/purge/replay in both persistence
   modes; recovery from a snapshot plus a log tail; the dedup window and
   membership index survive; and the soak's apply rate stops halving as the
   history doubles — the measurement that proves the quadratic cost is gone.
3. **Projections and the index.** Rebuild determinism (same record → identical
   projection and identical index), permission scoping (a caller never receives a
   hit they may not read), and staleness bounds ([search.md](search.md)).

## See also

- design.md (`workpad/design.md`) — D2 (`workpad/design.md#d2--storage-engine`) (storage engine and
  this amendment), D13 (`workpad/design.md#d13--history-is-append-only-checkpoints-carry-state`)
  (append-only record), D9 (`workpad/design.md#d9--storage-of-cold-read-model-state`)
  (projections), D11 (`workpad/design.md#d11--outbox-subjects-stream-naming-and-dedup-identity`)
  (the outbox reads the same applied index)
- [search.md](search.md) — the index beside this database
- checkpoint-policy.md (`workpad/research/checkpoint-policy.md`) — why checkpoints are awaited
- persistence-hardening.md (`workpad/benchmarks/persistence-hardening.md`) — the measurements
  that motivate the layout
