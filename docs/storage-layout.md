# Storage layout

**Status: decided, not yet implemented.** This is the layout
[D2](design.md#d2--storage-engine)'s amendment and
[D13](design.md#d13--history-is-append-only-checkpoints-carry-state) describe. Where
the code differs today, the difference is marked **today** — the current
implementation keeps every kind of data in one key space per group, and rewrites
the whole state (including the event list) on every apply.

The two steps that build this are in
[D13's order of work](design.md#d13--history-is-append-only-checkpoints-carry-state):
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
| `state` | the fold, and the small indexes the fold maintains | `agg:{stream_id}` → aggregate state; `m:{user_id}` → membership record; `meta:applied` → applied `LogId`; `meta:membership` → last applied membership; `dedup:{causation_key}` → dedup entry; `snapshot` → the latest Raft snapshot record |
| `events` | **the append-only record** | `e:{log_index:016x}:{pos:02x}` → `Event` (a batched entry holds several events, hence `pos`) |
| `projections` | read models ([D9](design.md#d9--storage-of-cold-read-model-state)) | projection-specific; dropped and rebuilt at will |

Big-endian keys are what make a lexicographic range scan equal a numeric one: `l`
and `e` both iterate in log order, which is what history reads and Raft recovery
need.

**today:** one key space, with `l{index}` entries, `vote`, `committed`, `purged`,
`state` (the whole `SnapshotData`: aggregate state *and* every applied event *and*
the dedup window *and* membership), `snapshot`, `state_persistence`. There is no
`events` key — the events live inside the `state` blob.

## One apply, one atomic batch

Every apply writes **one synchronous batch** (`sync=true`), and a batch may span
families:

| Written | When |
|---|---|
| `agg:{stream_id}` for each aggregate the batch's commands touched | always |
| `meta:applied`, `meta:membership` | always |
| `dedup:{key}` for each command | always (window eviction deletes the oldest) |
| `e:{index}:{pos}` for each event the batch produced | always |
| `l{index}` (Raft entries) | at append, before apply — Raft owns this |
| deletion of purged `l{index}` + `purged` | when OpenRaft purges, after a snapshot covers them |

Because the batch is atomic across families, **"the state moved and these events
happened" is one durable fact**: a crash leaves both or neither. And because
`events` is written at apply time — always before OpenRaft purges the entries
carrying those events — purging the Raft log cannot lose history. That is why this
layout has no archive-on-purge step.

**today:** one `state` key per apply, containing the entire `SnapshotData`. The
cost of an apply is the size of the whole history, which the soak measures as
quadratic: 162 → 34 → 17 commands per second at 200 → 1,000 → 2,000
([persistence hardening](benchmarks/persistence-hardening.md)).

## Recovery

1. Open the database, listing **every** family. RocksDB refuses to open otherwise
   (`Column families not opened: …`).
2. Check the `format` marker and the pinned `state_persistence` mode; refuse on
   anything unknown or inconsistent.
3. Read `meta:applied`, `meta:membership` and the membership index; install the
   `snapshot` record if one is newer than the fold.
4. Replay `raft_log` entries after `meta:applied`.
5. Serve. The `events` family is **not** replayed into state: it is the record, not
   the fold. History reads scan it directly.

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
- **The applied-event list**: gone. History reads scan `events`; nothing keeps a
  second copy of the record in RAM.
- The applied-index watch channel (u64) stays: it drives the outbox and the
  read-your-writes gate.

## Reads

| Read | Serves it |
|---|---|
| history of a workspace or organization | a range scan over `events` (today: a filter over the in-memory list) |
| read-your-writes (`X-Min-Index`) | `meta:applied` from `state`, as today |
| board/list queries | `projections` ([D9](design.md#d9--storage-of-cold-read-model-state)) |
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

- [design.md](design.md) — [D2](design.md#d2--storage-engine) (storage engine and
  this amendment), [D13](design.md#d13--history-is-append-only-checkpoints-carry-state)
  (append-only record), [D9](design.md#d9--storage-of-cold-read-model-state)
  (projections), [D11](design.md#d11--outbox-subjects-stream-naming-and-dedup-identity)
  (the outbox reads the same applied index)
- [search.md](search.md) — the index beside this database
- [checkpoint-policy.md](research/checkpoint-policy.md) — why checkpoints are awaited
- [persistence-hardening.md](benchmarks/persistence-hardening.md) — the measurements
  that motivate the layout
