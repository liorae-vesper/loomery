---

> **Research note for the Loomery project.** Informs decision D2 (storage).

# Storage Engine Alternatives — Research

Evaluates per-node embedded storage options for Loomery: the hand-rolled
**segment files + in-memory index** design vs sled, Redb, RocksDB, SQLite, and
others — all behind OpenRaft's `RaftLogStorage`/`RaftStateMachine` traits.
Includes the **vector database constraint** (Capture phase) with
backup/restore strategies.

Companion to [[openraft-vs-alternatives]] (why we run Raft groups at all).
Informs the Phase 1 storage spike (decision D2).

---

## 1. What "segment files + in-memory index" means

The design is not a database — it's a **purpose-built event log engine** in the
unified-log tradition (WAL + memtables + segment writer + snapshot writer, the
architecture made famous by `ra` and adopted by OpenRaft-adjacent stores). Two
primitives:

**In-memory index** (e.g. `dashmap`/`BTreeMap`/`IndexMap` under normal `Arc`):
- Microsecond reads; a `BTreeMap<u64, Offset>` keyed by log index gives
  index-ordered scans — exactly what the unified log needs.
- Volatile: lost on crash. **It is the read cache, never the source of truth.**

**Segment files** — append-only immutable files on disk:
- Entries appended sequentially: `[CRC32][length][payload]` framing (see
  `indexed-segment-file-format.md` for the indexed-footer variant).
- Never mutated in place → once a segment is sealed, it is **immutable forever**
  (huge for backup, see §5).
- Read via the in-memory offset index (hot) or sequential scan (recovery).

### The engine lifecycle (the proven pattern)

```
WRITE:   command → OpenRaft apply → append to current WAL file
         → buffer until batch full or the channel drains → one fsync per batch
         → copy entry into the in-memory index → notify "index K is durable"
ROLLOVER: WAL file reaches size limit → seal into a segment file (+ footer
         index) → delete old WAL
READ:    in-memory index (hot) → segment file via offset (cold) → never the WAL
SNAPSHOT: snapshot writer serializes in-memory state → snapshot file
         → segments at/below the snapshot index are purged (log compaction)
RECOVER: load latest snapshot → replay segments newer than the snapshot index
         → rebuild in-memory index
```

**Why one fsync funnel:** parallel `fsync(2)` calls destroy throughput. A single
tokio task (`spawn_blocking` fsync worker) sizes each batch to whatever
accumulated during the previous fsync — latency adapts to load automatically.

### Mapping to the OpenRaft storage traits

| OpenRaft interface | Hand-rolled realization |
|---|---|
| `RaftLogStorage` (log) | WAL file → sealed segment files + in-memory `BTreeMap` index |
| `RaftStateMachine` (state) | In-memory projected state + versioned snapshot files |
| `get_log_state()` / `save_committed()` | Index head + committed index persisted atomically |

---

## 2. Alternatives compared (Rust)

| Engine | Crate | Type | Write model | Backup story | Verdict for Loomery |
|---|---|---|---|---|---|
| **Segment files + index** (hand-rolled) | — | Purpose-built log | Single batched-fsync WAL | Immutable segments = copy-once | ✅ Event store / unified log |
| **sled** | `sled` | Ordered KV, log-structured | Concurrent, batched | Tree snapshots; copy-on-write friendly | ✅ OpenRaff-friendly (example impl exists) |
| **Redb** | `redb` | Immutable B-tree, ACID, mmap | Single writer | File copy (crash-safe) | ⚠️ Read-model store candidate |
| **RocksDB** | `rocksdb` | LSM KV (C++, via `librocksdb-sys`) | Multi-threaded | Checkpoints; in-place compaction mutates files | ⚠️ Defensible; D2 escape hatch |
| **SQLite** | `rusqlite` | Relational B-tree | Single writer, WAL mode | File copy / Online Backup API | ⚠️ Strong for read models + vectors |
| **sqlite-vec** | extension | Vector KNN in SQLite | Same as SQLite | Same file = same backup | ✅ Vector store candidate |
| **hnswlib** | `hnswlib-rs` | ANN index (HNSW) | In-memory build | Index file save/load | ⚠️ ANN when brute force tops out |

## 3. Why not the obvious choices

### RocksDB
**What it solves:** raw throughput and a rich KV surface; OpenRaft ships a
RocksStore example. **What it costs:**
- **Crash blast radius** — a C++ segfault takes down the whole node (all Raft
  groups + tenants). The same argument applies to any C/C++ dep.
- **Tuning surface** — hundreds of options; write/compaction stalls are a
  literature of their own.
- **Design-fit friction (the decisive axis):** the unified log wants explicit
  durability control (batched fsync → notify → CRC framing → torn-tail
  recovery); RocksDB does durability its own way and Loomery layers on top.
  In-place compaction mutates files → harder delta backup than immutable
  sealed segments. Block cache duplicates hot data already held in-memory.
- **Build/deploy burden** — C++ toolchain, compile time (clone `rocksdb` once).

**Verdict:** defensible, not reckless — kept as the documented fallback. If the
segment-engine spike misses its targets (≥10k entries/s sustained, p99 latency
under burst, recovery time, memory under tenant cycling), the RocksDB variant
is the comparison baseline and the sane escape hatch (D2).

### SQLite (rusqlite)
- **WAL mode:** concurrent readers + one writer; single-file semantics. Backup
  is a solved problem (file copy at checkpoint, or Online Backup API).
- **Why not for the event log:** SQL/B-tree impedance for what is fundamentally
  an append-only ordered log; the Raft log *already is* the durability layer —
  putting a second transactional engine under it is double bookkeeping. SQLite
  earns its place **beside** the log (read models, vectors — D9), not **under** it.

### sled vs Redb for "just use a KV store"
- **sled**: the reference implementation is an actual OpenRaft `SledStore`
  example — lowest integration risk; good enough for the control group and
  small tenant groups; occasional API churn noted by users.
- **Redb**: simpler, mmap-based, very fast reads, single-writer — nice for read
  models; less battle-tested with OpenRaft.

---

## 4. The vector database constraint

### Where vectors live in Loomery
Phase 4 (Capture) and semantic features: embeddings of captured notes, tasks,
projects — used for extraction assistance, dedup detection, semantic search.

### The key architectural insight: vectors are *derived data*

```
Domain event (TaskCreated, ThoughtCaptured)
       │  (projector calls embedding model)
       ▼
Embedding vector  ──►  Vector index  (KNN queries)
       │
       └── Store the vector bytes in the projection/checkpoint
```

- The **event store is the source of truth**; the vector index is a
  **projection** — same category as read models. It can always be rebuilt by
  replay.
- **Rule:** persist computed embedding vectors alongside projection data
  (snapshot or checkpoint), so a restore never needs to re-call the LLM (cost,
  latency, and model-version drift all avoided).
- Scale check: B2B tenant data is ~10³–10⁵ items per workspace. Exact KNN over
  100k × 768-dim f32 ≈ 300 MB and tens of ms with `ndarray` — **brute force is
  viable at tenant scale**; ANN (HNSW) only for platform-wide search.

### Vector store options

| Option | ANN? | Storage | Backup/restore | Risk |
|---|---|---|---|---|
| **`ndarray` brute force** | Exact | Vectors in projection snapshot | Nothing extra — vectors already in the snapshot | Only viable to ~10⁵–10⁶ vectors |
| **sqlite-vec** via rusqlite | No — exact KNN | Inside the SQLite file | Same file = same backup | Pre-v1, breaking changes possible |
| **hnswlib-rs** | Yes — HNSW | In-memory index, file save/load | Checkpoint = write index at known event index; restore = load + top-up replay | Approximate recall |
| **pgvector** | Yes (IVFFlat/HNSW) | Postgres | pg_dump / WAL archiving | Contradicts embedded direction |

### Backup & restore strategy per data class

| Data class | Mutability | Backup | Restore |
|---|---|---|---|
| **Sealed segment files** (event history) | Immutable forever | Copy-once (S3/rsync); the file can never change under you | Copy back; replay from snapshot index |
| **Live WAL file** | Append-only, fsync-batched | Snapshot-first backup: trigger snapshot + WAL rollover, back up the sealed result; or copy and discard torn tail (CRC detects) | Truncate to last valid CRC frame |
| **Snapshot files** | Immutable once written | Copy-once, versioned by index | Load latest ≤ restored log position |
| **`raft_meta` / committed index** | Tiny, atomically rewritten | Copy with any snapshot | Recoverable from log (term/vote are safety hints; entries decide). OpenRaft's `save_committed` makes this explicit |
| **Vector index** | Derived | **Optional:** skip (rebuild by replay) or checkpoint stamped with the event index it covers | Load checkpoint at N → replay > N → top-up; never re-call the LLM if vector bytes are in the checkpoint |
| **Read models** | Derived | Rebuildable; checkpoint for speed | Replay from last checkpoint index |

**The golden rule:** backup consistency anchors to the **event index**, not the
wall clock. Every derived-store checkpoint records `last_applied_index`; any
restore is "snapshot + replay forward." Immutable segments make the primary
backup a plain file copy.

---

## 5. Recommendation for Loomery

| Layer | Choice | Why |
|---|---|---|
| Unified log / event store | **Segment files with periodic flush** behind `RaftLogStorage` (or **sled** for the first slice) | No C++ dep, trivial backup, deterministic durability semantics; sled drops integration risk for Phase 1 |
| Read models (hot) | `dashmap` + projection snapshots | Already the architecture |
| Read models (durable/queryable, D9) | **Redb** or sled | Pure Rust, ACID, append-friendly |
| Vector store | **`ndarray` brute force** primary; sqlite-vec as a persistence fallback | No ANN needed at tenant scale |
| Vector store (later, scale demands) | **hnswlib-rs**, checkpointed per §4 | ANN when brute force tops out |
| Avoid | SQLite-as-event-log, second transactional engine under the log | Double bookkeeping |
| Accepted-risk fallback | **RocksDB** (`rocksdb` crate, RocksStore crib) | If the segment/sled spike misses targets; eyes open on tuning + observability |

**Validation gates for the Phase 1 spike (D2):**
- Segment/sled log must sustain ≥10k entries/s per group with p99 latency
  stable under burst, and recover 200k entries in <200 ms (footer-indexed).
- OpenRaft 0.9 storage calls (`try_append_entry`, `get_log_state`,
  `save_committed`, snapshot builder) must round-trip against the chosen
  backend in the control-group spike.

## Sources

- OpenRaft storage impls (MemStore/SledStore/RocksStore examples):
  https://github.com/databendlabs/openraft/tree/main/examples
- Sled: https://github.com/spacejam/sled
- Redb: https://github.com/cberner/redb
- RocksDB Rust binding: https://github.com/rust-rocksdb/rust-rocksdb
- SQLite WAL mode: https://sqlite.org/wal.html · rusqlite: https://github.com/rusqlite/rusqlite
- sqlite-vec: https://github.com/asg017/sqlite-vec
- hnswlib-rs: https://github.com/rust-nx/hnswlib
- ra internals (WAL/segments/snapshots — the pattern cribbed above):
  https://github.com/rabbitmq/ra/blob/main/docs/internals/INTERNALS.md

*Compiled: 2026-08 (reshaped for the Rust/Tokio/OpenRaft stack).*