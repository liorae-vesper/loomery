# Read-model store options (D9)

[D9](../design.md#d9--storage-of-cold-read-model-state) asks where **durable
read models** live — the projections that answer status and query reads once
[D13](../design.md#d13--history-is-append-only-checkpoints-carry-state) takes them
off the in-memory applied list. This note is the decision input: the requirements
that narrow the choice, the candidates with their current facts, and what each one
implies.

D13 fixes the shape this must fit: the **record is append-only** (the log plus an
archive of purged segments) and projections are **derived** — so a projection may
always be thrown away and rebuilt from the record. That is the property that makes
this decision low-risk and reversible, and it is assumed throughout.

## What the store has to do

| Requirement | Why |
|---|---|
| Ordered range scans with **keyset pagination** | the Phase-2 query API pages by `(created_at, id)`, never by offset |
| **Secondary lookups** (by workspace, project, assignee, status) | the queries the UI actually makes |
| **Single writer** is enough; concurrent readers are not optional | one projector per group writes; every HTTP read reads |
| **Rebuildable**: delete and replay from the record | projections are derived (D13); a torn projection is not a data-loss event |
| **Local** (no network hop), per group | reads answer from the replica that served the write |
| Crash-safe, no torn reads after a kill | a read must never see half a projection |
| Passes `cargo deny`/`audit`/licence checks **with no new ignore** | the workspace policy; see [guardrails](../guardrails.md) |

A second transactional engine *under* the log was already rejected
([storage-engine-alternatives.md](storage-engine-alternatives.md)); this note is
about a store **beside** it.

## Candidates

| | **redb** | **rusqlite (bundled SQLite)** | **RocksDB, reused** | **sled** | **Consensus-backed projections** |
|---|---|---|---|---|---|
| What it is | pure-Rust ACID KV, copy-on-write B+trees | SQL, SQLite 3.53.2 compiled from source | the store the log already uses | pure-Rust KV, 0.34 line | projections as state-machine records |
| New dependency | `redb` 4.3.0 | `rusqlite` 0.40.2 + `libsqlite3-sys` 0.38.2 + a C build | none | `sled` 0.34.7 | none |
| Licence | MIT OR Apache-2.0 ✅ allowlisted | crates MIT ✅; bundled SQLite public domain ✅ | already bundled ✅ | MIT/Apache ✅ | — |
| Advisories | none found; confirm with `mise run audit` once added | none expected (SQLite ships its own CVE process) | already accepted | **unmaintained deps**: `instant` (RUSTSEC-2024-0384) and `fxhash` — a documented ignore would be required | — |
| Maintenance | active: 4.3.0 (2026-09), 2.6.x line also patched | very active | vendor-managed, already tuned here | 0.34.7 is from 2022; 1.0 has been in alpha for years | our code |
| Concurrency | single writer + MVCC readers, serializable, non-blocking reads | single writer + WAL readers | full multi-writer, but hand-rolled consistency | concurrent | one writer (consensus) |
| Queries | hand-encoded keys, range scans | **SQL**: joins, `ORDER BY`, `WHERE`, indexes | hand-encoded keys, range scans | hand-encoded keys | hand-rolled in Rust |
| Rebuild | drop the file, replay | `DROP TABLE`, replay | delete a key prefix, replay | drop, replay | replay through consensus |
| Isolation from the log | separate file per projection | separate file | **shares** the group's database | separate | — |

Versions and licences were checked on crates.io/docs.rs (2026-10); the sled
advisories are from the upstream issue trail (`spacejam/sled#1513`, `#1514`).

## What each implies

**redb** — the closest match to the requirement list. One writer with MVCC
readers *is* the projector model, so there is no impedance mismatch; it is ACID
and crash-safe; it is pure Rust, so the licence and build story stay as they are;
and it is exactly what
[storage-engine-alternatives.md](storage-engine-alternatives.md) shortlisted for
this slot before RocksDB was chosen for the log. The cost is that query encoding
is ours: ordered keys with the query's leading terms first, so a page is a range
scan and a secondary lookup is a second table maintained by the projector.

**rusqlite (bundled)** — buys real SQL: ad-hoc queries, joins, `ORDER BY`, and
later `sqlite-vec` in the same file ([D7](../design.md#d7--vector-store-phase-4)).
The cost is a C dependency compiled at build time, a second engine's failure modes
(pragma tuning, WAL checkpoints), and version skew between the bundled SQLite and
what we assume. It is the choice to make if Phase 4 needs vectors and SQL-side
filtering in one artifact, and the wrong one if we want to keep the tree pure Rust
and the projections dumb.

**RocksDB, reused** — no new dependency and no new operational surface, at the
price of putting derived data in the same database (and therefore the same
backup, compaction and locking story) as the log. A key-space split makes it
correct but not isolated; "rebuild this projection" becomes a prefix delete that
must never touch log keys. Defensible for a first slice; not what I would choose
for the long-lived read path.

**sled** — the option the earlier note leaned towards, now clearly the wrong one:
0.34.7 predates the unmaintained advisories in its own dependency tree, and 1.0
has stayed in alpha. Taking it would mean documenting advisory ignores to run a
store that the ecosystem is moving off. **Not recommended**, even for a spike —
redb is the pure-Rust option that does not need an exception.

**Consensus-backed projections** — no rebuild path, no new dependency, reads are
local and always consistent. But it re-creates what D13 is undoing: the state
machine grows with the projection, checkpoints carry it, and a rebuild has to go
through consensus. Right for *small hot* projections (the in-memory `dashmap` +
snapshot pattern already in the architecture), wrong for the growing ones.

## Recommendation

**redb**, one database file per group (holding all of that group's projection
tables), rebuilt by replaying the append-only record. Rationale: it satisfies
every requirement without a C build, a new advisory ignore, or a query-time
network hop; its single-writer/MVCC model matches the projector exactly; and the
decision is reversible — projections are derived, so changing the store later
costs a rebuild, not a migration.

Take **rusqlite** instead if Phase 4 wants vectors and SQL in one file before the
first projection ships; take **RocksDB reuse** only if adding a dependency is
worse than sharing the log's database for the first slice.

**Decided by:** open — this note is the input; the register entry
[D9](../design.md#d9--storage-of-cold-read-model-state) is updated once chosen.

## Sources

- redb: <https://github.com/cberner/redb> · <https://crates.io/crates/redb> ·
  <https://github.com/cberner/redb/blob/master/docs/design.md> (MVCC, single
  writer)
- rusqlite: <https://crates.io/crates/rusqlite> (licences, bundled builds) ·
  SQLite copyright: <https://www.sqlite.org/copyright.html>
- sled maintenance: <https://github.com/spacejam/sled/issues/1513> ·
  <https://github.com/spacejam/sled/issues/1514>
- RustSec advisories: <https://rustsec.org/advisories/>
