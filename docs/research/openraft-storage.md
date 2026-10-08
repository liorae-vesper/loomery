---

> **Research note for the Loomery project.** Informs decisions D1 + D2 (OpenRaft
> consensus + storage). Replacements/observations verified against the OpenRaft
> 0.9 example implementations and upgrade guides at the time of writing —
> re-verify against the crate's own docs before relying on specifics.

# OpenRaft Storage Interfaces — Research

> Current implementation: OpenRaft 0.10.0-alpha.36 with the split storage traits (`RaftLogStorage`/`RaftLogReader`, `RaftStateMachine`) and `RaftNetworkV2` RPCs `append_entries`, `vote`, and `full_snapshot` — the last fragmented and reassembled by our transport, not by the core. Earlier V2/example sketches below are research context, not the implemented API; the migration and its behaviour changes are recorded in [openraft-010-migration.md](openraft-010-migration.md). See [the shell reference](../shell.md) and [checkpoint policy](checkpoint-policy.md) for current recovery behavior.

OpenRaft 0.9 splits storage into two traits that Loomery implements behind
decision D2: **`RaftLogStorage`** (the replicated log) and **`RaftStateMachine`**
(apply + snapshots). This note collects what a first implementation needs to
know, keyed to the `crates/` layout in `design.md`.

---

## 1. The split (0.8 → 0.9)

In 0.8 the two were fused in a single `RaftStorage`. In 0.9 they are separate
so log and state-machine operations can run in parallel:

```rust
// 0.8 style:
let store = MyRaftStorage::new();
Raft::new(..., store);

// 0.9 style:
let (log_store, sm) = Adapter::new(store); // or implement both traits directly
Raft::new(..., log_store, sm);
```

Loomery implements `RaftLogStorage` + `RaftStateMachine` directly (no Adapter),
or starts from the examples in §5.

## 2. `RaftLogStorage`

Owns the log; supports reading, appending, and purging. Key methods
(pseudonymous — check the trait for exact signatures):

- `get_log_state() -> Result<LogState, StorageError>` — the log's
  `last_purged_log_id` and `last_log_id` (what exists vs what's been purged).
  Correct on restart: replicas rely on it to know where the log resumes.
- `try_append_entry(&mut self, entry) -> Result<_, StorageError>` and
  `try_append_entries(&mut self, entries)` — append at the given log IDs.
  Contracts: append `(log_id)` must be `(prev_log_id + 1)`; conflicting entries
  (per Raft invariants) must be removed first.
- Reading: `get_log_reader()` (a `RaftLogReader` for ranges) — used for
  replication and snapshot building.
- `save_committed(&self, committed: u64)` — **optional**; persists the
  committed index so startup can immediately apply committed-but-unapplied
  entries to the state machine. Not implementing it doesn't affect
  correctness, only recovery speed. Loomery should implement it (fast RYW
  catch-up after restart).

**Gotchas observed:**
- The log and state machine run in **parallel** in 0.9 — the log must keep
  entries the state machine hasn't applied yet; `get_log_state` answers
  "what does the state machine still need?"
- Storage errors must be `StorageError::IO`-cosmetic for OpenRaft's error
  surface (`ErrorSubject`/`ErrorVerb`) — map filesystem errors cleanly, don't
  panic.
- Log appends are the hot path of every `client_write`; a batched-fsync WAL
  (see `indexed-segment-file-format.md`) is what keeps commit latency stable.

## 3. `RaftStateMachine`

Holds the applied state; supports apply and snapshot:

- `apply(&mut self, entries) -> Vec<R::AppDataResponse>` — fold committed log
  entries into in-memory state. **This is where Loomery's pure `apply` runs**:
  `StateMachine::apply` decodes the command envelope and calls the pure core.
  Must be deterministic — replicas must derive identical state from identical
  entries.
- `get_snapshot_builder() -> SnapshotBuilder` then `build_snapshot()` — build a
  snapshot from current state (async, spawnable; keep the hot path clear).
  `SnapshotMeta` carries `last_log_id` + `last_membership` so a snapshot is a
  consistent restart point.
- `begin_receiving_snapshot()`, `install_snapshot()`, `get_current_snapshot()`
  — ingest + serve snapshots for lagging replicas (leader-side `full_snapshot`
  RPC, optionally chunked via `Chunked::send_snapshot`).

**Gotchas observed:**
- Snapshotting is **async and idempotent**: snapshot builders can be abandoned
  if a newer one starts; checkpoint data by `last_applied_index` (the golden
  backup rule in `storage-engine-alternatives.md`).
- After `install_snapshot`, everything with log id ≤ snapshot's is gone from
  the log store — `get_log_state` must reflect that for the coordinator.
- State machine serialization is application-defined: Loomery snapshots are
  versioned projections (envelope-versioned; upcast on load — D3).

## 4. Wiring into a Loomery Raft group

```
Command (HTTP gateway)
   └─> Raft::client_write(ClientWriteRequest)
         └─> log replication (RaftNetworkV2: append_entries/vote/full_snapshot)
               └─> RaftLogStorage::try_append_entries (leader + followers)
               └─> commit
                     └─> RaftStateMachine::apply  →  core::aggregate::apply
                           └─> in-memory state (dashmap read model)
                                 └─> outbox tailer → NATS JetStream
```

The `raft-kv-memstore` example is the reference for this flow; Loomery swaps
the KV store for the pure-core `apply`/`Execution` decoding.

## 5. Example implementations to crib

| Example | Backend | Notes |
|---|---|---|
| `MemStore` | In-memory | Simplest correct baseline; tests + property harness |
| `SledStore` | sled | Lowest integration risk for Phase 1 (D2) |
| `RocksStore` | rocksdb | Throughput escape hatch; C++ dep |
| (an `sqlite-raft`-style store) | rusqlite | If read models ever share a DB |

## 6. Version-specific caveats

- v0.9 removed `RaftNetwork::send_xxx()` in favor of `RaftNetwork::xxx()`
  (e.g. `append_entries`, `vote`, `full_snapshot`), all taking an `RPCOption`
  (hard/soft TTL).
- Async traits use `#[openraft-macros::add_async_trait]` (not
  `#[async_trait]`); `Raft<C, N, LS, SM>` generic parameterization was
  simplified to `Raft<C>` (components resolved from `RaftTypeConfig`).
- Pin OpenRaft to a single 0.9.x version in the workspace: the API is still
  moving between minors; `cargo deny` bans wildcards already.

## Sources

- OpenRaft getting-started + upgrade guides (0.7→0.8, 0.8→0.9):
  https://github.com/databendlabs/openraft/tree/main/openraft/src/docs
- Storage/examples (MemStore, SledStore, RocksStore): https://github.com/databendlabs/openraft/tree/main/examples
- Change log (0.9 split, RaftNetwork API changes, `save_committed`):
  https://github.com/databendlabs/openraft/blob/main/change-log.md

*Compiled: 2026-08 (replaces the OTP-era `otp29-dets-binary-path.md` note
from the Elixir design; no longer applicable).*