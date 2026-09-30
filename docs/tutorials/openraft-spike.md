# OpenRaft spike — a working control group

This walkthrough builds the original in-memory baseline. Persistent networking,
TLS and both recovery modes are now implemented; see the
[current shell reference](../shell.md) and [configuration guide](../raft-configuration.md).


Design §5, Phase 1, item 1: keep the consensus machinery behind
[`GroupOps`](shell-group.md) and get a group that can commit a command and hand
its events back.

## Definition of done, in three levels

1. **Single node, in process:** `Raft::new` on an in-memory log + state machine,
   `initialize` as a one-node cluster, one `client_write` → the response comes
   back applied. Tests green, no network.
2. **The port:** a `GroupOps` adapter over that handle, so
   [`bootstrap::run`](../../crates/shell/src/bootstrap.rs) drives genesis on a
   real group, and the tests from `genesis-worker.md` §9 pass against it.
3. **Multi-node:** `RaftNetworkFactory`/`RaftNetwork` over tonic, a second node
   joining, then leader failover. Only after 1 and 2 are boring.

Do not start with 3. Almost every "OpenRaft is hard" story is a storage bug
(level 1) that level 3 made hard to see.

## 0. Facts verified against the pinned crate (0.9.25)

Everything below was read out of the vendored source
(`~/.cargo/registry/src/*/openraft-0.9.25/src/…`), not from memory. Re-check it
there when you upgrade — the API moved between every 0.7/0.8/0.9 and 0.10 is
currently an alpha, so **stay on one 0.9.x**.

| Fact | Value | Source |
|---|---|---|
| Workspace version | `0.9.25` | Cargo.lock |
| **Required feature** | `storage-v2` — the split `RaftLogStorage`/`RaftStateMachine` traits only exist with it (without it you get the legacy, fused `RaftStorage`) | `src/lib.rs:108`, `src/storage/mod.rs:3` |
| Serialization | `serde` feature | `Cargo.toml` |
| Construct | `Raft::new(id, Arc<Config>, network, log_store, state_machine) -> Result<Raft<C>, Fatal<NodeId>>` | `src/raft/mod.rs:230` |
| First boot | `initialize(members)` (one node for level 1) | `src/raft/mod.rs:712` |
| Write | `client_write(app_data: C::D) -> Result<ClientWriteResponse<C>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>` | `src/raft/mod.rs:651` |
| Write response | `{ log_id: LogId<NodeId>, data: C::R, membership: Option<Membership<..>> }` | `src/raft/message/client_write.rs:22` |
| Write errors | `ClientWriteError::{ForwardToLeader, ChangeMembershipError}` | `src/error.rs:188` |
| Log store | `get_log_state`, `get_log_reader`, `save_vote`, `read_vote`, `save_committed`*, `read_committed`*, `append(entries, LogFlushed<C>)`, `truncate`, `purge` (*defaulted) | `src/storage/v2.rs:50-143` |
| Log reader | `try_get_log_entries(range)` | `src/storage/mod.rs:159`, guide §3 |
| State machine | `applied_state`, `apply(entries) -> Vec<C::R>`, `get_snapshot_builder`, `begin_receiving_snapshot`, `install_snapshot`, `get_current_snapshot`; `type SnapshotBuilder: RaftSnapshotBuilder<C>` | `src/storage/v2.rs:153-256` |
| Snapshot builder | `build_snapshot()` | `src/storage/mod.rs:200` |
| Network | `RaftNetworkFactory { type Network: RaftNetwork<C>; async fn new_client(target, node) }`; RPCs `append_entries` / `vote` / `install_snapshot` | `src/network/factory.rs:17`, `src/network/network.rs:41,76,104` |
| Runtime | `AsyncRuntime` is an associated type with a built-in `openraft::TokioRuntime` — no feature flag needed | `src/async_runtime.rs:105` |
| Type config | `declare_raft_types!` (see §2) | `src/type_config.rs:33`, guide §5 |

The crate also ships its own guide inside the source tree — read it there, it is
the authoritative version:

```
~/.cargo/registry/src/*/openraft-0.9.25/src/docs/getting_started/getting-started.md
  §1 define request/response · §2 define types · §3 implement the two storage
  traits · §4 implement RaftNetwork · §5 put it together · §6 run the cluster
```
plus `src/docs/faq/`, `cluster_control/`, `feature_flags/`, `upgrade_guide/`.

## 1. Add the dependency

```toml
# crates/shell/Cargo.toml
openraft = { version = "0.9.25", features = ["storage-v2", "serde"] }
```

`cargo deny check` is happy (MIT/Apache-2.0, crates.io) — but re-run
`mise run verify` after adding it, because the advisory/licence set changes.

## 2. App data, response, type config

```rust
use loomery_core::envelope::Command;
use openraft::TokioRuntime;

/// What clients write to a group. Level 1 only ever writes commands.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum AppData {
    /// A domain command for the pure core.
    Command(Command),
}

/// What the state machine answers a writer with — the map to
/// `group::ProposeOutcome` is one-to-one.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Applied {
    /// The command produced events; the first landed at this index.
    Appended { first_log_index: u64 },
    /// The causation key was already processed; nothing new was appended.
    Replayed { first_log_index: u64 },
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D            = AppData,
        R            = Applied,
        NodeId       = u64,
        Node         = openraft::BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);
```

`D`/`R` must satisfy `AppData`/`AppDataResponse` (blanket-implemented for
`Clone + Debug + Serialize + Deserialize + Send + Sync + 'static` types) — if the
macro complains, check the bound list in `src/entry/`.

## 3. The log store (level 1: in memory)

Implement `RaftLogReader` + `RaftLogStorage<TypeConfig>` over
`Arc<Mutex<…>>`-ish state. The rules that actually need care:

* **`get_log_state`** returns `(last_purged_log_id, last_log_id)` — what exists
  versus what has been compacted away. It must be correct after a restart; it
  must *not* consult the state machine's applied index.
* **`append`** must remove any conflicting suffix first (Raft re-writes the tail
  after a leadership change), must honour the `LogFlushed` callback once the
  entries are durable, and is the hot path of every write.
* **`save_committed`/`read_committed`** are optional but implement them: they
  let a restarted node apply committed-but-unapplied entries immediately, which
  is exactly the fast Read-Your-Writes catch-up in design §4.
* **`truncate`/`purge`** must not throw away entries the state machine has not
  applied. The log and the state machine run in parallel in 0.9.
* Map every I/O failure to `StorageError` with a sensible `ErrorSubject`/verb;
  never panic — this code runs inside the Raft core.

The `MemStore` example referenced by
[`docs/research/openraft-storage.md`](../research/openraft-storage.md) §5 is the
shape to copy for the in-memory version (`read_index`, `log_state`, a `VecDeque`
of entries, the flush sender).

## 4. The state machine

```rust
async fn apply<I>(&mut self, entries: I) -> Result<Vec<Applied>, StorageError<u64>>
where
    I: IntoIterator<Item = C::Entry> + OptionalSend,
    I::IntoIter: OptionalSend,
{
    // for each entry: EntryPayload::Blank => no-op,
    //                 EntryPayload::Normal(AppData::Command(cmd)) => {
    //     let outcome = core process(cmd) against the aggregate state;
    //     record the dedup entry; return Applied::{Appended,Replayed}
    // }
}
```

* `applied_state()` returns `(last_applied_log_id, last_membership)` — the
  membership half matters for restarts and snapshots.
* The apply body is where the **pure core** runs (`AggregatePlan::process` /
  `apply`). Keep it deterministic: no clock, no randomness. The dedup registry
  ([`dedup::Registry`](../../crates/core/src/dedup.rs)) is *folded state*, so a
  replay rebuilds it — which is what makes the "committed but not yet recorded"
  window survivable.
* Keep the committed events where
  [`GroupOps::committed_events`](shell-group.md) can read them per
  `organization_id` (write-through to an event store, or read the applied log).
* Snapshots: level 1 can store a snapshot of the applied state and return it;
  `begin_receiving_snapshot`/`install_snapshot` can start minimal, but they must
  compile and be correct enough that a follower can catch up later.

## 5. Boot a single node, write once

```rust
let config = Arc::new(openraft::Config::default().validate()?);
let raft = openraft::Raft::new(NODE_ID, config, network, log_store.clone(), sm.clone()).await?;
raft.initialize([(NODE_ID, openraft::BasicNode::default())]).await?;   // one-node cluster

let response = raft.client_write(AppData::Command(command)).await?;
//  response.log_id, response.data: Applied::{Appended{first_log_index}|Replayed{..}}
```

Then assert the events are readable back through `sm` — that is the end of
level 1. Write the test first: it pins the storage semantics before the network
exists.

## 6. The `GroupOps` adapter (level 2)

```rust
pub struct RaftGroup { raft: openraft::Raft<TypeConfig>, events: Arc<EventStore> }

impl GroupOps for RaftGroup {
    fn committed_events(&self, organization_id: &Id) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send { … }

    fn propose(&mut self, command: Command) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send {
        async move {
            let response = self.raft.client_write(AppData::Command(command)).await.map_err(classify)?;
            Ok(match response.data {
                Applied::Appended { first_log_index } => ProposeOutcome::Appended { first_log_index },
                Applied::Replayed { first_log_index } => ProposeOutcome::Replayed { first_log_index },
            })
        }
    }
}
```

* Use `client_write`, **not** `client_write_ff`: genesis must know ① committed
  before proposing ②.
* `classify` maps `RaftError::APIError(ClientWriteError::ForwardToLeader(_))` and
  transport errors to *retryable*; the caller's contract (see
  `shell-group.md`) is that every error is an unknown outcome, so retry means
  "re-read, then decide".
* Then run `bootstrap::run(&mut port, &bootstrap)` against it in a test — the
  genesis acceptance list in `genesis-worker.md` §9 now exercises real storage,
  real apply and real dedup.

## 7. Multi-node (level 3, implemented)

* `RaftNetworkFactory::new_client(target, node)` → your `RaftNetwork`, whose
  RPCs are `append_entries`, `vote`, `install_snapshot` (tonic service in the
  shell). The factory is passed to `Raft::new`; it is *not* a connection.
* `BasicNode { addr }` carries the address; the shell's router (design §4) owns
  the `organization_id → group` mapping and leader hints.
* Membership changes go through the Raft API (`change_membership`), and the
  genesis worker's job (design §5 item 1 + research note) includes driving the
  initial membership so the group is born with voters.
* Only now wire Read-Your-Writes (`X-Min-Index` + `RaftMetrics`), and only now
  bother with `save_committed`-driven catch-up.

## 8. Spike acceptance

- [x] level 1: one node, one write, events readable; **restart the group from
      the same store and see the state survive**
- [x] level 1: a second write with the *same* `AppData` yields `Replayed`, not a
      second event
- [x] level 2: `bootstrap::run` reaches `Progress::is_complete()` through
      `RaftGroup`, with exactly one event per genesis step
- [x] level 2: crash-resume — drop the port mid-bootstrap (or restart the
      process), re-run, still exactly one event per step
- [x] `mise run verify` and `mise run test` green

The levels 1–2 store lives in `crates/shell/src/raft/` and is additionally held
to `openraft::testing::Suite` (`raft/suite.rs`), so the log/purge/snapshot
semantics are checked against OpenRaft's own contract, not only against the
group's tests.

## 9. Gotchas

* **Forgetting `storage-v2`** leaves you implementing the older, fused trait —
  the split-trait research note and this doc both assume the feature is on.
* **`get_log_state` consulting the applied index** is the classic bug: the log
  must remember more than the state machine has applied.
* **Non-deterministic `apply`** (a clock read, a minted id, a `HashMap`
  iteration order that leaks into state) diverges replicas. The core is built to
  make this hard — keep the adapter dumb.
* **Snapshotting is async and may be abandoned**; key everything by
  `last_applied_index`.
* **Do not bump the minor version casually** — 0.9.x API churn is real, and 0.10
  is an alpha. One pin, one upgrade at a time.
