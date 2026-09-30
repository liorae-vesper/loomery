# The Loomery shell — how `crates/shell` works

This is the reference for the imperative shell as it exists today: the group
port, the async genesis worker, and the in-process OpenRaft group behind them
(spike **levels 1–2**). It is written to be read alongside the design and the
tutorials, not instead of them:

| Document | Read it for |
|---|---|
| [`design.md`](design.md) §2, §4 | why the shell exists and what it owns |
| [`tutorials/shell-group.md`](tutorials/shell-group.md) | the `GroupOps` contract, narrated |
| [`tutorials/genesis-worker.md`](tutorials/genesis-worker.md) | the worker, stage by stage |
| [`tutorials/openraft-spike.md`](tutorials/openraft-spike.md) | glueing OpenRaft 0.9 to the port |
| [`research/openraft-storage.md`](research/openraft-storage.md) | the storage traits' semantics |

Everything below is what the code actually does, down to method names. Where a
piece is planned but not built, it says so.

---

## 1. Where the shell sits

Loomery is a **functional core, imperative shell** system
([`design.md`](design.md) §2). The core (`crates/core`) is pure: it takes a
command plus state and returns events; it never reads a clock, mints an id,
touches I/O, or knows that consensus exists. The shell is everything that *is*
allowed to do those things.

```text
                            ┌──────────────── crates/shell ────────────────┐
  control plane  ──┐        │                                              │
  gateway        ──┼──► GroupOps { committed_events, propose }            │
  sagas/workers  ──┘        │        │                                     │
                            │        ├── bootstrap::run   (the worker)     │
                            │        └── raft::RaftGroup  (the adapter)    │
                            │                 │                            │
                            │        ┌────────▼─────────┐                  │
                            │        │ OpenRaft 0.9     │                  │
                            │        │ ├ MemLogStore    │ log + vote       │
                            │        │ └ MemStateMachine│ apply + snapshot │
                            │        └────────┬─────────┘                  │
                            └─────────────────┼────────────────────────────┘
                                              │ AggregatePlan::process / apply
                                              ▼
                                   crates/core (pure) + crates/genesis (script)
```

The shell's job list ([`design.md`](design.md) §2.2) and what is built:

| Shell responsibility | Today |
|---|---|
| **Consensus** — one Raft group per organization | in-memory single node (`raft::RaftGroup`) |
| **Persistence** — `RaftLogStorage` + `RaftStateMachine` | in-memory (`MemLogStore`, `MemStateMachine`) |
| **Workers** — drive pure scripts through consensus | genesis bootstrap worker (`bootstrap::run`) |
| **Transport** — `RaftNetwork` over tonic | not built (single node uses `NoopNetworkFactory`) |
| **Routing** — `organization_id → group` | not built (no control group yet) |
| **Gateway** — axum command plane, edge pre-compute | not built |
| **Outbox** — NATS JetStream tailer | not built |
| **Recovery** — snapshot + replayed log on boot | built into the store/SM; no process wiring yet |

**One group = one `organization_id`.** A group is an independent Raft instance
with its own log and state machine. The same binary is meant to host many
co-resident groups ([`design.md`](design.md) D1); today the shell can boot one
at a time.

---

## 2. Crate map

```text
crates/shell/src/
├── lib.rs                  crate docs, module list, the test-only lint escape hatch
├── group.rs                GroupOps + ProposeOutcome — the port (no Raft in sight)
├── bootstrap.rs            the genesis worker: bootstrap::run, Genesis, Error
├── test_support.rs         cfg(test)-only fake group + fixtures
└── raft/
    ├── mod.rs              wire types (AppData, Applied) + TypeConfig
    ├── log_store.rs        MemLogStore — RaftLogReader + RaftLogStorage
    ├── state_machine.rs    MemStateMachine — RaftStateMachine + RaftSnapshotBuilder
    ├── network.rs          NoopNetworkFactory — single-node only
    ├── port.rs             RaftGroup — the GroupOps adapter, ProposeError
    └── suite.rs            cfg(test)-only OpenRaft conformance suite run
```

Dependency direction is one-way and enforced by the manifests:

```text
loomery-shell ──► loomery-genesis ──► loomery-core
       └────────────────────────────► loomery-core
```

`loomery-core` never imports the shell. That is what keeps the core pure and
replayable; a core module that "just needs one thing from the shell" is a
design error, not a convenience.

The crate-level lint escape hatch in `lib.rs` matters when reading the code:

```rust
#![cfg_attr(test, allow(
    clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing,
    clippy::string_slice, clippy::arithmetic_side_effects
))]
```

Production code in `crates/shell` has **no** `unwrap`/`expect`/panicking index —
the workspace denies them. Only `#[cfg(test)]` modules may use them, and they do
freely.

---

## 3. The port — `group.rs`

Everything in the shell that writes to a group or reads what it committed goes
through two methods. Nothing above this line knows about Raft, storage, or
leadership.

```rust
pub trait GroupOps: Send + Sync {
    fn committed_events(
        &self,
        organization_id: &Id,
    ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send;

    fn propose(
        &mut self,
        command: Command,
    ) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send;
}

pub enum ProposeOutcome {
    Appended { first_log_index: u64 },
    Replayed { first_log_index: u64 },
}
```

### Why the futures are spelled out

The methods return `impl Future<Output = …> + Send` instead of using `async fn`.
An `async fn` in a trait gives **no `Send` guarantee** (and warns,
`async_fn_in_trait`), and the worker is spawned onto Tokio — it must be `Send`.
Writing the bound out is the lint-clean way to promise it. (In Rust 2024 the
returned opaque type also captures the `&self`/`&mut self` lifetime, so
implementations are ordinary borrow-checked code.)

### The three contracts

These are the load-bearing rules; the rest of the shell is written assuming
them.

1. **Reads are *applied*, not *appended*.** `committed_events` answers from the
   state machine's applied state. If it read the log tail, a caller asking "did
   my command commit?" could be told "no" while the events are already durable
   behind an unapplied entry.
2. **`propose` errors mean *unknown outcome*.** A timeout, a leadership change,
   a lost response — never "nothing happened". The caller must re-read before
   re-proposing. This is *the* reason the worker re-reads on every iteration.
3. **`propose` is idempotent.** Identity is derived (D12), so the state
   machine's dedup window answers `Replayed` for a command it has already
   processed. That is what makes at-least-once retries safe and lets two
   workers race a bootstrap without duplicating it.

### What is deliberately *not* in the port

- **No `is_recorded`/dedup pre-check.** The state machine answers that when the
  command is proposed (`Replayed`). A worker-side pre-check would add a race for
  no benefit.
- **No `progress()` cache.** Re-reading the log is what makes restarts trivial.
- **No `mint_id()`.** Nothing in the worker creates identity; the command
  envelope carries everything.

Every extra method here is one more thing each implementation (production Raft
port, test fake) must get right, for callers that never need it.

---

## 4. The genesis worker — `bootstrap.rs`

The genesis script (`crates/genesis`) knows *what* the first three commands of
a tenant are; the worker knows *how* to get them committed. It is the smallest
possible client of the port, and the template every later worker/saga follows.

```rust
pub async fn run(port: &mut impl GroupOps, bootstrap: &Bootstrap) -> Result<Genesis, Error> {
    let mut appended = Vec::new();

    loop {
        // Always re-read: a previous iteration — or a previous process — may
        // have committed a step without us ever learning the result.
        let events = port.committed_events(&bootstrap.organization_id).await
            .map_err(Error::Read)?;
        let progress = bootstrap.progress(&events);          // pure script

        let Some(step) = progress.next() else {
            return Ok(Genesis { appended, progress });       // ①②③ all in the log
        };

        let command = bootstrap.command(step)?;              // pure script

        match port.propose(command).await
            .map_err(|source| Error::Propose { step, source })?
        {
            ProposeOutcome::Appended { .. } => appended.push(step),
            ProposeOutcome::Replayed { .. } => {}            // someone got there first
        }
    }
}
```

Read it as **"ask, act, ask again"**:

1. **Ask** the group for its applied events.
2. Ask the **script** which steps those events contain.
3. Ask the script for the next step's command (or stop).
4. **Act**: propose it.
5. Repeat — the re-read is what tells you whether step 4 committed.

### The result

```rust
pub struct Genesis {
    pub appended: Vec<Step>,   // only what THIS run appended
    pub progress: Progress,    // complete on every Ok return
}
```

`appended` is per-run on purpose: a resumed run reports only the steps it was
missing, which is exactly what a reconciliation log wants.

### The error surface

```rust
pub enum Error {
    Read(#[source] anyhow::Error),                     // the group could not be read
    Propose { step: Step, source: anyhow::Error },     // a step could not be appended
    Plan(#[from] genesis::Error),                      // the plan refused to build a command
}
```

`Error::Plan` cannot happen in practice — payloads are ids and strings — but it
is kept in the surface rather than `unwrap`ped, because production code here
does not panic.

### Why a crash has no recovery code

Every crash lands in one of four windows, and all four are covered by "ask
again":

| Crash point | What the group has | What the resume does |
|---|---|---|
| before `propose` | nothing new | proposes the same command (derived identity) |
| during `propose` (timeout, leader change, lost response) | maybe the command, maybe nothing | re-reads first: if it committed, the step is done |
| after append + apply, before the client heard | the event is in the log | `progress` sees it; the step is skipped |
| after everything | complete | `progress.next() == None` → returns immediately |

### The two easy mistakes

- **`Replayed` is not an error.** It means another attempt (yours, retried, or
  a second worker) got there first — that is *success*. It must not be pushed
  onto `appended` and must not be retried.
- **The re-read is not an optimisation.** Remove it and the loop is wrong the
  first time a proposal times out after committing: you would propose ② again
  and rely on the dedup window instead of on the log. The window is bounded; the
  log is the truth.

The worker mints nothing: no `Id::new()`, no `Timestamp::now()`. The only
nondeterministic input is `Bootstrap::occurred_at`, stamped once by the caller,
so two attempts at the same step produce byte-identical envelopes.

---

## 5. The Raft group — `raft/`

This is the storage half of the port. `raft/mod.rs` holds the wire types and
the type config; the rest is one concern per file.

### 5.1 Wire types and type config

```rust
pub enum AppData { Command(Command) }          // what clients write

pub enum Applied {                             // what the state machine answers
    Appended { first_log_index: u64 },
    Replayed { first_log_index: u64 },
    Rejected { code: String, message: String },
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppData, R = Applied,
        NodeId = u64, Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
```

`Appended`/`Replayed` map one-to-one onto `ProposeOutcome`. `Rejected` has no
port equivalent: it is a committed command whose payload the aggregate refused,
and `RaftGroup::propose` surfaces it as an error. Rejection is *supposed* to be
impossible for genesis — validation belongs at the gateway, before a command
enters consensus (D10) — but the type has to represent it, because "probably
can't happen" is not a reason to make a state machine panic.

`NodeId = u64` is deliberately process-local for the spike; the shell's router
is what will map `organization_id → group`.

### 5.2 The log store — `log_store.rs`

`MemLogStore` is a `BTreeMap<u64, Entry<_>>` plus the vote and commit pointers,
behind a `tokio::sync::RwLock` and cloned as an `Arc`.

Three correctness rules, all visible in the code:

- **`get_log_state` never consults the state machine's applied index.** It
  returns `(last_purged_log_id, last_log_id)` from the log alone — the log must
  remember more than the state machine has applied.
- **`append` drops any conflicting suffix first.** If the incoming batch starts
  at index *i*, everything at ≥ *i* is removed before writing, so a re-written
  tail after a leadership change cannot leave a hole. Once the lock is released
  the entries are "durable" and the `LogFlushed` callback is fired.
- **`purge` does not touch the state machine.** It removes entries at or below
  the purged id and records the new floor; deciding that the state machine no
  longer needs them is OpenRaft's job.

`save_committed`/`read_committed` are implemented (not just defaulted): they are
what lets a restarted node catch up by re-applying committed-but-unapplied
entries — the mechanism the restart test exercises.

### 5.3 The state machine — `state_machine.rs`

This is where the **pure core actually runs**. One group's state:

```rust
struct GroupState {
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    streams: BTreeMap<Id, AggregateState>,   // one entry per aggregate instance
    events: Vec<Event>,                      // every applied event, in log order
    registry: Registry,                      // the core's dedup window
    dedup: Vec<(Key, Key, usize)>,           // the window in insertion order (snapshots)
}
```

`AggregateState` is the per-stream tag: `Organization`, `Workspace`, or
`Membership`, each wrapping the core's state struct. The `streams` map is keyed
by `aggregate_id`, so a group hosts many aggregate instances the way a group of
one organization should.

#### The apply pipeline

For every committed entry, `RaftStateMachine::apply`:

1. advances `last_applied_log`;
2. matches the payload:
   - `Blank` → no-op, answers `Appended`;
   - `Membership` → stores `last_membership`, answers `Appended`;
   - `Normal(Command)` → **dispatch**: `apply_command` matches
     `command.command_type` and drives the matching plan.
3. For a command, the plan is run through the core:

```text
   state          = streams[command.aggregate_id]  (or Default if new)
   Processed      = AggregatePlan::process(state, &registry, command)
   ├─ Executed(execution) → for each event: state = Plan::apply(state, event);
   │                          group.events.push(event);
   │                        streams[aggregate_id] = state;
   │                        registry.insert(causation_key, fingerprint, log_index);
   │                        dedup = registry.window_entries();
   │                        → Applied::Appended { log_index }
   ├─ Replayed { index }  → Applied::Replayed { index }        (nothing appended)
   └─ Error(domain)       → Applied::Rejected { code, message }
```

Two things are load-bearing here:

- **The dedup entry is recorded *after* the events are applied.** That is the
  core's critical-section contract (`crates/core/src/aggregate.rs`): a failed
  append needs no rollback, and a crash between append and record simply leaves
  a plain replay to repair.
- **A `Replayed` answer appends nothing.** The replayed command's original log
  index — not the retry's — is carried back, which is the Read-Your-Writes
  anchor a caller would use.

`committed_events(&organization_id)` returns `group.events` filtered by
organization. Because one group owns one organization, that is the group's whole
applied-event log; the filter keeps the answer honest if a group is ever shared.
It is *applied* state, so it satisfies the port's first contract.

#### Which plans exist today

`apply_command` dispatches three `command_type`s, the frozen genesis wire
contract:

| `command_type` | plan | `event_type` |
|---|---|---|
| `organization.assign_leader` (①) | `core::org::Organization` | `organization.leader_assigned` |
| `workspace.create` (②) | `core::workspace::Workspace` | `workspace.created` |
| `membership.add_owner` (③) | `core::membership::WorkspaceMembership` | `membership.owner_added` |

These are the **bootstrap slices** of the Phase-0 aggregates: real
`AggregatePlan` implementations, but only the commands genesis issues. Any other
`command_type` is answered `Rejected { "unknown_command" }` rather than guessed
at. An `aggregate_id` already owned by another aggregate kind is
`Rejected { "aggregate_kind_mismatch" }`.

> Adding a plan is a three-step change — see §9.

#### Snapshots

`SnapshotData` is the serializable projection of `GroupState`: applied log id,
membership, streams, events, and the dedup window in insertion order. The
`Registry` itself is not serializable, so:

- **build**: serialize `SnapshotData` (including `group.dedup`) into a
  `Cursor<Vec<u8>>`, record it as `current_snapshot`, tag it with the applied
  log id;
- **install**: decode, replace streams/events/membership/applied-id, then
  **rebuild** `Registry::new(DEDUP_WINDOW)` by inserting `dedup` entries back in
  order.

Rebuilding by insertion order reproduces the exact same window, which is why the
mirror exists instead of duplicating eviction logic. `DEDUP_WINDOW` is 4096 keys
— bounded on purpose: the window is a cache, the log is the durable answer.

`build_snapshot` takes the `current_snapshot` lock *before* releasing the state
lock, so a newer build cannot install over an older one (the ordering the
reference store uses).

### 5.4 The network — `network.rs`

`NoopNetworkFactory` exists only to satisfy `Raft::new` for a one-node cluster.
A single node never replicates, so the RPCs are never called; if one *is* called
it returns an `Unreachable` error with "this single-node group has no peers"
rather than silently pretending a peer answered. Replacing it with a real
`RaftNetwork` (tonic gRPC) is spike level 3.

### 5.5 The adapter — `port.rs`

`RaftGroup` holds the OpenRaft handle and the shared state machine:

```rust
pub struct RaftGroup {
    raft: Raft<TypeConfig>,
    state_machine: Arc<MemStateMachine>,
}
```

| Port method | What it does |
|---|---|
| `committed_events` | clones the `Arc<MemStateMachine>` and reads applied events directly — no consensus round trip |
| `propose` | `raft.client_write(AppData::Command(cmd))` and maps the response |

It uses **`client_write`, never `client_write_ff`**: genesis must know ① is
applied before proposing ②, and only `client_write` waits for apply and returns
the state machine's `Applied` value.

#### Failure classification

```rust
fn classify(error: RaftError<u64, ClientWriteError<u64, BasicNode>>) -> anyhow::Error
```

| OpenRaft error | `ProposeError` | Meaning for the caller |
|---|---|---|
| `APIError(ForwardToLeader)` | `ForwardToLeader { leader }` | route there and retry |
| `APIError(ChangeMembershipError)` | `Fatal` | blind retry is unsafe |
| anything else (timeout, transport, fatal) | `Unknown` | *unknown outcome* — re-read the log, then retry |

Backoff and routing live **outside** the worker: the port only says what
happened. Every variant except `Rejected` is "retryable" in the specific sense
that re-reading first is always correct.

#### Booting

```rust
RaftGroup::boot_single_node(node_id)     // fresh log + fresh state machine
RaftGroup::boot(node_id, log_store, sm)  // over existing stores; initializes only if pristine
RaftGroup::shutdown()                    // stop the OpenRaft task
```

`boot` with the **same log store** and a **fresh state machine** is the restart
test: OpenRaft sees `read_committed`, re-applies the committed log into the new
state machine, and the group's applied events are back. No snapshot needed, and
no bookkeeping in the worker.

---

## 6. End to end: what genesis actually does

```text
  caller (control plane, not built yet)
    │  Bootstrap { organization_id, leader_user_id, occurred_at }
    ▼
  bootstrap::run(&mut port, &bootstrap)
    │
    ├─ port.committed_events(org) ──► MemStateMachine::committed_events
    │        (empty)                    = applied events for org
    ├─ bootstrap.progress(events) ──► Progress { none }
    ├─ bootstrap.command(AssignLeader) ──► Command (derived ids/keys, ①)
    ├─ port.propose(cmd)
    │     └─ Raft::client_write(AppData::Command(cmd))
    │          ├─ MemLogStore::append        (log entry, index N)
    │          └─ MemStateMachine::apply
    │               └─ process::<OrganizationState, …>(…)
    │                    └─ event → group.events, registry.insert
    │               ◄── Applied::Appended { N }
    ├─ ... repeat for ② create workspace, ③ add owner ...
    └─ progress.next() == None  →  Genesis { appended: [①②③], progress }

  afterwards
    port.committed_events(org) ──► three events, in ①②③ order,
                                   actor = Saga{ control-plane:Bootstrap },
                                   correlation_key = bootstrap_correlation_key(org)
```

Properties this sequence guarantees, each asserted by a test (§8):

- **exactly one event per step**, even if the worker ran five times;
- **byte-identical commands** from two independent attempts (derived identity);
- **crash-resume**: a run that dies after ① proposes only ②③ on restart;
- **zero proposals** for an already-complete group;
- **restart from the log** rebuilds the applied events on a fresh state machine.

---

## 7. Invariants worth memorising

1. **The shell decides nothing.** It asks the script/aggregate what to do and
   proposes it. No ids, timestamps, or payloads are built in the loop.
2. **Nothing is minted in a worker.** The only clock read is the injected
   `occurred_at`, stamped once by the caller.
3. **Progress is read from the log, never remembered.** "Did my earlier attempt
   commit?" is a fact, not a guess.
4. **Read applied, not appended** — both in the port contract and in
   `MemStateMachine::committed_events`.
5. **`Replayed` is success.** Counting it as an append makes metrics lie and
   tests flaky.
6. **Re-read before re-proposing.** An error means unknown outcome.
7. **The dedup window is a cache, the log is truth.** It is bounded (4096) and
   eventually evicts; never build a decision on it that the log cannot answer.
8. **The core records dedup *after* apply.** Do not move that insert earlier to
   "simplify" — a failed append would then need a rollback.
9. **Production code does not panic.** `unwrap`/`expect`/indexing are test-only
   by lint, including in storage code that runs inside OpenRaft's task.

---

## 8. Testing the shell

Run everything with the repo gates:

```sh
mise run verify      # cargo check + clippy -D warnings + fmt --check + cargo deny
mise run test        # cargo test --workspace
cargo test -p loomery-shell   # just this crate
```

Today: **82 core + 24 genesis + 22 shell tests + 3 doctests.**

### The fake — `test_support.rs` (`cfg(test)` only)

`FakeGroup` is an in-memory `GroupOps`: it appends commands, "applies" an event
for each, dedups by `causation_key`, and — via `lose_response_after` — can
commit the *n*th append and then report a failure. That single knob reproduces
the nastiest crash window ("the write succeeded, the client never heard back")
without a cluster. `bootstrap_value()` and `organization()` are the pinned
fixtures; `committed()` counts events by step key.

### Where the tests live

| File | Tests | What they pin down |
|---|---|---|
| `bootstrap.rs` | 8 | the worker against the fake: happy path, exactly-once, completed group, lost response, mid-script start, retry storm, byte-identical attempts, step-carrying errors |
| `raft/log_store.rs` | 4 | empty state, range reads, truncate drops the suffix, purge moves the floor |
| `raft/state_machine.rs` | 4 | apply appends the event, replay dedups, unknown command rejects, snapshot round-trips state + dedup |
| `raft/port.rs` | 5 | **acceptance**: ①②③ in order with actor/correlation/workspace checks, crash-resume, replay on re-propose, restart from the same log rebuilds state, a completed group makes zero proposals (counting wrapper) |
| `raft/suite.rs` | 1 | OpenRaft's own `testing::Suite` over the store and state machine |

The worker tests use a hand-written fake so a crash can be positioned exactly;
the acceptance tests use a real `Raft` task, real log, and real apply, so
idempotency is exercised through consensus rather than around it.

`suite.rs` is the strongest single signal: `openraft::testing::Suite::test_all`
drives membership-in-log, purge/tail, snapshot transfer, and re-applying
committed entries against the store, so a storage bug surfaces here before a
cluster could hide it.

---

## 9. Extending the shell

### Add a new aggregate plan

1. **Core**: add the module (`crates/core/src/<area>.rs`) with the typed command
   and event payloads, the state struct, a code enum, and the
   `AggregatePlan` impl. Build events with
   `aggregate::event_from_command(&command, index, EVENT_TYPE, payload)` so
   identity stays derived.
2. **Dispatch**: extend `apply_command` in `raft/state_machine.rs` with the new
   `command_type`.
3. **State**: add a variant to `AggregateState` and a match arm in the relevant
   `apply_*` helper. `AggregateState` is serde, so old snapshots are forward-
   decoded only when their variant still matches — treat it as a wire format.
4. If the command is part of a script, add it to `crates/genesis` (or the future
   saga crate) and its golden tests.

### Add a new caller (gateway, saga, worker)

Depend on `GroupOps`, not on `raft`. The pattern is always:

```text
loop {
    let events  = port.committed_events(&org).await?;   // applied truth
    // decide from events (pure)
    // build the command with injected metadata (shell-side minting)
    match port.propose(command).await {
        Ok(Appended) => continue,
        Ok(Replayed) => continue,                        // success
        Err(_)       => continue,                        // unknown outcome → re-read
    }
}
```

If the decision logic gets hairy, it belongs in `crates/core` as a pure plan —
not in the caller.

### Add a real (durable) store

Implement `RaftLogReader` + `RaftLogStorage` and `RaftStateMachine` +
`RaftSnapshotBuilder` (`docs/research/openraft-storage.md` is the map), then
**run it through `openraft::testing::Suite`** exactly as `raft/suite.rs` does.
Only then wire it into `RaftGroup::boot`. A store that has not passed the Suite
is not ready to be believed.

### Go multi-node / add the control group

Replace `NoopNetworkFactory` with a `RaftNetwork` (`append_entries`, `vote`,
`full_snapshot`, `install_snapshot`) over tonic, put real `BasicNode` addresses
in the membership, and add the router that maps `organization_id → group`
(projected by the control group's state machine). That is spike level 3 —
[`tutorials/openraft-spike.md`](tutorials/openraft-spike.md) §7.

---

## 10. Current limits and next steps

- **In-memory only.** No persistence, no snapshots on disk. D2 (sled/rocksdb vs
  the hand-rolled segment engine) is still open.
- **Single node.** No network, no failover, no membership changes.
- **No routing or control group.** One group at a time; the router read model
  and RYW `X-Min-Index` hold are not built.
- **Stage 8 wiring.** The worker is ready but nothing calls it yet: tenant
  creation, startup reconciliation, and the optional retry sweep need the
  control plane (and an `organization_id → group` map) to exist.
- **Bootstrap-slice aggregates only.** `org`, `workspace`, and `membership`
  implement just the genesis commands; the full Phase-0 aggregates (and the
  other three) are still to come. Event/command names are the frozen contract,
  so they will not drift.
- **Proposed but unbuilt:** outbox → NATS, gateway, saga runner, observability
  (`tracing`), persistence.

The authoritative checklist is [`design.md`](design.md) §8; the session handoff
is [`CONTINUE.md`](CONTINUE.md).

---

## Reference

| File | What it is |
|---|---|
| `crates/shell/src/group.rs` | `GroupOps`, `ProposeOutcome` — the port |
| `crates/shell/src/bootstrap.rs` | `bootstrap::run`, `Genesis`, `Error` — the worker |
| `crates/shell/src/raft/mod.rs` | `AppData`, `Applied`, `TypeConfig` |
| `crates/shell/src/raft/log_store.rs` | `MemLogStore` |
| `crates/shell/src/raft/state_machine.rs` | `MemStateMachine`, `DEDUP_WINDOW`, snapshots |
| `crates/shell/src/raft/port.rs` | `RaftGroup`, `ProposeError`, `classify`, boot/shutdown |
| `crates/shell/src/raft/network.rs` | `NoopNetworkFactory` |
| `crates/shell/src/raft/suite.rs` | OpenRaft conformance run |
| `crates/shell/src/test_support.rs` | `FakeGroup` and fixtures (test-only) |
| `crates/genesis/src/script.rs` | `Bootstrap`, `Progress`, the ①②③ plan |
| `crates/core/src/aggregate.rs` | `AggregatePlan`, `process`, `fold`, `Execution` |
| `crates/core/src/dedup.rs` | `Registry`, `window_entries` |
