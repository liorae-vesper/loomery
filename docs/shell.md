# Shell reference

`crates/shell` owns the imperative boundary around the deterministic core:
consensus, storage, peer networking and the genesis worker. It remains a library:
the gateway, control-plane, outbox and saga modules exist, but no process is
deployed yet.
[implementation.md](implementation.md) traces build, startup and storage/network
wiring with a complete example. For a walkthrough,
read the [group port](tutorials/shell-group.md),
[genesis worker](tutorials/genesis-worker.md) and
[in-memory OpenRaft baseline](tutorials/openraft-spike.md) tutorials.

## Implementation map

| Module | Responsibility |
|---|---|
| `group.rs` | `GroupOps` and `ProposeOutcome`; callers do not depend on Raft |
| `bootstrap.rs` | `bootstrap::run`, genesis progress and error handling |
| `config.rs` | `GroupConfig`, transport/storage/proposal tuning, TLS and persistence mode |
| `raft/mod.rs` | `AppData`, `Applied` and `TypeConfig` |
| `raft/port.rs` | `RaftGroup`, proposal error mapping, boot and shutdown |
| `raft/proposal.rs` | Shared bounded proposal queue and opt-in command batching |
| `raft/log_store.rs` | In-memory `MemLogStore` baseline |
| `raft/rocks_log_store.rs` | Durable log, vote, committed index, truncation and purge |
| `raft/disk.rs` | Synchronized RocksDB operations on Tokio's blocking pool |
| `raft/state_machine.rs` | Core application, applied events, dedup and snapshots |
| `raft/network.rs` | No-op network for the single-node baseline |
| `raft/transport.rs` | Shared tonic listener and per-group peer clients |
| `raft/tls.rs` | Certificate/identity validation and tonic TLS configuration |
| `raft/suite.rs`, `raft/persistent_tests.rs`, `raft/tls_tests.rs` | Storage, recovery, networking and TLS tests |
| `raft/append_tests.rs`, `raft/proposal_tests.rs` | Flush callbacks, batching, limits and batched recovery |
| `control/` | Tenant records projected into the `Router`; `provision`/`incomplete`/`resume` |
| `gateway/` | Identity + admin claim, argon2 pre-compute, command plane, `X-Min-Index` gate, axum adapter |
| `outbox/` | Applied-event tailer, D11 message identity and a resumable cursor |
| `saga/` | Consumer, `SagaRunner` retry classification and the invitation acceptance saga |
| `test_support.rs` | Test-only fake group and fixtures |

Dependencies point from shell to genesis/core; the core never imports the shell.
One tenant group owns one organization and one independent Raft log/state
machine. Multiple groups can register on the same listener. Routing tenant IDs
and managing their lifecycle remain control-plane work.

## Group contract

`GroupOps` exposes `committed_events(organization_id)` and `propose(command)`.

Concurrent producers can clone `RaftGroup::writer()` and call its `propose`
method. Opt-in `GroupConfig.proposals` settings collect ordered commands into
one durable Raft entry while preserving individual outcomes. Batching defaults
to disabled; all replicas must support batch entries before enabling it.
Commands in a batch share a Raft read-barrier index. See
[configuration](raft-configuration.md#opt-in-command-batching) and
[benchmarks](benchmarks/batching.md).

- `committed_events` reads locally applied state without a consensus round trip.
  It does not establish a linearizable read or enforce a session minimum index.
- `propose` uses `Raft::client_write`, waits for committed application, and maps
  `Applied::Appended` or `Applied::Replayed` to the port response. Domain
  rejection and Raft errors become proposal errors.
- A proposal error means the outcome may be unknown. Callers re-read applied
  events before deciding whether to propose the same intent again.
- `Replayed` is successful deduplication, not another appended event.

Read-your-writes is enforced in the gateway ([`gateway.md`](gateway.md)):
`ensure_min_index` waits for the `X-Min-Index` position to be applied, then
serves, forwards to the leader or answers `503`.

## Genesis worker

The pure `loomery-genesis::Bootstrap` script defines three commands: assign the
organization leader, create the default workspace, and add its Owner. IDs,
causation keys and timestamps are supplied or deterministically derived.

`bootstrap::run` repeatedly reads committed events, computes progress, proposes
only the next missing command and re-reads. It does not mint IDs, retry a failed
proposal blindly or rely exclusively on the bounded dedup window. A restarted
worker resumes from applied events. The control plane still needs to invoke it
for tenant creation, startup reconciliation and retry sweeping.

## Raft lifecycle and transport

- `boot_single_node` creates the in-memory baseline and initializes membership.
- `boot_persistent` validates configuration, preflights client TLS, opens a
  distinct RocksDB database and creates Raft. It does not initialize membership.
- `raft()` exposes initialization, learner admission, membership changes,
  metrics and triggers. `shutdown()` stops the Raft task.
- `TonicTransport::register` attaches a group to the shared listener. Its
  `RaftNetworkV2` implementation handles `append_entries` and `vote` over a
  group-routed protobuf/JSON envelope, `full_snapshot` as a client-streamed
  `InstallSnapshot` RPC, and replication as a bidirectional `StreamAppend` RPC.
  0.10 gives the network the whole snapshot rather than one chunk at a time, so
  the sender fragments and the follower reassembles within the request, aborting a
  stream that ends without the final fragment. `StreamAppend` needs no such
  framing: one HTTP/2 stream keeps both directions ordered, so results come back
  in request order, and the transport bounds a reply-less stream by openraft's
  `soft_ttl` rather than by the much smaller `hard_ttl`.
- TLS/mTLS is opt-in. Configured clients require HTTPS and verify the CA and
  peer name. The listener sets TCP_NODELAY and configured keepalive on accepted
  connections. No automatic certificate reload is implemented.

See [raft-configuration.md](raft-configuration.md) for complete startup examples
and tuning. Heartbeats/elections follow OpenRaft configuration; idle groups are
not automatically silent.

## State and durability

`MemStateMachine` serves both in-memory and persistent groups. It folds committed
commands through the core into organization/workspace/membership state, appends
domain events and records dedup after application. Its state also includes the
applied log ID and membership. Unknown commands and invalid domain operations
return `Applied::Rejected` without appending domain events.

Persistent log, vote and committed-index writes synchronize the RocksDB WAL.
Truncation/purge batches are atomic; the purge floor is saved with deletion.
Database calls run on the blocking pool and storage failures stop the Raft node.

| `storage.state_persistence` | Apply completion | Recovery |
|---|---|---|
| `checkpoint` (default) | Await synchronized full-state checkpoint per batch | Restore checkpoint, replay committed suffix |
| `snapshot` (experimental) | Apply in memory after quorum commit | Restore durable snapshot, replay committed suffix |

The database synchronously records its mode on first startup and rejects changes
on subsequent starts. Nonempty legacy databases without a marker require
checkpoint mode. This protection is per database, not a control-plane policy.

Raft snapshots are separate from apply checkpoints. OpenRaft schedules them
(default `LogsSinceLast(5000)`); persistence completes before publication.
Installation persists the snapshot and checkpoint atomically before replacing
live state. OpenRaft controls log purge using snapshot coverage and retention.
Snapshot v1 includes aggregate state, events, membership, applied index and the
serialized dedup window. Legacy missing versions map to v1; unsupported versions
fail. Snapshot copying/serialization still hold a state read lock; serialization
runs on the blocking pool.

See [checkpoint-policy.md](research/checkpoint-policy.md) for recovery contracts
and [the paired spike](benchmarks/checkpoint-spike.md) for measured tradeoffs.
Full-history checkpoints grow with history. Large-state snapshot contention and
interrupted-write failure testing remain work before changing the default.

## Validation and remaining scope

Both RocksDB modes and the in-memory store pass OpenRaft's storage suite.
Acceptance tests cover three-node replication, snapshot transfer, restart/dedup,
TLS/mTLS and immutable mode protection. The
[controlled benchmark](benchmarks/README.md) exercises real replica processes,
leader failures and whole-cluster recovery.

The gateway, tenant router/control group, RYW middleware, outbox and saga
runner are implemented ([gateway.md](gateway.md), [control-plane.md](control-plane.md),
[outbox-and-sagas.md](outbox-and-sagas.md)); the OIDC and NATS bindings are
deployment wiring, and observability is still to come. The authoritative roadmap
is [design.md §8](design.md#8-progress-tracker); next work is summarized in
[CONTINUE.md](CONTINUE.md).
