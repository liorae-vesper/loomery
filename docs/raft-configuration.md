# Persistent Raft groups

`loomery_shell::raft::RaftGroup::boot_persistent` opens a RocksDB database
and starts an OpenRaft replica with tonic peer networking. Use a distinct
database directory for every `(group_id, node_id)` pair. The same `group_id`
must be used on every replica. An existing database restores the log, vote,
commit pointer, applied aggregate state, dedup window and snapshots.

The shell remains a library: the embedding application loads configuration,
binds listeners, registers groups and owns shutdown. `GroupConfig` derives
serde serialization/deserialization; omitted fields use defaults and unknown
shell settings fail validation. Every field, its default and what it does is in
[configuration.md](configuration.md#the-shape) — this document is about behaviour.

`GroupConfig::raft` exposes OpenRaft's configuration directly: heartbeat and
election intervals, maximum replication payload, snapshot policy/chunk size,
retained logs and purge batch sizing. `GroupConfig::validate` validates consensus
and resource settings before opening storage. Apply changed settings on restart.

## Startup and membership

```rust,ignore
use loomery_shell::{config::GroupConfig, raft::{RaftGroup, transport::TonicTransport}};
use openraft::BasicNode;
use std::{collections::BTreeMap, path::Path};

let config = GroupConfig::default();
let listener = tokio::net::TcpListener::bind("127.0.0.1:7001").await?;
let group = RaftGroup::boot_persistent(
    1, "control".into(), Path::new("data/control/1"), config.clone(),
).await?;
let transport = TonicTransport::default();
transport.register("control".into(), group.raft()).await?;
let server = tokio::spawn(async move {
    transport.serve(listener, config.transport, shutdown_signal()).await
});

// Only the designated bootstrap replica initializes a new cluster.
// On restart, existing membership is recovered; do not initialize again.
if !group.raft().is_initialized().await? {
    group.raft().initialize(BTreeMap::from([
        (1, BasicNode::new("http://127.0.0.1:7001")),
    ])).await?;
}
```

Start additional replicas and register them with their own local transport.
On the leader, call `group.raft().add_learner(node_id, BasicNode::new(peer_uri),
true)` to wait for catch-up, then `change_membership(voter_ids, false)` to
promote them. Membership addresses are complete tonic endpoint URIs.
Never independently initialize each replica. One transport registry and
listener can serve many groups. Stop groups with `shutdown`, unregister them,
resolve the server shutdown future and await the server before reopening paths.

## Tuning and storage behavior

### Opt-in command batching

Set `group.proposals` in benchmark configuration, or `proposals` directly in
the embedding application's `GroupConfig`:

```json
{
  "proposals": {
    "max_batch_commands": 8,
    "max_batch_bytes": 262144,
    "max_delay_ms": 1,
    "queue_capacity": 1024
  }
}
```

The default `max_batch_commands` is **1**, which bypasses the queue and retains
the existing single-command log format. Larger values enable a shared writer
for each group. Call `group.writer().propose(command).await` from concurrent
producers; cloned writers share the same queue. `GroupOps::propose` uses that
writer too. A sequential bootstrap still waits for each result and therefore
does not batch its dependent steps. Direct `group.raft().client_write(...)`
bypasses the proposal queue.

The writer collects commands in enqueue order, limited by count and the sum
of serialized command bytes. Collection waits at most `max_delay_ms` after
starting a batch; zero drains only commands already queued. This is not an
end-to-end deadline: queue wait and quorum/application time are additional.
The bounded channel holds `queue_capacity` commands; a collected/in-flight
batch and at most one deferred command can also be held by the worker. Producers
wait for channel capacity. Individual commands larger than `max_batch_bytes`
are rejected locally when batching is enabled. Validation reserves at least
half the transport message limit for encoding overhead; replication payload
and snapshot limits still need appropriate sizing.

Multiple commands become one `AppData::Batch` Raft entry, one synchronized
append and one committed-pointer advance for that entry. They apply in order
under the state-machine lock, retaining independent dedup/rejection outcomes.
Rejection of one command does not roll back successful siblings. Every caller
waits for quorum commit and completed application; checkpoint mode also waits
for its durable checkpoint. This is command batching, not an atomic domain
transaction or an early acknowledgment. There is no asynchronous WAL worker.

All commands in a batch share a Raft log index, including the indices returned
in `ProposeOutcome`; an index is a read barrier, not a unique command identifier.
Use command/event IDs for identity. Snapshot thresholds and retention count
**Raft entries**, so their command coverage increases with batch size.
Cancelling a proposal after enqueue can still leave it committed. Shutdown
interrupts queued/in-flight proposals with unknown outcomes and stops the
writer before stopping Raft; it does not promise to drain the queue.

**Upgrade every replica before enabling batching.** Older binaries cannot
decode `AppData::Batch`; mixed-version replication and downgrade after batched
logs exist are unsupported. Current binaries read both entry formats and can
stop producing batches by setting the count back to 1; existing batches still
replay normally. The immutable checkpoint/snapshot recovery-mode protection
remains enforced independently of these tuning settings.

See [the batching benchmark](benchmarks/batching.md) for measurements and
`mise run bench-batching -- --output benchmark-results/batching-comparison`
for a controlled comparison.

### Transport and RocksDB

Transport requests have the lesser of OpenRaft's RPC TTL and
`request_timeout_ms`; channels reconnect automatically. Message limits apply
to both clients and servers. Plaintext HTTP/2 is the default; TLS and mutual
TLS are available through the opt-in settings below.

`InstallSnapshot` is a stream: raw-byte fragments sized by
`raft.snapshot_max_chunk_size`, so `GroupConfig::validate` requires that size to be
at most half of `max_message_bytes`. Replication itself is openraft's default
sequential `stream_append` — one request, one response — because a bidirectional
`StreamAppend` measured within a few percent of it in both directions; the design
record is in
[the migration note](research/openraft-010-migration.md#pipelined-append-leg-5-built-measured-removed).

The limits themselves are ceilings rather than promises, and how they combine with
concurrency — including what an inert limit looks like in `batch_stats()` — is in
[configuration.md](configuration.md#group-proposals).

The transport's versioned protobuf envelope carries the pinned OpenRaft JSON
request/Result types; rolling wire upgrades need compatibility review.

RocksDB options are per database, so memory and background jobs multiply by
the number of resident groups. Larger write buffers and block caches consume
more memory; background jobs trade CPU/I/O for compaction throughput.
`max_open_files = -1` removes the file limit. LZ4 compression is enabled.

Log entries use ordered binary index keys. Purge deletion and its floor marker
are committed atomically. Blocking database operations run on Tokio's blocking
pool. Raft stops on storage failure.

### What is synchronised, and what that buys

The *Raft log* retains the WAL and
synchronizes it before acknowledgment: an entry is durable before it is committed
and applied, and that is the durability boundary in both persistence modes. The
apply batch that follows — the append-only record, the touched aggregate state and
the applied marker, one atomic `WriteBatch` — is written *without* an explicit
sync. A power cut can therefore lose the last window of applies, and the replica
comes back behind its log and replays the difference; it cannot come back torn,
because the marker is written in the same batch as the record it describes. The
trade is one fsync per apply instead of two: 9.5% of throughput on the batched
path ([measured](benchmarks/deployment-scale.md#the-per-command-cost-two-fsyncs-per-batch)).
Snapshot mode has always worked this way, persisting no per-apply state at all.
Snapshot persistence and purge stay synchronised.

The initial durable state machine awaits a complete applied-state checkpoint
(including applied events and dedup) after each apply batch. Raft snapshots are separate and follow
`raft.snapshot_policy` (default: every 5000 logs since the last snapshot).
See [checkpoint-policy.md](research/checkpoint-policy.md) for the difference
between background scheduling and durable synchronization. This favors simple
recovery but serialization and write cost grow with group history. Incremental
state persistence and event archival remain future work; benchmark expected
group sizes before choosing production capacity.

Build prerequisites: `protoc`, a C++ compiler and libclang (RocksDB bindings).
Run builds and checks through `mise exec --` to use the pinned Rust toolchain.
The storage conformance tests cover RocksDB; network tests cover three-node
membership/replication, database reopen and snapshot transfer.

## Opt-in TLS and mutual TLS

Add these settings to `transport` for encrypted peer connections:

```json
{
  "server_tls": {
    "identity": {
      "certificate": "/etc/loomery/tls/server-chain.pem",
      "private_key": "/etc/loomery/tls/server-key.pem"
    }
  },
  "client_tls": {
    "ca_certificate": "/etc/loomery/tls/peer-ca.pem"
  }
}
```

`server_tls` configures the shared listener. `client_tls` configures each
group's outbound connections. Neither setting is enabled by default. Use
`https://node.example:7001` addresses in Raft membership when `client_tls` is
set; the client verifies the certificate's identity against the address host.
Set `client_tls.server_name` only when an explicit shared certificate name is
needed (for example connecting by IP to a DNS certificate). This still verifies
the specified name; verification cannot be disabled.

For mutual TLS, add `server_tls.client_ca_certificate` with the PEM CA bundle
that signs allowed client certificates. The listener then requires a valid
client certificate. Add `client_tls.identity` with `certificate` and
`private_key` file paths for the outbound client identity. Omit the server's
client CA setting for server-authenticated TLS without client certificates.
CA bundles must contain parseable certificates. PEM files are read using async
file I/O; the configuration contains only paths, never key material.

TLS-enabled clients reject `http://` addresses; plaintext clients reject
`https://` addresses. TLS errors fail rather than fall back to plaintext.
`connect_timeout_ms` also bounds TLS handshakes. The listener validates server
credentials before serving; persistent group startup preflights client
credentials before opening the database. Existing channels reuse loaded
credentials. Restart groups and recreate the shared listener to rotate them;
automatic reload is not supported. All groups sharing a listener share its
server identity and client CA policy.

Tests cover three-replica replication and recovery over mTLS, standalone TLS
and mTLS handshakes, plaintext snapshot transfer, and rejection of wrong CA,
wrong certificate name, missing client identity and invalid configuration.

### Experimental snapshot-backed recovery

The layout these modes write into is being replaced by
[column families](storage-layout.md): the same two modes remain (how state is made
durable), but they will write into `state`/`events` families rather than one key
space, and `snapshot` mode's record will no longer carry the event list.

`GroupConfig.storage.state_persistence` accepts `"checkpoint"` (default) or
`"snapshot"` (experimental). Set the configuration flag when creating a new
replica database:

```json
{
  "storage": {
    "state_persistence": "snapshot"
  }
}
```

In benchmark configuration this lives under `group.storage.state_persistence`.
In Rust, use `config.storage.state_persistence = StatePersistence::Snapshot`.
Snapshot mode removes the per-apply full-state checkpoint, keeping synchronized
log writes and durable scheduled snapshots. Startup restores the durable
snapshot and replays committed logs.

The protection is always enabled: first startup synchronously persists the
selected mode. Every later startup checks that marker before recovery or
starting Raft. A mismatch fails with the stored and requested modes; it does not
rewrite the marker. Nonempty legacy databases without a marker are restricted
to checkpoint mode, even if they have no applied-state checkpoint yet. Invalid
markers fail startup. Choose the same mode for all replicas of a group. This is
per-database protection, not a cluster-wide control-plane setting.

There is no force-switch option or automatic migration. Use fresh database
paths for comparisons; changing the configuration on an existing deployment
fails startup. See [the persistence spike](benchmarks/checkpoint-spike.md) before
selecting snapshot mode.
