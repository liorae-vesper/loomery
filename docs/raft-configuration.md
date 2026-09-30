# Persistent Raft groups

`loomery_shell::raft::RaftGroup::boot_persistent` opens a RocksDB database
and starts an OpenRaft replica with tonic peer networking. Use a distinct
database directory for every `(group_id, node_id)` pair. The same `group_id`
must be used on every replica. An existing database restores the log, vote,
commit pointer, applied aggregate state, dedup window and snapshots.

The shell remains a library: the embedding application loads configuration,
binds listeners, registers groups and owns shutdown. `GroupConfig` derives
serde serialization/deserialization; omitted fields use defaults and unknown
shell settings fail validation. JSON example:

```json
{
  "transport": {
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 5000,
    "max_message_bytes": 16777216,
    "tcp_keepalive_ms": 30000,
    "stream_window_bytes": 1048576,
    "connection_window_bytes": 4194304
  },
  "storage": {
    "write_buffer_bytes": 67108864,
    "max_write_buffers": 2,
    "max_background_jobs": 2,
    "max_open_files": 512,
    "block_cache_bytes": 67108864
  }
}
```

`GroupConfig::raft` exposes OpenRaft's configuration directly: heartbeat and
election intervals, maximum replication payload, snapshot policy/chunk size,
retained logs and purge batch sizing. Change these using Rust or serde fields
from the pinned OpenRaft version. `GroupConfig::validate` validates consensus
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

Transport requests have the lesser of OpenRaft's RPC TTL and
`request_timeout_ms`; channels reconnect automatically. Message limits apply
to both clients and servers. Snapshot chunks use JSON byte arrays, so leave
room for encoding overhead when selecting the consensus chunk size and the
gRPC message limit. This first transport uses plaintext HTTP/2; deploy it on
a trusted private network. Its versioned protobuf envelope carries the pinned
OpenRaft JSON request/Result types; rolling wire upgrades need compatibility
review.

RocksDB options are per database, so memory and background jobs multiply by
the number of resident groups. Larger write buffers and block caches consume
more memory; background jobs trade CPU/I/O for compaction throughput.
`max_open_files = -1` removes the file limit. LZ4 compression is enabled.

Log entries use ordered binary index keys. Purge deletion and its floor marker
are committed atomically. All writes retain the WAL and synchronize it before
acknowledgment; these correctness guarantees are fixed. Blocking database
operations run on Tokio's blocking pool. Raft stops on storage failure.

The initial durable state machine saves a complete state/snapshot checkpoint
(including applied events and dedup) after each apply batch. This favors simple
recovery but serialization and write cost grow with group history. Incremental
state persistence and event archival remain future work; benchmark expected
group sizes before choosing production capacity.

Build prerequisites: `protoc`, a C++ compiler and libclang (RocksDB bindings).
Run builds and checks through `mise exec --` to use the pinned Rust toolchain.
The storage conformance tests cover RocksDB; network tests cover three-node
membership/replication, database reopen and snapshot transfer.
