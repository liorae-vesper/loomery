# Configuration

One file configures a host, and one shape configures every group it runs. This is the
single reference: every field, its default, what it does — and then how the parts
constrain each other, in the order they are applied. Behaviour and measurements are in
[host.md](host.md), [raft-configuration.md](raft-configuration.md) and
benchmarks/ (`workpad/benchmarks/README.md`); this document is the map of the knobs themselves.

## The shape

```
host.json ──► HostConfig ──┬── http    the gateway
                           ├── oidc    optional, but required to serve
                           ├── nats    optional: outbox publisher and saga consumer
                           └── group ──┬── raft       OpenRaft's own Config, passed through
                                      ├── transport gRPC limits, TLS
                                      ├── storage   RocksDB, recovery mode
                                      └── proposals batching and the proposal queue
```

Loaded by `loomery-server --config host.json` (or `LOOMERY_CONFIG=host.json`), as JSON.
Every struct is `#[serde(default, deny_unknown_fields)]`: an omitted field takes the
default below, and a misspelled one is an error rather than a silent no-op. The whole
tree is validated before a database is opened or a listener bound — see
[How it comes together](#how-it-comes-together).

```json
{
  "node_id": 1,
  "data_dir": "data",
  "control_group": "control",
  "node_address": "http://127.0.0.1:7001",
  "http": { "bind": "127.0.0.1:8080", "ryw_hold_ms": 50 },
  "oidc": { "issuer": "https://idp.example.com/realms/main", "admin_group": "admins" },
  "nats": { "url": "nats://127.0.0.1:4222", "stream": "LOOMERY_OUTBOX" },
  "group": {
    "raft": { "heartbeat_interval": 50, "election_timeout_min": 150 },
    "transport": { "max_message_bytes": 16777216 },
    "storage": { "state_persistence": "checkpoint" },
    "proposals": { "max_batch_commands": 1 }
  }
}
```

## Host

| Field | Default | Meaning |
|---|---|---|
| `node_id` | `1` | This replica's Raft id, unique within every group it hosts |
| `data_dir` | `data` | Where group databases and outbox cursors live |
| `control_group` | `control` | The control group's id, which is also its directory name |
| `node_address` | `http://127.0.0.1:7001` | This node's address as its peers dial it |

Validation: `node_id` positive, `data_dir` non-empty, `control_group` non-empty and free
of dots (it is a NATS subject segment, D11), `node_address` non-empty.

## Gateway (`http`)

| Field | Default | Meaning |
|---|---|---|
| `bind` | `127.0.0.1:8080` | `host:port`; port `0` asks the OS for a free port |
| `ryw_hold_ms` | `50` | How long a read waits for the caller's own write (`X-Min-Index`) |

## Identity (`oidc`, optional — required to serve)

Nothing here is provider-specific: Keycloak, Entra ID, Auth0, Okta and self-hosted
providers all work through the same adapter. Absent, the gateway refuses to start
rather than serve unauthenticated.

| Field | Default | Meaning |
|---|---|---|
| `issuer` | — | Drives discovery at `{issuer}/.well-known/openid-configuration` |
| `jwks_uri` | discovered | Set it directly for a provider without discovery |
| `audience` | unset | The `aud` a token must carry; unset skips the check |
| `subject_claim` | `sub` | The claim carrying the user id |
| `email_claim` | `email` | The claim carrying the caller's address |
| `require_verified_email` | `true` | Trust an address only when the provider marks it verified |
| `groups_claim` | `groups` | Dot path to group/role membership (`realm_access.roles`, …) |
| `admin_group` | `admins` | The value in that claim that grants `is_admin` |
| `subject_namespace` | unset | Derive a `UUIDv5` id from a non-UUID subject |
| `leeway_seconds` | `30` | Clock skew allowed on `exp`/`nbf` |
| `jwks_ttl_seconds` | `600` | How long a fetched key set is trusted |
| `timeout_ms` | `5000` | Discovery/JWKS deadline |

Validation: `issuer` must start with `http://` or `https://`; `jwks_uri` non-empty when
set; the three claim names and `admin_group` non-empty; `timeout_ms` positive.
[host.md](host.md#oidc-optional-required-to-serve) explains how tokens are checked.

## Broker (`nats`, optional)

Absent means no broker: the gateway still serves, no outbox or saga workers run.

| Field | Default | Meaning |
|---|---|---|
| `url` | `nats://127.0.0.1:4222` | Broker URL |
| `stream` | `LOOMERY_OUTBOX` | The stream the outbox publishes into (D11) |
| `subjects` | `loomery.>` | The subjects that stream captures |
| `filter_subject` | `subjects` | The narrower set the saga consumer pulls |
| `durable` | `loomery-sagas` | The durable consumer name |
| `ack_wait_ms` | `30000` | Redelivery delay for an unacked message |
| `duplicate_window_secs` | `120` | The broker's dedup window (only for a new stream) |
| `deliver_all` | `true` | Replay the stream's backlog through the sagas |
| `connect_timeout_ms` | `5000` | Connection deadline |
| `publish_timeout_ms` | `5000` | Publish deadline |

Validation: `url`, `stream`, `subjects` and `durable` non-empty; `filter_subject`
non-empty when set; both timeouts positive.

## Group: `raft` — OpenRaft's own `Config`

Passed straight through to `openraft::Config` and validated by OpenRaft's own
`validate()`. **This repo sets none of these**: the values below are the pinned
`openraft 0.10.0-alpha.36` defaults, so the table is a list of what you *may* change
rather than what we chose.

| Field | Default | What it governs |
|---|---|---|
| `heartbeat_interval` | `50` | Leader heartbeat period, ms. Also the `hard_ttl` OpenRaft derives its own reply deadlines from — which is why a stream must never use it as a per-response bound (see the wedge (`workpad/benchmarks/deployment-scale.md#the-sweep-that-wedged-and-why`)) |
| `election_timeout_min` / `election_timeout_max` | `150` / `300` | Randomized election window, ms |
| `enable_tick` / `enable_heartbeat` / `enable_elect` | `true` | The three background loops |
| `install_snapshot_timeout` | `200` | Per-chunk deadline 0.9 applied to snapshot transfer, ms |
| `send_snapshot_timeout` | `0` | Snapshot send deadline; `0` disables it |
| `max_payload_entries` | `300` | Entries per replication payload |
| `replication_lag_threshold` | `5000` | Lag that marks a follower as lagging |
| `snapshot_policy` | `LogsSinceLast(5000)` | When a snapshot is built — and therefore when the log may be purged |
| `snapshot_max_chunk_size` | `3145728` | Snapshot fragment size, bytes. The transport must fit it (see the cross-checks) |
| `max_in_snapshot_log_to_keep` | `1000` | Logs kept behind the snapshot before purging |
| `purge_batch_size` | `1` | Entries deleted per purge step |
| `api_channel_size` / `api_batch_capacity` / `api_batch_linger_ms` | `65536` / `4096` / `0` | Client API channel and batching |
| `notification_channel_size` / `state_machine_channel_size` | `65536` / `1024` | Notification and apply channels |
| `backoff` | `200ms` | Replication backoff |
| `removed_leader_step_down` | `After(150)` | What a leader does when it is removed from membership |
| `quorum_loss_probe_interval` | unset | Periodic quorum probe; unset disables it |
| `enable_pre_vote` | unset | Pre-vote before campaigning |
| `cluster_name` | `"foo"` | OpenRaft's placeholder; cosmetic, unset by this repo |

Left at their defaults and not described here — OpenRaft documents them, and they are
advanced enough that a wrong sentence would be worse than none: `heartbeat_min_interval`
(`0`), `max_append_entries` (`4096`), `log_stage_capacity` (unset),
`reset_backoff_on_transfer_leader` (unset), `allow_log_reversion` (unset),
`enable_leader_restore` (unset).

## Group: `transport`

| Field | Default | Meaning |
|---|---|---|
| `connect_timeout_ms` | `1000` | Connection establishment deadline |
| `request_timeout_ms` | `5000` | Upper bound on an RPC deadline (also capped by OpenRaft's TTL) |
| `max_message_bytes` | `16777216` | Maximum encoded request or response size |
| `tcp_keepalive_ms` | `30000` | TCP keepalive interval |
| `stream_window_bytes` | `1048576` | HTTP/2 initial stream window |
| `connection_window_bytes` | `4194304` | HTTP/2 initial connection window |
| `server_tls` | unset | Inbound TLS; unset means plaintext HTTP/2 |
| `client_tls` | unset | Outbound TLS; requires HTTPS peer addresses |

`server_tls` carries `identity { certificate, private_key }` and an optional
`client_ca_certificate` — setting the latter requires every peer to present a
certificate (mutual TLS). `client_tls` carries `ca_certificate`, an optional
`identity { … }` and an optional `server_name` (defaults to the peer URI host, for
connecting by IP to a certificate with a DNS identity). File contents are never part of
serialized configuration or debug output. [raft-configuration.md](raft-configuration.md#opt-in-tls-and-mutual-tls)
has the setup steps.

Validation: every limit positive; TLS paths non-empty; `server_name` non-empty when set.

## Group: `storage`

| Field | Default | Meaning |
|---|---|---|
| `state_persistence` | `checkpoint` | Recovery contract: `checkpoint` or `snapshot` (experimental). **Fixed for the lifetime of a database** |
| `write_buffer_bytes` | `67108864` | Bytes allocated per memtable |
| `max_write_buffers` | `2` | Maximum memtables (at least 2) |
| `max_background_jobs` | `2` | Background flush/compaction jobs |
| `max_open_files` | `512` | Open file limit (`-1` unlimited) |
| `block_cache_bytes` | `67108864` | Shared block cache |

These are per database, so memory and background jobs multiply by the number of
resident groups. Validation rejects zero buffers, fewer than two memtables, zero jobs
and a zero file limit.

## Group: `proposals`

| Field | Default | Meaning |
|---|---|---|
| `max_batch_commands` | `1` | Maximum commands sharing one Raft entry. **`1` disables batching** |
| `max_batch_bytes` | `262144` | Maximum sum of serialized command bytes per batch |
| `max_delay_ms` | `1` | Maximum collection delay after the first queued command; `0` collects only what is already queued |
| `queue_capacity` | `1024` | Channel capacity; producers wait when full |

Validation: the three limits positive; and if batching is on, `max_batch_bytes` must be
at least `max_batch_commands` (every command is at least one byte, so a smaller budget
makes the count unreachable).

## Environment overrides

`HostConfig::load` parses the file and *then* applies a fixed set of `LOOMERY_*`
variables over it, so a host can vary a field or two without a separate file. The file
wins for everything else; precedence is file first, environment second.

| Variable | Overrides |
|---|---|
| `LOOMERY_CONFIG` | the path to load — read by the server before anything else, so it is the one variable that does not need a file |
| `LOOMERY_NODE_ID` | `node_id` (must parse as a number) |
| `LOOMERY_DATA_DIR` | `data_dir` |
| `LOOMERY_HTTP_BIND` | `http.bind` |
| `LOOMERY_NATS_URL` | `nats.url` |
| `LOOMERY_OIDC_ISSUER` | `oidc.issuer` |
| `LOOMERY_OIDC_JWKS_URI` | `oidc.jwks_uri` |
| `LOOMERY_OIDC_AUDIENCE` | `oidc.audience` |
| `LOOMERY_OIDC_ADMIN_GROUP` | `oidc.admin_group` |

The broker and identity variables **create their section when it is absent**, which is
the point of them: `LOOMERY_NATS_URL=…` alone turns the outbox and saga workers on, and
`LOOMERY_OIDC_ISSUER=…` alone is enough for the gateway to serve. This is deliberately
a short list — the fields a deployment varies *per host* — and not a generic mapping: a
field that is not here cannot be set from the environment, and the rest belong in the
file, where they are visible and validated together.

## How it comes together

**Boot order.** `HostConfig::validate` runs first, then each group is validated
(`GroupConfig::validate`) and only then is its database opened and its replica started;
the transport listener is registered afterwards. A configuration error therefore costs
a process start, not a partially opened cluster. Opening an existing database also
checks the stored `state_persistence` marker: a mismatch fails with both values rather
than rewriting it.

**The cross-checks, and why they exist.** Configuration is validated in one place
because these pairs only make sense together:

| Rule | Why |
|---|---|
| `max_batch_bytes <= max_message_bytes / 2` (when batching) | A batch is one Raft entry and must survive transport encoding, which reserves at least half the message limit for overhead |
| `raft.snapshot_max_chunk_size * 2 <= max_message_bytes` | Snapshot fragments are the transport's own framing: a chunk that cannot fit a transport message turns a lagging follower into a transfer that can never complete |
| `max_batch_commands == 1 \|\| max_batch_bytes >= max_batch_commands` | A byte budget below the count limit binds first, every time |
| `raft.validate()` | Election windows, heartbeat and snapshot policy have their own internal rules |

**The batching model.** A batch is not the count limit; it is
`min(commands in flight, max_batch_commands, max_batch_bytes / frame)` plus whatever
`max_delay_ms` lets accumulate. Only the first term reflects the deployment, which is
why a count limit above the writes a group receives concurrently is dead configuration:
eight concurrent callers produce batches of eight whatever the limit says.
`ProposalWriter::batch_stats()` publishes what actually happened — entries, commands,
mean and largest batch, which limit bound each batch, and how many of the largest
command the byte budget holds — and the host prints it per group at shutdown.
[raft-configuration.md](raft-configuration.md#opt-in-command-batching) has the
semantics: one entry, one append, one commit advance, independent dedup outcomes per
command, and the rule that **every replica must be upgraded before batching is
enabled**.

**Persistence modes and the durability boundary.** `checkpoint` writes what each apply
changed — the touched aggregate states, the dedup entries, the applied marker and the
record — as one atomic `WriteBatch`, so an apply costs the size of its batch rather than
the size of the history. `snapshot` writes no per-apply state and recovers by restoring
a snapshot and replaying the committed log. In both modes the **Raft log is the
durability boundary**: it is synced before the entry it carries is committed and
applied, and the apply batch is not synced, so a power cut can lose the last window of
applies and the replica replays the difference on restart. Nothing can come back torn —
the marker and the record are one batch — and a store whose record has no marker is
refused rather than guessed at. [raft-configuration.md](raft-configuration.md#what-is-synchronised-and-what-that-buys)
states the model; the trade is worth **+34%** on the default deployment path
(measured (`workpad/benchmarks/deployment-scale.md#the-per-command-cost-two-fsyncs-per-batch`)).

**Snapshots, purge and retention.** `snapshot_policy` decides when a snapshot is built;
`max_in_snapshot_log_to_keep` and `purge_batch_size` decide what is dropped behind it.
Snapshot coverage counts **Raft entries**, so a larger batch size covers more commands
per snapshot, and snapshot persistence stays synchronised even though the apply batch
does not.

**What the knobs buy.** Concurrency — how many writes the caller keeps in flight — is
the dominant lever, and it is not a server setting: 1,225 w/s at 8 in flight against
9,272 at 128 (unbatched) on the deployment path. The batching knobs matter only when
concurrency supplies the depth; at 8 in flight, turning batching on is a wash
(1,231 w/s against 1,225) because a batch of 8 commands does not cost 8× a batch of one
to write. Storage buffers and background jobs trade memory and CPU for compaction
throughput. deployment-scale.md (`workpad/benchmarks/deployment-scale.md`) has the numbers and
the caveats, including which of them are inside the trial-to-trial spread.

## Deliberately not configurable

These are properties, not settings, because a knob for them would be a way to be
silently wrong:

- **The Raft log's WAL sync.** The log is the durability boundary; a configuration that
  could disable it would be a configuration that loses committed commands.
- **The atomic apply batch.** State, record and applied marker move together, which is
  what makes recovery-after-loss possible at all.
- **The mode marker's immutability**, and the refusal to open a record without a marker.
- **`deny_unknown_fields`.** A typo in a key is an error, not a default.
- **The tenant-scoping rules** (D11 subject and dedup identity): derived, not chosen.
- **The batch statistics.** Published whether or not anyone reads them.

## Deliberately out of scope

Two things are configuration, but not *deployment* configuration, and this document
does not cover them: the **benchmark harness's own config**
(`examples/consensus_bench` — points, duration, concurrency, trials), which is
measurement tooling; and the **test and benchmark environment variables**
(`LOOMERY_TEST_*`, `LOOMERY_APPLY_TIMINGS`), which are read by test and example paths
rather than by the runtime.
