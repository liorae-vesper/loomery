# The runtime host

The wiring described here is drawn in
[architecture.md](architecture.md#the-runtime-host).

`loomery-server` is Loomery's entry point: it reads one configuration, builds the
real adapters, and hands them to [`loomery_shell::host::Host`], which owns the
wiring. Everything the earlier phases built — the control plane, the Raft groups,
the gateway, the outbox and the saga runner — is assembled here into a process
that serves traffic.

```
host.json ──► HostConfig ──► Host::boot ──┬── control group (tenant records)
                                          ├── one Raft group per active tenant
                                          ├── OidcAuthenticator (warmed once)
                                          └── CommandPlane (auth → route → propose)
                                                   │
   HTTP ──► gateway router ───────────────────────┘
                                                   │ committed events
   outbox worker per group ──► NATS JetStream ─────┤ (persisted cursor per group)
   saga runner ◄───────────── NATS JetStream ──────┘
```

## Running it

```sh
mise run svc-up                      # Keycloak + NATS (the compose stack)
mise run server -- --config host.json # or: LOOMERY_CONFIG=host.json loomery-server
```

A minimal configuration:

```json
{
  "node_id": 1,
  "data_dir": "/var/lib/loomery",
  "control_group": "control",
  "http": { "bind": "0.0.0.0:8080", "ryw_hold_ms": 50 },
  "nats": { "url": "nats://127.0.0.1:4222", "durable": "loomery-sagas" },
  "oidc": { "issuer": "https://idp.example.com/realms/main" }
}
```

`--config` names the file; `LOOMERY_*` environment variables override it, so a
deployment can keep its shape in a file and its endpoints in the environment:

Top-level settings are `node_id` (this replica's Raft id), `data_dir`, the
control group's `control_group` name, and `node_address` — the address this node
records in every placement it writes (a single-node host needs it only for the
record).

| Variable | Overrides |
|---|---|
| `LOOMERY_CONFIG` | the configuration path (instead of `--config`) |
| `LOOMERY_NODE_ID` | `node_id` |
| `LOOMERY_DATA_DIR` | `data_dir` |
| `LOOMERY_HTTP_BIND` | `http.bind` |
| `LOOMERY_NATS_URL` | `nats.url` (and turns the broker runtime on) |
| `LOOMERY_OIDC_ISSUER` | `oidc.issuer` (and turns OIDC on) |
| `LOOMERY_OIDC_JWKS_URI` | `oidc.jwks_uri` |
| `LOOMERY_OIDC_AUDIENCE` | `oidc.audience` |
| `LOOMERY_OIDC_ADMIN_GROUP` | `oidc.admin_group` |

A write and a read, with a token from the identity provider:

```sh
token=$(curl -s -X POST "$ISSUER/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=loomery-gateway \
  -d username=ada -d password=ada -d scope=openid | jq -r .access_token)

curl -s -X POST "http://127.0.0.1:8080/organizations/$ORG/commands" \
  -H "Authorization: Bearer $token" -H 'content-type: application/json' \
  -d '{"aggregate_id":"'"$TASK"'","command_type":"task.create","payload":{"title":"hello"}}'

curl -s "http://127.0.0.1:8080/organizations/$ORG/events" -H "Authorization: Bearer $token"
```

## Configuration reference

### `http`

| Field | Default | Meaning |
|---|---|---|
| `bind` | `127.0.0.1:8080` | `host:port`; port `0` asks the OS for a free port |
| `ryw_hold_ms` | `50` | how long a read waits for the caller's own write (`X-Min-Index`) |

### `nats` (optional)

Absent means no broker: the gateway still serves, no outbox or saga workers run.

| Field | Default | Meaning |
|---|---|---|
| `url` | `nats://127.0.0.1:4222` | broker URL |
| `stream` | `LOOMERY_OUTBOX` | the stream the outbox publishes into (D11) |
| `subjects` | `loomery.>` | the subjects that stream captures |
| `filter_subject` | `subjects` | the narrower set the saga consumer pulls |
| `durable` | `loomery-sagas` | the durable consumer name |
| `ack_wait_ms` | `30000` | redelivery delay for an unacked message |
| `duplicate_window_secs` | `120` | the broker's dedup window (only for a new stream) |
| `deliver_all` | `true` | replay the stream's backlog through the sagas |
| `connect_timeout_ms` / `publish_timeout_ms` | `5000` | connection and publish deadlines |

### `oidc` (optional; required to serve)

Nothing here is provider-specific: Keycloak, Entra ID, Auth0, Okta and a
self-hosted provider all work through the same adapter.

| Field | Default | Meaning |
|---|---|---|
| `issuer` | — | drives discovery at `{issuer}/.well-known/openid-configuration` |
| `jwks_uri` | discovered | set it directly for a provider without discovery |
| `audience` | unset | the `aud` a token must carry; unset skips the check |
| `subject_claim` | `sub` | the claim carrying the user id |
| `email_claim` | `email` | the claim carrying the caller's address |
| `require_verified_email` | `true` | trust an address only when the provider marks it verified |
| `groups_claim` | `groups` | dot path to group/role membership (`realm_access.roles`, …) |
| `admin_group` | `admins` | the value in that claim that grants `is_admin` |
| `subject_namespace` | unset | derive a `UUIDv5` id from a non-UUID subject |
| `leeway_seconds` | `30` | clock skew allowed on `exp`/`nbf` |
| `jwks_ttl_seconds` | `600` | how long a fetched key set is trusted |
| `timeout_ms` | `5000` | discovery/JWKS deadline |

Tokens are validated **locally**: the signature is checked against the JWKS entry
whose `kid` the token names, the algorithm comes from the *key* (never from the
token, so `HS256`-against-RSA is refused), and `iss`/`exp`/`nbf` (and `aud` when
configured) are enforced. A failed key refresh is rate-limited so an unknown
`kid` cannot make the gateway hammer the provider. The gateway **fails closed**:
configuring no provider stops startup rather than serving unauthenticated.

### `group`

Consensus, transport, storage and proposal settings, shared by every group the
host runs: see [Raft configuration](raft-configuration.md) and
[implementation.md](implementation.md).

## On disk today, and where it is going

Each group owns one RocksDB directory under `data_dir` (`data_dir/<group_id>/`,
with the control group at `data_dir/<control_group>/`), and the outbox cursor for a
group lives beside it. Within that database every kind of data currently shares one
key space: Raft entries, the aggregate state, and — inside the state record — every
applied event and the dedup window.

That layout has been replaced by [column families](storage-layout.md): `default`
(markers), `raft_log`, `state`, `events` and `projections`, with the append-only
`events` family holding every applied event from the moment it is folded. Two parts
of the target are still open — writing state as deltas, and the search index in a
sibling `index/` directory — and [storage-layout.md](storage-layout.md) is where the
difference is tracked.

## What it does at boot

1. Opens the **control group**. A data directory that does not exist yet is a new
   deployment: the group is initialized with this node as its only voter. An
   existing directory is recovery, and is never re-initialized.
2. Waits for a leader, then projects the tenant records into the router and boots
   one **persistent Raft group per active tenant**.
3. Warms the identity provider; a misconfigured issuer fails here, at startup.
4. Builds the `CommandPlane` — the only path from untrusted input to consensus.
5. Starts one **outbox worker per tenant group** (each resuming from the cursor
   persisted beside that group's database) and the **saga runner**.

On `SIGTERM`/`Ctrl-C` it stops serving, aborts the workers and closes every group.

## Provisioning

`POST /organizations` (admin only) onboards a tenant:

```sh
curl -s -X POST http://127.0.0.1:8080/organizations \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d '{"organization_id":"'"$ORG"'","leader_user_id":"'"$OWNER"'"}'
# {"organization_id":"…","group_id":"tenant-…"}
```

The route is wired to the gateway's `Provisioner` seam, which `Host` implements:
it records the placement in the control group, runs genesis, activates, routes,
boots the tenant's group and starts its outbox worker. It is **idempotent** —
every key and id derives from the business tuple (D12) — so a retry after an
unknown outcome answers the same placement.

A crash between the placement and genesis is not a problem: the boot
**reconciles**, finishing any registered-but-inactive tenant from the record
alone (the placement carries the genesis leader), and the same pass runs every
30 seconds. `Host::resume_provisioning` remains for an operator with a bootstrap
in hand.

## Access control

| Which caller | Workspace read | Organization log | Workspace commands | Membership/archive | Invite | Provision |
|---|---|---|---|---|---|---|---|
| Anyone with a valid token | ❌ unless a role | ❌ | ❌ | ❌ | ❌ | ❌ |
| `Viewer` in the workspace | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `Member` in the workspace | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ |
| `Owner` of any workspace of the organization | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| A system administrator (`is_admin`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

`invitation.accept` is the single write open to non-members: the invitee is a
stranger until the acceptance (and the saga that follows) makes them a member.
The role tables live in one place
([`required_workspace_role`](../crates/shell/src/gateway/identity.rs)), and the
membership index that answers them is derived from the applied events.

## Known gaps

Stated rather than hidden, each with the work that closes it:

- **Multi-node topologies are manual.** The control group is bootstrapped as a
  single voter and each tenant group is hosted by one process; adding replicas
  means driving `RaftGroup::raft` membership and a shared placement, which is
  the deployment runbook's job (Phase 7). `HostConfig::node_address` is already
  the address a placement records.
- **A read model is still the plan.** Reads are scoped (a workspace read answers
  one workspace, the organization log needs ownership), but they are answered by
  filtering the applied log — `O(history)` per read, and a workspace's own events
  are matched by aggregate. Projections replace both.
- **No observability stack.** Worker reports, reconciliation and startup go to
  stderr; tracing and OpenTelemetry are Phase 7.
- **Snapshot-backed recovery is still experimental** (`group.storage.state_persistence`).
  It is also the mode that scales: checkpoint mode serializes the whole state on
  every apply, so its cost is quadratic in the history (~17 commands/s at 2,000
  commands, against ~1,319/s for snapshot). See
  [persistence hardening](benchmarks/persistence-hardening.md).

[`loomery_shell::host::Host`]: ../crates/shell/src/host.rs
