# The gateway

The gateway (`crates/shell/src/gateway`) is the only place untrusted input meets
the system. Every request goes through one pipeline:

```text
  HTTP ──► authenticate ──► authorize ──► pre-compute ──► mint ──► route ──► propose
           (token)          (admin-only)   (argon2)        (keys)   (Router)  (GroupOps)
```

The framework-agnostic half ([`CommandPlane`](../crates/shell/src/gateway/command.rs))
is pure where it can be, so the identity and pre-compute rules are testable
without a cluster; the axum adapter ([`http`](../crates/shell/src/gateway/http.rs))
only parses input and maps errors onto status codes.

## 1. Identity (`identity.rs`)

```rust
pub struct Identity { pub user_id: Id, pub is_admin: bool }

pub trait Authenticator: Send + Sync {
    fn authenticate(&self, token: Option<&str>) -> Result<Identity, AuthError>;
}
```

`StaticAuthenticator` is the test/dev implementation (a fixed token table); the
OIDC implementation is deployment wiring behind the same trait, and reads the
system-admin claim from the IdP's `groups` claim (`design.md` §4).

`is_admin_only(command_type)` is an **explicit** list — `organization.archive`,
`user.deactivate`, and the control-plane `tenant.*` commands. A non-admin
attempting one gets `AuthError::Forbidden` (HTTP `403`).

## 2. The command plane (`command.rs`)

```rust
pub struct CommandRequest {
    pub organization_id: Id, pub aggregate_id: Id, pub workspace_id: Option<Id>,
    pub command_type: String, pub payload: serde_json::Value,
    pub causation_id: Option<String>, pub correlation_id: Option<String>,
    pub token: Option<String>,
}
```

- `build_command` is **pure**: it authenticates, authorizes, runs pre-compute,
  and mints the envelope (fresh `id` and `occurred_at`, actor = the user).
- `submit` resolves the tenant's **active** group (`Router` + `GroupRegistry`),
  proposes, and — because `ProposeOutcome::Replayed` now carries the *recorded*
  fingerprint — answers `CommandError::KeyReused` when a client-supplied
  `causation_id` comes back with a different intent (D12). That is a conflict
  (`409`), never the recorded result.

### Causation keys

A client may supply a `causation_id` (a canonical `UUIDv5` string); otherwise
the gateway mints one by deriving a `Key` from a random seed. Either way the key
is the idempotency identity: an exact retry replays, and minted keys are
`UUIDv5` because `Key` is derive-only (D12).

### Error mapping

| `CommandError` | HTTP |
|---|---|
| `Auth(Missing/Unknown)` | `401` |
| `Auth(Forbidden)` | `403` |
| `UnknownOrganization` | `404` |
| `NotActive`, `KeyReused` | `409` |
| `InvalidKey`, `Serialize` | `400` |
| `GroupUnavailable`, `Propose` | `503` |
| `PreCompute` | `500` |

## 3. Edge pre-compute (`precompute.rs`)

Blocking-but-pure work runs **before** the command enters consensus: a request's
`password` field is replaced by an argon2 `password_hash`, so the plaintext
never reaches the replicated log, and every replica sees only the derived value.
`verify_password` is the same primitive in reverse.

## 4. Read-your-writes (`ryw.rs`)

```rust
pub async fn ensure_min_index(group: &RaftGroup, min_index: u64, hold: Duration) -> RywOutcome
```

A client that wrote at log index *N* sends `X-Min-Index: N`. The replica waits
up to `hold` for its applied index to reach *N*:

- `Recent` — serve the local applied state;
- `ForwardToLeader { leader }` — the replica cannot catch up and another node is
  leader: the HTTP layer answers `307` with `x-leader-id`;
- `Unavailable` — no reachable leader (this replica is the leader, or leadership
  is unknown): `503`, never stale data.

The forwarding policy is a pure `classify()` so it is unit-tested without a
cluster.

## 5. HTTP surface

| Method | Path | Body / headers |
|---|---|---|
| `POST` | `/organizations/{organization_id}/commands` | JSON command body; `Authorization: Bearer <token>` |
| `GET` | `/organizations/{organization_id}/events` | optional `X-Min-Index` |

Inbound ids are **parsed** with `Id::parse`, never adopted, so untrusted strings
cannot become identity (D10/D12).

## 6. Limits

- OIDC authentication and the deployment's HTTP server wiring are not
  implemented; `StaticAuthenticator` is the seam.
- `GET /events` returns the group's whole applied-event log for the
  organization, so a read is `O(history)`. A real read model (projections)
  replaces this in the gateway's read plane.
- No rate limiting, request-size limits or tracing yet.
