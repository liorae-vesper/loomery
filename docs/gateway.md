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
| `Propose` (a plan rejection) | `409`, with the plan's code in the body — or `400` for `invalid_payload`/`unknown_command` |
| `Propose` (anything else: timeout, lost leadership) | `503` — the outcome is unknown, so re-read before retrying |
| `GroupUnavailable` | `503` |
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
| `POST` | `/organizations` | `{ organization_id, leader_user_id, group_id? }`; **admin only** |
| `POST` | `/organizations/{organization_id}/commands` | JSON command body; `Authorization: Bearer <token>` |
| `GET` | `/organizations/{organization_id}/events` | bearer token + optional `X-Min-Index` |

**Authorization, not just authentication.** Three checks, in order:

1. **Membership** — a caller must belong to the organization (an
   `OrganizationAssignment` from the invitation flow, or a workspace role) or
   carry the admin claim; otherwise `403`. This gates reads and writes alike.
2. **Role**, for a workspace-scoped command — the caller's role *in the workspace
   the command names* must satisfy the requirement, and the command must name one:
   a scoped command without a scope is refused rather than treated as unscoped.
   The table is one place
   ([`required_workspace_role`](../crates/shell/src/gateway/identity.rs)):

   | Command | Required role |
   |---|---|
   | `workspace.rename`, `task.*` | `Member` or better (a `Viewer` reads) |
   | `workspace.archive`, `membership.add_owner`/`add_member`/`change_role`/`remove_member` | `Owner` |
3. **Organization ownership**, for administrative commands — `invitation.create`
   requires owning at least one workspace of the organization (roles live on
   workspace memberships, so "organization administrator" means exactly that).

`invitation.accept` is the single write exempt from all three: the invitee is a
stranger until the acceptance (and the saga that follows) makes them a member.
The admin claim bypasses every check.

**Failures are classified.** `401` no or unknown token, `403` a failed check,
`404` unknown organization, `409` the plan refused the command (its stable code
travels in the body, so a client branches on the code and not the message),
`400` a malformed request or payload, `503` the group is unavailable or the
proposal's outcome is unknown — retry that one.

`POST /organizations` is wired to the host's `Provisioner` seam, so the gateway
needs to know nothing about groups or genesis; a host without it answers `503`.
It is idempotent — every key and id derives from the business tuple (D12) — which
is also why replaying it answers the same placement instead of creating a second
organization.

Inbound ids are **parsed** with `Id::parse`, never adopted, so untrusted strings
cannot become identity (D10/D12).

## 6. Limits

- Authentication is wired: `OidcAuthenticator` validates tokens locally against
  any provider's JWKS, and `loomery-server` serves this router
  ([host.md](host.md)). *Authorization* is not: reads and writes both require a
  valid token, but nothing yet checks that the caller belongs to the
  organization. That check arrives with the read models.
- `GET /events` requires a bearer token (an organization's event log is tenant
  data) and is rate-limited by nothing but the provider's own latency.
- `GET /events` returns the group's whole applied-event log for the
  organization, so a read is `O(history)`. A real read model (projections)
  replaces this in the gateway's read plane.
- No rate limiting, request-size limits or tracing yet.
