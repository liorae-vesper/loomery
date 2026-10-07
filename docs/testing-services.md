# Test services: Keycloak and NATS JetStream

The default test suite is **self-contained**: it needs no broker and no identity
provider. Two optional docker services let the integration suite exercise the
real thing:

| Service | Purpose | Shell seam |
|---|---|---|
| **NATS JetStream** | the outbox's broker: durable, deduplicating delivery | [`outbox::Publisher`](../crates/shell/src/outbox/mod.rs) |
| **Keycloak** | OIDC tokens and the `groups` → admin claim | [`gateway::Authenticator`](../crates/shell/src/gateway/identity.rs) |

The adapters live behind the `test-services` cargo feature, and the integration
tests require the env vars below, so nothing here runs in `mise run test`.

## 1. Start the services

```sh
mise run svc-up                # docker compose up -d + readiness wait
mise run test-services         # up, lint the adapters, run the integration tests
mise run bench-services-stress # up, then the load profiles (see §6)
mise run svc-down              # stop and delete the state
```

Ports are overridable so the stack can coexist with other local services:

```sh
NATS_PORT=4223 NATS_MONITOR_PORT=8223 KEYCLOAK_PORT=8081 mise run svc-up
```

The compose file is [`compose.test.yaml`](../compose.test.yaml); the realm
import is [`scripts/test-services/keycloak/loomery-realm.json`](../scripts/test-services/keycloak/loomery-realm.json).

## 2. What the services are configured with

### Keycloak

`start-dev --import-realm`, with a realm `loomery` importing:

| Item | Value |
|---|---|
| client | `loomery-gateway` — public, password grant enabled |
| user `ada` | password `ada`, no groups |
| user `admin` | password `admin`, member of `/admins` |
| client scope `loomery-groups` | maps group membership to the `groups` claim (token **and** userinfo) |

The gateway's [Keycloak authenticator](../crates/shell/src/gateway/keycloak.rs)
calls the realm's `userinfo` endpoint with the bearer token, maps `sub` to the
user id, and sets `is_admin` when `groups` contains `admins`
(`with_admin_group` changes the group).

### NATS JetStream

`nats -js` (JetStream on, monitoring on `:8222`). The publisher creates the
stream on connect:

| Item | Value |
|---|---|
| stream | `LOOMERY_OUTBOX` |
| subjects | `loomery.>` |
| dedup | the publish sets `Nats-Msg-Id` to the outbox's `<group>:<log_index>:e<pos>` (D11) |

## 3. Environment

| Variable | Default used by the tasks | Meaning |
|---|---|---|
| `LOOMERY_TEST_NATS_URL` | `nats://127.0.0.1:4222` | NATS client URL |
| `LOOMERY_TEST_KEYCLOAK_URL` | `http://127.0.0.1:8080` | Keycloak base URL |
| `LOOMERY_TEST_KEYCLOAK_REALM` | `loomery` | realm name |
| `LOOMERY_TEST_KEYCLOAK_CLIENT` | `loomery-gateway` | client id for the password grant |
| `LOOMERY_TEST_HTTPS_PROBE` | unset | when set, the suite performs one real HTTPS request to this URL (TLS smoke test) |
| `NATS_PORT`, `NATS_MONITOR_PORT`, `KEYCLOAK_PORT` | `4222`, `8222`, `8080` | compose host ports |

The integration tests fail loudly if a required variable is missing rather than
skipping quietly, so a green run always means the services actually answered.

## 4. What the integration suite proves

`crates/shell/tests/test_services.rs` (run with `--features test-services`):

- **JetStream dedup** — publishing two messages with the same `Nats-Msg-Id` and
  one with a different id grows the stream by exactly **two** messages.
- **Keycloak identity** — `ada`'s token authenticates as a non-admin, `admin`'s
  token authenticates as an admin, and a garbage or missing token is rejected.
- **Concurrent authentication** — six workers authenticate tokens round-robin
  and assert that every result is *that token's* identity, with the negative
  paths still classed as `Unknown` and `Missing`.
- **Broker dedup under replay** — a booted group's events are published, then
  replayed from a fresh cursor: the stream grows by exactly the distinct count
  and not at all on the replay.
- **The command plane under load** — two tenants take commands concurrently
  through the real `CommandPlane` (a Keycloak authentication per command); the
  test asserts that each event's recorded actor matches the token that submitted
  it, then publishes and replays those events to the broker.
- **The runtime OIDC adapter** — discovery, JWKS and local JWT validation against
  the live realm: an admin token authenticates with the admin claim, another
  user's token does not, and a truncated token is refused.
- **The runtime NATS consumer** — a published message is peeked (returned again
  until acked) and disappears once acknowledged.
- **The host against the real broker** — `Host::boot` with the real publisher
  provisions a tenant, takes a write over the gateway, and the outbox worker
  publishes it to `JetStream` with a persisted cursor.

> The realm declares the `sub` protocol mapper explicitly. A Keycloak realm
> imported without it issues access tokens with **no subject claim**, which no
> resource server can authenticate; the fixture mirrors the built-in `basic`
> scope for that reason.
- **TLS (opt-in)** — with `LOOMERY_TEST_HTTPS_PROBE` set, one real HTTPS request
  proves the `rustls-no-provider` stack and the installed `ring` provider complete
  a handshake:

  ```sh
  LOOMERY_TEST_HTTPS_PROBE=https://example.com \
    cargo test -p loomery-shell --features test-services --test test_services https_probe
  ```

```sh
LOOMERY_TEST_NATS_URL=nats://127.0.0.1:4222 \
LOOMERY_TEST_KEYCLOAK_URL=http://127.0.0.1:8080 \
cargo test -p loomery-shell --features test-services --test test_services
```

## 5. CI

The `Test services` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
starts the same compose stack on the runner, waits for readiness, runs the
integration tests, and always tears the stack down.

## 6. Stress profiles

[`benchmarks/services-stress.md`](benchmarks/services-stress.md) documents three
load profiles over the same adapters (`mise run bench-services-stress`):
concurrent authentication, Raft → outbox → JetStream with a crash replay, and the
whole command plane with a real Keycloak authentication per command. Each profile
asserts its invariants (identity integrity, dedup on replay, per-actor agreement)
and exits non-zero when one fails.

They are not part of CI: they need a warmed-up stack and their numbers depend on
the host. The same invariants run at a tenth of the size in the integration suite
above, which *is* in the `Test services` job.

## 7. Limits

- The adapters are behind `test-services`; the default `mise run verify/test`
  and the coverage gate do not compile or score them.
- **TLS is enabled.** `reqwest` uses `rustls-no-provider` and the workspace's
  `ring` provider. `rustls-no-provider` means a process must install a provider
  **before** building a `reqwest::Client`; `KeycloakAuthenticator` does that
  itself, and [`loomery_shell::gateway::install_tls_provider`] is public for any
  other TLS user. Verify it end to end with the HTTPS probe above.
- **One allowlist addition.** reqwest's TLS resolves `webpki-root-certs`
  (CDLA-Permissive-2.0) for the `wasm32` target, and `cargo-deny` resolves every
  target even though Loomery never builds for wasm, so that license is on the
  `deny.toml` allowlist. The §2.1 obligation — the agreement text travels with
  the data — is met by the committed
  [third-party license bundle](third-party-licenses.md) (`THIRDPARTY.yml`).
- **`async-nats` is trimmed** to `default-features = false, features =
  ["jetstream", "nkeys"]`, so the WebSocket transport is not pulled in; the
  outbox needs only the core client and JetStream.
- The Keycloak realm is a **development** realm (`start-dev`, no TLS, fixed
  passwords). A deployment uses its own realm and the same authenticator.
