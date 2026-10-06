# Services stress profiles

The integration suite ([testing-services.md](../testing-services.md)) proves the
Keycloak and NATS JetStream adapters work. These profiles push the same adapters
through the shell's real code paths at a size the suite cannot afford, and they
assert that the properties the design depends on still hold under concurrency.

The harness is [`crates/shell/examples/services_stress.rs`](../../crates/shell/examples/services_stress.rs),
run by [`scripts/bench-services-stress.py`](../../scripts/bench-services-stress.py):

```sh
mise run bench-services-stress                      # all three profiles
mise run bench-services-stress -- --quick           # smoke-size profiles
mise run bench-services-stress -- --scenario e2e    # one profile
```

The driver starts the compose stack, builds the example in release mode, runs the
profiles and writes the raw JSON to `benchmark-results/services-stress/`.

## What each profile does

| Profile | Workload | Knobs (defaults) |
|---|---|---|
| `auth` | Password-grant token acquisition at full concurrency, then concurrent `userinfo` authentication (three quarters valid tokens, the rest split between a bad token and no token) | `WORKERS=32`, `REQUESTS=8000`, `GRANTS=200` |
| `outbox` | Boots `GROUPS` single-node groups, proposes `COMMANDS` `task.create` commands into each, then publishes every applied event through one outbox per group | `GROUPS=8`, `COMMANDS=1000`, `WORKERS=8` |
| `e2e` | The same, but every command goes through the command plane, so each one performs a **real Keycloak authentication** before it is proposed | `GROUPS=4`, `COMMANDS=500`, `WORKERS=16` |

Every knob is a `LOOMERY_STRESS_*` environment variable; the service URLs are the
`LOOMERY_TEST_*` variables the integration suite uses.

## What each profile asserts

These are the point of the profiles: a run that violates one exits non-zero.

- **`auth`** — under concurrency, every valid token resolves to *its own*
  identity (the whole `Identity`, so neither the user id nor the admin claim can
  cross), a bad token is `Unknown`, a missing token is `Missing`, and the
  observed counts of the three classes equal the plan exactly.
- **`outbox`** — every command produces exactly one applied event; the first
  publish grows the stream by exactly the number of distinct messages; and a
  **crash replay** (fresh outboxes, cursors at the start, so every message is
  re-sent) leaves the stream unchanged, because the broker's `Nats-Msg-Id` dedup
  window absorbs it. The harness reads that window and fails if the replay would
  fall outside it.
- **`e2e`** — every command is accepted (an authentication or proposal failure
  fails the run), and for every tenant the actor recorded on each event matches
  the token that submitted it, per user. It then repeats the publish and the
  crash replay of `outbox`.

## Measured

One host (Linux, one process, release build), the compose stack from
`compose.test.yaml` on loopback, three consecutive runs of the default profiles:

| Profile | Run | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|---|
| `auth` (8,000 calls, 32 workers) | 1 | 53,393/s | 589 us | 1,152 us | 1,586 us |
| | 2 | 59,151/s | 509 us | 999 us | 1,639 us |
| | 3 | 60,401/s | 521 us | 1,010 us | 1,340 us |
| `outbox` (8 groups x 1,000 events) | 1 | 22,453 events/s | 343 us | 634 us | 784 us |
| | 2 | 22,630 events/s | 350 us | 628 us | 711 us |
| | 3 | 23,848 events/s | 331 us | 588 us | 639 us |
| `e2e` (2,000 commands, one real auth each) | 1 | 25,965 commands/s | 581 us | 931 us | 1,059 us |
| | 2 | 25,334 commands/s | 610 us | 992 us | 1,131 us |
| | 3 | 25,992 commands/s | 570 us | 971 us | 1,083 us |

Supporting numbers from the same runs:

| Measurement | Value |
|---|---|
| Password grants (200, 32 workers) | 301-311/s, p50 90-97 ms |
| Outbox publish (8,000 messages, 8 groups concurrently) | 0.18-0.21 s, i.e. 38k-44k messages/s |
| `e2e` publish (2,000 messages, 4 groups concurrently) | ~0.08 s, i.e. ~25k messages/s |
| JetStream stream growth, first pass vs crash replay | `+8000` then `+0` (`e2e`: `+2000` then `+0`) |
| JetStream duplicate window observed | 120 s |
| Invariant violations across all runs | 0 |

Two things the runs show that are worth recording:

- **Keycloak warms up.** A cold run of `auth` measured 20,590/s (p50 1,517 us,
  p99 4,127 us) and an early small run 7,414/s; the settled runs above measure
  2-3x that. Token *grants* are orders of magnitude slower than `userinfo` calls
  (p50 ~95 ms vs ~0.5 ms) because each one hashes a password — which is why the
  grant phase is measured separately.
- **The `e2e` rate is bounded by loopback HTTP, not by Raft.** Each command needs
  one `userinfo` round trip, so the profile measures the gateway path with a real
  identity provider in it, not the consensus path. `outbox` is the profile that
  measures propose → apply → outbox → broker.

## What the numbers do not say

- **One process, no disk, no network.** The Raft groups are in-memory
  single-node groups; there is no peer replication, no RocksDB fsync and no
  gateway HTTP server in the loop. Publish rates are *acknowledged* publishes to
  a JetStream store on tmpfs — not durable-write rates. For storage and
  multi-process numbers see [the consensus benchmark](README.md).
- **Not an isolated component measurement.** Host CPU, the Keycloak dev-mode
  server, the broker and the JVM all share the machine. Percentiles include
  queueing in every one of them.
- **A failure is a finding, not a flake.** If the host or Keycloak saturates, a
  valid token can fail to authenticate and the run fails loudly. Lower
  `LOOMERY_STRESS_WORKERS` rather than retrying, and treat the lower ceiling as
  the result.
- **Small profiles are dominated by setup.** Boot, token acquisition and stream
  info calls are included in wall-clock time, so the default (larger) profiles
  are the ones to compare; `--quick` exists for smoke runs, not for numbers.
- **The 120 s dedup window is a real bound.** A crash replay later than the
  window would deliver duplicates; the outbox's correctness argument covers a
  crash *and* a retry inside it, and the harness checks that its replay is inside
  it. Production deployments should set the window to exceed their worst-case
  restart.

## Relationship to the integration suite

`crates/shell/tests/test_services.rs` runs the same three invariants at a tenth
of the size (`concurrent_authentication_keeps_identities_apart`,
`a_replayed_outbox_batch_is_absorbed_by_the_broker`,
`commands_through_the_plane_hold_up_with_real_auth_and_the_broker`), which is why
they belong in the `Test services` CI job. The profiles here are deliberately not
in CI: they need a warmed-up dev-mode Keycloak and would make the job's runtime
and stability depend on the runner's CPU.
