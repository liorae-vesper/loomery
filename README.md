# Trellis

**An event-sourced backend for team collaboration, built in Rust.**

Trellis is a distributed backend for organizations, workspaces, projects,
tasks, documentation, and AI-assisted workflows. It is built around a **pure,
deterministic core** and an **imperative shell**, with consensus provided by
[OpenRaft](https://github.com/openraft/openraft) on the [Tokio](https://tokio.rs)
async runtime.

> **Status: early development.** The pure core (`crates/core`) is in progress;
> the distributed shell (Raft consensus, gateway, outbox) is planned.

---

## Goals

Trellis is designed around a small set of non-negotiable principles:

1. **Append-only.** Events are never mutated; corrections are compensating events.
2. **Pure core.** `prepare`/`apply` are deterministic. All I/O lives in the shell.
3. **One consensus group per organization** plus a control group (users, orgs, router).
4. **Genesis bootstrap.** Tenant groups are born with their first three events committed.
5. **Read-Your-Writes.** Post-write reads carry `X-Min-Index` session tokens.
6. **WAL-tailing outbox.** No direct network calls from the consensus loop.
7. **Edge pre-computation.** Blocking-but-pure work (e.g. password hashing) at the gateway.
8. **Frozen payloads + upcast + writer gating.** Old events decode forever.
9. **DedupIndex.** The only idempotency store, folded into group state.
10. **No 2PC.** Cross-group coordination is choreography via NATS + sagas.

The result is an algebra that every Raft replica can rely on:

```
fold(fold(state, e1), e2) == fold(state, e1 ++ e2)
```

Identical commands produce identical applies — the foundation of deterministic
replay and consistent state across replicas.

---

## Architecture

Trellis follows Gary Bernhardt's **Functional Core, Imperative Shell** pattern.

- **The core (pure)** — business logic as deterministic functions:
  - `prepare(state, command) -> Result<Execution, DomainError>`
  - `apply(state, event) -> State`
  - The core never reads the clock, generates IDs, or touches I/O. IDs and
    timestamps are injected through the command envelope.
- **The shell (imperative, Tokio)** — transport, edge pre-computation, routing,
  consensus (OpenRaft), persistence, a WAL-tailing outbox to NATS JetStream,
  saga runners, and startup recovery.

```
[HTTP gateway (axum)] -> [edge pre-compute] -> [router] -> [Raft::client_write]
                                                             │  propose → log → quorum
                                                             v
                               execute/apply (pure core) → in-memory state
                                                             │  (+ async snapshot)
                                                             v
                              WAL tailer → NATS JetStream (outbox)
```

---

## Repository layout

```
trellis/
├── crates/
│   ├── core/          # trellis-core — the pure, deterministic domain core
│   └── genesis/       # trellis-genesis — the deterministic bootstrap script
├── docs/              # design docs, research notes, and guardrails
├── deny.toml          # cargo-deny policy (licenses, advisories, bans)
├── hk.pkl             # pre-commit hooks (commit message lint + quality gates)
├── mise.toml          # tool versions + dev tasks
└── rustfmt.toml
```

The core crate currently provides `Id` (canonical UUID: minted `v7` or derived
`v5`), `Key` (derived `UUIDv5` identity — causation keys, event ids, intent
fingerprints), `Timestamp`, `Actor`, `Event`/`Command`, `DomainError`, the
`AggregatePlan` trait (`prepare`/`apply`), the `Registry` idempotency window,
and versioning/upcast machinery. See D12 in `docs/design.md` for the identity
model. The genesis crate builds on it: the bootstrap's three commands with
derived identity, plus crash-resume progress read back from the log.

---

## Prerequisites

Trellis uses [mise](https://mise.jdx.dev) to manage the Rust toolchain and
developer tools at pinned versions. Install mise first:

```sh
# macOS / Linux
curl https://mise.jdx.dev/install.sh | sh
```

> All tool versions are pinned in `mise.toml` and locked in `mise.lock`.
> Installs are enforced from the lockfile (`settings.locked = true`), so the
> toolchain never silently re-resolves to a new version.

---

## Install dependencies

From the repository root:

```sh
mise install
```

This installs the exact pinned versions of:

- `rust` (1.98.1, edition 2024) — via rustup
- `cargo-deny`, `cargo-audit`, `cargo-llvm-cov`, `cargo-crap` — cargo subcommands
- `cocogitto` — conventional-commit tooling
- `hk` — pre-commit hooks

---

## Getting started

All developer commands are exposed as [mise tasks](https://mise.jdx.dev/tasks/).
Run one with `mise run <task>`.

| Task | What it does |
|------|--------------|
| `mise run verify` | `cargo check`, clippy (`-D warnings`), `fmt --check`, `cargo deny check`, `cargo package --workspace` |
| `mise run test` | `cargo test --workspace` |
| `mise run coverage` | Generate an LCOV coverage report (`cargo llvm-cov --workspace`) |
| `mise run crap` | Compute **CRAP** scores from coverage; fails above the threshold |
| `mise run audit` | `cargo audit` for security advisories |

The **one-command local gate** is:

```sh
mise run verify
```

### Running the tests

```sh
mise run test
```

The core is property-tested with [proptest](https://proptest-rs.github.io/proptest/),
including model-based tests for the `DedupIndex` eviction window and fold
associativity/determinism for the aggregate algebra.

---

## Continuous integration

GitHub Actions runs four parallel jobs (`.github/workflows/ci.yml`), each
driven by a mise task rather than hardcoded commands:

- **verify** — `mise run verify`
- **test** — `mise run test`
- **quality** — `mise run crap` (coverage + CRAP gate)
- **audit** — `mise run audit`

CI uses [`jdx/mise-action`](https://github.com/jdx/mise-action) to install the
locked toolchain (with default caching) and
[`Swatinem/rust-cache`](https://github.com/Swatinem/rust-cache) to cache cargo
build artifacts.

---

## Documentation

- [`docs/design.md`](docs/design.md) — architecture, decisions, and the phased build plan
- [`docs/guardrails.md`](docs/guardrails.md) — formatting, linting, and testing gates
- [`docs/CONTINUE.md`](docs/CONTINUE.md) — current work-in-progress and next steps
- [`docs/research/`](docs/research/) — notes on consensus, storage, and integrations

---

## License

Trellis is licensed under the [Mozilla Public License 2.0](LICENSE).
