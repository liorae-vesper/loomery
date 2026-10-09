# Loomery

**An event-sourced backend for team collaboration, built in Rust.**

Loomery is a distributed backend for organizations, workspaces, projects,
tasks, documentation, and AI-assisted workflows. It is built around a **pure,
deterministic core** and an **imperative shell**, with consensus provided by
[OpenRaft](https://github.com/openraft/openraft) on the [Tokio](https://tokio.rs)
async runtime.

> **Status: early development.** The pure core (`crates/core`) is in progress;
> the shell has OpenRaft consensus, tonic networking and RocksDB persistence.
> The gateway, control plane and outbox remain planned.

---

## Goals

Loomery is designed around a small set of non-negotiable principles:

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

Loomery follows Gary Bernhardt's **Functional Core, Imperative Shell** pattern.

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
loomery/
├── crates/
│   ├── core/          # loomery-core — the pure, deterministic domain core
│   ├── genesis/       # loomery-genesis — the deterministic bootstrap script
│   ├── server/        # loomery-server — the runtime entry point (config + wiring)
│   └── shell/         # loomery-shell — Raft, tonic, RocksDB, gateway and workers
├── docs/              # the published documentation site (VitePress; index: docs/index.md)
├── workpad/           # the engineering record: decisions, benchmarks, research — staged
├── THIRDPARTY.yml     # bundled third-party license texts (docs/third-party-licenses.md)
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

Loomery uses [mise](https://mise.jdx.dev) to manage the Rust toolchain and
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
- `cargo-deny`, `cargo-audit`, `cargo-llvm-cov`, `cargo-crap`,
  `cargo-bundle-licenses` — cargo subcommands
- `cocogitto` — conventional-commit tooling
- `hk` — pre-commit hooks

---

## Getting started

All developer commands are exposed as [mise tasks](https://mise.jdx.dev/tasks/).
Run one with `mise run <task>`.

| Task | What it does |
|------|--------------|
| `mise run verify` | `cargo check`, clippy (`-D warnings`), `fmt --check`, `cargo deny check` |
| `mise run test` | `cargo test --workspace` |
| `mise run coverage` | Generate an LCOV coverage report (`cargo llvm-cov --workspace`) |
| `mise run crap` | Compute **CRAP** scores from coverage; fails above the threshold |
| `mise run audit` | `cargo audit` for security advisories |
| `mise run server` | Run the runtime host: `mise run server -- --config host.json` |
| `mise run licenses` | Regenerate the bundled third-party license texts (`THIRDPARTY.yml`) |
| `mise run licenses-check` | Fail if `THIRDPARTY.yml` is stale (also a CI job) |

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

Buildkite runs six parallel steps (`.buildkite/pipeline.yml`), each driven by a
mise task rather than hardcoded commands:

- **verify** — `mise run verify`
- **test** — `mise run test`
- **test-services** — the Keycloak + NATS integration suite (`compose.test.yaml`)
- **licenses** — `mise run licenses-check`
- **quality** — `mise run crap` (coverage + CRAP gate)
- **audit** — `mise run audit`

Every step runs inside the image built from
[`.buildkite/Dockerfile`](.buildkite/Dockerfile): the Rust toolchain and cargo
tools pinned by `mise.lock` (installed with `mise install`), plus `protoc`,
`libclang` and the Docker CLI the build and the integration suite need. Nothing
is installed at run time, so the pipeline reads the same as
`docker run … mise run <task>`.

The image build step uploads `loomery-ci.tar.gz` as a build artifact. Each
parallel step downloads and loads that archive into its own Docker daemon,
so hosted agents do not need to share local images or use a separate registry.
Image tags include the Buildkite build ID to isolate concurrent builds.

[Linux hosted cache volumes](https://buildkite.com/docs/agent/buildkite-hosted/cache-volumes)
retain Cargo's registry sources, Git dependencies,
security advisory databases and the complete build output (including RocksDB's
native library). Each step has its own cache so parallel jobs and coverage flags
do not overwrite one another. The image build also caches all Docker build
layers, including the tools installed by mise. The first successful run warms
each cache; later runs reuse dependencies whose versions and build settings
still match. Failed jobs do not save their cache volumes.
Increment the Rust cache names' `v1` suffix when changing native compilers or
system libraries in the CI image, to force a fresh native build.

---

## Documentation

The published documentation is a VitePress site in `docs/`, built with
`mise run docs-site` and entered at [its home page](docs/index.md):

- [Architecture at a glance](docs/architecture.md) and the [domain model](docs/domain-model.md)
- [Configuration](docs/configuration.md) and [Raft configuration](docs/raft-configuration.md)
- [Shell reference](docs/shell.md), [runtime host](docs/host.md) and [search](docs/search.md)
- The [tutorials](docs/tutorials/shell-group.md), in order

The engineering record — the decisions register, the implementation walkthrough,
the benchmark notes and the research behind them — is staged in
[`workpad/`](workpad/documentation-index.md). It is not published, and no page of
the site links to it.

---

## Running it

`loomery-server` wires the crates into a process: the control group, one Raft
group per tenant, the axum gateway, the outbox workers and the saga runner,
with a provider-agnostic OIDC adapter for identity.

```sh
mise run svc-up                        # Keycloak + NATS for local development
mise run server -- --config host.json  # see docs/host.md for the configuration
```

## License

Loomery is licensed under the [Mozilla Public License 2.0](LICENSE).

The license texts of every dependency are bundled in
[`THIRDPARTY.yml`](THIRDPARTY.yml) and ship with release artifacts; see
[third-party licenses](docs/third-party-licenses.md) for how the bundle is
generated, checked and kept complete.
