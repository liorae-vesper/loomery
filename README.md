# Loomery

**An event-sourced backend for team collaboration, built in Rust.**

Loomery is a distributed backend for organizations, workspaces, projects,
tasks, documentation, and AI-assisted workflows. It is built around a **pure,
deterministic core** and an **imperative shell**, with consensus provided by
[OpenRaft](https://github.com/openraft/openraft) on the [Tokio](https://tokio.rs)
async runtime.

> **Status: early development.** The deterministic core, the consensus shell and the
> runtime host are in place: OpenRaft consensus over tonic, RocksDB persistence, the
> gateway and control plane, the outbox and sagas, and a Phase-1 end-to-end acceptance
> suite. Reads still answer from the applied log rather than from durable projections,
> and the search index is not built. [CONTINUE.md](workpad/CONTINUE.md) records what is
> done and what is next.

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
and versioning/upcast machinery. See D12 in `workpad/design.md` for the identity
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

GitHub Actions runs seven parallel gates
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)), each driven by a mise
task rather than hardcoded commands:

- **verify** — `mise run verify`
- **test** — `mise run test`
- **test-services** — the Keycloak + NATS integration suite (`compose.test.yaml`)
- **licenses** — `mise run licenses-check`
- **docs** — `mise run docs-mermaid` and `mise run docs-links`
- **quality** — `mise run crap` (coverage + CRAP gate)
- **audit** — `mise run audit`

No job runs `cargo` directly — each calls `mise run <task>`, and the task in
`mise.toml` is the only place a cargo invocation lives. Cargo then runs through
[mr-boxington](https://mr-boxington.jdx.dev/), because the Rust tool in
`mise.toml` carries `mr_boxington = true`. The toolchains come from
`mise.toml` + `mise.lock` through
[`jdx/mise-action`](https://github.com/jdx/mise-action), and
[`jdx/mr-boxington-action`](https://github.com/jdx/mr-boxington-action) restores
the Cargo target tree and the registry/git downloads from an earlier run, so a
job recompiles only what changed. Both actions are pinned by major tag.

[`scripts/install-host-deps.sh`](scripts/install-host-deps.sh) installs the
system packages the build needs that the runner image does not carry:
`protobuf-compiler` for `build.rs`, `clang` and `libclang-dev` for the build-time
bindgen in `librocksdb-sys`, and `zlib1g-dev` for RocksDB. Everything else — the
Rust toolchain, the cargo tools and Node — is a mise tool.

The `publish-docs` job builds the VitePress site with `mise run docs-site` and
deploys it to GitHub Pages after a push to `main` passes every gate; the site
lives at <https://liorae-vesper.github.io/loomery/>. The repository's Pages
source has to be set to **GitHub Actions** for it to deploy.

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
