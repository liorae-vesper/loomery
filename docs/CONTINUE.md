# Continue Trellis — Session Handoff

You are continuing development on **Trellis**, an event-sourced backend for
team collaboration, built in **Rust** on **Tokio** with **OpenRaft** for
consensus. The project lives at
`/home/john/Workspace/Liorae/trellis` (git branch: `main`, linear history).

Read `docs/design.md` first — it is the source of truth for the architecture,
crate layout, phase plan, and the decisions register.

---

## What Trellis is

Organizations, workspaces, projects, tasks, documentation, and AI-assisted
workflows. Architecture: a **pure functional core** (deterministic
`execute`/`apply` — no I/O, no wall clock, no randomness) wrapped by an
imperative shell (Tokio + OpenRaft consensus, storage, NATS outbox)
planned for Phases 1+.

## Current state (DONE)

- **Scaffold + guardrails** — complete (Cargo workspace, edition 2024,
  `rustfmt.toml`, `deny.toml` license/advisory policy, hk hooks, cog
  conventional commits, `mise run verify`). See `docs/guardrails.md`.
- **Pure core — scaffolded, in progress** (`docs/design.md` §3, §8 tracker):
  - `core::id::Id` — UUIDv7 newtype (`uuid 1.26`, `now_v7`), canonical string
    form, `Deref<Target = str>`, `From<&str>`/`From<String>` for injected ids;
    shell-side `Id::new()`; serde round-trip + shape tests.
  - `core::envelope` — `Event` (fields per §6 of `design.md`) with a
    nested, versioned `Payload { version, data }`; serde round-trip + exact
    wire-format snapshot tests (frozen payloads, D3).
  - `core::timestamp::Timestamp` — injected i64 ms-since-epoch (D5);
    `From<i64>`/`as_millis()`/`Deref`, `now()` is shell-side only.
  - 18 unit tests passing (serde round-trips, wire-format snapshot, id
    shape/uniqueness, timestamp ordering + clock sanity, actor + command
    round-trips, error display/equality/source).

## Conventions (non-negotiable)

- **Domain-first modules**: `core::task::Task`, never `core::domain::task`.
- State is a struct per aggregate; commands/events are **typed serde structs**
  with a `kind`/`event_type` discriminator, JSON payloads inside envelopes.
- `fn execute(state, command) -> Result<Execution, DomainError>` and
  `fn apply(state, event) -> State`; **creation events build state regardless
  of prior state** (re-add/re-assign safe).
- `Aggregate` trait with shared `process` (dedup hit → `Replayed`; miss →
  execute) and `fold` (sequential apply) helpers — module/trait first.
- Errors: `DomainError<C>` + per-area code enums (machine-readable
  discriminators). Never rename an existing code.
- **The core never reads the clock or generates IDs** — timestamps/IDs are
  injected through the command envelope (test helpers build deterministic
  envelopes).
- Processing a command does NOT record the dedup entry — the shell records it
  after the events are durably appended and applied (contract documented in
  `core::aggregate`).
- Validate untrusted input against bounded, compile-time schemas at the
  boundary (see D10 in `design.md`) — never build types from user strings.

## Development guardrails (keep green)

```bash
mise run verify                    # cargo check + clippy -D warnings + fmt --check + deny + package
cargo test --workspace             # unit/property tests
cargo llvm-cov --workspace         # coverage floor 80% (cargo-llvm-cov)
cargo audit                        # dependency advisories
cargo deny check                   # license allowlist + bans (deny.toml)
```

- Pre-commit hooks run via `hk` (commit-msg: cog verify + fmt/deny/clippy);
  commits are made with `mise exec -- cog commit <type> "<message>"`
  (types: feat, fix, refactor, chore, docs, test, style, ci, perf...).
- A commit message containing `wip` bypasses the gates (escape hatch).
- **Linear history on `main` only** — merge commits are blocked by hk; use
  rebase.

## Dependency stack

Core (in `Cargo.lock`): `uuid` (v7). Planned core: `serde` + `serde_json`
(envelope/payload D3), `thiserror` (domain errors).
Shell (Phase 1+): **`tokio`** (multi-thread), **`openraft` 0.9.x** +
`tonic` (gRPC `RaftNetwork`), `async-nats` (JetStream outbox), `axum`
(gateway), `tracing` (+ `tracing-opentelemetry` later), `dashmap` (read
models), `sled`/`rocksdb` (storage spike, D2).
Dev/test: `proptest` (property tests), `cargo-llvm-cov`, `cargo-audit`,
`cargo-deny` (already in `mise.toml`).

## Next: Phase 0 pure core, then Phase 1 control plane

See `docs/design.md` §5 and the roadmap tracker. Scope:

1. **Finish the pure core** — `Command`
   (done — `core::envelope::Command`), `Error`/`Code`,
   `Versioning`, `Execution`/
   `IntegrationEvent`, `DedupIndex`, the `Aggregate` trait, then the six
   aggregates (Organization, User, Workspace, Task, OrganizationAssignment,
   WorkspaceMembership) with `proptest` property tests (transition matrices,
   replay determinism, fold associativity, dedup window).
2. **Phase 1 — control plane & onboarding**
   1. **OpenRaft 0.9 spike**: control group `RaftLogStorage`/`RaftStateMachine`
      + `RaftNetwork` over tonic; `Raft::new`/`RaftServer` bootstrap.
   2. Router read model (`dashmap`) + RYW `X-Min-Index` session tokens
      (50 ms hold → leader redirect).
   3. Genesis bootstrap worker — tenant groups born with their first three
      events committed (assign leader → create default workspace → add Owner),
      `actor = Saga { user_id: None, name: "control-plane:Bootstrap" }`, deterministic
      causation;
      crash-resume idempotency.
   4. axum gateway — command routing, `causation_id` minting, edge pre-compute
      hooks (argon2), system-admin flag from OIDC `groups` claim.
   5. Invitation domain + acceptance saga + outbox email event.
   6. NATS outbox tailer first slice via `async-nats` — one stream, one
      consumer, dedup by `(group_id, log_index)` (D8/D11).
   7. Saga-runner seed — consumer task + cursor + retry classification.

**Phase 1 E2E gate:** register org → genesis ①②③ → workspace + Owner-led;
invite by email → accept → provisioned → can log in and read the board;
crash mid-provisioning resumes with no duplicate genesis; duplicate
`causation_id` dedup-hits with RYW honored; admin-only commands enforced.

**Decisions to revisit at Phase 1:** D1 (OpenRaft transport), D2 (storage:
sled/rocksdb vs hand-rolled segment engine), D8 (async-nats wiring). Open
decisions D6/D7 (FTS, vectors) are Phase 4.

## Open questions for you during Phase 1

- Decide how the control group maps the router table (OpenRaft `client_write`
  into the control group's state machine, then project to `dashmap`).
- Decide `causation_id` minting rules at the gateway (client-generated
  preferred, warn otherwise).
- Decide the NATS subject/stream naming for the first slice (D11 — carries
  the v1 convention).
- 15 unit tests green (envelope wire-format snapshot, id/timestamp/actor
  serde);
  next: `Error`/`Code`, `Versioning`, then the aggregates, then Phase 1.

## Verification before committing

```bash
mise run verify && cargo test --workspace && cargo llvm-cov --workspace
```
Then commit with `mise exec -- cog commit ...` on `main`.