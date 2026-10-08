# Development Guardrails

The project's development guardrails — the tools and gates that keep the
codebase formatted, lint-clean, type-safe, tested, and dependency-safe.

| Guardrail | Loomery (Rust) |
|---|---|
| Formatter | `cargo fmt --check` (`rustfmt.toml`: 2024 edition, max_width 100, tabs 4) |
| Lint / static analysis | `cargo clippy --workspace --all-targets -- -D warnings` |
| Tests | `cargo test --workspace` |
| Coverage report/floor | `cargo llvm-cov` via `cargo-llvm-cov` (floor: 80%) |
| Dependency advisories | `cargo audit` |
| License allowlist + bans | `cargo deny check` (`deny.toml`) |
| Pre-commit hooks | `hk` (commit-msg + check/fix hooks) |
| Commit message lint | `cog verify` (conventional commits, cocogitto) |
| Combined dev gate | `mise run verify` (see `mise.toml`) |
| Complexity gate | `mise run crap` (coverage-based CRAP scores via cargo-crap; skips `**/*_tests.rs`, `loomery-server`'s `main.rs` (the binary is a single file) and the `JetStream` adapter) |
| Diagram syntax | `mise run docs-mermaid` (`tools/mermaid-check/`: every ```` ```mermaid ```` block in `docs/`, `README.md` and `workpad/` must parse) |
| Lint policy | `[workspace.lints]` in `Cargo.toml`: deny `unwrap_used`, `expect_used`, `indexing_slicing`, `string_slice`, `arithmetic_side_effects`, `unchecked_time_subtraction`, `todo`, `unimplemented`, `panic`, `unconditional_panic`; warn on `missing_docs`. Enforced by `cargo clippy --workspace --all-targets -- -D warnings` |
| Documentation links | `mise run docs-links` (`tools/docs-links/`: relative links resolve, `#anchors` match a heading in the target, every published document is reachable from `docs/README.md`, a benchmark run named in the prose exists in the committed results, and no published page links into `workpad/`) |

## The `cargo deny` policy (`deny.toml`)

- **Advisories:** fail on any unmaintained or unsound crate (direct or
  transitive); yanked versions denied.
- **Licenses:** explicit allowlist (MIT, Apache-2.0, BSD-2/3-Clause, ISC,
  MPL-2.0, Unlicense, Zlib) at 0.8 confidence.
- **Bans:** no `openssl` (prefer rustls), no `md-5`, no `sha1`; multiple
  versions of a dependency warn; wildcard versions denied.
- **Sources:** locked to crates.io only; unknown registries/git deps denied.

## How they fit together

1. **`hk` `commit-msg` hook** runs on every commit: `cog verify` (conventional
   commits, fail-fast), then the cargo quality gates (`cargo fmt --check`,
   `cargo deny check`, `cargo clippy --workspace --all-targets -- -D warnings`).
   - A commit message containing the word `wip` bypasses *all* gates (the
     informal escape hatch).
   - Merge-shaped messages are rejected, `pre-merge-commit` blocks `git merge`,
     and `pre-push` refuses ranges containing merge commits — **linear history
     only** (rebase workflow).
   - While a crate has no `Cargo.toml` yet, the cargo gates skip gracefully.
2. **`mise run verify`** is the one-command local gate (also runs in the
   `hk check` hook): `cargo check`, `cargo check -p loomery-shell`, clippy with
   `-D warnings`, fmt check, `cargo deny check`. The second command is not
   redundant with the first: in a workspace build the server's
   `features = ["nats", "oidc"]` unify features into the shell, so the shell is
   never compiled in its own default configuration, and a feature it needs —
   `rand_core/getrandom`, which `gateway::precompute` needs for `OsRng` — was absent
   for as long as nothing built the crate alone. A per-package check is the only build
   that sees that; `--workspace --no-default-features` does not, because the masking
   feature is requested explicitly rather than inherited from a default.
3. **`hk fix`** runs `cargo fmt` to auto-format.
4. **CI** — [`.buildkite/pipeline.yml`](../.buildkite/pipeline.yml) runs verify,
   tests, licenses, docs, quality (`mise run crap`), audit and the service
   integration tests as separate steps. Every step runs inside the image built from
   [`.buildkite/Dockerfile`](../.buildkite/Dockerfile), which carries the
   toolchain `mise.lock` pins, `protobuf-compiler` for `tonic-prost-build` (the
   shell's `build.rs` compiles `proto/raft.proto`), `libclang-dev` for the
   build-time `bindgen` inside `librocksdb-sys`, and the Docker CLI the
   integration step drives the compose stack with. No step installs anything.
5. **That image is how a change is checked before it lands** — build it once and
   run the same tasks inside it, so the local check and CI share one toolchain:

   ```sh
   docker build --file .buildkite/Dockerfile --tag loomery-ci .
   docker run --rm -v "$PWD:/workdir" -w /workdir -v loomery-ci-target:/target \
     -e CARGO_TARGET_DIR=/target loomery-ci mise run verify
   ```

   Swap the task for any one the pipeline calls (`mise run test`,
   `mise run licenses-check`, `mise run docs-mermaid`, `mise run crap`,
   `mise run audit`). The container
   runs as root, exactly as the pipeline's steps do, so `CARGO_TARGET_DIR` points
   at a named volume: build artifacts stay out of the working tree instead of
   appearing in `target/` owned by root. To iterate on the pipeline itself,
   `bk pipeline validate --file .buildkite/pipeline.yml` checks it locally,
   without a Buildkite account.

   The `test services` step runs the compose stack through the host Docker daemon
   and the host network (see
   [`.buildkite/scripts/test-services.sh`](../.buildkite/scripts/test-services.sh)),
   so `mise run svc-up` must be running for it to pass when run by hand.

## Working agreements

Not enforced by a hook, so they are stated once, here rather than in a note:

- **One branch per part of the work; linear history; fast-forward only into `main`.**
  No merge commits, and a branch merges only once the CI-equivalent gate passes:
  `mise run verify`, `mise run test`, `mise run licenses-check`,
  `mise run docs-mermaid`, `mise run docs-links`, `mise run crap`, `mise run audit`.
- **Never commit a tree that does not compile**, on `main` or on a branch. The
  `wip` marker exists to bypass the gates while a commit is genuinely in progress,
  not to land one.
- **The default test suite stays self-contained**: fakes and in-process services.
  Real NATS, OIDC and JetStream stay behind the `nats`, `oidc` and `test-services`
  features ([testing-services.md](testing-services.md)).
- **Probes and soaks run `--release`.** A debug build measures the compiler.
- **Documentation is part of done.** A change to behaviour, configuration or a
  measurement updates the document that describes it, and working notes are
  promoted into `docs/` rather than left in `workpad/` — which is staging, not an
  archive, and whose rules are in that directory's own README.
- **Raise it, don't guess**: if a design decision is unspecified, a new dependency
  is needed, or a gate fails for environmental reasons, say so before implementing.

## Coverage data flow

`cargo test --workspace` runs the suite; coverage uses
[cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov)
(`cargo llvm-cov --workspace --lcov --output-path lcov.info`), floor 80%.

## Notes

- Dependencies are governed by `cargo-deny` and must keep the license
  allowlist green (`cargo deny check licenses`).
- Toolchain is pinned via `mise` (`rust 1.98.1`, edition 2024); `cargo`
  commands should run through `mise exec --` if rust isn't on PATH.
- The CRAP gate scores production code only: `cargo-crap`'s default exclusions
  (`tests/**`, `benches/**`, `examples/**`) are extended with `**/*_tests.rs`,
  the repo's convention for in-crate integration tests that need private APIs
  (`persistent_tests.rs`, `tls_tests.rs`, …). Those still run in
  `mise run test`; they are simply not scored as production functions.
- Two slices are excluded for the same reason in reverse — the coverage run
  cannot reach them because they need live services: the `loomery-server` binary
  (argument parsing, signal handling and wiring; its logic is
  `shell::host::Host`, which *is* scored and unit-tested) and
  `crates/shell/src/outbox/nats.rs` (the `JetStream` client; the subject and
  message-id parsing it relies on, the cursor and the worker are scored and
  tested). Both run in `mise run test-services`.

---

*Current tasks and tool versions are defined in [mise.toml](../mise.toml);
hook behavior is defined in [hk.pkl](../hk.pkl).*