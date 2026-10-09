# Development Guardrails

The project's development guardrails — the tools and gates that keep the
codebase formatted, lint-clean, type-safe, tested, and dependency-safe.

| Guardrail | Loomery (Rust) |
|---|---|
| Formatter | `cargo fmt --check` (`rustfmt.toml`: 2024 edition, max_width 100, tabs 4) |
| Lint / static analysis | `cargo clippy --workspace --all-targets -- -D warnings` |
| Tests | `cargo test --workspace` |
| Coverage report/floor | `cargo llvm-cov` via `cargo-llvm-cov` (floor: 80%) |
| Dependency advisories | `cargo audit`. CI runs it `--no-fetch` against the database the CI image clones at build time (`/opt/rustsec-advisory-db`): fetching into the mounted cache volume failed on the agent with `Device or resource busy`, and a database cloned during the image build is as fresh as the run |
| License allowlist + bans | `cargo deny check` (`deny.toml`) |
| Pre-commit hooks | `hk` (commit-msg + check/fix hooks) |
| Commit message lint | `cog verify` (conventional commits, cocogitto) |
| Combined dev gate | `mise run verify` (see `mise.toml`) |
| Complexity gate | `mise run crap` (coverage-based CRAP scores via cargo-crap; skips `**/*_tests.rs`, `**/build.rs`, `loomery-server`'s `main.rs` (the binary is a single file) and the `JetStream` adapter) |
| Diagram syntax | `mise run docs-mermaid` (`tools/mermaid-check/`: every ```` ```mermaid ```` block in `docs/`, `README.md` and `workpad/` must parse) |
| Documentation links | `mise run docs-links` (`tools/docs-links/`): links, anchors, run citations and site reachability. A broken link or a page missing from the site's navigation fails in `docs/`; in `workpad/` it is reported as a warning, because the staging tree is allowed to be mid-edit |
| Documentation site | `mise run docs-site` (VitePress in `docs/`, navigation in `docs/.vitepress/config.mts`): a dead link or a missing page fails the build |
| Lint policy | `[workspace.lints]` in `Cargo.toml`: deny `unwrap_used`, `expect_used`, `indexing_slicing`, `string_slice`, `arithmetic_side_effects`, `unchecked_time_subtraction`, `todo`, `unimplemented`, `panic`, `unconditional_panic`; warn on `missing_docs`. Enforced by `cargo clippy --workspace --all-targets -- -D warnings` |

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
4. **CI** — [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs verify,
   tests, licenses, docs (`docs-mermaid` **and** `docs-links`), quality
   (`mise run crap`), audit and the service integration tests as seven separate jobs.
   It runs on every pull request and on pushes to `main`, so a pull request is gated
   rather than only `main`. No job runs `cargo` directly: each calls a `mise run` task,
   and `mise.toml` is the only place a cargo invocation lives. Cargo then goes through
   mr-boxington (`rust` carries `mr_boxington = true`), and
   [`jdx/mr-boxington-action`](https://github.com/jdx/mr-boxington-action) restores the
   Cargo target tree and registry downloads across runs.
   [`scripts/install-host-deps.sh`](../scripts/install-host-deps.sh) adds the system
   packages the build needs and the runner lacks: `protobuf-compiler` for
   `tonic-prost-build` (the shell's `build.rs` compiles `proto/raft.proto`),
   `clang`/`libclang-dev` for the build-time `bindgen` inside `librocksdb-sys`, and
   `zlib1g-dev` for RocksDB. A final `publish-docs` job builds the VitePress site and
   deploys it to GitHub Pages, but only for a push to `main` that passed every gate.
5. **Reproduce a gate locally the way CI runs it** — install the pinned toolchain and
   run the same task, so the local check and CI share one configuration:

   ```sh
   mise install
   mise run verify
   ```

   Swap the task for any one the workflow calls (`mise run test`,
   `mise run licenses-check`, `mise run docs-mermaid`, `mise run crap`,
   `mise run audit`). Every `mise run` task reaches cargo through mr-boxington, so the
   compiler cache is shared with the working tree. To check the workflow file itself,
   run it in a container with [act](https://github.com/nektos/act):

   ```sh
   act -n -P ubuntu-latest=catthehacker/ubuntu:act-latest       # plan only
   act -j docs -P ubuntu-latest=catthehacker/ubuntu:act-latest  # run one job
   ```

   act 0.2.89 needs two flags that GitHub does not, and neither belongs in the
   workflow — the runner supplies both:

   * `--env ACTIONS_RUNTIME_TOKEN=act` — act does not inject the cache runtime token
     the mr-boxington action needs.
   * `--container-options "--network host"` — the `test-services` job publishes the
     compose stack's ports on the host, which a bridged act container cannot reach at
     `127.0.0.1`.

   ```sh
   act -j audit -P ubuntu-latest=catthehacker/ubuntu:act-latest --env ACTIONS_RUNTIME_TOKEN=act
   act -j test-services -P ubuntu-latest=catthehacker/ubuntu:act-latest \
     --env ACTIONS_RUNTIME_TOKEN=act --container-options "--network host"
   ```

   The `test services` job drives the compose stack through the host Docker daemon, so
   `mise run svc-up` must be running for it to pass when run by hand.

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
  features ([testing-services.md](../docs/testing-services.md)).
- **Probes and soaks run `--release`.** A debug build measures the compiler.
- **Documentation is part of done.** A change to behaviour, configuration or a
  measurement updates the document that describes it. Published pages live in `docs/`,
  the VitePress site, and a new page is added to its navigation; the engineering record
  (decisions, benchmarks, research) stays in `workpad/`, which is staging rather than an
  archive and whose rules are in that directory's own README.
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
- `**/build.rs` is excluded for a third reason: `cargo llvm-cov` does not
  instrument build scripts, so a function in one is permanently 0% covered and a
  coverage-derived score can only flag it however simple it is.
  `crates/shell/build.rs` links the C++ runtime when the build is handed a
  prebuilt `RocksDB`; it runs during the build of every job, it is just never
  measured. Without the exclusion its match on the target OS scores CRAP 42.

---

*Current tasks and tool versions are defined in [mise.toml](../mise.toml);
hook behavior is defined in [hk.pkl](../hk.pkl).*