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
| Complexity gate | `mise run crap` (coverage-based CRAP scores via cargo-crap) |

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
   `hk check` hook): `cargo check`, clippy with `-D warnings`, fmt check,
   `cargo deny check`.
3. **`hk fix`** runs `cargo fmt` to auto-format.
4. **CI** — [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs
   verify, tests, quality (`mise run crap`) and audit as separate jobs.

## Coverage data flow

`cargo test --workspace` runs the suite; coverage uses
[cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov)
(`cargo llvm-cov --workspace --lcov --output-path lcov.info`), floor 80%.

## Notes

- Dependencies are governed by `cargo-deny` and must keep the license
  allowlist green (`cargo deny check licenses`).
- Toolchain is pinned via `mise` (`rust 1.98.1`, edition 2024); `cargo`
  commands should run through `mise exec --` if rust isn't on PATH.

---

*Current tasks and tool versions are defined in [mise.toml](../mise.toml);
hook behavior is defined in [hk.pkl](../hk.pkl).*