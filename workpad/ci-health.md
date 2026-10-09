# Buildkite CI health

Status: **build #22 ran the fixes and confirmed two of them on the agent; it also
surfaced two failures nobody had seen.** Everything below is recorded, and the fixes for
the new two are in the tree.

What build #22 (`a524413`, the run after the CI commit) showed:

| step | result |
|---|---|
| Build CI image | **passed** — including the advisory-database clone the audit fix depends on |
| **Audit** | **passed** — the fix works on the agent (it failed every build before) |
| Verify, Licenses | passed |
| **Test services** | **the wait passed**: `nats is ready` → `keycloak is ready` → `the loomery realm is ready`. That is the point 11 of 11 builds failed at; the job then went on to compile and run the suite |
| Docs (Mermaid + links) | **failed** — my own regression: the step gained `docs-links`, which is Python, and the image had no `python3` (`sh: 1: python3: not found`, status 127). Fixed by installing it |
| Test | **failed** — a real latent race, not a regression: see below |
| Quality (CRAP) | was still running when the run was read |

## The race CI found in the Test step

`three_replicas_commit_and_recover_genesis` panicked on `change_membership` with

```
ChangeMembershipError(InProgress { committed: None,
  membership_log_id: Some(LogId { leader_id: LeaderId { term: 0, node_id: 1 }, index: 0 }) })
```

— a membership change asked for while the `initialize` entry at index 0 was still
uncommitted. The test waited for a leader, which can exist before its first entry
commits. It now waits for `last_applied == last_log_index` first, the same precondition
`proposal_tests::boot` already used. **No product code calls `add_learner` or
`change_membership`** — membership is set by the control plane — so this was test-only,
and it is the kind of thing a local machine's timing hides.

## What was wrong, and what changed

| Failure | Cause found | Fix |
|---|---|---|
| `audit`: `couldn't fetch advisory database: … Device or resource busy (os error 16)` | The step fetched the RustSec database into a mounted cache volume; libgit2's fetch fails there | The CI image clones the database at build time; the step runs `mise run audit -- --no-fetch --db /opt/rustsec-advisory-db`. The two advisory volume mounts are gone |
| `test services`: `keycloak did not become ready` in **11/11 builds** | The realm arrived through a bind mount of the checkout and failed **silently** — Keycloak booted, no realm, readiness timed out on a 404, and the job log said nothing else | The realm is baked into `loomery-test-keycloak:26.0`; readiness is two-stage (`/realms/master`, then `/realms/<realm>`) and a timeout dumps the stack's state and logs |
| No PR was ever gated | `branch_configuration: main` | Set to `*` through the API and read back |

## Verified locally, and what that does not cover

- `cargo audit --no-fetch --db <clone>` — loads 1295 advisories, scans 371 crates; also
  with `.git` removed, which is how the image carries it. **Not covered:** the image
  build itself, and whether the agent's daemon builds it as CI does.
- `mise run test-services` with the baked realm — the full step, 9 integration tests,
  exit 0. **Not covered:** the agent, where the failure was.
- The wait's failure path against a bogus realm — names that half, prints `compose ps`
  and the Keycloak logs, exits 1.
- `bk pipeline validate` on the edited pipeline; `docker compose config` on the edited
  compose file.

## What is left

1. **A run on the next commit**: the two fixes above (python3, the membership wait) have
   not been through the agent yet. The fixes they follow have.
2. `crap` was flaky in CI before this work (same commit, pass and fail, most likely the
   coverage cache). Untouched, and worth watching in the same run.
3. `branch_configuration` is `*` now, but no branch other than `main` has been pushed
   since, so the change is read back from the API rather than observed in a build.

## The five minutes of RocksDB, and where it went (builds #24 and #25)

#23 lost `crap` to the store-release race the next commit fixed — it was never
the coverage cache — and #24 and #25 passed. Both spent most of
their longest step compiling RocksDB's C++ from scratch. Read from the job logs
(`bk job log`, which carry the `_bk;t=` timestamps):

| step | #24 | #25 | what dominated it |
|---|---|---|---|
| verify | 373s | 407s | `cargo check -p loomery-shell`, the second command of `mise run verify`: `Finished` in 5m 20s and 5m 47s, with `librocksdb-sys(build)` the only unit still running for the last ~313s |
| test | 116s | 90s | — |
| test-services | 433s | 464s | the compose stack plus the suite |
| quality | 99s | 92s | — |
| licenses | 33s | 21s | — |
| audit | 40s | 20s | — |

The same step was **fully warm** two builds earlier: verify took 49s, 49s and 50s
in #21–#23, and 123s in #15, with no librocksdb-sys compile at all. #20 compiled
it twice (925s). It was never a configuration difference — `cargo build -p
librocksdb-sys` and the workspace build resolve the same features (`static`,
`bindgen-runtime`, `lz4`, the last from `crates/shell/Cargo.toml`'s `rocksdb`),
and the recorded `rustc` line carries the same `--cfg feature=…` either way. What
changes between builds is the cache volume: Buildkite attaches hosted volumes
best-effort and commits them only for a step that succeeds, and the verify volume
reported 2.0 GB and 1.5 GB used of its 40 GB. A volume that comes up without the
~319 MB build-script output pays the compile again, and nothing in the pipeline
could prevent it.

## What changed

| Where | What |
|---|---|
| `.buildkite/Dockerfile` | Builds `librocksdb.a` once from the workspace manifests alone and installs it at `/opt/rocksdb/lib`, with `ROCKSDB_LIB_DIR`/`ROCKSDB_STATIC` set. `try_to_find_and_link_lib` then returns before `build_rocksdb()`: bindgen still runs (measured 0.8s), no C++ is compiled, and the `rerun-if-changed=rocksdb/` that made the script re-run is never declared |
| `crates/shell/build.rs` | `link_cxx_runtime_of_a_prebuilt_rocksdb` emits `cargo:rustc-link-lib=stdc++` when `ROCKSDB_LIB_DIR` is set. The directive lives inside `build_rocksdb()` (`cc`'s `cpp_link_stdlib`), so skipping that build drops it and every `std::` symbol goes undefined at link time |
| `mise.toml`, `mise.lock` | `mr-boxington` pinned, and the Rust tool carries `mr_boxington = true`, so mise wraps cargo with mbx inside every task |
| `.buildkite/pipeline.yml` | Each Rust step's volume also carries `/cache/loomery-cargo/mbx` (`MBX_CACHE_DIR=/mbx-cache`, `MBX_GC_MAX_SIZE=20GiB`): mr-boxington's content-addressed store, which restores actions Cargo's fingerprints no longer match |
| `README.md`, `workpad/guardrails.md` | What the image and the caches carry |

Verified locally: with `ROCKSDB_LIB_DIR` set, `cargo check -p loomery-shell`
finishes in **3.5s** and `cargo test -p loomery-shell --lib --no-run` links in 7s,
where without the build script directive the same link fails on undefined `std::`,
`__cxa_*` and `operator new` symbols; the linked binary lists `libstdc++.so.6`,
and two RocksDB-touching tests (`a_boot_waits_for_a_store_that_is_still_held`,
`persistent_tests::snapshot_mode_recovers_log_only_and_snapshot_with_purged_prefix`)
pass. The mise wrapping was checked with an on/off pair of throwaway configs: with
`mr_boxington = true`, `cargo` inside a `mise run` task resolves to mise's command
wrapper and the build prints mbx's summary lines; without it, neither. The
Dockerfile's prebuild `RUN` block was executed verbatim in a container with the
cargo line stubbed. **Not covered:** the image build itself, and the agent — the
next run covers both.

## What to watch in the next run

1. **The first run is slower, once.** `ROCKSDB_LIB_DIR` changes the build
   script's fingerprint and mbx stops Cargo compiling incrementally, so every Rust
   step recompiles its graph once. Established caches do not go back.
2. **verify should lose about five minutes** and print mbx's summary lines
   (`mbx[cache]: …`, naming hits, misses and stores). A verify run that still
   reports a five-minute `Finished` means the step did not have `ROCKSDB_LIB_DIR`:
   the image's `ENV` and the step's mounts are the two places to look.
3. **`test`, `test-services` and `quality` link**, which is where the `stdc++`
   directive matters. A link error naming `std::` in one of them means the build
   script did not emit it.
