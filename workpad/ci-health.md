# Buildkite CI health

Status: **red on `main`, and it has never been green.** 13 builds since the pipeline
was created (2026-10-07T15:37Z), all failed; 11 of them reached the current step set
and every one of those failed on `test services`. The two failures below are
environmental, not code, and they are **not** caused by the openraft 0.10 branch —
they reproduce on `main` before it.

Investigated 2026-10-08 with `bk` 3.60.0 (org `vespers`, pipeline `loomery`).

## The branch this was found on was never tested

```
$ bk pipeline list
  "repository": "https://tangled.org/liorae-vesper.neocities.org/loomery",
  "default_branch": "main",
  "branch_configuration": "main",
```

`branch_configuration: main` builds only `main`. The 22-commit
`feat/openraft-010-migration` branch has **zero builds** (`bk build list --branch …`
is empty), so CI has never gated it. Whatever the PR shows, it is not a CI result for
the branch — the only green evidence for the migration is a local run of the same
eight steps (all pass; see `openraft-010-handoff.md`).

That setting is the first thing to change if PR branches are meant to be gated: build
`main` plus the feature branches, or all branches.

## Failure 1: `test services` — Keycloak never becomes ready (11/11 builds)

`.buildkite/scripts/test-services.sh`, build #13 at commit `1fd2c66`, 270 s:

```
[test-services] nats is ready
[test-services] keycloak did not become ready: http://127.0.0.1:8080/realms/loomery
```

`scripts/test-services/wait-for-services.sh` retries 90 times with `sleep 2`, so the
180 s budget runs out. The compose stack comes up (both images pull, both containers
start, NATS answers), so this is Keycloak readiness specifically — either it is slower
than 180 s on a cold CI disk, or it never answers behind the host-network hop the step
uses. **It passes locally** (`mise run test-services`: 9 integration tests,
Keycloak 26.0 and NATS 2.10 via `compose.test.yaml`), so the difference is the CI
container's host networking or the container itself, not the tests.

## Failure 2: `audit` — advisory database cannot be fetched

Build #13, 44 s, deterministic across the recent builds:

```
Fetching advisory database from `https://github.com/RustSec/advisory-db.git`
error: couldn't fetch advisory database: I/O operation failed: Device or resource busy (os error 16)
```

The step mounts `/cache/loomery-cargo/advisory-db` and `/cache/loomery-cargo/advisory-dbs`;
cargo-audit wants to update a git checkout inside one of them and the mount refuses.
Cargo's other caches on the same volume are fine. Passes locally
(`mise run audit`: 1295 advisories, 371 crates, nothing reported).

## `crap` is flaky in CI

Failed on builds #7, #9 and #12 but passed on #11 and #13 — and #11, #12, #13 are all
commit `1fd2c66`. So the same tree both passes and fails, most likely depending on
whether the coverage cache was warm. This matters for judging this branch: a green
`crap` locally (476 functions, none over threshold) is stronger evidence than a red
`crap` in CI. Build #12's log is the place to start if it needs understanding.

The older builds (#2–#5) also failed `verify`, `test` and `licenses`; those steps have
passed since 2026-10-07 and are not part of this.

## What is worth doing, in order

1. **Fix the Keycloak wait** — raise the budget or find why it never answers under the
   step's host networking. Until this passes, no PR can ever be green.
2. **Fix the advisory-db volume** so `audit` can fetch.
3. **Enable branch builds**, so the next piece of work is actually gated. Watch for the
   `crap` flakiness before trusting it as a gate.
4. When 1–3 are done, promote whatever remains true into `docs/guardrails.md` (how CI
   is wired, what is known-flaky) and delete this note — `workpad/` is staging.

## Evidence

Build list and per-job detail came from `bk build list` and
`bk build view -p loomery <number> -o json`; logs from `bk job log <job-id>` — plain
output hides the interesting part behind progress frames and collapsed groups, so
`bk job log <job-id> --agent --no-window` is the readable form. Build #13:
`https://buildkite.com/vespers/loomery/builds/13`, failed jobs
`01a11d74-06a9-4b1c-a425-51990a422e60` (test services) and
`01a11d74-06d2-4b17-950c-6d2d4c9198a9` (audit).
