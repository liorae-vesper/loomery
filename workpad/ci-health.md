# Buildkite CI health

Status: **the three known failures are fixed in the repository; a pipeline run is the
last check.** Verified locally where the fix allows it, listed below. The diagnosis that
found them is in the commit messages (`fix(ci): …`) and the behaviour is documented in
`docs/guardrails.md` and `docs/testing-services.md`, which is where it belongs now — so
this note is down to what is *not* yet proven.

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

1. **Run the pipeline** and read the result. Everything above is a local reproduction of
   a CI-only failure; only a run on the agent proves the agent was the problem.
2. If `test services` still fails, the job log will now name the half — Keycloak not up,
   or up without the realm — and carry the container logs, which is the difference
   between this and the 11 failures before it.
3. `crap` was flaky in CI before this work (same commit, pass and fail, most likely the
   coverage cache). Untouched, and worth watching in the same run.
