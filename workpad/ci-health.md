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
