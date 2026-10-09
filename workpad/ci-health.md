# CI health

Status: **the pipeline is GitHub Actions now**
([`.github/workflows/ci.yml`](../.github/workflows/ci.yml)). It replaced the Buildkite
pipeline, and it has not run on GitHub yet — the first push to `main` is what proves it.

## What a first run must prove

1. **Every gate passes on a runner.** All seven run locally, and each is a `mise run`
   task rather than a command line, but a GitHub-hosted runner is not this machine.
2. **`publish-docs` deploys.** The repository's Pages source has to be set to
   **GitHub Actions**, and the site's `base` is `/loomery/`
   ([`config.mts`](../docs/.vitepress/config.mts)), which is where a project Pages site
   is served from.
3. **The caches warm and are then reused.** `jdx/mise-action` caches the installed
   toolchains and `jdx/mr-boxington-action` the Cargo target tree and registry
   downloads; a second run on unchanged inputs should be visibly faster.
4. **`crap` stays stable.** It was flaky on Buildkite — the same commit passing and then
   failing, most likely the coverage cache. The task is untouched and worth watching.

## A race only CI found

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

The lesson carries over unchanged: a green local run is not evidence that the suite is
deterministic, only that this machine did not hit the window. Watch the first few runs on
the runner for the same shape of failure timing a slower or faster machine exposes.
