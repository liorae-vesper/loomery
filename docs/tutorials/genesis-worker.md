# Tutorial — implementing the genesis bootstrap worker

This tutorial takes you from "the genesis script exists" to "a group is born
with ①②③ committed, and a crash mid-provisioning cannot duplicate them".

It is split in two halves:

| Stages | What they need | Verified? |
|---|---|---|
| **1–5** (the algorithm) | nothing but the crates in this repo | yes — the code below is extracted from a crate that compiles and passes its tests with this workspace's lint set |
| **6–7** (async + OpenRaft) | the Phase-1 OpenRaft spike | yes — stage 6 and the stage-7 adapter are implemented in `crates/shell/src/raft` (in-memory baseline; persistent groups are covered in the [shell reference](../shell.md)), re-checked against the pinned OpenRaft crate (0.9.25 when written, 0.10.0-alpha.36 now — see [openraft-010-migration.md](../research/openraft-010-migration.md)) and held to `openraft::testing::log::Suite` |

If you only want the algorithm, stop after stage 5: it is the whole decision
surface, and it is testable without a cluster.

**This doc is the *genesis client* of a generic boundary.** The trait it drives
(`GroupOps`, `ProposeOutcome`) belongs to the shell, not to genesis — the gateway
and the saga runner use the same port. That port, its contracts and its async
shape live in [`shell-group.md`](shell-group.md); the Raft implementation behind
it lives in [`openraft-spike.md`](openraft-spike.md). Read this doc for the
genesis loop, those two for the plumbing.

**Prerequisites**

```sh
mise install          # toolchain + tools at pinned versions
mise run verify       # the gate you must keep green
mise run test         # 82 core + 24 genesis + 22 shell tests + 3 doctests today
```

---

## 0. What you are building

```text
                    ┌──────────────────────────── the shell (I/O) ───────────┐
Bootstrap{org,      │  loop {                                                │
  leader,           │      progress = script.progress(committed_events())     │
  occurred_at} ───► │      step     = progress.next()      ── None ──► done   │
                    │      command  = script.command(step)                    │
                    │      propose(command)  ──► append+apply through Raft    │
                    │  }                                                      │
                    └────────────────────────────────────────────────────────┘
                                     ▲ everything above is `crates/genesis` (pure)
```

The loop is the only new logic. It rests on three invariants:

1. **The worker decides nothing.** It asks
   [`Bootstrap`](../../crates/genesis/src/script.rs) what the next command is and
   submits it. No ids, no timestamps, no payloads are built here.
2. **Progress is read from the log, never remembered.** After any restart the
   worker re-reads the group's committed events and asks the *script* which
   steps they contain — so "did my earlier attempt commit?" is a fact, not a
   guess.
3. **`propose` is idempotent.** The commands carry derived identity (D12), so
   re-submitting one is a *replay* in the state machine's dedup window rather
   than a second genesis. That is what makes an at-least-once loop safe.

### What already exists (`crates/genesis`)

| Item | What it gives you |
|---|---|
| `Bootstrap { organization_id, leader_user_id, occurred_at }` | one attempt's inputs; `occurred_at` is the only nondeterministic one |
| `Bootstrap::progress(&[Event]) -> Progress` | which steps the log already contains (matched by **causation key**) |
| `Progress::{next, is_done, is_complete}` | the sequential cursor: `next()` is the only step that may run |
| `Bootstrap::command(step) -> Result<Command, genesis::Error>` | the ready-to-propose envelope |
| `Bootstrap::next_command(progress)` | convenience for callers that only need the command |
| `Step::{ALL, slug, command_type, event_type}` | ①②③, their wire names |
| `step_key`, `bootstrap_correlation_key`, `command_id`, `default_workspace_id`, `owner_membership_id` | the derivations (frozen by golden tests) |

---

## 1. Read these first (15 min)

* `docs/design.md` §4 (shell layout) and §5 → Phase 1, items **1** (OpenRaft
  spike) and **3** (this worker).
* `docs/design.md` **D12** — *why* derived identity is what makes this worker
  simple. Skim it before you write any code.
* `crates/core/src/aggregate.rs` (module docs) — the dedup contract: the entry is
  recorded *after* durable append + apply, and the registry is **folded state**,
  so a replay rebuilds it.
* `docs/research/openraft-storage.md` — the storage traits and the
  `client_write` flow you will attach to in stage 7.

---

## 2. Create the shell crate (20 min)

Everything from here is shell-side. Create `crates/shell`:

```toml
# crates/shell/Cargo.toml
[package]
name = "loomery-shell"
version.workspace = true
edition.workspace = true
license.workspace = true
publish.workspace = true
description = "Imperative shell: consensus, storage, gateway, workers"

[dependencies]
anyhow = "1.0.104"
thiserror = "2.0.20"
loomery-core = { workspace = true }
loomery-genesis = { workspace = true }
tokio = { version = "1", features = ["rt", "macros", "time"] }

[lints]
workspace = true
```

```rust
// crates/shell/src/lib.rs
// SPDX-License-Identifier: MPL-2.0

//! The imperative shell around the pure core: consensus, storage, the gateway,
//! and the workers that drive the pure scripts.

// Strict lints (unwrap/expect/panicking slicing/overflowing math) are denied in
// production code — test code may use them freely, via a single crate-level
// escape hatch active only under `cfg(test)`.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects
    )
)]

pub mod bootstrap;
pub mod group;              // the generic port: see docs/tutorials/shell-group.md

#[cfg(test)]
pub(crate) mod test_support;
```

Register it in the workspace table, next to the other members:

```toml
# Cargo.toml
[workspace.dependencies]
loomery-core = { path = "crates/core" }
loomery-genesis = { path = "crates/genesis" }
loomery-shell = { path = "crates/shell" }
```

> No version requirement, on purpose: internal crates are path-only, and
> `deny.toml` already allows version-less *path* wildcards
> (`[bans] allow-wildcard-paths = true`) while still rejecting real `*` ones.

**Verify**

```sh
cargo test -p loomery-shell   # 0 tests, but it must compile
mise run verify               # fmt + clippy -D warnings + cargo deny
```

---

## 3. The port it drives (15 min)

The worker does not own the group abstraction. `GroupOps` and `ProposeOutcome`
live in `crates/shell/src/group.rs`, because the gateway command plane and the
saga runner need exactly the same two operations; **[shell-group.md defines them and their contracts](shell-group.md)** — read that first if you have
not.

Genesis only needs to know two things about it:

* `committed_events(&organization_id)` answers "which steps are already in the
  log?" when fed to `Bootstrap::progress`.
* `propose(command)` answers `Appended` or `Replayed`, and an `Err` means
  *unknown outcome* — never "nothing happened".

**What is deliberately *not* in this port**

* No `is_recorded` / dedup lookup: the *state machine* answers that when the
  command is proposed (`Replayed`). Asking the worker to pre-check would add a
  race for no benefit.
* No `progress()` cache: re-reading the log is what makes restarts trivial.
* No `mint_id()`: nothing in this worker creates identity (D12).

---

## 4. The loop (30 min)

```rust
/// How far the worker got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Genesis {
    /// The steps *this run* appended — a resumed run appends only what the log
    /// was missing.
    pub appended: Vec<Step>,
    /// Where the script stands afterwards; complete on every `Ok` return.
    pub progress: Progress,
}

/// Drives the script to completion for one group.
///
/// Safe to run again at any time, from any process: a completed group appends
/// nothing, and an interrupted run resumes at the first step the log does not
/// have yet.
///
/// # Errors
///
/// [`Error`] when the group cannot be read, a step cannot be appended, or the
/// plan refuses to build a command.
pub fn run(port: &mut impl GroupOps, bootstrap: &Bootstrap) -> Result<Genesis, Error> {
    let mut appended = Vec::new();

    loop {
        // Always re-read: the previous iteration — or a previous process — may
        // have committed a step without us ever learning the result.
        let events = port
            .committed_events(&bootstrap.organization_id)
            .map_err(Error::Read)?;
        let progress = bootstrap.progress(&events);

        let Some(step) = progress.next() else {
            // Every step is in the log: genesis is complete for this group.
            return Ok(Genesis { appended, progress });
        };

        let command = bootstrap.command(step)?;

        match port
            .propose(command)
            .map_err(|source| Error::Propose { step, source })?
        {
            ProposeOutcome::Appended { .. } => appended.push(step),
            // Another attempt — or an earlier one of ours — already committed
            // this step. The next read confirms it.
            ProposeOutcome::Replayed { .. } => {}
        }
    }
}

/// What went wrong while driving the script.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The group's committed events could not be read.
    #[error("could not read the group's committed events")]
    Read(#[source] anyhow::Error),
    /// A step's command could not be appended.
    #[error("could not append the {step:?} command")]
    Propose {
        /// The step whose command failed.
        step: Step,
        /// The failure the group reported.
        #[source]
        source: anyhow::Error,
    },
    /// The plan itself refused to build a command.
    #[error(transparent)]
    Plan(#[from] PlanError),
}
```

Read the loop as *"ask, then act, then ask again"*. Every crash lands in one of
these windows, and none of them needs recovery code:

| Crash point | What the group has | What the resume does |
|---|---|---|
| before `propose` | nothing new | proposes the same command (derived identity) |
| during `propose` (timeout, leader change, lost response) | maybe the command, maybe nothing | re-reads first: if it committed, the step is already done |
| after append + apply, before the client heard | the event is in the log | `progress` sees it; the step is skipped |
| after everything | complete | `progress.next() == None` → returns immediately |

Two details that are easy to get wrong:

* **`Replayed` is not an error.** It means someone else (a retried attempt of
  yours, or a second worker) got there first, which is *success*. Do not push it
  onto `appended`, and do not retry it.
* **The re-read is not an optimisation.** Removing it makes the loop wrong the
  first time a proposal times out after committing — you would propose ② again,
  and although the dedup window would catch it, you would be relying on the
  window instead of on the log. The window is bounded; the log is the truth.

---

## 5. Test it against a fake (45 min)

The loop is worth testing *before* any cluster exists, because every failure
mode you care about is a crash window, and a fake can hit those windows exactly.

```rust
// crates/shell/src/test_support.rs
// SPDX-License-Identifier: MPL-2.0

//! Test doubles shared by the sync and async worker tests.

use crate::group::{GroupOps, ProposeOutcome};
use loomery_core::envelope::{Command, Event, Payload};
use loomery_core::id::Id;
use loomery_core::timestamp::Timestamp;
use loomery_genesis::{Bootstrap, Step, step_key};

/// A well-formed organization id for the tests below.
pub(crate) fn organization() -> Id {
    Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9")
}

/// The worker's inputs, with the injected timestamp pinned.
pub(crate) fn bootstrap_value() -> Bootstrap {
    Bootstrap {
        organization_id: organization(),
        leader_user_id: Id::from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0"),
        occurred_at: Timestamp::from(1_700_000_000_000),
    }
}

/// The group, faked: it appends commands, "applies" the event an aggregate
/// would produce, and can lose a response so the tests can crash the worker at
/// the worst possible moment.
#[derive(Default)]
pub(crate) struct FakeGroup {
    pub(crate) events: Vec<Event>,
    pub(crate) appended: Vec<Command>,
    /// When set to `n`, the `n`th append is committed and *then* reported as a
    /// failure — the classic "the write succeeded, the client never heard
    /// back".
    pub(crate) lose_response_after: Option<usize>,
}

impl GroupOps for FakeGroup {
    fn committed_events(&self, _organization_id: &Id) -> anyhow::Result<Vec<Event>> {
        Ok(self.events.clone())
    }

    fn propose(&mut self, command: Command) -> anyhow::Result<ProposeOutcome> {
        // The state machine's dedup window: same intent, no new events.
        if self
            .appended
            .iter()
            .any(|previous| previous.causation_key == command.causation_key)
        {
            return Ok(ProposeOutcome::Replayed { first_log_index: 0 });
        }

        self.appended.push(command.clone());
        self.events.push(event_for(&command));

        if self.lose_response_after == Some(self.appended.len()) {
            anyhow::bail!("the response was lost");
        }

        Ok(ProposeOutcome::Appended { first_log_index: 0 })
    }
}

/// What the aggregate's `apply` would put in the log. The script reads only the
/// causation key back out, so the fake copies the command's identity.
pub(crate) fn event_for(command: &Command) -> Event {
    Event {
        envelope_version: command.envelope_version,
        id: command.event_id(0),
        aggregate_id: command.aggregate_id.clone(),
        organization_id: command.organization_id.clone(),
        workspace_id: command.workspace_id.clone(),
        occurred_at: command.occurred_at.clone(),
        causation_key: command.causation_key.clone(),
        correlation_key: command.correlation_key.clone(),
        actor: command.actor.clone(),
        event_type: "genesis.test".to_owned(),
        payload: Payload {
            version: 1,
            data: "{}".to_owned(),
        },
    }
}

/// How many events carry `step`'s causation key — the "no duplicate genesis"
/// assertion.
pub(crate) fn committed(group: &FakeGroup, step: Step) -> usize {
    let key = step_key(&organization(), step);
    group
        .events
        .iter()
        .filter(|event| event.causation_key == key)
        .count()
}
```

Now the tests that matter. Put them in `mod tests` at the bottom of
`bootstrap.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeGroup, bootstrap_value, committed};
    use loomery_genesis::Step;

    /// The append succeeded, the worker never heard back: resuming must
    /// propose ② — not a second ①.
    #[test]
    fn a_lost_response_does_not_duplicate_the_step() {
        let mut group = FakeGroup {
            lose_response_after: Some(1),
            ..FakeGroup::default()
        };

        assert!(run(&mut group, &bootstrap_value()).is_err());
        assert_eq!(committed(&group, Step::AssignLeader), 1);

        let outcome = run(&mut group, &bootstrap_value()).unwrap();

        assert_eq!(
            outcome.appended,
            [Step::CreateWorkspace, Step::AddOwner],
            "the resumed run must not repeat ①"
        );
        for step in Step::ALL {
            assert_eq!(committed(&group, step), 1, "{step:?}");
        }
    }

    /// Hammering the worker must not accumulate genesis events.
    #[test]
    fn retrying_the_worker_never_duplicates_genesis() {
        let mut group = FakeGroup::default();

        for _ in 0..5 {
            run(&mut group, &bootstrap_value()).unwrap();
        }

        for step in Step::ALL {
            assert_eq!(committed(&group, step), 1, "{step:?}");
        }
        assert_eq!(group.appended.len(), 3);
    }

    /// Two processes, same inputs: identical envelopes (this is D12 showing up
    /// in the shell).
    #[test]
    fn two_attempts_propose_byte_identical_commands() {
        let mut first = FakeGroup::default();
        let mut second = FakeGroup::default();

        run(&mut first, &bootstrap_value()).unwrap();
        run(&mut second, &bootstrap_value()).unwrap();

        assert_eq!(first.appended, second.appended);
    }
}
```

The rest of the suite — each is a handful of lines:

| Test | What it pins down |
|---|---|
| `the_happy_path_commits_the_three_steps_in_order` | `appended == Step::ALL` and the three `command_type`s, in order |
| `every_step_is_committed_exactly_once` | exactly one event per step key after a clean run |
| `a_completed_group_appends_nothing` | re-running a finished group is a no-op |
| `a_worker_started_mid_script_picks_up_the_remaining_steps` | ①② pre-seeded → only ③ is proposed |
| `a_failed_propose_reports_the_step` | errors carry the step (`Error::Propose { step, .. }`) |

**Verify**

```sh
cargo test -p loomery-shell      # expect: 8 passed
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

> The lint set is strict (`panic`, `unwrap_used`, `expect_used`,
> `indexing_slicing`, `arithmetic_side_effects`, pedantic). Production code in
> this crate must have no `unwrap`; the `cfg_attr(test, allow(...))` block in
> `lib.rs` is what lets the tests use them, exactly as `crates/core` does.

---

## 6. Make it async (20 min)

The loop is I/O-bound, so the real port is async. Three mechanical changes:

```rust
// 1. the trait: mark it spawn-safe and declare the futures explicitly
use std::future::Future;

pub trait GroupOps: Send + Sync {
    fn committed_events(
        &self,
        organization_id: &Id,
    ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send;

    fn propose(
        &mut self,
        command: Command,
    ) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send;
}
```

```rust
// 2. the loop: `async fn`, two `.await`s, nothing else changes
pub async fn run(port: &mut impl GroupOps, bootstrap: &Bootstrap) -> Result<Genesis, Error> {
    // ...
        let events = port
            .committed_events(&bootstrap.organization_id)
            .await
            .map_err(Error::Read)?;
    // ...
        match port
            .propose(command)
            .await
            .map_err(|source| Error::Propose { step, source })?
    // ...
}
```

```rust
// 3. the tests: the bodies gain `.await`, the attribute gains a runtime
#[tokio::test]
async fn the_async_worker_commits_the_three_steps_in_order() {
    let mut group = FakeGroup::default();
    let outcome = run(&mut group, &bootstrap_value()).await.unwrap();
    assert_eq!(outcome.appended, Step::ALL);
}
```

Three things that will bite you here, so budget for them:

* **Write the trait as `-> impl Future<Output = T> + Send`, not `async fn`.**
  `async fn` in a trait triggers the `async_fn_in_trait` warning and gives you no
  `Send` guarantee — and the worker *will* be `tokio::spawn`ed. The explicit form
  is the lint-clean way to promise `Send`.
* **An impl with no `.await` cannot be an `async fn`** under this workspace's
  lints (`clippy::unused_async_trait_impl` is denied). So the *fake* from stage 5
  changes shape: keep its body sync in an inherent `apply`, and hand it back
  through `std::future::ready`:

  ```rust
  impl GroupOps for FakeGroup {
      fn committed_events(
          &self,
          _organization_id: &Id,
      ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send {
          std::future::ready(Ok(self.events.clone()))
      }

      fn propose(
          &mut self,
          command: Command,
      ) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send {
          std::future::ready(self.apply(&command))
      }
  }

  impl FakeGroup {
      /// The append path, sync so the tests can pre-seed a group without a
      /// runtime, and wrapped in a ready future above.
      pub(crate) fn test_propose(&mut self, command: &Command) -> anyhow::Result<ProposeOutcome> {
          self.apply(command)
      }

      fn apply(&mut self, command: &Command) -> anyhow::Result<ProposeOutcome> {
          // …the stage-5 body, with `command` now a reference…
      }
  }
  ```

  A *real* port awaits, so there it is a plain `async fn` — which is exactly
  what the trait's `impl Future + Send` bound accepts. (`needless_pass_by_value`
  is why this `apply` takes `&Command`: it never moves the command.)
* **`tokio` is only needed for tests at this stage** (`rt` + `macros`); it becomes
  a real dependency when you spawn the worker in stage 8 (`rt`, `macros`,
  `time`).

---

## 7. Attach the real group (2–4 h — needs the OpenRaft spike)

The storage/network side of this has its own doc:
[`openraft-spike.md`](openraft-spike.md) — it carries the API surface verified
against the pinned crate (0.9.25). This section only covers what the *genesis*
side has to know.

### What the state machine owes the worker

Before writing the port, make sure the group's state machine (design §5 item 1)
does these three things, because the worker's simplicity depends on them:

1. **Decode + dispatch each committed command** through the pure core
   (`AggregatePlan::process`), and apply the resulting events.
2. **Return an `AppDataResponse`** that distinguishes appended from replayed, so
   `ProposeOutcome` is a straight map:

   ```rust
   /// What `apply` reports back to the client that proposed the command.
   pub enum Applied {
       /// The command produced events; the first one landed at this index.
       Appended { first_log_index: u64 },
       /// The causation key was already in the dedup window.
       Replayed { first_log_index: u64 },
   }
   ```

   **As implemented** (`crates/shell/src/raft/mod.rs`) there is a third
   variant, `Rejected { code, message }`: a committed command whose payload the
   aggregate refuses. `ProposeOutcome` keeps its two variants — the port maps a
   rejection to an error — because a rejection means a command reached
   consensus that validation should have stopped at the gateway (D10).
3. **Keep committed events readable per group** — that is what
   `committed_events(&organization_id)` returns. If the read model is a
   projection, read from the state machine's applied state (or a store the state
   machine writes through), not from the log tail: the answer must reflect
   *applied*, not merely *appended*.

### The port implementation (sketch)

The implemented adapter is `RaftGroup` in `crates/shell/src/raft/port.rs`.
API names below come from `docs/research/openraft-storage.md` §4/§6 and are
*pseudonymous by that note's own warning* — they were re-checked against the
pinned 0.9.25 crate when the adapter landed:

```rust
struct RaftGroupOps {
    raft: Raft<TypeConfig>,              // the group's client handle
    events: Arc<AppliedEvents>,          // whatever serves committed events
}

impl GroupOps for RaftGroupOps {
    fn committed_events(
        &self,
        organization_id: &Id,
    ) -> impl Future<Output = anyhow::Result<Vec<Event>>> + Send {
        let organization_id = organization_id.clone();
        async move { self.events.committed(&organization_id).await }
    }

    fn propose(
        &mut self,
        command: Command,
    ) -> impl Future<Output = anyhow::Result<ProposeOutcome>> + Send {
        async move {
            // `client_write` takes the app data directly in 0.9 (verified) and
            // waits for the entry to be applied.
            let response = self
                .raft
                .client_write(AppData::Command(command))
                .await
                .map_err(classify)?;

            Ok(match response.data {
                Applied::Appended { first_log_index } => {
                    ProposeOutcome::Appended { first_log_index }
                }
                Applied::Replayed { first_log_index } => {
                    ProposeOutcome::Replayed { first_log_index }
                }
            })
        }
    }
}
```

**Write `client_write`, not `client_write_ff`.** Genesis needs to know the
command is applied before moving to the next step; `client_write` waits for the
commit, and the response's `data` field carries the state machine's `Applied`
value (`ClientWriteResponse { log_id, data, membership }`).

### Retry policy

`propose` failing means *the outcome is unknown*, not *nothing happened*. So the
policy is: treat it as retryable, back off, and let the loop re-read the log —
the loop is already the recovery mechanism.

```rust
/// Whether a failed proposal is worth retrying.
fn classify(
    error: RaftError<u64, ClientWriteError<u64, openraft::BasicNode>>,
) -> ProposeError {
    match error {
        // Another node is leader: retry (the shell can route to `leader_id`).
        RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => ProposeError::Retryable,
        // Membership changes cannot be retried blind — surface them.
        RaftError::APIError(ClientWriteError::ChangeMembershipError(_)) => ProposeError::Fatal,
        // Timeouts, transport and shutdown-ish failures: retry the whole loop.
        _ => ProposeError::Retryable,
    }
}
```

Backoff belongs *outside* `run`: wrap calls in your retry helper
(`tokio::time::sleep` + jitter, capped), and surface a fatal classification
through `Error::Propose` so the caller can decide to keep a worker per group
alive versus failing the deployment.

### Test the real port in-process

The research note's `MemStore` example is the cheapest way to get a single-node
group in a test. Then the whole acceptance suite from stage 9 runs without a
cluster — including the crash-resume test, which you can drive by dropping the
port mid-run and re-running against the same store.

---

## 8. Wire it into the process

**Where it gets called**

1. **Tenant creation** (control plane, design §5 item 3): after the organization
   exists and the router knows the group, hand the group off to a bootstrap
   worker. Build the inputs *here*, once per attempt:

   ```rust
   let bootstrap = Bootstrap {
       organization_id: organization_id.clone(),
       leader_user_id: creator.clone(),
       occurred_at: Timestamp::now(),   // injected shell-side, once per attempt
   };
   let outcome = bootstrap_worker::run(&mut port, &bootstrap)?;
   ```
2. **Startup reconciliation:** on boot, sweep the groups whose genesis is
   incomplete and run the worker for each. This is what turns "the process died
   mid-provisioning" into "pick up where it left off" — and it is safe precisely
   because `run` is idempotent.
3. **A retry sweep** (optional): a periodic pass over incomplete groups with
   backoff, so a transient consensus outage self-heals without a restart.

**Concurrency and leadership**

* One worker per group is enough. Two are *safe* (derived identity + dedup), but
  wasteful and noisy — serialize per group (own the group's task, or a
  `DashMap<Id, Mutex<()>>`).
* `propose` may hit a follower or a leaderless moment. Retry; do not try to
  outsmart consensus. Read-Your-Writes (`X-Min-Index`) is a *gateway read*
  concern and does not change this loop.

**Observability**

Log `(organization_id, step, Appended|Replayed, duration)` on every proposal and
one line per run with `appended`. A gauge of groups with incomplete genesis is
the single most useful dashboard panel for this phase: it should sit at zero.

---

## 9. Acceptance — definition of done

The Phase-1 gate in `docs/design.md` §5 says:

> register org → genesis ①②③ → workspace + Owner; crash mid-provisioning resumes
> with no duplicate genesis.

Make that a test, not a claim. The assertions that prove it:

```rust
// 1. exactly three genesis events, in order
let keys: Vec<_> = events
    .iter()
    .filter(|event| Step::ALL.iter().any(|step| event.causation_key == step_key(&org, *step)))
    .map(|event| event.causation_key.clone())
    .collect();
assert_eq!(
    keys,
    Step::ALL.iter().map(|step| step_key(&org, *step)).collect::<Vec<_>>()
);

// 2. the workspace exists, with the derived id, and the creator is its Owner
assert!(events.iter().any(|event| event.event_type == "workspace.created"
    && event.workspace_id.as_deref() == Some(&*default_workspace_id(&org))));
assert!(events.iter().any(|event| event.event_type == "membership.owner_added"
    && event.actor == bootstrap_actor()));

// 3. a crash between steps resumes without a second genesis
//    (drop the worker after ①, re-run, then assert 1. again)
```

Checklist:

- [x] `cargo test -p loomery-shell` — unit tests from stages 5–6 green
- [x] in-process single-node integration test: happy path + crash-resume
- [x] `mise run verify` and `mise run test` green
- [x] every genesis event carries `actor = Saga { user_id: None, name:
      "control-plane:Bootstrap" }` and the organization's derived
      `correlation_key`
- [x] re-running genesis for a completed group performs **zero** proposals
      (assert on the port, not just on the events)

---

## 10. Gotchas

* **Never mint anything in the worker.** No `Id::new()`, no `Timestamp::now()`
  except the single injected `occurred_at`. If you find yourself wanting an id in
  the loop, the identity belongs in `crates/genesis` (and in its golden tests).
* **Do not skip the re-read before re-proposing.** It is the difference between
  "idempotent by derivation" and "idempotent by luck".
* **`Replayed` is success.** Counting it as an append will make your metrics
  lie and your tests flaky.
* **Read applied, not appended.** A read model fed from the log tail can report
  a step as missing while its event is already committed.
* **Keep the command/event names in sync with the aggregates.** `Step::command_type`
  /`event_type` are the wire contract; when `trellis-core` grows
  `organization`/`workspace`/`membership`, one side must import the other's
  strings.
* **Don't cache `Progress` across restarts** — and don't trust the dedup window
  as memory either: it is bounded FIFO and genesis keys will eventually evict.
  The log is the only durable answer.
* **Don't run stage 7 with `client_write_ff`.** Fire-and-forget cannot tell you
  whether ① committed before you propose ②.
* **`genesis::Error::Payload` cannot happen** (payloads are ids and strings), but
  keep it in the error surface rather than unwrapping — the workspace denies
  `unwrap`/`expect` in production code by design.

---

## Reference

* The generic port this worker drives: [`shell-group.md`](shell-group.md)
* Implementing the Raft side of it: [`openraft-spike.md`](openraft-spike.md)
* Script + derivations: `crates/genesis/src/{script,identity}.rs`
* Identity model and *why*: `docs/design.md` **D12**
* Shell layout / phases: `docs/design.md` §4, §5 (Phase 1)
* OpenRaft storage interfaces, the `client_write` flow, and the example stores:
  `docs/research/openraft-storage.md` (re-verify API names against the pinned
  0.9.x version)
* Dedup contract and `process`/`fold`: `crates/core/src/aggregate.rs`,
  `crates/core/src/dedup.rs`
* Gates: `docs/guardrails.md` (`mise run verify`, `mise run test`)
