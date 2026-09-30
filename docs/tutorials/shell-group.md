# The group port — the shell's consensus boundary

Every part of the shell that writes to a Raft group, or reads back what it
committed, goes through one small trait: [`GroupOps`](../../crates/shell/src/group.rs).
Genesis is only the first client of it — the gateway command plane and the saga
runner use the same port, which is why it is generic.

```text
   gateway ─┐
   sagas   ─┼─► GroupOps { committed_events, propose } ─► OpenRaft group
   genesis ─┘        (this doc)                            (the spike)
```

## The port

```rust
/// What a proposal did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeOutcome {
    /// The command was appended and applied.
    Appended { first_log_index: u64 },
    /// The group had already processed this `causation_key`: nothing new was
    /// appended, and the recorded result is the authority.
    Replayed { first_log_index: u64 },
}

/// The group operations the rest of the shell needs.
pub trait GroupOps {
    fn committed_events(&self, organization_id: &Id) -> anyhow::Result<Vec<Event>>;
    fn propose(&mut self, command: Command) -> anyhow::Result<ProposeOutcome>;
}
```

That is the whole surface — deliberately. Every extra method is something each
implementation (production Raft port, test fake) must fake, and the shell's
callers only ever need these two questions answered: *"what has this group
committed?"* and *"append this, and tell me what happened."*

## The three contracts

1. **Reads are *applied*, not *appended*.** `committed_events` must reflect the
   state machine's applied state. If it reads the log tail, a caller asking "did
   my command commit?" can be told "no" while the events are already durable
   behind an unapplied entry.
2. **Errors mean *unknown outcome*.** `propose` returning `Err` never means
   "nothing happened" — it means the client could not learn what happened
   (timeout, leadership moved, network). Callers must re-read before re-proposing;
   they must not assume failure.
3. **`propose` is idempotent.** Ids and keys are derived (D12), so the state
   machine's dedup window answers [`ProposeOutcome::Replayed`] for a command it
   has already processed. That is what makes at-least-once retries safe, and what
   lets two workers race a bootstrap without duplicating it.

## Who implements it

| Implementation | Where | Notes |
|---|---|---|
| Raft-backed port | `crates/shell/src/raft/port.rs` (`RaftGroup`) | maps `client_write` → `ProposeOutcome`; in-memory spike levels 1–2 (`MemLogStore` + `MemStateMachine`), see `docs/tutorials/openraft-spike.md` |
| Fake group | `crates/shell/src/test_support.rs` | in-memory append + dedup window; can `lose_response_after` a commit to simulate the nastiest crash |

## Who uses it

| Caller | How it uses the port |
|---|---|
| **Genesis worker** ([`bootstrap`](../../crates/shell/src/bootstrap.rs)) | derives progress from `committed_events` (matching the script's own causation keys), then proposes the next step |
| **Gateway command plane** | `propose`s a client command; on `Replayed` it must also compare the recorded *intent fingerprint* and answer `409` if a reused key carried a different request |
| **Saga runner** | proposes follow-up commands and watches `committed_events` for the events it is waiting on |

## Async shape (what the shell will actually use)

The worker runs on Tokio, so the production trait declares `Send` futures
explicitly — `async fn` in a trait gives no `Send` guarantee and warns
(`async_fn_in_trait`):

```rust
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

Two lints in this workspace will teach you the rest: an implementation whose body
never `.await`s cannot be an `async fn` (`clippy::unused_async_trait_impl` — use
`std::future::ready(...)` or keep the body sync and wrap it), and a fake that
takes a `Command` by value without consuming it trips
`clippy::needless_pass_by_value`.

## Next

* The genesis client of this port: `docs/tutorials/genesis-worker.md`
* Implementing the real (Raft) side: `docs/tutorials/openraft-spike.md`
