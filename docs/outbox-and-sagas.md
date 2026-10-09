# The outbox and sagas

Cross-group coordination in Loomery is **choreography via committed events**,
never 2PC (`design.md` principle 10). Two pieces implement it:

* the **outbox** tails a group's applied events and publishes them;
* **sagas** consume those events and propose the follow-up commands.

```text
  tenant group ──► Outbox ──► broker ──► Consumer ──► SagaRunner ──► GroupOps
   (applied events)  publish  (JetStream)  peek/ack    ack/retry     (next group)
```

## 1. The outbox (`crates/shell/src/outbox`)

The consensus loop makes no network calls. Instead the state machine records
every applied event **with its log index** (`AppliedEvent { log_index, event }`),
and the tailer publishes each one:

| Piece | Value |
|---|---|
| subject | `loomery.events.<group_id>.<event_type>` (D11) |
| message id | `<group_id>:<log_index>:e<pos>` (D11) |
| cursor | `(log_index, position)`, advanced only after a publish succeeds |

Because the message id is stable, a re-publish is absorbed by the broker's dedup
window; because the cursor only moves on success, a broker outage or a crash
resumes exactly where it stopped instead of skipping an event.

`Publisher` is the seam:

```rust
pub trait Publisher: Send + Sync {
    fn publish(&self, message: OutboxMessage) -> impl Future<Output = Result<(), PublishError>> + Send;
}
```

Tests use an in-process fake; the NATS JetStream adapter is deployment wiring
behind an explicit `LOOMERY_NATS_URL` and is **not implemented yet**. The
default test suite is self-contained.

## 2. Sagas (`crates/shell/src/saga`)

A `Consumer` (`crates/shell/src/saga/mod.rs`) has a durable cursor: `next`
**peeks** the next unacked message (a real broker redelivers), `ack` removes it.
[`SagaRunner`] processes one message at a time:

1. if the subject is not this handler's, ack and skip;
2. if this host does not run the message's group, leave it (its host will);
3. handle it: `Ok` → ack; `Err(Retry::Retryable)` → leave it unacked for
   redelivery; `Err(Retry::Fatal)` → ack and drop (a poison message must not
   block the cursor).

### The invitation acceptance saga

`InvitationAcceptance` consumes `invitation.accepted` and, on the tenant group:

1. proposes `organization.assign_member` (the user joins the organization);
2. proposes `membership.add_member` with the invited role.

Both commands derive their **causation keys and entity ids** from the
`(organization, workspace, user)` tuple (D12), so a redelivered acceptance
replays instead of provisioning twice — proven by a test that delivers the same
event twice and asserts the group still holds exactly two events.

## 3. Wiring a host

A host that runs a tenant group:

1. keeps the `RaftGroup` handle;
2. periodically calls `Outbox::flush(group_id, &state_machine.applied_events(org))`
   and publishes through its broker connection;
3. runs a `SagaRunner` over its consumer with the host's `GroupRegistry`, so a
   saga can write to whichever group hosts the follow-up work.

`SagaRunner` needs no scheduler of its own: `run_once` is the unit, and a host
loops it (with backoff when it returns `false`).

## 4. Limits

- No NATS/JetStream binding yet; the cursor lives in the consumer, so a durable
  bus (JetStream consumer) owns persistence in production.
- One saga (`InvitationAcceptance`); the runner is generic.
- No dead-letter stream: fatal failures are acked and dropped, so a host must
  log them (observability is still to come).
