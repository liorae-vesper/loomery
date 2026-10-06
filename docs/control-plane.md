# The Loomery control plane

The control plane is the part of the shell that answers *"which group owns this
organization, and is it ready?"*. It is not a separate service: it is a Raft
group — the **control group** — plus a projection of it. One group per
organization holds that organization's domain events ([`design.md`](design.md)
principle 3); the control group holds the *placement* of every such group.

```text
  create tenant ──► control group ──┐
                      │             │ project
                      │  tenant.*   ▼
                      │           Router { organization_id → group }
                      ▼
                 tenant group ──► genesis ①②③ ──► active
```

## 1. The tenant record (`loomery_core::tenant`)

One stream per organization, `aggregate_id = organization_id`, living in the
control group.

| command | event | meaning |
|---|---|---|
| `tenant.register` | `tenant.registered` | record the group id and intended replicas |
| `tenant.activate` | `tenant.activated` | genesis committed; traffic allowed |
| `tenant.tombstone` | `tenant.tombstoned` | retire the tenant (append-only) |

`TenantStatus` is `Unregistered → Registering → Active`, with `Tombstoned` as a
monotonic terminal state. The payload carries `Replica { node_id, address }`
entries; placement validation rejects blank/oversized group ids and addresses,
empty or oversized replica sets, and duplicate node ids. `tenant.tombstone`
works from `Registering` or `Active`; a retired tenant keeps its record so a
delayed worker cannot resurrect it.

The plan is a normal [`AggregatePlan`](../crates/core/src/aggregate.rs):
deterministic, payload-typed, unit + transition-matrix + invariant + replay
property tested.

## 2. The router (`shell::control::Router`)

A `DashMap<Id, Route>` where

```rust
struct Route { group_id: String, replicas: Vec<Replica>, active: bool }
```

`route()` answers `organization_id → group` without a lock, so the gateway can
read while the control group folds.

- `apply(organization_id, tenant)` — upsert one record. Registration inserts;
  activation flips `active` in place; **tombstone withdraws the route**; an
  unregistered or group-less record is never routable.
- `rebuild(&[(Id, TenantState)])` — re-derive the whole table from the control
  group's applied records. This is the restart path: rebuilding from the same
  log is idempotent.

`active` is the traffic fence: a route exists while a tenant is `Registering`,
but only an `Active` route may carry application traffic.

## 3. The controller (`shell::control::provision`)

```rust
provision(control, tenant, router, group_id, replicas, &bootstrap) -> Genesis
```

1. **Record the placement** in the control group (`tenant.register`) *before*
   touching the tenant group, so a crash is reconcilable from the control log.
2. **Run genesis** on the tenant group (`bootstrap::run`, ①②③).
3. **Activate** the tenant (`tenant.activate`) — only after genesis committed.
4. **Publish the route** by projecting the control records into the router.

Steps 1 and 3 are idempotent by **derived identity** (D12): each control
command's causation key is `(organization, action)`, so a retry replays instead
of duplicating. Step 2 resumes from the tenant group's own log. The ordering of
steps 3–4 is the safety property: **a caller can never observe a half-born
tenant**, and a failed genesis leaves no route.

### Host wiring (not the controller's job)

The controller starts from an already booted, initialized tenant group. A host
owns the rest, as [`implementation.md`](implementation.md) describes:

- `RaftGroup::boot_persistent(node_id, group_id, path, config)` per replica;
- register each handle with the shared `TonicTransport`;
- `initialize` membership **once**, on the bootstrap replica of a *new* group
  (never on a recovered one);
- keep `RaftGroup` handles in the host's own registry for workers, reads and
  lifecycle.

## 4. Reconciliation (`shell::control::{incomplete, resume}`)

- `incomplete(&control)` — the registered-but-inactive tenants, in id order.
  This is the startup sweep's and the retry sweep's work list.
- `resume(control, tenant, router, &bootstrap)` — completes genesis and
  activation for one such tenant. It **never calls `initialize`**: a recovered
  group already has its membership, and re-initializing is the classic
  split-brain mistake.

A reconciliation loop is therefore: read `incomplete()`, have the host reopen
each tenant's databases (same paths, recovered membership), call `resume()`,
and let `provision`'s fence decide when the tenant is safe to route.

## 5. Current limits

- The host registry of running groups, placement persistence and the periodic
  sweep are **not** implemented; the building blocks above are.
- Deletion is a tombstone in the control log; physical replica cleanup is a
  host/ops workflow (`implementation.md` §"Remove a group").
- The gateway's `X-Min-Index` read-your-writes hold is separate (see
  `docs/shell.md` and the gateway work).

## Reference

- [`crates/core/src/tenant.rs`](../crates/core/src/tenant.rs) — the aggregate
- [`crates/shell/src/control/`](../crates/shell/src/control) — router, controller, reconciliation
- [`crates/shell/src/raft/state_machine.rs`](../crates/shell/src/raft/state_machine.rs) — dispatch + `tenants()`
- [`design.md`](design.md) §4, §8 — the planned shell layout and tracker
