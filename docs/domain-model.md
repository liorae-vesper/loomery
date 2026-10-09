# Loomery domain model — the Phase-0 aggregates

**Status: implemented (Phase 0).** All six aggregates live in `crates/core/src`
and are covered by unit, transition-matrix, invariant and replay property
tests. The names and payload shapes below are the frozen contract they honor
(`design.md` §6, D12): a name is never renamed and a payload struct is never
mutated — a change is a new version plus an upcast chain
(`loomery_core::versioning`).

Three names are already frozen by the genesis script
(`docs/tutorials/genesis-worker.md`); they are marked **(frozen)** below and must
keep their exact spelling and payload shape.

> Provisional decisions are flagged in [§6](#6-decisions-taken). When one
> is settled, remove the flag and treat the name as frozen.

---

## 1. How an aggregate is written here

An aggregate is a **plan type**, not a value (`crates/core/src/aggregate.rs`):

```rust
pub struct Workspace;                                   // the plan (zero-sized)
pub struct WorkspaceState { /* folded truth */ }        // per-stream state
pub enum WorkspaceCode { /* stable codes */ }           // machine-readable errors
pub enum CreateWorkspace { /* typed command payload */ }// serde structs
pub enum WorkspaceCreated { /* typed event payload */ }
impl AggregatePlan<WorkspaceState, WorkspaceCode> for Workspace { ... }
```

Rules that every aggregate obeys:

- **Pure.** `prepare`/`apply` do no I/O, read no clock, mint no id. Timestamps,
  actor and keys arrive inside the `Command` envelope (D5/D12).
- **Derived identity.** Event ids come from
  `aggregate::event_from_command(&command, index, EVENT_TYPE, payload)`, which
  derives `event_id` from the command's `causation_key`. A retry therefore
  proposes byte-identical events.
- **Typed payloads.** `Payload.data` is a JSON string of a typed struct; a
  malformed payload is `InvalidPayload` with the decode error as `cause`.
- **Append-only.** There is no update or delete: corrections are compensating
  events (`archive`, `remove_member`, `deactivate`), and `apply` folds them.
- **Total `apply`.** An event the plan does not recognise leaves state unchanged
  rather than panicking; `prepare` is the only place that rejects.
- **Stable error codes.** Codes are compared by machines and never renamed.
- **Versioned payloads.** Each aggregate owns a closed `KnownPayload` enum and
  an exhaustive `upcast` chain; adding a version is a compile-time forcing
  function (see `versioning.rs`).

### Naming convention

| Element | Pattern | Example |
|---|---|---|
| command type | `<aggregate>.<verb>` or `<aggregate>.<verb>_<noun>` | `workspace.rename` |
| event type | `<aggregate>.<verb>ed` or `<aggregate>.<noun>_<verb>ed` | `workspace.renamed` |
| state struct | `<Aggregate>State` | `WorkspaceState` |
| code enum | `<Aggregate>Code` | `WorkspaceCode` |
| plan type | `<Aggregate>` | `Workspace` |

The three frozen genesis names show the two event shapes in use:
`workspace.create → workspace.created` and `membership.add_owner →
membership.owner_added`.

### What the property tests must prove

Each aggregate gets, at minimum:

- **Transition matrix** — for every `(state shape, command)` pair the plan
  either produces the documented event or the documented code. No panic, no
  silent success.
- **Replay determinism** — `fold(state, events)` is identical across runs.
- **Creates are unconditional** — creation events build state regardless of
  prior state (re-add / re-assign safe), so a replay after a snapshot loss still
  converges.
- **Random commands never panic** — `proptest` feeds arbitrary payload bytes.

---

## 2. Organization

The organization's tenant-scoped facts. Genesis ① lands here (the leader); the
control plane's *registry* of organizations is a separate concern handled in the
control-plane part.

| | |
|---|---|
| `aggregate_id` | the organization id |
| identity | organization id is minted by the control plane at registration (D12) |

**State**

```rust
pub struct OrganizationState {
    pub name: Option<String>,          // None until renamed
    pub leader_user_id: Option<Id>,    // None until genesis ①
    pub archived: bool,
}
```

**Commands → events**

| command | payload | event | event payload |
|---|---|---|---|
| `organization.assign_leader` **(frozen)** | `{ user_id }` | `organization.leader_assigned` **(frozen)** | `{ user_id }` |
| `organization.rename` | `{ name }` | `organization.renamed` | `{ name }` |
| `organization.archive` | `{}` | `organization.archived` | `{}` |

**Transitions**

| state | `assign_leader` | `rename` | `archive` |
|---|---|---|---|
| fresh (`{}`) | ✅ sets leader | ❌ `NotCreated`¹ | ✅ sets archived |
| active with name | ✅ re-assign (last write wins) | ✅ replaces name | ✅ sets archived |
| archived | ❌ `Archived` | ❌ `Archived` | ❌ `AlreadyArchived` |

¹ Whether renaming an unprovisioned organization is legal depends on where
registration lives; see [§6](#6-decisions-taken). The conservative rule is
to allow `rename` only once the organization exists (a leader was assigned or a
name was set).

**Codes:** `UnknownCommand`, `InvalidPayload`, `UnserializableEvent`,
`NotCreated`, `Archived`, `AlreadyArchived`, `InvalidName`.

---

## 3. User

A person known to Loomery. Users are provisioned by the OIDC path (Phase 1) and
live with the control-plane data.

| | |
|---|---|
| `aggregate_id` | the user id |
| identity | minted by the OIDC provisioner, or adopted from the IdP subject mapping |

**State**

```rust
pub struct UserState {
    pub email: Option<String>,        // None until provisioned
    pub display_name: Option<String>,
    pub active: bool,                 // false once deactivated
}
```

**Commands → events**

| command | payload | event | event payload |
|---|---|---|---|
| `user.provision` | `{ email, display_name }` | `user.provisioned` | `{ email, display_name }` |
| `user.update_profile` | `{ display_name }` | `user.profile_updated` | `{ display_name }` |
| `user.deactivate` | `{}` | `user.deactivated` | `{}` |

**Transitions**

| state | `provision` | `update_profile` | `deactivate` |
|---|---|---|---|
| fresh | ✅ provisions | ❌ `NotProvisioned` | ❌ `NotProvisioned` |
| active | ❌ `AlreadyProvisioned` | ✅ replaces name | ✅ deactivates |
| inactive | ❌ `AlreadyProvisioned` | ❌ `Inactive` | ❌ `AlreadyInactive` |

**Codes:** `UnknownCommand`, `InvalidPayload`, `UnserializableEvent`,
`NotProvisioned`, `AlreadyProvisioned`, `Inactive`, `AlreadyInactive`,
`InvalidProfile`.

---

## 4. Workspace

A collaboration space inside an organization. Genesis ② creates the default one
with a **derived** id (`default_workspace_id(org)`), so a resumed bootstrap
cannot create a second.

| | |
|---|---|
| `aggregate_id` | the workspace id (derived for the default workspace, minted otherwise) |

**State**

```rust
pub struct WorkspaceState {
    pub workspace_id: Option<Id>,     // None until created
    pub name: Option<String>,
    pub archived: bool,
}
```

**Commands → events**

| command | payload | event | event payload |
|---|---|---|---|
| `workspace.create` **(frozen)** | `{ workspace_id, name }` | `workspace.created` **(frozen)** | `{ workspace_id, name }` |
| `workspace.rename` | `{ name }` | `workspace.renamed` | `{ name }` |
| `workspace.archive` | `{}` | `workspace.archived` | `{}` |

**Transitions**

| state | `create` | `rename` | `archive` |
|---|---|---|---|
| fresh | ✅ creates | ❌ `NotCreated` | ❌ `NotCreated` |
| active | ❌ `AlreadyCreated` | ✅ replaces name | ✅ sets archived |
| archived | ❌ `AlreadyCreated` | ❌ `Archived` | ❌ `AlreadyArchived` |

**Codes:** `UnknownCommand`, `InvalidPayload`, `UnserializableEvent`,
`AlreadyCreated`, `NotCreated`, `Archived`, `AlreadyArchived`, `InvalidName`.

The frozen `created` event carries `workspace_id`, and its envelope
`workspace_id` is set to that same id (the genesis acceptance test asserts
this).

---

## 5. The three membership-ish aggregates

Three separate aggregates describe "who is in what". They are deliberately not
merged: their streams have different lifetimes and different groups of origin.

| Aggregate | Stream | Question it answers |
|---|---|---|
| `WorkspaceMembership` | one per (workspace, user) | "what may this user do in this workspace?" |
| `OrganizationAssignment` | one per (organization, user) | "is this user a member of this organization?" |
| `User` | one per user (control plane) | "who is this person?" |

### 5.1 WorkspaceMembership

| | |
|---|---|
| `aggregate_id` | the membership id (derived for the Owner at genesis, minted for later members) |

**Roles** — `Owner` / `Member` / `Viewer`, ordered `Owner > Member > Viewer`.

**State**

```rust
pub struct WorkspaceMembershipState {
    pub user_id: Option<Id>,
    pub role: Option<Role>,           // None once removed
}
```

**Commands → events**

| command | payload | event | event payload |
|---|---|---|---|
| `membership.add_owner` **(frozen)** | `{ user_id }` | `membership.owner_added` **(frozen)** | `{ user_id }` |
| `membership.add_member` | `{ user_id, role }` | `membership.member_added` | `{ user_id, role }` |
| `membership.change_role` | `{ user_id, role }` | `membership.role_changed` | `{ user_id, role }` |
| `membership.remove_member` | `{ user_id }` | `membership.member_removed` | `{ user_id }` |

The frozen `owner_added` payload is `{ user_id }`; the role is implied by the
event type. Later events carry the role explicitly.

**Transitions**

| state | `add_owner` / `add_member` | `change_role` | `remove_member` |
|---|---|---|---|
| fresh | ✅ sets `user_id`, role | ❌ `NotMember` | ❌ `NotMember` |
| member | ❌ `AlreadyMember` | ✅ sets role (same role → `NoChange`) | ✅ clears role |
| removed | ❌ `AlreadyMember` ² | ❌ `NotMember` | ❌ `NotMember` |

² A re-add after removal is a new intent with a new causation key; whether it
reuses the same membership id or mints a new one is a control-plane decision.

**Codes:** `UnknownCommand`, `InvalidPayload`, `UnserializableEvent`,
`AlreadyMember`, `NotMember`, `NoChange`, `UserMismatch`.

### 5.2 OrganizationAssignment

| | |
|---|---|
| `aggregate_id` | the assignment id (derived from `(organization, user)`) |

**State**

```rust
pub struct OrganizationAssignmentState {
    pub user_id: Option<Id>,
    pub assigned: bool,
}
```

**Commands → events**

| command | payload | event | event payload |
|---|---|---|---|
| `organization.assign_member` | `{ user_id }` | `organization.member_assigned` | `{ user_id }` |
| `organization.remove_member` | `{ user_id }` | `organization.member_removed` | `{ user_id }` |

**Transitions**

| state | `assign_member` | `remove_member` |
|---|---|---|
| fresh | ✅ assigns | ❌ `NotAssigned` |
| assigned | ❌ `AlreadyAssigned` | ✅ clears |
| removed | ❌ `AlreadyAssigned` ² | ❌ `NotAssigned` |

> The `organization.*` namespace is shared with the Organization aggregate on
> purpose: dispatch is by `command_type`, and each type maps to exactly one
> plan. If that reads badly in practice, `organization_assignment.*` is the
> alternative — see [§6](#6-decisions-taken).

**Codes:** `UnknownCommand`, `InvalidPayload`, `UnserializableEvent`,
`AlreadyAssigned`, `NotAssigned`, `UserMismatch`.

### 5.3 Task

The Phase-0 slice of the work core: enough for a workspace board to exist.
Projects, comments, dependencies and follow-ups are Phase 2 (`design.md` §5).

| | |
|---|---|
| `aggregate_id` | the task id (minted) |
| scope | `workspace_id` is set on the envelope |

**State**

```rust
pub struct TaskState {
    pub title: Option<String>,        // None until created
    pub status: TaskStatus,           // Open | Done
}
```

**Commands → events**

| command | payload | event | event payload |
|---|---|---|---|
| `task.create` | `{ title }` | `task.created` | `{ title }` |
| `task.rename` | `{ title }` | `task.renamed` | `{ title }` |
| `task.complete` | `{}` | `task.completed` | `{}` |
| `task.reopen` | `{}` | `task.reopened` | `{}` |

**Transitions**

| state | `create` | `rename` | `complete` | `reopen` |
|---|---|---|---|---|
| fresh | ✅ creates (Open) | ❌ `NotCreated` | ❌ `NotCreated` | ❌ `NotCreated` |
| Open | ❌ `AlreadyCreated` | ✅ replaces title | ✅ → Done | ❌ `NotDone` |
| Done | ❌ `AlreadyCreated` | ✅ replaces title ³ | ❌ `AlreadyDone` | ✅ → Open |

³ Renaming a completed task is allowed; if that proves wrong, the rule is a
new `TaskCode::Done`, not a payload change.

**Codes:** `UnknownCommand`, `InvalidPayload`, `UnserializableEvent`,
`AlreadyCreated`, `NotCreated`, `AlreadyDone`, `NotDone`, `InvalidTitle`.

---

## 6. Decisions taken

The implementation fixed the provisional points above; treat these as the
current contract (change requires a new payload version, not an edit):

| # | Decision | Taken |
|---|---|---|
| 1 | Organization registration | Control-plane tenant registry (part 2); the tenant-group Organization aggregate tracks leader/name/archived |
| 2 | `organization.rename` before provisioning | Rejected with `NotCreated` |
| 3 | Org membership namespace | `organization.*` (`assign_member` / `remove_member`) |
| 4 | Workspace roles | `Owner / Member / Viewer` |
| 5 | Re-adding a removed member | The membership stream is single-use: a re-add is `AlreadyMember`; the control plane mints a new membership id |
| 6 | Length bounds (D10) | org/workspace/display name ≤ 200 bytes; email ≤ 320; task title ≤ 500; blank values rejected |
| 7 | Task rename while done | Allowed (the rule is a state transition, not a title rule) |

## 7. What lives in which group (informative)

The core is group-agnostic, so this is a **shell** decision, fixed when the
control plane lands:

| Data | Intended group |
|---|---|
| User, organization registry, router records | control group |
| Organization (leader/name), Workspace, WorkspaceMembership, OrganizationAssignment, Task | tenant group (`organization_id`) |

`design.md` principle 3 ("one consensus group per organization + a control group
(users, orgs, router)") is the source for this split; genesis ① lands in the
tenant group, which is why the Organization aggregate exists there.

---

## Reference

- Contract: `crates/core/src/aggregate.rs` (`crates/core/src/aggregate.rs`)
- Versioning/upcast: `crates/core/src/versioning.rs` (`crates/core/src/versioning.rs`)
- Errors: `crates/core/src/error.rs` (`crates/core/src/error.rs`)
- Frozen genesis names: `crates/genesis/src/script.rs` (`crates/genesis/src/script.rs`)
- Identity model: `design.md` (`workpad/design.md`) D12
- Envelope and versioning: `design.md` (`workpad/design.md`) §6
