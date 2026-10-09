---
layout: home

hero:
  name: Loomery
  text: An event-sourced backend for team collaboration
  tagline: A pure, deterministic core with an imperative shell — OpenRaft consensus, tonic and RocksDB, in Rust.
  actions:
    - theme: brand
      text: Architecture at a glance
      link: /architecture
    - theme: alt
      text: Configuration reference
      link: /configuration

features:
  - title: Append-only
    details: Events are never mutated. Corrections are compensating events, and old payloads decode forever through frozen payloads and upcast.
  - title: Pure core, imperative shell
    details: prepare and apply are deterministic; the core never reads the clock, generates IDs or touches I/O. Every replica folds the same events into the same state.
  - title: One group per organization
    details: Each tenant has its own Raft group and its own database, plus a control group for users, organizations and the router. Cross-group work is choreography, never two-phase commit.
---

## Where to start

- **[Architecture at a glance](/architecture)** — the crate graph, the write and read
  paths, authorization, onboarding, group internals and host wiring, as diagrams.
- **[Domain model](/domain-model)** — the aggregates, their commands and events, and the
  legal transitions between states.
- **[Configuration](/configuration)** — every knob, its default, and how the parts
  constrain each other.
- **[Runtime host](/host)** — `loomery-server`: configuration, wiring and boot sequence.
- **[Tutorials](/tutorials/shell-group)** — build the core-to-shell path in order, from the
  group port to the OpenRaft storage spike.

## The invariant everything rests on

```
fold(fold(state, e1), e2) == fold(state, e1 ++ e2)
```

Identical commands produce identical applies. That is what makes deterministic replay
possible, and what lets every replica of a group agree on state without agreeing on a
machine.
