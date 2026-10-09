# The engineering record

This is the staging tree. It holds the reasoning, the measurements and the decisions
behind the published documentation in `../docs/` — the material a maintainer needs and
a reader of the project does not. Nothing here is user-facing, and nothing here is an
archive: a note leaves once it has been promoted, folded into a published page, or
refuted.

The published site is the other direction: [Loomery documentation](../docs/index.md).

## Where the material lives

| Area | What is in it |
|---|---|
| [Design](design.md) | Architecture, the decisions register and the phased roadmap; distinguishes implemented and planned work |
| [Implementation guide](implementation.md) | Build pipeline, Raft startup, storage/network wiring and tenant group lifecycle |
| [Continuation](CONTINUE.md) | Concise handoff; the design tracker is the full backlog |
| [Development guardrails](guardrails.md) | Toolchain, verification tasks, hooks and CI |
| [Benchmarks](benchmarks/README.md) | The controlled harness: workload, phases, parameters, and every measurement note |
| [Research](research/openraft-vs-alternatives.md) | The alternatives and reasoning behind decisions, kept as provenance |
| [OpenRaft 0.10 handoff](openraft-010-handoff.md) | The pickup point for the migration work |
| [CI health](ci-health.md) | What the pipeline has proved, and what a run must still prove |
| [Writing](blog-tuning-raft-and-rocksdb.md) | The tuning post, staged until it has a home |

The benchmark configs (`benchmarks/consensus.json`, `benchmarks/batch-matrix.json`) and
the collected runs (`benchmarks/results/`) live beside the notes that quote them, so a
number in a note can be traced to the run it came from.

## What a staged note may do

- Link into `../docs/` freely — published pages are stable.
- Link to its neighbours here; the links are checked, but a broken one is a warning
  rather than a failure, because this tree is allowed to be mid-edit.
- Name a benchmark run (`20261008171930-shipped-deployment-scale`). A run that is not in
  the committed results fails `mise run docs-links` in either tree.
