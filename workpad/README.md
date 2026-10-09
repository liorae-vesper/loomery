# Workpad

Working notes: handoffs, in-progress designs, and the reasoning behind a number
before it is worth publishing. This is where work is staged, not a second docs
tree.

## The rule

`docs/` is the published site: what a reader of the project should find — how it works,
what was decided, how to run and configure it. Everything else starts here. When a note
is refined enough to still be true a year from now, move it into `docs/` as a page of the
site (and add it to the site's navigation), or fold it into a page that is already there
— **and then delete it from here**. A note that was promoted and a note that turned out
to be wrong both leave. Nothing here is an archive and nothing here is user-facing.

    workpad/<subject>.md  ->  docs/<page>.md  ->  (workpad copy deleted)

The engineering material that is *not* user-facing stays here by design: the decisions
register, the implementation walkthrough, the benchmark record and the research notes.
Those are for maintainers, and the site does not link to them.

## What belongs here

- **Handoffs.** What landed, what is left, the constraints and the traps — the
  pickup point for the next session or the next reviewer. Template below.
- **In-progress notes.** An investigation whose conclusion is not settled, kept
  because the next session should not repeat it.
- **Scratch reasoning** worth carrying across sessions but not worth publishing:
  the configuration behind a measurement, the commands that produced it, the
  alternative that was ruled out.

## What does not

- **Anything user-facing.** If a reader of the project needs it, it belongs in
  `docs/`; the [engineering record](documentation-index.md) says where each kind goes.
- **Raw benchmark output.** `benchmark-results/` is the gitignored home for node
  databases and per-trial samples. A workpad note records the *numbers and the
  command*; the data itself stays out of the repository.
- **Secrets, tokens, or host-specific paths.**

## Links go one way

A workpad note may link into `docs/`. A `docs/` page must never link back here,
because a published page cannot depend on a file that is expected to be deleted.
`mise run docs-links` enforces that direction, along with the usual link, anchor
and run-citation checks. A broken link in `docs/` fails it; a broken one here is
reported as a warning, because this tree is allowed to be mid-edit. A file here
needs no place in the site's navigation.

## Handoff template

```markdown
# <subject> handoff

Status: <what is true right now, in a sentence or two, with branch and commit>

## Where it stands
## What is left
## Constraints that will bite
## Evidence
```

Cite names, not adjectives: `20261008171930-shipped-deployment-scale` is
checkable, "much faster" is not. A number that cannot be traced to a run in the
committed results will fail `mise run docs-links`.
