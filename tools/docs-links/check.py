#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Check the documentation's internal links, anchors, citations and reachability.

`mise run docs-links` runs this. The Mermaid check next door
(`tools/mermaid-check/`) proves the diagrams parse; this proves the prose can be
navigated, which no renderer will tell you about:

* **Links.** Every relative link points at a file that exists, in either tree.
* **Anchors.** A `file.md#heading` fragment matches a heading in that file, using
  GitHub's slug rules, so renaming a section fails the check instead of silently
  breaking every link that pointed at it.
* **Reachability.** Every *published* document — anything under `docs/` — is
  reachable from `docs/README.md` by following relative links, so the index is the
  whole map: a page nobody linked is reported here rather than found by accident
  later. A file in `workpad/` is staged rather than published, so it needs no place
  in the index.
* **Direction.** A `docs/` page must not link into `workpad/`, because a published
  page cannot depend on a file that is expected to be deleted. Notes flow the other
  way: workpad may link into `docs/`.
* **Citations.** A benchmark run named in the prose (`20261008171930-…`) exists in
  the committed results under `docs/benchmarks/results/`, so a quoted number can be
  traced back to the run it came from instead of to somebody's memory of it.

Fenced code is skipped, so a link written as an example inside a fence is not a
link. External links are not fetched — this is an offline check.

Usage:

    python3 tools/docs-links/check.py            # docs/, workpad/ and README.md
    python3 tools/docs-links/check.py docs README.md workpad
"""

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
DEFAULT_TARGETS = ("docs", "README.md", "workpad")
# The documents a reader is expected to arrive at. Everything *published* must be
# reachable from one of these, so the index cannot quietly lose a page.
ROOTS = ("docs/README.md", "README.md")
# The published tree, and the staging tree whose notes are not published yet.
PUBLISHED = "docs"
PAD = "workpad"
EXTERNAL = ("http://", "https://", "mailto:", "ftp://", "//")

LINK = re.compile(r"\[[^\]]*\]\(\s*<?([^)\s>]+)>?\s*\)")
RUN = re.compile(r"\b20\d{6}\d{6}-[a-z0-9][a-z0-9-]*")
FENCE = re.compile(r"^\s*(```|~~~)")
HEADING = re.compile(r"^#{1,6}\s+(.*?)\s*$")
INLINE_CODE = re.compile(r"`[^`]*`")


def slug(heading: str) -> str:
    """GitHub's heading anchor: lowercase, drop punctuation, then space -> hyphen.

    The one-space-one-hyphen rule is what makes `D1 — Consensus` an anchor of
    `#d1--consensus`: the em dash goes, both spaces stay.
    """
    text = re.sub(r"[^\w\- ]", "", heading.strip().lower())
    return text.replace(" ", "-")


def prose(path: Path):
    """Yield (line number, line) for the lines outside fenced code blocks."""
    in_fence = False
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if FENCE.match(line):
            in_fence = not in_fence
            continue
        if not in_fence:
            yield number, line


def headings(path: Path) -> set:
    """Every anchor GitHub would generate for the file, duplicates numbered."""
    found, seen = set(), {}
    for _, line in prose(path):
        match = HEADING.match(line)
        if not match:
            continue
        base = slug(match.group(1))
        count = seen.get(base, 0)
        seen[base] = count + 1
        found.add(base if count == 0 else f"{base}-{count}")
    return found


def documents(targets) -> list:
    files = []
    for name in targets:
        target = Path(name)
        target = target if target.is_absolute() else REPO / target
        if target.is_dir():
            files.extend(target.rglob("*.md"))
        elif target.is_file():
            files.append(target)
        else:
            raise SystemExit(f"docs-links: no such path: {name}")
    return sorted({path.resolve() for path in files})


def known_runs() -> set:
    """Every benchmark run the repository records, from the committed JSON."""
    names = set()
    for path in (REPO / "docs").rglob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        stack = [data]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
            elif isinstance(item, str):
                names.update(RUN.findall(item))
    return names


def relative(path: Path) -> str:
    return str(path.relative_to(REPO)) if path.is_relative_to(REPO) else str(path)


def main(argv) -> int:
    files = documents(argv[1:] or DEFAULT_TARGETS)
    known = set(files)
    cache, edges, problems = {}, {}, []
    broken_link = broken_anchor = unknown_run = pad_link = 0
    runs = known_runs()
    published, pad = REPO / PUBLISHED, REPO / PAD

    for path in files:
        for number, line in prose(path):
            for name in RUN.findall(line):
                if name not in runs:
                    problems.append((path, number, f"no such recorded run: {name}"))
                    unknown_run += 1
            for raw in LINK.findall(INLINE_CODE.sub("", line)):
                if raw.startswith(EXTERNAL):
                    continue
                target, _, fragment = raw.partition("#")
                if raw.startswith("/"):  # repo-root relative
                    dest = (REPO / raw[1:].split("#")[0]).resolve()
                elif target:
                    dest = (path.parent / target).resolve()
                else:
                    dest = path

                if target and not dest.exists():
                    problems.append((path, number, f"link target does not exist: {raw}"))
                    broken_link += 1
                    continue
                source_published = published in path.parents or path == REPO / "README.md"
                if source_published and pad in dest.parents:
                    problems.append(
                        (path, number, f"published page links into the workpad: {raw}")
                    )
                    pad_link += 1
                    continue
                if dest in known:
                    edges.setdefault(path, set()).add(dest)
                if fragment and dest.suffix == ".md" and dest in known:
                    if dest not in cache:
                        cache[dest] = headings(dest)
                    if fragment not in cache[dest]:
                        problems.append((path, number, f"no heading for anchor: {raw}"))
                        broken_anchor += 1

    reached, queue = set(), [REPO / root for root in ROOTS]
    while queue:
        current = queue.pop().resolve()
        if current in reached:
            continue
        reached.add(current)
        queue.extend(edges.get(current, ()))
    # Only the published tree is indexed; a staged note is allowed to be unlinked.
    unreachable = [path for path in files if published in path.parents and path not in reached]

    for path, number, message in problems:
        print(f"  {relative(path)}:{number}: {message}")
    for path in unreachable:
        print(f"  {relative(path)}: not reachable from any index document")
    print(
        f"docs-links: {len(files)} file(s), {broken_link} broken link(s), "
        f"{broken_anchor} broken anchor(s), {len(unreachable)} unindexed published document(s), "
        f"{unknown_run} untraceable run citation(s), {pad_link} published-to-workpad link(s)"
    )
    return 1 if problems or unreachable else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
