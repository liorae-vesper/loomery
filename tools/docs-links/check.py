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
* **Reachability.** Every page of the published site — everything under `docs/` — is
  reachable either from `docs/index.md` or from the site's navigation, which is read
  out of `docs/.vitepress/config.mts`. The sidebar *is* the index of the site: a page
  added to `docs/` and forgotten in the config is reported here, and a sidebar entry
  pointing at a page that does not exist is reported as a broken link. A file in
  `workpad/` is staged rather than published, so it needs no place in an index.
* **Direction.** A published page must not link into `workpad/`, because the site
  cannot depend on a file that is expected to be deleted; a staged note names the
  path in code instead. Notes flow the other way: `workpad/` may link into `docs/`.
  The repository's own `README.md` is not part of the site, so it may point at either.
* **Citations.** A benchmark run named in the prose (`20261008171930-…`) exists in
  the committed results, so a quoted number can be traced back to the run it came from
  instead of to somebody's memory of it. The results live under `workpad/benchmarks/`
  with the rest of the measurement record.

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
# The published site's home. Everything under `docs/` must be reachable from here or
# from the navigation, so the site cannot quietly lose a page.
ROOTS = ("docs/index.md",)
# The published tree (the VitePress site), and the staging tree of internal notes.
PUBLISHED = "docs"
PAD = "workpad"
# The site's navigation, whose `link:` values are routes (`/configuration`).
SITE_CONFIG = "docs/.vitepress/config.mts"
# Directories inside the published tree that are toolchain or build output, not pages.
IGNORED = {"node_modules", "dist", "cache", ".vitepress"}
EXTERNAL = ("http://", "https://", "mailto:", "ftp://", "//")

LINK = re.compile(r"\[[^\]]*\]\(\s*<?([^)\s>]+)>?\s*\)")
ROUTE = re.compile(r"""link:\s*['"]([^'"]+)['"]""")
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
    return sorted({p.resolve() for p in files if not IGNORED.intersection(p.parts)})


def site_routes() -> list:
    """The `link:` values of the site's navigation, as (line number, route)."""
    config = REPO / SITE_CONFIG
    if not config.exists():
        return []
    return [
        (number, match.group(1))
        for number, line in enumerate(config.read_text(encoding="utf-8").splitlines(), 1)
        if (match := ROUTE.search(line))
    ]


def route_target(route: str) -> Path | None:
    """The page a site route names, or None when it names something else.

    A route may carry a fragment (`/architecture#the-write-path`) or be an external
    URL; only the page part is resolved here.
    """
    if route.startswith(EXTERNAL) or not route.startswith("/"):
        return None
    page = route.split("#")[0].strip("/")
    for candidate in (REPO / PUBLISHED / f"{page}.md", REPO / PUBLISHED / page / "index.md"):
        if candidate.exists():
            return candidate.resolve()
    return None


def known_runs() -> set:
    """Every benchmark run the repository records, from the committed JSON."""
    names = set()
    for tree in (REPO / PUBLISHED, REPO / PAD):
        for path in tree.rglob("*.json"):
            if IGNORED.intersection(path.parts):
                continue
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
    cache, edges, problems, warnings = {}, {}, [], []
    broken_link = broken_anchor = unknown_run = pad_link = bad_route = pad_link_broken = 0
    runs = known_runs()
    published, pad = REPO / PUBLISHED, REPO / PAD
    home = (REPO / ROOTS[0]).resolve()

    # The site's navigation counts as an index: every route it names is reachable,
    # and a route naming no page is a broken link.
    for number, route in site_routes():
        target = route_target(route)
        if target is None:
            problems.append((REPO / SITE_CONFIG, number, f"no page for site route: {route}"))
            bad_route += 1
            continue
        edges.setdefault(home, set()).add(target)

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
                # Only a page of the site is published; the repository's README is not.
                source_published = published in path.parents
                # A broken link is a failure in the published site and a warning in the
                # staging tree, which is allowed to be mid-edit: the site is what a
                # reader navigates, and a staged note may point at something not yet
                # moved. Everything else below applies to both trees.
                report = problems if source_published else warnings
                # In a published page, `/configuration` is a route the site serves, not a
                # path in the repository. Anywhere else it is a repository path.
                if raw.startswith("/") and source_published:
                    dest = route_target(raw)
                    if dest is None:
                        problems.append((path, number, f"no page for site route: {raw}"))
                        bad_route += 1
                        continue
                    edges.setdefault(path, set()).add(dest)
                    if fragment and dest in known:
                        if dest not in cache:
                            cache[dest] = headings(dest)
                        if fragment not in cache[dest]:
                            problems.append((path, number, f"no heading for anchor: {raw}"))
                            broken_anchor += 1
                    continue
                if raw.startswith("/"):  # repo-root relative
                    dest = (REPO / raw[1:].split("#")[0]).resolve()
                elif target:
                    dest = (path.parent / target).resolve()
                else:
                    dest = path

                if target and not dest.exists():
                    report.append((path, number, f"link target does not exist: {raw}"))
                    if source_published:
                        broken_link += 1
                    else:
                        pad_link_broken += 1
                    continue
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
                        report.append((path, number, f"no heading for anchor: {raw}"))
                        if source_published:
                            broken_anchor += 1

    reached, queue = set(), [home]
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
        print(f"  {relative(path)}: not reachable from the site index or its navigation")
    if warnings:
        print(f"  ({len(warnings)} link(s) in the staging tree point at nothing; not a failure)")
    print(
        f"docs-links: {len(files)} file(s), {broken_link} broken link(s) in the site, "
        f"{broken_anchor} broken anchor(s) in the site, {len(unreachable)} unindexed "
        f"published page(s), {unknown_run} untraceable run citation(s), "
        f"{pad_link} published-to-workpad link(s), {bad_route} broken site route(s), "
        f"{pad_link_broken} staging link(s) to fix when convenient"
    )
    return 1 if problems or unreachable else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
