#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Collect the benchmark runs under benchmark-results/ into one committed file.

`benchmark-results/` is gitignored — it holds node databases and per-trial raw
samples, which do not belong in the repository — so the medians a benchmark note
quotes would otherwise be reproducible only on the machine that ran them. This turns
the runs into `workpad/benchmarks/results/deployment-path.json`: for every run, the
config overrides it used, the medians per point, the harness's own verification
counts, and the environment it ran on. Each entry names the directory it came from, so
a reader can re-run the sweep (`mise run bench-deployment-scale`) and regenerate it.

Usage:

    python3 scripts/bench-collect-results.py            # writes the committed file
    python3 scripts/bench-collect-results.py --print    # ...and prints a summary
"""

import argparse
import json
import statistics
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "benchmark-results"
OUT = REPO / "workpad/benchmarks/results/deployment-path.json"
MEDIAN_KEYS = ("throughput_per_second", "p50_us", "p95_us", "p99_us",
               "events_verified", "commands_per_log_index", "distinct_log_indices")


def median(rows, key):
    values = [row[key] for row in rows if row.get(key) is not None]
    return statistics.median(values) if values else None


def summarize(directory):
    """The medians of one run, from what the harness wrote."""
    summary = json.loads((directory / "summary.json").read_text())
    points = []
    for point in summary["points"]:
        good = [row for row in point.get("trials", []) if not row.get("failed")]
        entry = {
            "mode": point["mode"],
            "operations": point["operations"],
            "harness_ok": point["harness_ok"],
            "failed_trials": point.get("failed_trials"),
            "trials": len(point.get("trials", [])),
            "bytes_on_disk": point.get("bytes_on_disk"),
            "seconds": point.get("seconds"),
        }
        for key in MEDIAN_KEYS:
            entry[key] = median(good, key)
        points.append(entry)
    # The harness writes its environment beside each point, not at the run root.
    environments = sorted(directory.glob("*/environment.json"))
    environment = json.loads(environments[0].read_text()) if environments else {}
    return {
        "directory": directory.name,
        "overrides": summary.get("overrides", {}),
        "phase_timeout_ms": summary.get("phase_timeout_ms"),
        "git_commit": environment.get("git_commit"),
        "cpu": (environment.get("cpu", "").strip().splitlines() or [""])[-1].strip(),
        "memory_kb": environment.get("memory", ""),
        "build_profile": environment.get("build_profile"),
        "points": points,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--print", action="store_true", dest="show",
                        help="print a one-line summary per run")
    args = parser.parse_args()

    runs = []
    skipped = []
    for directory in sorted(RESULTS.glob("*")):
        if not (directory / "summary.json").exists():
            continue
        # benchmark-results/ also holds other benchmarks' output (services-stress, the
        # consensus harness's own runs); only the deployment-path sweep has points.
        if "points" not in json.loads((directory / "summary.json").read_text()):
            skipped.append(directory.name)
            continue
        runs.append(summarize(directory))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({
        "note": "Medians of the deployment-path runs; regenerate the runs with "
                "`mise run bench-deployment-scale` and this file with "
                "`python3 scripts/bench-collect-results.py`. Raw output lives in "
                "benchmark-results/, which is gitignored.",
        "runs": runs,
    }, indent=2) + "\n")

    if args.show:
        for run in runs:
            for point in run["points"]:
                rate = point["throughput_per_second"]
                print(f"{run['directory']:44} {point['mode']:10} {point['operations']:6} "
                      f"{'-' if rate is None else format(rate, '.0f'):>8} w/s  "
                      f"p50 {'-' if point['p50_us'] is None else format(point['p50_us'], '.0f'):>8} us  "
                      f"trials {point['trials']} failed {point['failed_trials']}")
    if skipped:
        print(f"skipped (another benchmark's output): {', '.join(skipped)}")
    print(f"{len(runs)} run(s) → {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
