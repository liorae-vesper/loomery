#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Measure the deployment path: the multi-process 3-node harness at growing sizes.

Each point is one harness run — three replica processes with real loopback TCP,
RocksDB and the production apply path — with one generated config that differs from
its siblings only in the operation count (and, for the snapshot point, the
persistence mode). Everything else is held fixed, so the curve is about *scale*:
same nodes, trials, warmup, concurrency, payload, snapshot policy and timeouts.

The point of the exercise is an honest reading of the deployment path, so:

* every point keeps its raw harness output (config, environment, per-trial reports,
  the node databases), and the summary names the files each number came from;
* a point that fails is recorded as a failure — the script never shrinks a workload,
  drops a trial or retries a number into looking better;
* the sweep exits non-zero if anything failed, so it can gate.

Usage:

    python3 scripts/bench-deployment-scale.py --dry-run
    python3 scripts/bench-deployment-scale.py --points 2000 --trials 1   # smoke
    python3 scripts/bench-deployment-scale.py                           # the sweep
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE_CONFIG = REPO / "docs/benchmarks/consensus.json"
EXAMPLE = "consensus_bench"
RESULTS = REPO / "benchmark-results"

# A point is one config: (mode, operations). Checkpoint is the default mode, so it
# carries the curve; snapshot joins at the largest size to show the comparison.
DEFAULT_POINTS = [("checkpoint", 2_000), ("checkpoint", 5_000), ("checkpoint", 10_000),
                  ("checkpoint", 20_000), ("snapshot", 20_000)]


def comma_ints(value):
    """`2000,5000` → `[2000, 5000]`; an empty string asks for no such points."""
    if value == "":
        return []
    try:
        values = [int(part) for part in value.split(",") if part]
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"not a list of integers: {value}") from error
    if not values or any(size < 1 for size in values):
        raise argparse.ArgumentTypeError(f"must be positive sizes: {value}")
    return values


def positive(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def config_for(base, mode, operations, timeout_ms):
    """The base config, with only the size (and mode) changed."""
    config = json.loads(json.dumps(base))
    config["operations"] = operations
    # Failover belongs to `mise run test-consensus-failures`: in a scale curve it
    # would add an election's variance to every point.
    config["failover"] = False
    # Sized so a legitimate point finishes; a hung run still ends, bounded.
    config["phase_timeout_ms"] = timeout_ms
    config.setdefault("group", {}).setdefault("storage", {})["state_persistence"] = mode
    return config


def directory_size(path):
    """Bytes on disk under `path`, so a point's footprint is measurable."""
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                continue
    return total


def read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def summarize_point(directory, trials):
    """Every trial's numbers, and the median across trials.

    Reads what the harness wrote and nothing else: a trial without a report is a
    failure, reported as one.
    """
    rows = []
    for number in range(1, trials + 1):
        trial = directory / f"trial-{number:03d}"
        report = read_json(trial / "report.json")
        if report is None:
            error = read_json(trial / "error.json")
            rows.append({"trial": number, "failed": True, "error": error or "no report"})
            continue
        writes = report.get("writes", {}).get("summary", {})
        # The harness's own verification: each replica confirms the applied event
        # count it was told to expect.
        checks = report.get("checks", [])
        recovered = report.get("recovered", [])
        rows.append({
            "trial": number,
            "failed": False,
            "throughput_per_second": writes.get("throughput_per_second"),
            "successes": writes.get("successes"),
            "failures": writes.get("failures"),
            "distinct_log_indices": writes.get("distinct_log_indices"),
            "p50_us": writes.get("p50_us"),
            "p95_us": writes.get("p95_us"),
            "p99_us": writes.get("p99_us"),
            "events_verified": min((c.get("events", 0) for c in checks), default=None),
            "replicas_checked": len(checks),
            "events_after_restart": min((c.get("events", 0) for c in recovered), default=None),
            "initialization_us": report.get("initialization_us"),
            "restart_us": report.get("restart_us"),
        })

    good = [row for row in rows if not row["failed"]]
    median = {}
    for key in ("throughput_per_second", "p50_us", "p95_us", "p99_us", "events_verified"):
        values = [row[key] for row in good if row.get(key) is not None]
        median[key] = statistics.median(values) if values else None
    return {
        "trials": rows,
        "medians": median,
        "failed_trials": len(rows) - len(good),
        "bytes_on_disk": directory_size(directory),
    }


def run_point(binary, config_path, output, dry_run):
    """Runs one point. Returns (ok, seconds)."""
    if dry_run:
        return True, 0.0
    started = time.monotonic()
    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [binary, "--config", str(config_path), "--output", str(output)],
        cwd=REPO,
        check=False,
    )
    return completed.returncode == 0, time.monotonic() - started


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--points", type=comma_ints, default=None,
                        help="checkpoint sizes, comma separated (default 2000,5000,10000,20000)")
    parser.add_argument("--snapshot-points", type=comma_ints, default=[20_000],
                        help="snapshot sizes, comma separated (default 20000)")
    parser.add_argument("--trials", type=positive, default=3, help="trials per point (default 3)")
    parser.add_argument("--timeout-ms", type=positive, default=900_000,
                        help="per-phase timeout (default 900000: bounded, but room to finish)")
    parser.add_argument("--tag", default="deployment-scale", help="output directory tag")
    parser.add_argument("--extra", default="{}",
                        help="JSON object merged into every config, so one knob can be probed "
                             "without editing the base config (e.g. '{\"concurrency\": 64}')")
    parser.add_argument("--dry-run", action="store_true", help="write configs and the plan, run nothing")
    args = parser.parse_args()

    base = json.loads(BASE_CONFIG.read_text())
    base["trials"] = args.trials
    try:
        extra = json.loads(args.extra)
    except json.JSONDecodeError as error:
        sys.exit(f"--extra is not JSON: {error}")
    if not isinstance(extra, dict):
        sys.exit("--extra must be a JSON object")
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key].update(value)
        else:
            base[key] = value
    points = [("checkpoint", size) for size in (args.points or [size for _, size in DEFAULT_POINTS[:4]])]
    points += [("snapshot", size) for size in args.snapshot_points]

    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    root = RESULTS / f"{stamp}-{args.tag}"
    if root.exists():
        sys.exit(f"output directory already exists: {root}")
    (root / "configs").mkdir(parents=True)

    print(f"points: {', '.join(f'{mode}-{size}' for mode, size in points)}")
    print(f"trials per point: {args.trials}, phase timeout: {args.timeout_ms} ms")
    print(f"output: {root}")

    # Build once, then run the binary directly: a sweep that rebuilds per point
    # measures cargo, not consensus.
    binary = REPO / "target/release/examples" / EXAMPLE
    if not args.dry_run:
        built = subprocess.run(  # noqa: S603 - fixed argv, no shell
            ["cargo", "build", "--release", "-p", "loomery-shell", "--example", EXAMPLE],
            cwd=REPO, check=False,
        )
        if built.returncode != 0:
            sys.exit("the harness did not build")
        if not binary.exists():
            sys.exit(f"no harness binary at {binary}")

    summary = {"started": stamp, "base_config": str(BASE_CONFIG), "trials": args.trials,
               "phase_timeout_ms": args.timeout_ms, "overrides": extra,
               "points": [], "dry_run": args.dry_run}
    failures = 0

    for mode, size in points:
        name = f"{mode}-{size}"
        config_path = root / "configs" / f"{name}.json"
        config_path.write_text(json.dumps(config_for(base, mode, size, args.timeout_ms), indent=2) + "\n")
        # The harness creates the output directory itself and refuses an existing
        # one; creating it here makes every point fail with EEXIST.
        output = root / name
        print(f"\n== {name} ==")
        ok, seconds = run_point(binary, config_path, output, args.dry_run)
        point = {"mode": mode, "operations": size, "config": str(config_path.relative_to(REPO)),
                 "harness_ok": ok, "seconds": round(seconds, 1)}
        if ok and not args.dry_run:
            point.update(summarize_point(output, args.trials))
            medians = point["medians"]
            print(f"   {medians['throughput_per_second']:.0f} writes/s median, "
                  f"p50 {medians['p50_us']} us, p99 {medians['p99_us']} us, "
                  f"{point['failed_trials']} failed trial(s), "
                  f"{point['bytes_on_disk'] / 1e6:.1f} MB on disk, {seconds:.0f} s")
        if not ok or point.get("failed_trials", 0):
            failures += 1
        summary["points"].append(point)

    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nsummary: {root / 'summary.json'}")
    print("numbers are medians across trials; each is traceable to the per-trial report.json files")
    if failures:
        print(f"{failures} point(s) failed — recorded as failures, not adjusted away")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
