#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Run sequential checkpoint/snapshot comparisons using the real Raft harness."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess


def summarize(directory):
    reports = [json.loads(p.read_text()) for p in sorted(directory.glob("trial-*/report.json"))]
    if not reports:
        raise RuntimeError(f"No completed trials in {directory}")
    writes = [r["writes"]["summary"] for r in reports]
    return {
        "trials": len(reports),
        "failures": sum(w["failures"] for w in writes),
        "median_trial_writes_per_second": statistics.median(w["throughput_per_second"] for w in writes),
        "median_trial_p50_us": statistics.median(w["p50_us"] for w in writes),
        "median_trial_p99_us": statistics.median(w["p99_us"] for w in writes),
        "median_restart_us": statistics.median(r["restart_us"] for r in reports),
        "recovered_events": [[c["events"] for c in r["recovered"]] for r in reports],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("docs/benchmarks/consensus.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summarize", action="store_true", help="Summarize existing arm directories without running")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    if not args.summarize:
        args.output.mkdir(parents=True, exist_ok=False)
        subprocess.run(["mise", "exec", "--", "cargo", "build", "--release", "-p", "loomery-shell", "--example", "consensus_bench"], check=True)
        base = json.loads(args.config.read_text())
        for policy in (5000, 200):
            for mode in ("checkpoint", "snapshot"):
                config = json.loads(json.dumps(base))
                config.setdefault("group", {}).setdefault("storage", {})["state_persistence"] = mode
                config["group"].setdefault("raft", {})["snapshot_policy"] = {"LogsSinceLast": policy}
                config.update(snapshot_before_restart=False, crash_restart=True)
                name = f"{mode}-crash-{policy}"
                path = args.output / f"{name}.json"
                path.write_text(json.dumps(config, indent=2) + "\n")
                subprocess.run(["mise", "exec", "--", "target/release/examples/consensus_bench", "--config", str(path), "--output", str(args.output / name)], check=True)
    summary = {}
    for policy in (5000, 200):
        arms = {mode: summarize(args.output / f"{mode}-crash-{policy}") for mode in ("checkpoint", "snapshot")}
        arms["throughput_ratio"] = arms["snapshot"]["median_trial_writes_per_second"] / arms["checkpoint"]["median_trial_writes_per_second"]
        summary[str(policy)] = arms
    (args.output / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
