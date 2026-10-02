#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Compare unbatched/batched commands with identical durable Raft workloads."""
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
        "median_commands_per_log_index": statistics.median(w["commands_per_log_index"] for w in writes),
        "recovered_events": [[c["events"] for c in r["recovered"]] for r in reports],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("docs/benchmarks/consensus.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-commands", type=int, default=8)
    parser.add_argument("--summarize", action="store_true")
    args = parser.parse_args()
    if args.batch_commands < 2:
        parser.error("--batch-commands must be at least two")
    os.chdir(Path(__file__).resolve().parents[1])
    if not args.summarize:
        args.output.mkdir(parents=True, exist_ok=False)
        subprocess.run(["mise", "exec", "--", "cargo", "build", "--release", "-p", "loomery-shell", "--example", "consensus_bench"], check=True)
        base = json.loads(args.config.read_text())
        # Alternate pair order to reduce a systematic first/second-arm bias.
        for mode, sizes in (("checkpoint", (1, args.batch_commands)), ("snapshot", (args.batch_commands, 1))):
            for size in sizes:
                config = json.loads(json.dumps(base))
                config.setdefault("group", {}).setdefault("storage", {})["state_persistence"] = mode
                config["group"]["proposals"] = {"max_batch_commands": size, "max_batch_bytes": 262144, "max_delay_ms": 1, "queue_capacity": 1024}
                config.update(snapshot_before_restart=False, crash_restart=True)
                name = f"{mode}-batch-{size}"
                path = args.output / f"{name}.json"
                path.write_text(json.dumps(config, indent=2) + "\n")
                subprocess.run(["mise", "exec", "--", "target/release/examples/consensus_bench", "--config", str(path), "--output", str(args.output / name)], check=True)
    summary = {}
    for mode in ("checkpoint", "snapshot"):
        unbatched = summarize(args.output / f"{mode}-batch-1")
        batched = summarize(args.output / f"{mode}-batch-{args.batch_commands}")
        summary[mode] = {"unbatched": unbatched, "batched": batched, "throughput_ratio": batched["median_trial_writes_per_second"] / unbatched["median_trial_writes_per_second"]}
    (args.output / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
