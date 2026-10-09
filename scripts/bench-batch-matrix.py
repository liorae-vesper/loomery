#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Sweep command batch limits at fixed concurrency with randomized trial order."""
import argparse
from collections import Counter
import copy
import csv
import json
import os
from pathlib import Path
import random
import statistics
import subprocess


def positive(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def nonnegative(value):
    value = int(value)
    if value < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return value


def plan(base, batch_sizes, concurrencies, modes, seed):
    """Preserve the workload across arms; each run is one independent trial."""
    trials = base.get("trials", 3)
    if not isinstance(trials, int) or trials < 1:
        raise ValueError("trials must be positive")
    if not concurrencies or any(value < 1 for value in [*batch_sizes, *concurrencies]):
        raise ValueError("batch sizes and concurrencies must be positive")
    sizes = sorted(set([1, *batch_sizes]))
    arms = []
    for mode in dict.fromkeys(modes):
        for concurrency in sorted(set(concurrencies)):
            for size in sizes:
                config = copy.deepcopy(base)
                config.update(trials=1, concurrency=concurrency, snapshot_before_restart=False, crash_restart=True)
                group = config.setdefault("group", {})
                group.setdefault("storage", {})["state_persistence"] = mode
                proposal = group.setdefault("proposals", {})
                proposal.setdefault("max_batch_bytes", 262144)
                proposal.setdefault("max_delay_ms", 1)
                proposal.setdefault("queue_capacity", 1024)
                proposal["max_batch_commands"] = size
                arms.append({"name": f"{mode}-c{concurrency}-b{size}", "mode": mode, "concurrency": concurrency, "batch_size": size, "config": config})
    rng = random.Random(seed)
    order = []
    for repeat in range(1, trials + 1):
        names = [arm["name"] for arm in arms]
        rng.shuffle(names)
        order.extend({"arm": name, "repeat": repeat} for name in names)
    return {"schema_version": 1, "seed": seed, "trials_per_arm": trials, "arms": arms, "execution_order": order}


def summarize(root, manifest):
    rows = []
    for arm in manifest["arms"]:
        reports = []
        histogram = Counter()
        for repeat in range(1, manifest["trials_per_arm"] + 1):
            path = root / arm["name"] / f"repeat-{repeat:03}" / "trial-001" / "report.json"
            report = json.loads(path.read_text())
            config = arm["config"]
            writes = report["writes"]["summary"]
            samples = report["writes"]["batch"]["samples"]
            operations = config.get("operations", 1000)
            if writes["failures"] or writes["successes"] != operations or len(samples) != operations:
                raise RuntimeError(f"Incomplete or failed measurement: {path}")
            indices = Counter(sample["log_index"] for sample in samples if sample["error"] is None)
            if None in indices or max(indices.values()) > arm["batch_size"]:
                raise RuntimeError(f"Invalid observed batch indices: {path}")
            histogram.update(Counter(indices.values()))
            expected = config.get("warmup", 100) + operations
            if config.get("failover", True) and config.get("nodes", 3) > 1:
                expected += 1
            if len(report["recovered"]) != config.get("nodes", 3) or any(check["events"] != expected for check in report["recovered"]):
                raise RuntimeError(f"Recovery mismatch: {path}")
            reports.append(report)
        writes = [r["writes"]["summary"] for r in reports]
        throughput = [w["throughput_per_second"] for w in writes]
        row = {"mode": arm["mode"], "concurrency": arm["concurrency"], "batch_size": arm["batch_size"], "trials": len(reports),
               "median_writes_per_second": statistics.median(throughput), "min_writes_per_second": min(throughput), "max_writes_per_second": max(throughput),
               "median_p50_ms": statistics.median(w["p50_us"] for w in writes) / 1000,
               "median_p95_ms": statistics.median(w["p95_us"] for w in writes) / 1000,
               "median_p99_ms": statistics.median(w["p99_us"] for w in writes) / 1000,
               "median_commands_per_log_index": statistics.median(w["commands_per_log_index"] for w in writes),
               "failures": sum(w["failures"] for w in writes), "recovery_verified": True,
               "recovered_events": [[check["events"] for check in r["recovered"]] for r in reports],
               "batch_size_histogram": dict(sorted(histogram.items())),
               "trial_writes_per_second": throughput}
        rows.append(row)
    baselines = {(row["mode"], row["concurrency"]): row["median_writes_per_second"] for row in rows if row["batch_size"] == 1}
    for row in rows:
        row["throughput_ratio"] = row["median_writes_per_second"] / baselines[row["mode"], row["concurrency"]]
    return {"schema_version": 1, "seed": manifest["seed"], "rows": rows}


def save_summary(root, result):
    (root / "matrix.json").write_text(json.dumps(result, indent=2) + "\n")
    fields = ["mode", "concurrency", "batch_size", "median_commands_per_log_index", "median_writes_per_second", "throughput_ratio", "median_p50_ms", "median_p95_ms", "median_p99_ms", "min_writes_per_second", "max_writes_per_second", "trials", "failures", "recovery_verified"]
    with (root / "matrix.csv").open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(result["rows"])
    lines = ["| Mode | Concurrency | Batch limit | Observed commands/index | Writes/s | Gain | p50 ms | p99 ms |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in result["rows"]:
        lines.append(f"| {row['mode']} | {row['concurrency']} | {row['batch_size']} | {row['median_commands_per_log_index']:.2f} | {row['median_writes_per_second']:.1f} | {row['throughput_ratio']:.2f}× | {row['median_p50_ms']:.2f} | {row['median_p99_ms']:.2f} |")
    markdown = "\n".join(lines) + "\n"
    (root / "matrix.md").write_text(markdown)
    print(markdown, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("workpad/benchmarks/batch-matrix.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-sizes", nargs="+", type=positive, default=[8, 16, 32, 64, 128])
    parser.add_argument("--concurrencies", nargs="+", type=positive)
    parser.add_argument("--modes", nargs="+", choices=["checkpoint", "snapshot"], default=["checkpoint", "snapshot"])
    parser.add_argument("--trials", type=positive)
    parser.add_argument("--operations", type=positive)
    parser.add_argument("--warmup", type=nonnegative)
    parser.add_argument("--max-delay-ms", type=nonnegative)
    parser.add_argument("--max-batch-bytes", type=positive)
    parser.add_argument("--queue-capacity", type=positive)
    parser.add_argument("--seed", type=int, default=20261001)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--summarize", action="store_true")
    args = parser.parse_args()
    os.chdir(Path(__file__).resolve().parents[1])
    if args.summarize:
        manifest = json.loads((args.output / "manifest.json").read_text())
        save_summary(args.output, summarize(args.output, manifest))
        return
    base = json.loads(args.config.read_text())
    for name in ("trials", "operations", "warmup"):
        value = getattr(args, name)
        if value is not None:
            base[name] = value
    proposal = base.setdefault("group", {}).setdefault("proposals", {})
    for name in ("max_delay_ms", "max_batch_bytes", "queue_capacity"):
        value = getattr(args, name)
        if value is not None:
            proposal[name] = value
    manifest = plan(base, args.batch_sizes, args.concurrencies or [base.get("concurrency", 128)], args.modes, args.seed)
    if args.dry_run:
        print(json.dumps(manifest, indent=2))
        return
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    subprocess.run(["mise", "exec", "--", "cargo", "build", "--release", "-p", "loomery-shell", "--example", "consensus_bench"], check=True)
    arms = {arm["name"]: arm for arm in manifest["arms"]}
    for position, run in enumerate(manifest["execution_order"], 1):
        arm = arms[run["arm"]]
        root = args.output / arm["name"]
        root.mkdir(exist_ok=True)
        config = root / f"repeat-{run['repeat']:03}.json"
        config.write_text(json.dumps(arm["config"], indent=2) + "\n")
        print(f"[{position}/{len(manifest['execution_order'])}] {arm['name']} repeat {run['repeat']}", flush=True)
        subprocess.run(["mise", "exec", "--", "target/release/examples/consensus_bench", "--config", str(config), "--output", str(root / f"repeat-{run['repeat']:03}")], check=True)
    save_summary(args.output, summarize(args.output, manifest))


if __name__ == "__main__":
    main()
