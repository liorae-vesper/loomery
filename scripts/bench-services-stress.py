#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Run the services stress profiles (Keycloak + NATS JetStream) and collect results.

Starts the compose stack, builds `examples/services_stress` in release mode, runs
each profile, and writes the per-profile JSON next to a summary. Every profile
asserts its own invariants and fails the run when one is violated.
"""
import argparse
import json
import os
import subprocess
from pathlib import Path

# Each profile is (name, environment overrides). They mirror the knobs of
# `examples/services_stress.rs`; the integration suite runs the same invariants
# at a tenth of this size.
PROFILES = {
    "auth": {
        "LOOMERY_STRESS_SCENARIO": "auth",
        "LOOMERY_STRESS_WORKERS": "32",
        "LOOMERY_STRESS_REQUESTS": "8000",
        "LOOMERY_STRESS_GRANTS": "200",
    },
    "outbox": {
        "LOOMERY_STRESS_SCENARIO": "outbox",
        "LOOMERY_STRESS_GROUPS": "8",
        "LOOMERY_STRESS_COMMANDS": "1000",
        "LOOMERY_STRESS_WORKERS": "8",
    },
    "e2e": {
        "LOOMERY_STRESS_SCENARIO": "e2e",
        "LOOMERY_STRESS_GROUPS": "4",
        "LOOMERY_STRESS_COMMANDS": "500",
        "LOOMERY_STRESS_WORKERS": "16",
    },
}

# A deliberately small profile for smoke runs (`--quick`).
QUICK = {
    "auth": {
        "LOOMERY_STRESS_SCENARIO": "auth",
        "LOOMERY_STRESS_WORKERS": "8",
        "LOOMERY_STRESS_REQUESTS": "800",
        "LOOMERY_STRESS_GRANTS": "40",
    },
    "outbox": {
        "LOOMERY_STRESS_SCENARIO": "outbox",
        "LOOMERY_STRESS_GROUPS": "2",
        "LOOMERY_STRESS_COMMANDS": "100",
        "LOOMERY_STRESS_WORKERS": "4",
    },
    "e2e": {
        "LOOMERY_STRESS_SCENARIO": "e2e",
        "LOOMERY_STRESS_GROUPS": "1",
        "LOOMERY_STRESS_COMMANDS": "50",
        "LOOMERY_STRESS_WORKERS": "4",
    },
}


def service_environment():
    """The service URLs the harness expects, with compose defaults."""
    return {
        "LOOMERY_TEST_NATS_URL": os.environ.get(
            "LOOMERY_TEST_NATS_URL", "nats://127.0.0.1:4222"
        ),
        "LOOMERY_TEST_KEYCLOAK_URL": os.environ.get(
            "LOOMERY_TEST_KEYCLOAK_URL", "http://127.0.0.1:8080"
        ),
        "LOOMERY_TEST_KEYCLOAK_REALM": os.environ.get(
            "LOOMERY_TEST_KEYCLOAK_REALM", "loomery"
        ),
        "LOOMERY_TEST_KEYCLOAK_CLIENT": os.environ.get(
            "LOOMERY_TEST_KEYCLOAK_CLIENT", "loomery-gateway"
        ),
    }


def report_line(stdout):
    """The last `JSON {...}` line, which is the machine-readable result."""
    for line in reversed(stdout.splitlines()):
        if line.startswith("JSON "):
            return json.loads(line[len("JSON ") :])
    raise RuntimeError(f"the harness printed no JSON result:\n{stdout}")


def summarize(report):
    """A one-line human summary of a profile's report."""
    scenario = report["scenario"]
    if scenario == "auth":
        stats = report["authenticate_stats"]
        grants = report["grant_stats"]
        return (
            f"auth {stats['ops']} calls in {stats['seconds']:.2f}s = "
            f"{stats['per_second']:.0f}/s (p50 {stats['p50_us']}us, p99 {stats['p99_us']}us); "
            f"{grants['ops']} grants at {grants['per_second']:.0f}/s; "
            f"{report['mismatches']} mismatches"
        )
    if scenario == "outbox":
        stats = report["apply_stats"]
        return (
            f"outbox {report['groups']} groups, {report['events']} events applied in "
            f"{stats['seconds']:.2f}s = {stats['per_second']:.0f}/s (p50 {stats['p50_us']}us); "
            f"published {report['published']} in {report['publish_seconds']:.2f}s; "
            f"stream delta first={report['stream_delta_first']} replay={report['stream_delta_replay']}"
        )
    stats = report["submit_stats"]
    return (
        f"e2e {report['groups']} groups, {report['events']} commands with a real auth each in "
        f"{stats['seconds']:.2f}s = {stats['per_second']:.0f}/s (p50 {stats['p50_us']}us, "
        f"p99 {stats['p99_us']}us); published {report['published']}; "
        f"stream delta first={report['stream_delta_first']} replay={report['stream_delta_replay']}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=sorted(PROFILES),
        help="Run only these scenarios (repeatable); default is all of them",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmark-results/services-stress"),
        help="Directory for the per-profile JSON and the summary",
    )
    parser.add_argument("--quick", action="store_true", help="Use the small smoke profiles")
    parser.add_argument(
        "--no-services",
        action="store_true",
        help="Assume the compose stack is already running",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)

    if not args.no_services:
        subprocess.run(
            ["docker", "compose", "-f", "compose.test.yaml", "up", "-d"], check=True
        )
        subprocess.run(["bash", "scripts/test-services/wait-for-services.sh"], check=True)

    subprocess.run(
        [
            "mise",
            "exec",
            "--",
            "cargo",
            "build",
            "--release",
            "-p",
            "loomery-shell",
            "--features",
            "test-services",
            "--example",
            "services_stress",
        ],
        check=True,
    )

    profiles = QUICK if args.quick else PROFILES
    selected = args.scenario or sorted(profiles)
    environment = {**os.environ, **service_environment()}
    summary = {}

    for name in selected:
        profile = profiles[name]
        print(f"--- {name}: {' '.join(f'{k}={v}' for k, v in sorted(profile.items()))}")
        completed = subprocess.run(
            ["target/release/examples/services_stress"],
            env={**environment, **profile},
            check=True,
            capture_output=True,
            text=True,
        )
        report = report_line(completed.stdout)
        summary[name] = report
        print(f"    {summarize(report)}")

    args.output.mkdir(parents=True, exist_ok=True)
    for name, report in summary.items():
        (args.output / f"{name}.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"wrote {args.output / 'summary.json'}")


if __name__ == "__main__":
    main()
