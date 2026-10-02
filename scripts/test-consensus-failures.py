#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Inject failures into benchmark-owned tonic/RocksDB replica processes."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

REPO = Path(__file__).resolve().parents[1]
SCENARIOS = ("leader_crash", "follower_crash", "quorum_loss", "reply_loss")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def successful(samples):
    return {sample["sequence"]: sample for sample in samples if sample["error"] is None}


def verify_retry(original, retried, count):
    """An acknowledged command must retain its original Raft index on replay."""
    require(len(retried) == count, "incomplete retry response")
    recovered = successful(retried)
    require(set(recovered) == set(range(count)), "retry failed or omitted commands")
    for sequence, sample in successful(original).items():
        retry = recovered[sequence]
        require(retry["replayed"] and retry["log_index"] == sample["log_index"],
                f"acknowledged command {sequence} lost its dedup/index on recovery")


class Node:
    def __init__(self, binary, spec, root):
        self.binary, self.spec, self.root = binary, spec, root
        self.process = None
        self.log = None

    @property
    def alive(self):
        return self.process is not None and self.process.returncode is None

    async def start(self):
        self.log = (self.root / f"node-{self.spec['id']}.stderr.log").open("ab")
        self.process = await asyncio.create_subprocess_exec(
            str(self.binary), "--node", json.dumps(self.spec),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=self.log,
            limit=32 * 1024 * 1024)
        ready = (await self.receive())["Ready"]
        self.spec["listen"] = ready["address"].split("://", 1)[1]
        self.address = ready["address"]
        return ready

    async def send(self, request):
        self.process.stdin.write((json.dumps(request) + "\n").encode())
        await self.process.stdin.drain()

    async def receive(self):
        line = await asyncio.wait_for(self.process.stdout.readline(), 30)
        if not line:
            await self.process.wait()
            raise EOFError(f"node {self.spec['id']} exited: {self.process.returncode}")
        value = json.loads(line)
        require("Ok" in value, f"node {self.spec['id']}: {value}")
        return value["Ok"]

    async def request(self, value):
        await self.send(value)
        return await self.receive()

    async def retry(self, phase, count):
        value = await self.request({"RetryBatch": {"phase": phase, "count": count, "timeout_ms": 10000}})
        return value["Batch"]["samples"]

    async def kill(self):
        if self.alive:
            # Only the subprocess created by this runner; never accept an external PID.
            try:
                self.process.kill()
            except ProcessLookupError:
                pass  # The owned child may have exited between inspection and signal.
            await asyncio.wait_for(self.process.wait(), 5)
        if self.log:
            self.log.close()


async def elect(nodes):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        for node in nodes:
            if node.alive and (await node.request("Status"))["Status"]["is_leader"]:
                return node
        await asyncio.sleep(0.01)
    raise RuntimeError("leader election timed out")


async def recover_batch(nodes, phase, count, report, stage):
    """Treat transient routing/timeouts as unknown, preserving every attempt."""
    attempts = report.setdefault("recovery_attempts", {}).setdefault(f"{stage}:{phase}", [])
    acknowledged = {}
    for attempt in range(6):
        leader = await elect(nodes)
        samples = await leader.retry(phase, count)
        attempts.append({"node": leader.spec["id"], "samples": samples})
        current = successful(samples)
        for sequence, prior in acknowledged.items():
            if sequence in current:
                require(current[sequence]["replayed"] and current[sequence]["log_index"] == prior["log_index"],
                        "an acknowledged recovery attempt lost its original dedup/index")
        if len(current) == count:
            verify_retry(list(acknowledged.values()), samples, count)
            return samples
        acknowledged.update(current)
        await asyncio.sleep(0.05 * (2 ** attempt))
    raise RuntimeError(f"recovery retries exhausted for {stage}:{phase}; attempts retained")


async def audit(nodes, phases, index):
    events = None
    statuses = []
    for node in nodes:
        checked = (await node.request({"Audit": {"index": index, "phases": phases}}))["Audited"]
        if events is None:
            events = checked["events"]
        require(events == checked["events"], "replicas disagree on event order/content")
        statuses.append(checked["status"])
    return {"events": len(events), "statuses": statuses}


async def case(binary, root, mode, size, scenario, operations, concurrency):
    root.mkdir(parents=True)
    config = {
        "nodes": 3, "trials": 1, "warmup": 32, "operations": operations,
        "concurrency": concurrency, "operation_timeout_ms": 1000, "phase_timeout_ms": 30000,
        "group": {
            "raft": {"heartbeat_interval": 50, "election_timeout_min": 150,
                     "election_timeout_max": 300, "snapshot_policy": {"LogsSinceLast": 5000}},
            "storage": {"state_persistence": mode},
            "proposals": {"max_batch_commands": size, "max_delay_ms": 1}}}
    (root / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    nodes = [Node(binary, {"id": i, "path": str(root / str(i)), "listen": None, "config": config}, root)
             for i in range(1, 4)]
    report = {"mode": mode, "batch_size": size, "scenario": scenario, "passed": False,
              "retry_timeout_ms": 10000}
    journal = (root / "responses.jsonl").open("w")
    try:
        report["replicas"] = [await node.start() for node in nodes]
        (root / "config.json").write_text(json.dumps(report["replicas"][0]["config"], indent=2) + "\n")
        members = {str(n.spec["id"]): n.address for n in nodes}
        await nodes[0].request({"Initialize": {"members": members}})
        leader = await elect(nodes)
        warmup = (await leader.request({"Batch": {"phase": "warmup", "count": 32}}))["Batch"]["samples"]
        require(len(successful(warmup)) == 32, "warmup failed")
        report["warmup"] = warmup
        await audit(nodes, {"warmup": 32}, max(s["log_index"] for s in warmup))
        followers = [node for node in nodes if node is not leader]
        original, killed = [], []
        if scenario == "reply_loss":
            # Deterministic application-level lost response, without altering commit durability.
            discarded = (await leader.request({"Batch": {"phase": "fault", "count": operations}}))["Batch"]
            require(len(successful(discarded["samples"])) == operations, "response-loss setup failed")
            report["discarded_response"] = discarded
        else:
            await leader.send({"StreamBatch": {"phase": "fault", "count": operations}})
            while True:
                try:
                    reply = await leader.receive()
                except EOFError:
                    require(scenario == "leader_crash" and killed, "unexpected replica exit")
                    break
                if "Batch" in reply:
                    require(killed, "write phase finished before injection")
                    # Includes failures, even if the stream observer missed a response.
                    require(reply["Batch"]["samples"] == sorted(original, key=lambda s: s["sequence"]),
                            "stream and final samples disagree")
                    break
                sample = reply["Sample"]
                original.append(sample)
                journal.write(json.dumps(sample) + "\n")
                journal.flush()
                if not killed and sample["error"] is None:
                    targets = {"leader_crash": [leader], "follower_crash": followers[:1],
                               "quorum_loss": followers}[scenario]
                    for target in targets:
                        await target.kill()
                        killed.append(target.spec["id"])
                    report["injected_after_observed_successes"] = len(successful(original))
                    report["killed_nodes"] = killed.copy()
                    require(len(successful(original)) < operations, "no interrupted commands at injection")
            if scenario == "follower_crash":
                require(len(successful(original)) == operations, "one follower crash prevented majority writes")
            if scenario == "quorum_loss":
                # Earlier in-flight replies may cover already-committed entries. Test fresh
                # commands issued AFTER both followers have exited to prove no false quorum ACKs.
                isolated = (await leader.request({"Batch": {"phase": "isolated", "count": 8}}))["Batch"]
                report["without_quorum"] = isolated
                require(not successful(isolated["samples"]), "write acknowledged without a majority")
        report["observed_samples"] = original
        report["unknown_commands"] = operations - len(successful(original))
        os.fsync(journal.fileno())
        if scenario == "leader_crash":
            # Recover on the surviving majority before reviving the old leader.
            retry = await recover_batch(nodes, "fault", operations, report, "restored")
            report["recovered_with_old_leader_down"] = True
        for node in nodes:
            if not node.alive:
                await node.start()
        if scenario != "leader_crash":
            retry = await recover_batch(nodes, "fault", operations, report, "restored")
        report["retry"] = retry
        verify_retry(original, retry, operations)
        if scenario == "reply_loss":
            verify_retry(discarded["samples"], retry, operations)
            require(all(s["replayed"] for s in retry), "discarded successful replies were not deduplicated")
        report["unacknowledged_replays"] = sum(
            s["replayed"] for s in retry if s["sequence"] not in successful(original))
        phases = {"warmup": 32, "fault": operations}
        index = max(s["log_index"] for s in retry)
        if scenario == "quorum_loss":
            isolated = await recover_batch(nodes, "isolated", 8, report, "restored")
            verify_retry([], isolated, 8)
            report["isolated_retry"] = isolated
            phases["isolated"] = 8
            index = max(index, max(s["log_index"] for s in isolated))
        replayed_warmup = await recover_batch(nodes, "warmup", 32, report, "restored")
        verify_retry(warmup, replayed_warmup, 32)
        report["converged"] = await audit(nodes, phases, index)
        # SIGKILL the whole cluster with no final snapshot or graceful RocksDB close.
        for node in nodes:
            await node.kill()
        for node in nodes:
            await node.start()
        after_crash = await recover_batch(nodes, "fault", operations, report, "after-crash")
        report["recovered_retry"] = after_crash
        verify_retry(retry, after_crash, operations)
        recovered_warmup = await recover_batch(nodes, "warmup", 32, report, "after-crash")
        verify_retry(warmup, recovered_warmup, 32)
        report["recovered"] = await audit(nodes, phases, index)
        report["passed"] = True
        return report
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        journal.close()
        await asyncio.gather(*(node.kill() for node in nodes))
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")


async def run(args, binary):
    args.output.mkdir(parents=True, exist_ok=False)
    root = args.output.resolve()
    metadata = {"schema_version": 1, "git_commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "git_changes": subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True),
        "modes": args.modes, "batch_sizes": args.batch_sizes, "scenarios": args.scenarios,
        "operations": args.operations, "concurrency": args.concurrency, "trials": args.trials,
        "reports": []}
    shutil.copyfile(REPO / "Cargo.lock", root / "Cargo.lock")
    try:
        for repeat in range(1, args.trials + 1):
            for mode in args.modes:
                for size in dict.fromkeys(args.batch_sizes):
                    for scenario in args.scenarios:
                        name = f"{mode}-b{size}-{scenario}-{repeat:03}"
                        report = await case(binary, root / name, mode, size, scenario, args.operations, args.concurrency)
                        metadata["reports"].append({"name": name, "passed": report["passed"],
                                                    "events": report["recovered"]["events"]})
                        print(f"PASS {name}: {report['recovered']['events']} events on all 3 replicas", flush=True)
    finally:
        (root / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")


def positive(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(f"benchmark-results/failures-{time.time_ns()}"))
    parser.add_argument("--modes", nargs="+", choices=("checkpoint", "snapshot"), default=["checkpoint", "snapshot"])
    parser.add_argument("--batch-sizes", nargs="+", type=positive, default=[1, 32])
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS))
    parser.add_argument("--operations", type=positive, default=256)
    parser.add_argument("--concurrency", type=positive, default=128)
    parser.add_argument("--trials", type=positive, default=1)
    args = parser.parse_args()
    # Keep the entire experiment inside the production bounded dedup window.
    if args.operations < 2 or args.operations + 40 > 4096:
        parser.error("operations must be 2..4056 (within the 4096-command dedup window)")
    if max(args.batch_sizes) >= args.operations:
        parser.error("operations must exceed each batch size so injection interrupts a multi-entry workload")
    if args.concurrency < max(args.batch_sizes):
        parser.error("concurrency must cover every requested batch size")
    if args.output.exists():
        parser.error("output directory must be new")
    subprocess.run(["mise", "exec", "--", "cargo", "build", "--release", "-p", "loomery-shell",
                    "--example", "consensus_bench"], cwd=REPO, check=True)
    target = Path(os.environ.get("CARGO_TARGET_DIR", REPO / "target"))
    if not target.is_absolute():
        target = REPO / target
    asyncio.run(run(args, target / "release/examples/consensus_bench"))


if __name__ == "__main__":
    main()
