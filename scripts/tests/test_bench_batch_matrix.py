# SPDX-License-Identifier: MPL-2.0
"""Protect comparison controls and reject incomplete benchmark evidence."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("batch_matrix", Path(__file__).resolve().parents[1] / "bench-batch-matrix.py")
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


class MatrixTests(unittest.TestCase):
    def test_only_the_selected_dimensions_change_and_each_round_covers_all_arms(self):
        base = json.loads((Path(__file__).resolve().parents[2] / "workpad/benchmarks/batch-matrix.json").read_text())
        original = copy.deepcopy(base)
        result = matrix.plan(base, [8, 16, 32, 64, 128], [128], ["checkpoint", "snapshot"], 42)
        self.assertEqual(base, original)
        self.assertEqual(result, matrix.plan(base, [8, 16, 32, 64, 128], [128], ["checkpoint", "snapshot"], 42))
        self.assertEqual(len(result["arms"]), 12)
        for repeat in range(1, 4):
            names = [run["arm"] for run in result["execution_order"] if run["repeat"] == repeat]
            self.assertCountEqual(names, [arm["name"] for arm in result["arms"]])
        for arm in result["arms"]:
            actual = copy.deepcopy(arm["config"])
            actual["trials"] = base["trials"]
            actual["group"]["storage"].pop("state_persistence")
            actual["group"]["proposals"]["max_batch_commands"] = 1
            self.assertEqual(actual, base)

    @staticmethod
    def reports(root):
        base = {"trials": 2, "warmup": 0, "operations": 4, "nodes": 1, "failover": False}
        manifest = matrix.plan(base, [8], [8], ["snapshot"], 42)
        for arm in manifest["arms"]:
            for repeat in (1, 2):
                batched = arm["batch_size"] > 1
                indices = [1, 1, 2, 2] if batched else [1, 2, 3, 4]
                summary = {"successes": 4, "failures": 0, "throughput_per_second": (600 if repeat == 1 else 900) if batched else (100 if repeat == 1 else 200), "p50_us": 10000, "p95_us": 20000, "p99_us": 30000, "commands_per_log_index": 2 if batched else 1}
                report = {"writes": {"summary": summary, "batch": {"samples": [{"log_index": index, "error": None} for index in indices]}}, "recovered": [{"events": 4}]}
                path = root / arm["name"] / f"repeat-{repeat:03}" / "trial-001"
                path.mkdir(parents=True)
                (path / "report.json").write_text(json.dumps(report))
        return manifest

    def test_observed_batch_sizes_are_measured_instead_of_assumed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = matrix.summarize(root, self.reports(root))["rows"]
            batched = next(row for row in rows if row["batch_size"] == 8)
            self.assertEqual(batched["median_commands_per_log_index"], 2)
            self.assertEqual(batched["batch_size_histogram"], {2: 4})
            self.assertEqual(batched["throughput_ratio"], 5)
            self.assertEqual(batched["median_p99_ms"], 30)

    def test_missing_trial_and_recovery_mismatch_do_not_produce_valid_comparisons(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self.reports(root)
            path = root / "snapshot-c8-b8/repeat-002/trial-001/report.json"
            saved = path.read_text()
            path.unlink()
            with self.assertRaises(FileNotFoundError):
                matrix.summarize(root, manifest)
            report = json.loads(saved)
            report["recovered"][0]["events"] = 3
            path.write_text(json.dumps(report))
            with self.assertRaisesRegex(RuntimeError, "Recovery mismatch"):
                matrix.summarize(root, manifest)


if __name__ == "__main__":
    unittest.main()
