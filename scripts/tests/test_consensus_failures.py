# SPDX-License-Identifier: MPL-2.0
"""The fault runner must reject false acknowledgements and missing retries."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch, AsyncMock

spec = importlib.util.spec_from_file_location(
    "failures", Path(__file__).resolve().parents[1] / "test-consensus-failures.py")
failures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(failures)


def sample(sequence, index, replayed=False, error=None):
    return {"sequence": sequence, "log_index": index, "replayed": replayed, "error": error}


class RetryOracle(unittest.TestCase):
    def test_acknowledged_indices_survive_and_unknowns_may_append_or_replay(self):
        failures.verify_retry([sample(0, 10)], [sample(0, 10, True), sample(1, 11)], 2)
        failures.verify_retry([], [sample(0, 10, True), sample(1, 11)], 2)

    def test_lost_acknowledged_dedup_or_changed_index_is_rejected(self):
        for retry in [sample(0, 10), sample(0, 11, True)]:
            with self.assertRaises(RuntimeError):
                failures.verify_retry([sample(0, 10)], [retry], 1)

    def test_incomplete_failed_or_duplicate_retries_are_rejected(self):
        for retried in [[], [sample(0, None, error="timeout")], [sample(0, 1), sample(0, 1)]]:
            with self.assertRaises(RuntimeError):
                failures.verify_retry([], retried, 1 if len(retried) < 2 else 2)


class RecoveryOracle(unittest.IsolatedAsyncioTestCase):
    class Node:
        alive = True
        spec = {"id": 2}

        def __init__(self, attempts):
            self.attempts = iter(attempts)

        async def request(self, _):
            return {"Status": {"is_leader": True}}

        async def retry(self, _phase, _count):
            return next(self.attempts)

    async def test_partial_acknowledgements_and_failed_attempts_are_retained(self):
        first = [sample(0, 10), sample(1, None, error="timeout")]
        second = [sample(0, 10, True), sample(1, 11)]
        report = {}
        with patch.object(failures.asyncio, "sleep", new=AsyncMock()):
            result = await failures.recover_batch([self.Node([first, second])], "fault", 2, report, "restored")
        self.assertEqual(result, second)
        self.assertEqual(len(report["recovery_attempts"]["restored:fault"]), 2)
        self.assertEqual(report["recovery_attempts"]["restored:fault"][0]["samples"], first)

    async def test_partial_acknowledgement_lost_on_later_attempt_fails(self):
        first = [sample(0, 10), sample(1, None, error="timeout")]
        second = [sample(0, 12, True), sample(1, 11)]
        with patch.object(failures.asyncio, "sleep", new=AsyncMock()):
            with self.assertRaises(RuntimeError):
                await failures.recover_batch([self.Node([first, second])], "fault", 2, {}, "restored")


if __name__ == "__main__":
    unittest.main()
