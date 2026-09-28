"""Durable engine tests. Run: python -m unittest discover -s tests (or pytest)."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from orch8_engine import Engine, PermanentError

WORKER = Path(__file__).with_name("flow_worker.py")


def run_worker(tmp: str, mode: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(WORKER), os.path.join(tmp, "app.db"), os.path.join(tmp, "effects.log"), mode],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
    )


def effects(tmp: str) -> list:
    return Path(tmp, "effects.log").read_text().split()


class DurableRestartTest(unittest.TestCase):
    def test_resumes_after_hard_exit_between_steps(self):
        with tempfile.TemporaryDirectory() as tmp:
            crashed = run_worker(tmp, "crash-between")
            self.assertEqual(crashed.returncode, 137, crashed.stderr)
            self.assertEqual(effects(tmp), ["reserve"])

            resumed = run_worker(tmp, "resume")
            self.assertEqual(resumed.returncode, 0, resumed.stderr)
            result = json.loads(resumed.stdout)
            self.assertEqual(result["state"], "completed")
            self.assertEqual(
                result["outputs"],
                {
                    "reserve": {"sku": "A-1", "reserved": True},
                    "charge": {"charged": 42, "sku": "A-1"},
                    "ship": {"shipped": "A-1"},
                },
            )
            # Step 1 did not re-run in the second process.
            self.assertEqual(effects(tmp), ["reserve", "charge", "ship"])

    def test_hard_exit_inside_step_is_not_silently_rerun(self):
        with tempfile.TemporaryDirectory() as tmp:
            crashed = run_worker(tmp, "crash-inside")
            self.assertEqual(crashed.returncode, 137, crashed.stderr)

            resumed = run_worker(tmp, "resume")
            self.assertEqual(resumed.returncode, 0, resumed.stderr)
            result = json.loads(resumed.stdout)
            self.assertEqual(result["state"], "failed")
            self.assertIn("automatic redispatch is blocked", result["outputs"]["charge"]["message"])
            self.assertEqual(effects(tmp), ["reserve", "charge"])

    def test_retry_permanent_and_idempotency(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine = Engine.open(os.path.join(tmp, "app.db"))
            calls = []

            @engine.handler("flaky")
            def flaky(ctx):
                calls.append(ctx["attempt"])
                if len(calls) == 1:
                    raise RuntimeError("temporary outage")
                return {"ok": True}

            @engine.handler("boom")
            async def boom(ctx):
                raise PermanentError("card declined")

            engine.deploy({"name": "flaky", "blocks": [{
                "type": "step", "id": "x", "handler": "flaky",
                "retry": {"max_attempts": 3, "initial_backoff": 10, "max_backoff": 10}}]})
            engine.deploy({"name": "fail", "blocks": [{"type": "step", "id": "x", "handler": "boom"}]})

            self.assertEqual(engine.run(engine.start("flaky"), timeout=10)["state"], "completed")
            self.assertEqual(len(calls), 2)

            failed = engine.run(engine.start("fail"), timeout=10)
            self.assertEqual(failed["state"], "failed")
            self.assertIn("card declined", failed["outputs"]["x"]["message"])

            a = engine.start("fail", idempotency_key="k")
            self.assertEqual(a, engine.start("fail", idempotency_key="k"))
            with self.assertRaisesRegex(RuntimeError, "before the first"):
                engine.handler("late", lambda ctx: 1)
            engine.close()


if __name__ == "__main__":
    unittest.main()
