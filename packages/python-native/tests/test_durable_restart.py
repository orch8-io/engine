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
    def test_run_deadline_returns_before_its_physical_callback(self):
        code = """
import threading
from orch8_engine import Engine
entered = threading.Event()
release = threading.Event()
returned = threading.Event()
def host(ctx):
    entered.set()
    release.wait()
    return True
def watchdog():
    if entered.wait(5) and not returned.wait(3):
        release.set()
engine = Engine.open(':memory:')
engine.handler('held', host)
engine.deploy({'name': 'deadline', 'blocks': [
    {'type': 'step', 'id': 'work', 'handler': 'held', 'timeout': 10000}]})
instance = engine.start('deadline')
observer = threading.Thread(target=watchdog, daemon=True)
observer.start()
try:
    result = engine.run(instance, timeout=1)
    returned.set()
    finished_before_release = not release.is_set()
    assert entered.is_set(), 'fixture did not enter physical host work'
finally:
    release.set()
    observer.join(6)
try:
    assert finished_before_release, 'run deadline waited for physical callback completion'
    assert result['state'] != 'completed', result
    assert engine.run(instance, timeout=2)['state'] == 'completed'
    print('LOGICAL_RETURN_BEFORE_PHYSICAL_COMPLETION', flush=True)
finally:
    engine.close()
"""
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=15,
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("LOGICAL_RETURN_BEFORE_PHYSICAL_COMPLETION", result.stdout)

    def test_engine_collection_does_not_join_a_timed_out_host_callback(self):
        code = """
import gc
import threading
from orch8_engine import Engine
entered = threading.Event()
release = threading.Event()
returned = threading.Event()
def host(ctx):
    entered.set()
    release.wait()
    returned.set()
    return True
engine = Engine.open(':memory:')
engine.handler('host', host)
engine.deploy({'name': 'retire', 'blocks': [
    {'type': 'step', 'id': 'work', 'handler': 'host', 'timeout': 1500}]})
instance = engine.start('retire')
result = engine.run(instance, timeout=5)
assert entered.is_set(), 'fixture did not enter physical host work'
assert result['state'] == 'failed', result
engine.close()
del engine
gc.collect()
print('ENGINE_DROPPED', flush=True)
release.set()
assert returned.wait(5), 'physical callback failed to finish after release'
print('PHYSICAL_RETURNED', flush=True)
"""
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=15,
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["ENGINE_DROPPED", "PHYSICAL_RETURNED"])

    def test_run_preflight_rejects_invalid_timeouts_without_dispatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine = Engine.open(os.path.join(tmp, "app.db"))
            calls = []
            engine.handler("host", lambda ctx: calls.append(ctx["attempt"]) or True)
            engine.deploy({"name": "preflight", "blocks": [
                {"type": "step", "id": "work", "handler": "host"}]})
            instance = engine.start("preflight")
            try:
                for timeout in [-1, float("nan"), float("inf"), -float("inf")]:
                    with self.subTest(timeout=timeout):
                        with self.assertRaisesRegex(ValueError, "timeout"):
                            engine.run(instance, timeout=timeout)
                self.assertEqual(calls, [])
                self.assertEqual(engine.run(instance, timeout=0)["state"], "scheduled")
                self.assertEqual(calls, [])
                self.assertEqual(engine.run(instance, timeout=2)["state"], "completed")
                self.assertEqual(calls, [0])
            finally:
                engine.close()

    def test_handler_replacement_allows_reentrant_finalizer(self):
        code = """
from orch8_engine import Engine
engine = Engine.open(':memory:')
class Reentrant:
    def __call__(self, ctx):
        return {}
    def __del__(self):
        print('FINALIZER_ENTERED', flush=True)
        engine.handler('nested', lambda ctx: {})
        print('FINALIZER_RETURNED', flush=True)
callback = Reentrant()
engine.handler('host', callback)
del callback
engine.handler('host', lambda ctx: {})
print('REPLACEMENT_RETURNED', flush=True)
engine.close()
"""
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=10,
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), [
            "FINALIZER_ENTERED", "FINALIZER_RETURNED", "REPLACEMENT_RETURNED",
        ])

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
