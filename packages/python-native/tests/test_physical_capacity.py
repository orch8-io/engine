"""Physical blocking calls retain admission through runtime/engine replacement."""
import gc
import tempfile
import threading
import time
import unittest
from pathlib import Path

from orch8_engine import Engine


class PhysicalCapacityTest(unittest.TestCase):
    def settled_snapshot(self, engine, instance, expected):
        result = engine.get(instance)
        deadline = time.monotonic() + 5
        while result['state'] in ('running', 'scheduled') and time.monotonic() < deadline:
            time.sleep(0.01)
            result = engine.get(instance)
        self.assertIn(result['state'], expected)
        return result

    def deferred_snapshot(self, engine, instance):
        result = engine.run(instance, timeout=0.15)
        deadline = time.monotonic() + 5
        while result["state"] == "running" and time.monotonic() < deadline:
            time.sleep(0.01)
            result = engine.get(instance)
        self.assertEqual(result["state"], "scheduled")
        return result

    def test_timeouts_and_source_runtime_destruction_do_not_reset_capacity(self):
        release = []
        lock = threading.Lock()
        counts = {"active": 0, "peak": 0, "entered": 0}
        held_ids = set()

        def active_calls():
            with lock:
                return counts['active']

        def host(request):
            held = threading.Event() if request["params"].get("hold") else None
            with lock:
                counts["active"] += 1
                counts["entered"] += 1
                counts["peak"] = max(counts["peak"], counts["active"])
                if held is not None:
                    held_ids.add(request['instance_id'])
                    release.append(held)
            try:
                if held is not None:
                    held.wait()
                return True
            finally:
                with lock:
                    counts["active"] -= 1

        with tempfile.TemporaryDirectory(prefix="orch8-physical-") as directory:
            path = str(Path(directory, "app.db"))
            first, successor = Engine(path), None
            first.handler("host", host)
            try:
                first.deploy({"name": "held", "blocks": [{"type": "step", "id": "work", "handler": "host", "params": {"hold": True}, "timeout": 50}]})
                created = []
                fill_deadline = time.monotonic() + 30
                for _ in range(256):
                    if active_calls() >= 128 or time.monotonic() >= fill_deadline:
                        break
                    instance = first.start('held')
                    created.append(instance)
                    self.assertIn(first.run(instance, timeout=3)['state'], ('failed', 'running', 'scheduled'))
                    time.sleep(0)  # Let physically queued callbacks enter.
                self.assertEqual(counts["active"], 128)
                with lock:
                    physical = set(held_ids)
                self.assertEqual(len(physical), 128)
                for instance in physical:
                    self.settled_snapshot(first, instance, ('failed',))
                for instance in created:
                    if instance in physical or first.get(instance)['state'] == 'failed':
                        continue
                    first.signal(instance, 'cancel')
                    first.run(instance, timeout=3)
                    self.settled_snapshot(first, instance, ('failed', 'cancelled'))
                deferred = first.start("held")
                waiting = self.deferred_snapshot(first, deferred)
                self.assertEqual(waiting["outputs"], {})
                self.assertEqual(counts["entered"], 128)
                first.signal(deferred, "cancel")
                self.assertEqual(first.run(deferred, timeout=3)["state"], "cancelled")
                first.close()
                del first
                first = None
                gc.collect()  # Retire the original Python-owned Tokio runtime.
                successor = Engine(path)
                successor.handler("host", host)
                successor.deploy({"name": "fast", "blocks": [{"type": "step", "id": "work", "handler": "host", "params": {"hold": False}}]})
                fast = successor.start("fast")
                self.deferred_snapshot(successor, fast)
                self.assertEqual(counts["entered"], 128)
                release.pop(0).set()
                self.assertEqual(successor.run(fast, timeout=3)["state"], "completed")
                self.assertEqual(counts["active"], 127)
                self.assertEqual(counts["peak"], 128)
                self.assertEqual(counts["entered"], 129)
            finally:
                for settle in release:
                    settle.set()
                if first is not None:
                    first.close()
                if successor is not None:
                    successor.close()
                deadline = time.monotonic() + 5
                while counts["active"] and time.monotonic() < deadline:
                    time.sleep(0.01)
            self.assertEqual(counts["active"], 0)
