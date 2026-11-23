"""Current snapshot reads must not decode obsolete execution history."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid

from orch8_engine import Engine


def write_database(path, statement, parameters, *, many=False):
    # The extension and Python can link different SQLite copies. Opening the
    # same live database through both copies in one process can clear locks.
    # A separate process participates in SQLite's normal OS locking protocol.
    result = subprocess.run(
        [sys.executable, "-c", """
import json, sqlite3, sys
from contextlib import closing
statement, parameters, many = json.load(sys.stdin)
with closing(sqlite3.connect(sys.argv[1])) as database, database:
    cursor = (database.executemany if many else database.execute)(statement, parameters)
    print(cursor.rowcount)
""", path],
        input=json.dumps([statement, parameters, many]),
        capture_output=True, text=True, check=True, timeout=10,
    )
    return int(result.stdout)


class CurrentSnapshotTest(unittest.TestCase):
    def test_get_output_selects_one_attempt_after_reopening(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "engine.db")
            attempts = {}
            with Engine.open(path) as writer:
                @writer.handler("value")
                def value(ctx):
                    attempts[ctx["step_id"]] = ctx["attempt"]
                    return {"value": 42} if ctx["step_id"] == "wanted" else "x" * (256 * 1024)

                writer.deploy({"name": "selected-output", "blocks": [
                    {"type": "step", "id": "wanted", "handler": "value"},
                    {"type": "step", "id": "unrelated", "handler": "value"},
                ]})
                instance = writer.start("selected-output")
                self.assertEqual(writer.run(instance, timeout=5)["state"], "completed")

            self.assertGreater(write_database(
                path, "UPDATE block_outputs SET output = ? WHERE instance_id = ? AND block_id = ?",
                ("{damaged JSON", instance, "unrelated"),
            ), 0)
            with Engine.open(path) as reader:
                with self.assertRaisesRegex(RuntimeError, "serialization"):
                    reader.get(instance)
                attempt = attempts["wanted"]
                output = reader.get_output(instance, "wanted", attempt)
                self.assertIsNotNone(output)
                self.assertEqual(output["output"], {"value": 42})
                self.assertEqual(output["attempt"], attempt)
                self.assertEqual(output["instance_id"], instance)
                self.assertIsNone(reader.get_output(instance, "wanted", attempt + 1))

    def test_get_and_run_keep_current_retry_diagnostics_after_damaged_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "engine.db")
            engine = Engine.open(path)
            try:
                engine.deploy({"name": "snapshot", "blocks": [
                    {"type": "step", "id": "work", "handler": "noop"}]})
                instance = engine.start("snapshot", {"seed": 42})
                rows = [
                    (str(uuid.uuid4()), instance, "work", "{damaged obsolete JSON", None, 20, 0, "2000-01-01T00:00:00.000000Z"),
                    (str(uuid.uuid4()), instance, "work", json.dumps({"error": "retry diagnostic", "attempt": 2}), "__retry__", 50, 1, "2000-01-01T00:00:01.000000Z"),
                    (str(uuid.uuid4()), instance, "work", "{damaged progress JSON", "__in_progress__", 20, 2, "2000-01-01T00:00:02.000000Z"),
                    (str(uuid.uuid4()), instance, "_sla:trace", "{damaged internal JSON", None, 20, 0, "2000-01-01T00:00:03.000000Z"),
                ]
                self.assertEqual(write_database(
                    path,
                    "INSERT INTO block_outputs(id,instance_id,block_id,output,output_ref,output_size,attempt,created_at) VALUES (?,?,?,?,?,?,?,?)",
                    rows, many=True,
                ), len(rows))
                expected = {"work": {"error": "retry diagnostic", "attempt": 2}}
                for snapshot in [engine.get(instance), engine.run(instance, timeout=0)]:
                    self.assertEqual(snapshot["outputs"], expected)
                    self.assertEqual(snapshot["state"], "scheduled")
                    self.assertEqual(snapshot["data"], {"seed": 42})
                self.assertEqual(write_database(
                    path, "UPDATE block_outputs SET output = ? WHERE id = ?",
                    ("{damaged current diagnostic", rows[1][0]),
                ), 1)
                with self.assertRaisesRegex(RuntimeError, "serialization"):
                    engine.get(instance)
            finally:
                engine.close()
