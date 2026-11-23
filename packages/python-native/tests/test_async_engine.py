import asyncio
import json
import subprocess
import sys
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from orch8_engine import AsyncEngine, Engine, _wrap


class AsyncEngineTest(unittest.IsolatedAsyncioTestCase):
    async def test_transactional_batch_recovers_the_same_ids_after_reopen(self):
        with TemporaryDirectory() as directory:
            path = str(Path(directory) / "batch.db")
            sequence = {"name": "batch", "blocks": [
                {"type": "step", "id": "one", "handler": "noop"}
            ]}
            items = [
                {"name": "batch", "input": {"value": 1}, "idempotency_key": "one"},
                {"name": "batch", "input": {"value": 2}, "idempotency_key": "two"},
                {"name": "batch", "input": {"value": 99}, "idempotency_key": "one"},
            ]
            async with await AsyncEngine.open(path) as engine:
                await engine.deploy(sequence)
                with self.assertRaisesRegex(RuntimeError, "batch item 1"):
                    await engine.start_batch([
                        {"name": "batch", "idempotency_key": "rolled-back"},
                        {"name": "missing", "idempotency_key": "invalid-sequence"},
                    ])
                self.assertEqual([row async for row in engine.iter_instances()], [])
                ids = await engine.start_batch(items)
                self.assertEqual(ids[0], ids[2])
                self.assertNotEqual(ids[0], ids[1])
                self.assertEqual((await engine.get(ids[0]))["data"], {"value": 1})

            with Engine.open(path) as replacement:
                replacement.deploy(sequence)
                self.assertEqual(replacement.start_batch(items), ids)
                self.assertEqual(replacement.start("batch", idempotency_key="two"), ids[1])

    async def test_run_observer_timeout_leaves_the_admitted_attempt_running(self):
        with TemporaryDirectory() as directory:
            async with await AsyncEngine.open(str(Path(directory) / "observer.db")) as engine:
                entered = asyncio.Event()
                release = asyncio.Event()
                attempts = []

                @engine.handler("host")
                async def host(context):
                    attempts.append(context["attempt"])
                    entered.set()
                    await release.wait()
                    return {"value": 42}

                await engine.deploy({"name": "observed", "blocks": [
                    {"type": "step", "id": "one", "handler": "host"}
                ]})
                run_id = await engine.start("observed")
                observer = asyncio.create_task(engine.run(run_id, observer_timeout=0.5))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    with self.assertRaisesRegex(TimeoutError, "observer deadline"):
                        await asyncio.wait_for(observer, 2)
                finally:
                    release.set()
                    await asyncio.gather(observer, return_exceptions=True)
                result = await engine.run(run_id, timeout=5)
                self.assertEqual(result["outputs"]["one"], {"value": 42})
                self.assertEqual(len(attempts), 1)

    async def test_instance_iteration_filters_and_paginates_after_reopen(self):
        with TemporaryDirectory() as directory:
            path = str(Path(directory) / "pages.db")
            blocks = [{"type": "step", "id": "one", "handler": "host"}]
            async with await AsyncEngine.open(path) as engine:
                engine.handler("host", lambda context: None)
                sequence_id = await engine.deploy({"name": "pages", "blocks": blocks})
                await engine.deploy({"name": "other", "blocks": blocks})
                completed = await engine.start("pages")
                await engine.run(completed, timeout=5)
                expected = [await engine.start("pages") for _ in range(5)]
                await engine.start("other")
                rows = [row async for row in engine.iter_instances(
                    state="scheduled", sequence_id=sequence_id, page_size=2
                )]
                self.assertEqual([row["id"] for row in rows], sorted(expected, reverse=True))

            with Engine.open(path) as reopened:
                rows = list(reopened.iter_instances(state="completed", sequence_id=sequence_id, page_size=1))
                self.assertEqual([row["id"] for row in rows], [completed])
                self.assertEqual(rows[0]["sequence_id"], sequence_id)

    async def test_lifespan_deadline_keeps_the_same_close_in_progress(self):
        started = threading.Event()
        release = threading.Event()
        completed = threading.Event()

        class SlowClose:
            def _close_draining(self):
                started.set()
                release.wait(5)
                completed.set()

        engine = AsyncEngine(SlowClose(), asyncio.get_running_loop(), 0.05)
        try:
            with self.assertRaises(asyncio.TimeoutError):
                async with engine:
                    pass
            self.assertTrue(await asyncio.to_thread(started.wait, 5))
            self.assertFalse(completed.is_set())
        finally:
            release.set()
            await asyncio.wait_for(engine.close(), 5)
        self.assertTrue(completed.is_set())

    async def test_lifespan_holds_database_until_physical_handler_returns(self):
        with TemporaryDirectory() as directory:
            path = str(Path(directory) / "app.db")
            engine = await AsyncEngine.open(path, shutdown_timeout=0.05)
            entered = asyncio.Event()
            release = asyncio.Event()

            @engine.handler("host")
            async def host(context):
                entered.set()
                await release.wait()
                return {"finished": True}

            await engine.deploy({"name": "drain-test", "blocks": [
                {"type": "step", "id": "one", "handler": "host"}
            ]})
            run_id = await engine.start("drain-test", {"persisted": 42})
            execution = asyncio.create_task(engine.run(run_id))

            def replacement():
                return subprocess.run(
                    [sys.executable, "-c", """
import json
import sys
from orch8_engine import Engine
with Engine.open(sys.argv[1]) as engine:
    print(json.dumps(engine.get(sys.argv[2])))
""", path, run_id],
                    capture_output=True, text=True, timeout=8,
                )

            try:
                await asyncio.wait_for(entered.wait(), 5)
                with self.assertRaises(asyncio.TimeoutError):
                    await engine.__aexit__(None, None, None)
                busy = await asyncio.to_thread(replacement)
                self.assertNotEqual(busy.returncode, 0)
                self.assertIn("native database is already owned", busy.stderr)
            finally:
                release.set()
                await asyncio.wait_for(engine.close(), 5)
                await asyncio.gather(execution, return_exceptions=True)

            reopened = await asyncio.to_thread(replacement)
            self.assertEqual(reopened.returncode, 0, reopened.stderr)
            self.assertEqual(json.loads(reopened.stdout)["data"]["persisted"], 42)

    async def test_callback_bridge_runs_awaitable_on_application_loop(self):
        loop = asyncio.get_running_loop()
        owned = loop.create_future()
        owned.set_result(42)

        async def handler(request):
            self.assertIs(asyncio.get_running_loop(), loop)
            return request["value"] + await owned

        wrapped = _wrap(handler, loop)
        result = await asyncio.wait_for(asyncio.to_thread(wrapped, '{"value":1}'), 5)
        self.assertEqual(json.loads(result), {"ok": 43})

    async def test_workflow_awaits_application_work_and_persists_result(self):
        with TemporaryDirectory() as directory:
            path = str(Path(directory) / "app.db")
            engine = await AsyncEngine.open(path)
            entered = asyncio.Event()
            release = asyncio.Event()
            owner = asyncio.get_running_loop()
            attempts = []

            @engine.handler("host")
            async def host(context):
                self.assertIs(asyncio.get_running_loop(), owner)
                attempts.append(context["attempt"])
                entered.set()
                await release.wait()
                return {"answer": context["data"]["value"] + 1}

            try:
                await engine.deploy({"name": "async-test", "blocks": [
                    {"type": "step", "id": "one", "handler": "host"}
                ]})
                run_id = await engine.start("async-test", {"value": 41})
                execution = asyncio.create_task(engine.run(run_id))
                try:
                    await asyncio.wait_for(entered.wait(), 5)
                finally:
                    release.set()
                    snapshot = await asyncio.wait_for(execution, 5)
                self.assertEqual(snapshot["state"], "completed")
                self.assertEqual(snapshot["outputs"]["one"], {"answer": 42})
            finally:
                await engine.close()

            async with await AsyncEngine.open(path) as reopened:
                self.assertEqual((await reopened.get(run_id))["outputs"]["one"], {"answer": 42})
                selected = await reopened.get_output(run_id, "one", attempts[0])
                self.assertIsNotNone(selected)
                self.assertEqual(selected["output"], {"answer": 42})

            # A fresh engine can claim the same database after lifespan close.
            async with await AsyncEngine.open(path) as replacement:
                self.assertEqual((await replacement.get(run_id))["state"], "completed")
