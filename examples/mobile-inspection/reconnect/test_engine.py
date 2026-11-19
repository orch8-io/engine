"""Opt-in test against disposable production-mode engine and worker processes."""

from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from urllib.error import HTTPError
from urllib.request import urlopen

from submit import engine_client, provision_inspection
from test_submit import RECORD


HERE = Path(__file__).resolve().parent
WORKER = HERE.parents[1] / "private-workers" / "worker.py"


@contextmanager
def process(command, **kwargs):
    with tempfile.TemporaryFile() as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, **kwargs)
        child.test_log = log
        try:
            yield child
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()


def wait_until(read, expected, child):
    for _ in range(120):
        if child.poll() is not None:
            child.test_log.seek(0)
            log = child.test_log.read(8192).decode(errors="replace")
            raise AssertionError(f"child process exited with code {child.returncode}\n{log}")
        try:
            if expected(read()):
                return
        except HTTPError as error:
            error.close()
        except (OSError, ValueError, subprocess.CalledProcessError):
            pass
        time.sleep(0.1)
    raise AssertionError("expected engine state did not arrive")


@unittest.skipUnless(os.getenv("ORCH8_BIN") and os.getenv("ORCH8_SERVER_BIN"), "compiled engine and CLI required")
class EngineReconnectTest(unittest.TestCase):
    def test_reconnect_submits_once_and_advances_through_worker_and_approval(self):
        cli = str(Path(os.environ["ORCH8_BIN"]).resolve())
        server = str(Path(os.environ["ORCH8_SERVER_BIN"]).resolve())
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            home = Path(directory)

            def run(*args):
                result = subprocess.run([cli, *args], cwd=home, env=env, check=True,
                                        text=True, stdout=subprocess.PIPE, timeout=10)
                return result.stdout

            env = {key: value for key, value in os.environ.items() if not key.startswith("ORCH8_")}
            env["ORCH8_CONTEXTS_FILE"] = str(home / "contexts.json")
            run("init", ".")
            with socket.socket() as connection:
                connection.bind(("127.0.0.1", 0))
                port = connection.getsockname()[1]
            config = home / "orch8.toml"
            source = config.read_text(encoding="utf-8").replace("0.0.0.0:8080", f"127.0.0.1:{port}")
            config.write_text(source, encoding="utf-8")
            env.update(
                ORCH8_API_KEY=tomllib.loads(source)["api"]["api_key"],
                ORCH8_URL=f"http://127.0.0.1:{port}/api/v1",
                ORCH8_TENANT_ID="demo",
            )
            daemon = stack.enter_context(process([server, "--config", str(config)], cwd=home, env=env))
            request = engine_client(env["ORCH8_URL"], "demo", env["ORCH8_API_KEY"])
            def ready():
                with urlopen(f"http://127.0.0.1:{port}/health/ready", timeout=1) as response:
                    return response.status == 200

            wait_until(ready, bool, daemon)
            run("sequence", "apply", str(HERE / "sequence.json"))
            sequence = json.loads(run("--output", "json", "sequence", "lookup", "demo", "default",
                                      "review-field-inspection"))["id"]
            worker = stack.enter_context(process([sys.executable, str(WORKER), "--handler", "private_compute"], env=env))
            accepted = None

            def lose_accepted_response(path, body=None):
                nonlocal accepted
                accepted = request(path, body)
                raise OSError("Lost the connection after the server accepted the inspection")

            with self.assertRaises(OSError):
                provision_inspection(lose_accepted_response, sequence, "demo", RECORD)
            self.assertIsNot(accepted.get("deduplicated"), True)
            reopened = engine_client(env["ORCH8_URL"], "demo", env["ORCH8_API_KEY"])
            receipt = provision_inspection(reopened, sequence, "demo", RECORD)
            instance = receipt["server_execution_id"]
            self.assertEqual(instance, accepted["id"])
            self.assertEqual(receipt, provision_inspection(request, sequence, "demo", RECORD))
            status = lambda: request(f"/instances/{instance}")
            wait_until(status, lambda row: row["state"] == "waiting", worker)
            run("signal", instance, "custom:human_input:approve_inspection", "--payload", '{"value":"approved"}')
            wait_until(status, lambda row: row["state"] == "completed", daemon)
            self.assertEqual(status()["context"]["data"]["supervisor_decision"], "approved")
            self.assertEqual(receipt, provision_inspection(request, sequence, "demo", RECORD))
