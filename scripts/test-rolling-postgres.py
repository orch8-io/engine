#!/usr/bin/env python3
"""Qualify two Orch8 server binaries against a disposable PostgreSQL database."""

import argparse
import contextlib
import datetime
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.parse import parse_qsl, urlencode, urlsplit
import uuid


TENANT = "rolling-release"
HANDLER = "rolling_release_worker"
UNCERTAIN_HANDLER = "rolling_uncertain_worker"


def previous_database_url(database_url):
    """Start the previous pool in the same connection mode used before a rollout."""
    url = urlsplit(database_url)
    settings = [(key, value) for key, value in parse_qsl(url.query, keep_blank_values=True)
                if key != "statement-cache-capacity"]
    settings.append(("statement-cache-capacity", "0"))
    return url._replace(query=urlencode(settings)).geturl()


def request(port, path, body=None, *, expected=(200,)):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request(
            "GET" if body is None else "POST",
            path,
            body=None if body is None else json.dumps(body),
            headers={"Content-Type": "application/json", "X-Tenant-Id": TENANT},
        )
        response = connection.getresponse()
        payload = response.read().decode()
        if response.status not in expected:
            raise RuntimeError(f"{port}{path} returned HTTP {response.status}: {payload}")
        return json.loads(payload) if payload else None
    finally:
        connection.close()


def eventually(description, operation, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := operation():
            return result
        time.sleep(0.25)
    raise RuntimeError(f"timed out waiting for {description}")


def ports():
    with contextlib.ExitStack() as stack:
        listeners = [stack.enter_context(socket.socket()) for _ in range(2)]
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in listeners]


@contextlib.contextmanager
def server(binary, workdir, database_url, *, label, migrate):
    http_port, grpc_port = ports()
    config = workdir / f"{label}.toml"
    config.write_text(
        "[database]\n"
        'backend = "postgres"\n'
        "max_connections = 12\n"
        f"run_migrations = {str(migrate).lower()}\n"
    )
    environment = dict(
        ((name, value) for name, value in os.environ.items() if not name.startswith("ORCH8_")),
        ORCH8_DATABASE_URL=database_url,
        ORCH8_HTTP_ADDR=f"127.0.0.1:{http_port}",
        ORCH8_GRPC_ADDR=f"127.0.0.1:{grpc_port}",
        ORCH8_NODE_ROLE="all_in_one",
        ORCH8_WORKER_REAPER_TICK_SECS="1",
        ORCH8_WORKER_REAPER_STALE_SECS="8",
    )
    log = workdir / f"{label}.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            [str(binary), "--config", str(config), "--insecure"],
            cwd=workdir,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=environment,
        )
        try:
            def ready():
                if process.poll() is not None:
                    raise RuntimeError(f"{label} exited with status {process.returncode}")
                try:
                    request(http_port, "/health/ready")
                    return True
                except (ConnectionError, TimeoutError, RuntimeError):
                    return False

            eventually(f"{label} readiness", ready, timeout=90)
            yield http_port
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def create_sequence(port, handler):
    sequence_id = str(uuid.uuid4())
    request(
        port, "/api/v1/sequences",
        {
            "id": sequence_id, "tenant_id": TENANT, "namespace": "default",
            "name": f"rolling-{sequence_id}", "version": 1,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "blocks": [{"type": "step", "id": "work", "handler": handler}],
        },
        expected=(200, 201),
    )
    return sequence_id


def start_run(port, sequence_id, marker):
    created = request(
        port, "/api/v1/instances",
        {"sequence_id": sequence_id, "tenant_id": TENANT, "namespace": "default",
         "context": {"data": {"writer": marker}}},
        expected=(200, 201),
    )
    return created["id"]


def instance(port, run):
    result = request(port, f"/api/v1/instances/{run}")
    return result.get("instance", result)


def check_run(port, run, marker):
    if instance(port, run)["context"]["data"]["writer"] != marker:
        raise RuntimeError(f"instance {run} lost its original input")


def check_output(port, run, expected):
    outputs = request(port, f"/api/v1/instances/{run}/outputs")
    if [item["output"] for item in outputs if item["block_id"] == "work"] != [expected]:
        raise RuntimeError(f"instance {run} did not record exactly one expected worker output")


def poll_tasks(port, worker, handler):
    response = request(
        port, "/api/v1/workers/tasks/poll",
        {"handler_name": handler, "worker_id": worker, "limit": 1},
    )
    return response if isinstance(response, list) else response["tasks"]


def get_claim(port, worker, run, handler=HANDLER):
    def poll():
        tasks = poll_tasks(port, worker, handler)
        if not tasks:
            return None
        task, = tasks
        if task["instance_id"] != run:
            raise RuntimeError(f"{worker} claimed an unexpected run")
        return task
    return eventually(f"{worker} claim", poll)


def qualify(previous, candidate, workdir, database_url):
    # Keep the previous binary live while the candidate upgrades its schema.
    previous_url = previous_database_url(database_url)
    with contextlib.ExitStack() as previous_process:
        old = previous_process.enter_context(
            server(previous, workdir, previous_url, label="previous", migrate=True)
        )
        task_sequence = create_sequence(old, HANDLER)
        held_run = start_run(old, task_sequence, "previous-keeps-lease")
        held = get_claim(old, "previous-active-worker", held_run)
        uncertain = start_run(old, create_sequence(old, UNCERTAIN_HANDLER), "no-checkpoint")
        get_claim(old, "uncertain-worker", uncertain, UNCERTAIN_HANDLER)
        pending = start_run(old, task_sequence, "previous")
        first = get_claim(old, "previous-worker", pending)
        first_epoch = first.get("claim_epoch", 0)
        task_path = f"/api/v1/workers/tasks/{first['id']}"
        checkpoint = {"cursor": 3, "provider_receipt": "already-processed"}
        saved = request(old, f"{task_path}/heartbeat",
                        {"worker_id": "previous-worker", "claim_epoch": first_epoch,
                         "checkpoint_seq": 0, "checkpoint": checkpoint})
        if saved["checkpoint_seq"] != 1:
            raise RuntimeError("the previous worker's checkpoint was not saved")
        with server(candidate, workdir, database_url, label="candidate", migrate=True) as new:
            check_run(new, pending, "previous")
            overlap = start_run(new, create_sequence(new, "noop"), "candidate")
            prior_overlap = start_run(old, create_sequence(old, "noop"), "previous-during-overlap")
            check_run(old, overlap, "candidate")
            check_run(new, prior_overlap, "previous-during-overlap")
            eventually("candidate workflow completion", lambda: instance(old, overlap)["state"] == "completed")
            eventually("previous workflow completion", lambda: instance(new, prior_overlap)["state"] == "completed")

            # Exercise the old writer and scheduler after DDL. This worker
            # still owns its original claim and never redispatches its effect.
            check_run(new, held_run, "previous-keeps-lease")
            held_path = f"/api/v1/workers/tasks/{held['id']}/complete"
            request(old, held_path, {"worker_id": "wrong-worker", "output": {}}, expected=(404, 409))
            request(old, held_path, {"worker_id": "previous-active-worker", "output": {"held": True}})
            eventually("previous worker completion after migration", lambda: instance(new, held_run)["state"] == "completed")

            # Wait for the lease to expire naturally. Older servers may not
            # have minted a claim epoch that can authenticate a release.
            claim = get_claim(new, "previous-worker", pending)
            if claim["id"] != first["id"] or claim["claim_epoch"] <= first_epoch:
                raise RuntimeError("the candidate failed to acquire a fresh lease for the pending task")
            if claim["checkpoint_seq"] != 1 or claim["resume_checkpoint"] != checkpoint:
                raise RuntimeError("the candidate did not deliver the original checkpoint")
            if poll_tasks(new, "different-worker", UNCERTAIN_HANDLER) or poll_tasks(old, "different-worker", UNCERTAIN_HANDLER):
                raise RuntimeError("a coordinator redispatched work with an unresolved effect and no checkpoint")

            # Reuse the original worker ID to exercise the claim epoch fence.
            stale = {"worker_id": "previous-worker", "claim_epoch": first_epoch, "output": {"stale": True}}
            request(new, f"{task_path}/complete", stale, expected=(409,))
            request(old, f"{task_path}/complete", stale, expected=(404, 409))
            request(old, f"{task_path}/heartbeat",
                    {"worker_id": "previous-worker", "checkpoint_seq": 1, "checkpoint": {"invalid": True}},
                    expected=(404, 409))
            request(new, f"{task_path}/complete",
                    {"worker_id": "previous-worker", "claim_epoch": claim["claim_epoch"], "output": {"ok": True}},
                    expected=(200, 204))
            eventually("pending workflow completion", lambda: instance(new, pending)["state"] == "completed")
            check_output(new, pending, {"ok": True})
            check_output(old, pending, {"ok": True})
            if instance(old, pending)["state"] != "completed":
                raise RuntimeError("the old binary could not read a run completed by the candidate")

            previous_process.close()
            with server(previous, workdir, previous_url, label="restarted-previous", migrate=False) as old:
                check_run(old, pending, "previous")
                check_run(old, overlap, "candidate")
                check_output(old, pending, {"ok": True})
                if instance(old, pending)["state"] != "completed":
                    raise RuntimeError("the old binary could not restart against the upgraded schema")
    print("Rolling PostgreSQL upgrade verified checkpoint resumption, unresolved-effect fencing, stale leases, and cross-version reads.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    database_url = os.environ["ORCH8_ROLLING_DB_URL"]
    with tempfile.TemporaryDirectory(prefix="orch8-rolling-release-") as directory:
        workdir = Path(directory)
        try:
            qualify(args.previous.resolve(strict=True), args.candidate.resolve(strict=True), workdir, database_url)
        except Exception:
            for log in sorted(workdir.glob("*.log")):
                print(f"=== {log.name} ===", flush=True)
                print(log.read_text()[-20_000:], flush=True)
            raise


if __name__ == "__main__":
    main()
