#!/usr/bin/env python3
"""Pure private activity example using only Python's standard library."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import time
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener
import uuid


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class LeaseLost(Exception):
    pass


def run(handler):
    base = os.environ["ORCH8_URL"].rstrip("/")
    if not base.endswith("/api/v1"):
        base += "/api/v1"
    secret_file = os.environ.get("ORCH8_API_KEY_FILE")
    secret = Path(secret_file).read_text(encoding="utf8").strip() if secret_file else os.environ.get("ORCH8_API_KEY", "")
    tenant = os.environ.get("ORCH8_TENANT_ID", "default")
    worker = os.environ.get("ORCH8_WORKER_ID") or str(uuid.uuid4())
    version = os.environ.get("ORCH8_WORKER_VERSION", "1.0.0")
    labels = {"site": "private", "accelerator": "cpu", "handler_version": version}
    if residency := os.environ.get("ORCH8_RESIDENCY"):
        labels["residency"] = residency

    opener = build_opener(NoRedirect())
    draining = False

    def drain(*_):
        nonlocal draining
        draining = True

    signal.signal(signal.SIGTERM, drain)
    signal.signal(signal.SIGINT, drain)

    def post(path, payload, *, lease=False):
        request = Request(
            base + path,
            data=json.dumps(payload).encode(),
            headers={"content-type": "application/json", "x-tenant-id": tenant},
            method="POST",
        )
        if secret:
            request.add_header("X-API-Key", secret)
        try:
            with opener.open(request, timeout=10) as response:
                body = response.read()
                return json.loads(body) if body else None
        except HTTPError as error:
            if lease and error.code == 409:
                raise LeaseLost from None
            raise RuntimeError(f"{path} failed with HTTP {error.code}") from None

    def process(task):
        path = f"/workers/tasks/{task['id']}"
        claim = {"worker_id": worker, "claim_epoch": task["claim_epoch"]}
        remaining = task.get("params", {}).get("chunks", 5)
        if isinstance(remaining, bool) or not isinstance(remaining, int) or not 0 < remaining <= 100:
            post(path + "/fail", {**claim, "message": "chunks must be an integer from 1 to 100", "retryable": False}, lease=True)
            return
        next_chunk = (task.get("resume_checkpoint") or {}).get("next_chunk", 0)
        checkpoint_seq = task.get("checkpoint_seq", 0)
        for chunk in range(next_chunk, remaining):
            time.sleep(0.05)
            response = post(
                path + "/heartbeat",
                {**claim, "checkpoint": {"next_chunk": chunk + 1}, "checkpoint_seq": checkpoint_seq},
                lease=True,
            )
            checkpoint_seq = response["checkpoint_seq"]
            if checkpoint_seq == 1 and os.environ.get("ORCH8_RELEASE_BEFORE_EFFECT") == "1":
                # The example has written only its own checkpoint. No provider call has started.
                post(path + "/release", {**claim, "started": False}, lease=True)
                print("Released checkpointed task", flush=True)
                return
        post(path + "/complete", {**claim, "output": {"chunks": remaining, "resumed_from": next_chunk}}, lease=True)
        print(f"Completed task {task['id']}", flush=True)

    print(f"Worker {worker} serving {handler}", flush=True)
    while not draining:
        now = datetime.now(timezone.utc)
        reply = post("/workers/tasks/poll", {
            "worker_id": worker,
            "handler_name": handler,
            "version": version,
            "limit": 1,
            "capabilities": {
                "runtime_id": worker,
                "kind": "server",
                "trust": "registered",
                "handlers": [handler],
                "labels": labels,
                "observed_at": now.isoformat(),
                "expires_at": (now + timedelta(minutes=1)).isoformat(),
            },
        })
        for task in reply["tasks"]:
            try:
                process(task)
            except LeaseLost:
                print(f"Lease changed for task {task['id']}", flush=True)
        if not draining and not reply["tasks"]:
            time.sleep(max(250, reply.get("poll_after_ms", 0)) / 1000)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--handler", default="private_compute", choices=["private_prepare", "private_compute"])
    run(parser.parse_args().handler)
