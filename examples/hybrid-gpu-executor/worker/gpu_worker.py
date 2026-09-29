#!/usr/bin/env python3
"""Orch8 GPU worker: runs the `gpu_llm_generate` handler against a local
OpenAI-compatible endpoint (Ollama or vLLM).

Protocol: docs/WORKERS.md (poll -> heartbeat* -> complete | fail) with a
RuntimeCapabilities advertisement (docs/DISTRIBUTED_RUNTIMES.md) so steps
placed with `$runtime.hardware = ["gpu"]` are only claimed by this worker.

Standard library only, so it runs on any GPU box with Python 3.10+.

Environment:
  ORCH8_URL            control plane base URL incl. /api/v1 (Orch8 Cloud or self-hosted)
  ORCH8_API_KEY        worker API key
  ORCH8_TENANT_ID      tenant id sent as x-tenant-id
  ORCH8_WORKER_ID      stable UUID for this worker (default: random per process)
  ORCH8_HANDLER        handler name (default gpu_llm_generate)
  ORCH8_HARDWARE       comma list advertised as capabilities.hardware (default gpu)
  ORCH8_WORKER_LABELS  k=v,k=v advertised as capabilities.labels (default gpu=true;
                       only meaningful on engines that implement label placement)
  ORCH8_REGION         optional region advertised as capabilities.regions
  LLM_BASE_URL         OpenAI-compatible base URL (default http://ollama:11434/v1)
  LLM_MODEL            default model (default llama3.2:1b); a step may override via params.model
  LLM_API_KEY          bearer for the LLM endpoint if it needs one (vLLM --api-key)
  WORKER_SLOTS         max concurrent generations (default 2; size to GPU memory)
"""

from __future__ import annotations

import datetime as dt
import json
import os
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

ORCH8_URL = os.environ.get("ORCH8_URL", "http://orch8-control:8080/api/v1").rstrip("/")
API_KEY = os.environ.get("ORCH8_API_KEY", "")
TENANT = os.environ.get("ORCH8_TENANT_ID", "default")
WORKER_ID = os.environ.get("ORCH8_WORKER_ID") or str(uuid.uuid4())
HANDLER = os.environ.get("ORCH8_HANDLER", "gpu_llm_generate")
HARDWARE = [h for h in os.environ.get("ORCH8_HARDWARE", "gpu").split(",") if h]
LABELS = dict(
    kv.split("=", 1) for kv in os.environ.get("ORCH8_WORKER_LABELS", "gpu=true").split(",") if "=" in kv
)
REGION = os.environ.get("ORCH8_REGION", "")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://ollama:11434/v1").rstrip("/")
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.2:1b")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
SLOTS = max(1, int(os.environ.get("WORKER_SLOTS", "2")))
MAX_PROMPT_CHARS = 64_000
MAX_TOKENS_CAP = 4096

stop = threading.Event()
in_flight = threading.Semaphore(SLOTS)


def log(msg: str, **fields) -> None:
    print(json.dumps({"msg": msg, "worker_id": WORKER_ID, **fields}), flush=True)


def http(method: str, url: str, body=None, headers=None, timeout=30.0):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("content-type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def orch8(path: str, body) -> dict | list:
    headers = {"x-tenant-id": TENANT}
    if API_KEY:
        headers["x-api-key"] = API_KEY
    return http("POST", f"{ORCH8_URL}{path}", body, headers)


def capabilities() -> dict:
    now = dt.datetime.now(dt.timezone.utc)
    caps = {
        "runtime_id": WORKER_ID,  # must equal worker_id
        "kind": "server",
        "trust": "registered",
        "handlers": [HANDLER],
        "hardware": HARDWARE,
        "observed_at": now.isoformat(),
        # Advertisements live at most five minutes; every poll refreshes it.
        "expires_at": (now + dt.timedelta(minutes=4)).isoformat(),
    }
    if REGION:
        caps["regions"] = [REGION]
    if LABELS:
        caps["labels"] = LABELS
    return caps


def generate(params: dict) -> dict:
    prompt = params.get("prompt")
    messages = params.get("messages")
    if messages is None:
        if not isinstance(prompt, str) or not prompt:
            raise ValueError("params.prompt (string) or params.messages is required")
        messages = [{"role": "user", "content": prompt[:MAX_PROMPT_CHARS]}]
    if params.get("system"):
        messages = [{"role": "system", "content": str(params["system"])}] + list(messages)
    body = {
        "model": params.get("model") or LLM_MODEL,
        "messages": messages,
        "max_tokens": min(int(params.get("max_tokens", 512)), MAX_TOKENS_CAP),
        "temperature": float(params.get("temperature", 0.2)),
        "stream": False,
    }
    headers = {"authorization": f"Bearer {LLM_API_KEY}"} if LLM_API_KEY else {}
    started = time.monotonic()
    resp = http("POST", f"{LLM_BASE_URL}/chat/completions", body, headers, timeout=600)
    choice = (resp.get("choices") or [{}])[0]
    return {
        "text": (choice.get("message") or {}).get("content", ""),
        "finish_reason": choice.get("finish_reason"),
        "model": resp.get("model", body["model"]),
        "usage": resp.get("usage", {}),
        "latency_ms": round((time.monotonic() - started) * 1000),
        "worker_id": WORKER_ID,
    }


def heartbeat_loop(task: dict, done: threading.Event, interval: float) -> None:
    while not done.wait(interval):
        try:
            orch8(f"/workers/tasks/{task['id']}/heartbeat",
                  {"worker_id": WORKER_ID, "claim_epoch": task["claim_epoch"]})
        except Exception as exc:  # noqa: BLE001 - keep generating; the lease decides
            log("heartbeat failed", task_id=task["id"], error=str(exc))


def run_task(task: dict, heartbeat_s: float) -> None:
    done = threading.Event()
    threading.Thread(target=heartbeat_loop, args=(task, done, heartbeat_s), daemon=True).start()
    try:
        output = generate(task.get("params") or {})
        orch8(f"/workers/tasks/{task['id']}/complete",
              {"worker_id": WORKER_ID, "claim_epoch": task["claim_epoch"], "output": output})
        log("completed", task_id=task["id"], latency_ms=output["latency_ms"])
    except ValueError as exc:  # bad params: retrying cannot help
        fail(task, str(exc), retryable=False)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:  # LLM endpoint down/busy
        fail(task, f"llm endpoint: {exc}", retryable=True)
    except Exception as exc:  # noqa: BLE001
        fail(task, str(exc), retryable=True)
    finally:
        done.set()
        in_flight.release()


def fail(task: dict, message: str, retryable: bool) -> None:
    log("failed", task_id=task["id"], error=message, retryable=retryable)
    try:
        orch8(f"/workers/tasks/{task['id']}/fail",
              {"worker_id": WORKER_ID, "claim_epoch": task["claim_epoch"],
               "message": message[:2000], "retryable": retryable})
    except Exception as exc:  # noqa: BLE001 - lease expiry will requeue it
        log("fail report failed", task_id=task["id"], error=str(exc))


def wait_for_llm() -> None:
    """Don't claim work until the model server answers; a claimed task would
    otherwise fail (retryably) while the model is still loading."""
    headers = {"authorization": f"Bearer {LLM_API_KEY}"} if LLM_API_KEY else {}
    while not stop.is_set():
        try:
            http("GET", f"{LLM_BASE_URL}/models", headers=headers, timeout=5)
            log("llm endpoint ready", llm=LLM_BASE_URL)
            return
        except Exception as exc:  # noqa: BLE001
            log("waiting for llm endpoint", llm=LLM_BASE_URL, error=str(exc))
            stop.wait(3)


def main() -> int:
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    log("starting", url=ORCH8_URL, handler=HANDLER, hardware=HARDWARE, labels=LABELS,
        llm=LLM_BASE_URL, model=LLM_MODEL, slots=SLOTS)
    wait_for_llm()
    pool = ThreadPoolExecutor(max_workers=SLOTS)
    while not stop.is_set():
        free = 0
        while in_flight.acquire(blocking=False):
            free += 1
        if free == 0:
            stop.wait(0.2)
            continue
        try:
            resp = orch8("/workers/tasks/poll", {
                "handler_name": HANDLER, "worker_id": WORKER_ID, "limit": free,
                "version": "hybrid-gpu-executor/1", "capabilities": capabilities(),
            })
        except Exception as exc:  # noqa: BLE001
            for _ in range(free):
                in_flight.release()
            log("poll failed", error=str(exc))
            stop.wait(2)
            continue
        # Current engines return an envelope; older ones a bare list.
        tasks = resp if isinstance(resp, list) else resp.get("tasks", [])
        poll_after = 1000 if isinstance(resp, list) else resp.get("poll_after_ms", 1000)
        heartbeat_s = 15 if isinstance(resp, list) else resp.get("heartbeat_interval_secs", 15)
        for _ in range(free - len(tasks)):
            in_flight.release()
        for task in tasks:
            pool.submit(run_task, task, heartbeat_s)
        if not tasks:
            stop.wait(min(poll_after, 2000) / 1000)
    log("draining")
    pool.shutdown(wait=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
