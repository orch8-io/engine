# Hybrid GPU executor: local LLM steps, remote control plane

This template runs LLM steps on **your own GPU box** while the workflow control
plane stays in Orch8 Cloud or on a self-hosted control node. Prompts,
completions and model weights stay on the GPU host. The control plane only
stores what the workflow persists (step params, and outputs you choose to
return).

```
          control plane (Orch8 Cloud or self-hosted `control` + `executor`)
                     ▲  poll / heartbeat / complete  (HTTPS, outbound only)
                     │
   GPU host ─────────┼──────────────────────────────────────────────
   │  gpu-worker (Python, stdlib)  ── advertises hardware=["gpu"], labels {gpu: "true"}
   │        │ OpenAI-compatible /v1/chat/completions
   │        ▼
   │  ollama  (or vllm)  ── model weights on a local volume, not published
   ─────────────────────────────────────────────────────────────────
```

| File | Purpose |
|---|---|
| `docker-compose.yml` | Ollama plus the GPU worker. The `local-control` profile adds Postgres, a `control` node and an `executor` node. `vllm` swaps in vLLM. `executor-join` is the contract-§5 join path. |
| `docker-compose.gpu.yml` | Override that hands NVIDIA GPUs to Ollama. Without it, Ollama runs on CPU, which is enough for a smoke test. |
| `worker/gpu_worker.py` | The `gpu_llm_generate` handler: an [external worker](../../docs/WORKERS.md) that calls the local model. Standard library only. |
| `sequence.json` | A workflow that places the LLM step on GPU runtimes with `params.$runtime.hardware`. **Works on current engines.** |
| `sequence.placement.json` | The same workflow using step-level `placement: {"labels": {"gpu": "true"}}` (placement contract §7). **Requires an engine with step placement policies.** Engines without it reject the unknown `placement` field. |

## How placement works today

The worker polls `POST /api/v1/workers/tasks/poll` with a `RuntimeCapabilities`
advertisement (`kind: server`, `hardware: ["gpu"]`, `runtime_id == worker_id`).
The step in `sequence.json` carries `"$runtime": {"hardware": ["gpu"]}`. The
engine strips that from the params the handler sees, and only lets runtimes
that advertise `gpu` claim the task. A CPU-only worker that polls the same
handler never receives it. See [Distributed runtimes](../../docs/DISTRIBUTED_RUNTIMES.md).

The worker also advertises `labels: {"gpu": "true"}`. Current engines ignore
that field. Engines that implement contract §7 label placement use it to match
`sequence.placement.json`. Use one sequence file or the other, depending on your
engine.

## Mode A: Orch8 Cloud (or a remote control plane) + this GPU host

1. In your control plane, create a worker API key for the tenant and upload
   `sequence.json`, either with `orch8 sequence create sequence.json` or with
   `POST /api/v1/sequences`, adding your `tenant_id`/`namespace`.
2. On the GPU host (NVIDIA driver + NVIDIA Container Toolkit installed):

   ```bash
   export ORCH8_URL=https://<your-control-plane>/api/v1
   export ORCH8_API_KEY=<worker key>  ORCH8_TENANT_ID=<tenant>
   export ORCH8_WORKER_ID=$(uuidgen)   # keep it stable across restarts
   docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d
   docker compose logs -f gpu-worker   # "llm endpoint ready", then polls
   ```

3. Start an instance with `{"ticket_text": "..."}` in `context.data`. The
   `summarize` step completes on the GPU host, and its output
   (`text`, `usage`, `latency_ms`, `model`, `worker_id`) feeds the next step.

The GPU host needs **outbound HTTPS only**, so nothing on it is published to the
internet.

## Mode B: everything local (for testing)

```bash
export ORCH8_API_KEY=$(openssl rand -hex 32) ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
docker compose --profile local-control up -d          # add -f docker-compose.gpu.yml on a GPU box
docker compose exec orch8-control orch8 --url http://127.0.0.1:8080 health

# create the sequence and an instance (root key + tenant header)
curl -s -X POST http://127.0.0.1:8080/api/v1/sequences \
  -H "x-api-key: $ORCH8_API_KEY" -H "x-tenant-id: demo" -H "content-type: application/json" \
  -d "$(jq --arg id "$(uuidgen | tr A-Z a-z)" \
        '. + {id:$id, tenant_id:"demo", namespace:"default", version:1, created_at:(now|todate)}' sequence.json)"
```

`orch8-control` serves the API and runs no scheduler. `orch8-executor` runs the
engine against the same Postgres database and dispatches `gpu_llm_generate` to
the worker queue ([Node roles](../../docs/NODE_ROLES.md)). The `split` roles
need PostgreSQL. On SQLite, use a single `all_in_one` node instead.

## vLLM instead of Ollama

```bash
LLM_BASE_URL=http://vllm:8000/v1 LLM_MODEL=Qwen/Qwen2.5-0.5B-Instruct \
  docker compose --profile vllm up -d vllm gpu-worker
```

Any OpenAI-compatible server works (llama.cpp server, TGI with its OpenAI
route, LM Studio). Point `LLM_BASE_URL` at it and set `LLM_API_KEY` if it needs
one.

## Joining the GPU host as an executor node (contract §5)

The `executor-join` profile runs
`orch8 executor join --label gpu=true --run` with `ORCH8_JOIN_TOKEN` (an
`o8x1.…` token issued by Orch8 Cloud). It writes an `orch8.toml` with
`[node] role = "executor"` plus the `managed_control_*` fields, then starts the
node. That puts the host in the Cloud fleet view (liveness and drain).
**Requires an engine release that ships `orch8 executor join`.** Engines
without it exit with an unknown-subcommand error. The GPU work itself still
flows through the worker above. The managed-control session is control-only
and never carries workflow payloads ([Node roles](../../docs/NODE_ROLES.md#managed-cloud-outbound-control)).

## Handler contract (`gpu_llm_generate`)

| Param | Default | Notes |
|---|---|---|
| `prompt` or `messages` | required | `messages` is an OpenAI chat array. `prompt` is truncated to 64,000 characters. |
| `system` | none | Prepended as a system message. |
| `model` | `LLM_MODEL` | Per-step override. |
| `max_tokens` | 512 | Capped at 4096. |
| `temperature` | 0.2 | |

Failure classification: missing or invalid params fail the step with
`retryable: false`. An unreachable or erroring model server reports
`retryable: true`, and the step's `retry` policy applies. The worker
heartbeats at the server-advertised interval while a generation runs, so long
generations keep their lease. If the worker dies mid-generation, the lease
expires and the task is retried with a **new** `effect_id`. LLM generation has
no external side effect, so a retry is safe.

## Sizing and operations

- `WORKER_SLOTS` (default 2) is the number of concurrent generations. Size it
  to GPU memory: Ollama queues requests beyond `OLLAMA_NUM_PARALLEL`, and vLLM
  batches them. Measure before raising it.
- Run one worker per GPU host, and add hosts to scale. Workers are stateless.
  Claims use `FOR UPDATE SKIP LOCKED`, so hosts never double-claim.
- `docker compose stop gpu-worker` drains in-flight generations. The grace
  period is 120 s. Anything still running after that is retried elsewhere once
  its lease expires.
- The worker container runs as `nobody` with a read-only root filesystem. The
  model server isn't published outside the compose network.
