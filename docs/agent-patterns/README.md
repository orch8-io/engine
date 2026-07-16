# Agent Patterns

Reference sequence definitions for common AI agent architectures. Each JSON file is a complete sequence you can POST to `/sequences` (after setting up your tool dispatch endpoint and any external workers).

These are **starting points**, not finished products — adapt params (model, provider, tool schemas, endpoints) to your setup.

---

## Patterns

| File | Pattern | When to use |
|---|---|---|
| [`react-loop.json`](react-loop.json) | ReAct (Reason → Act → Observe) loop with tool calling | Research tasks, multi-step lookups, anything requiring tool invocation with reasoning between each call |
| [`tool-calling-pipeline.json`](tool-calling-pipeline.json) | Linear tool-call pipeline | Known-structure workflows: LLM picks from a fixed tool set, result flows to next step |
| [`multi-agent-delegation.json`](multi-agent-delegation.json) | Router → specialist sub-sequences | Coordinator delegates to domain experts; use when routing logic is cheap and specialists are expensive |
| [`guardrail-validation.json`](guardrail-validation.json) | Pre + post LLM call validation | Regulated content, safety-critical outputs, PII scrubbing, structured-output enforcement |

---

## Built-in handlers used

These patterns use the following **built-in** handlers (no external workers needed):

| Handler | Purpose | Used in |
|---|---|---|
| `llm_call` | Invoke an LLM provider (OpenAI, Anthropic, Gemini, Deepseek, Groq, Mistral, etc.) | All patterns |
| `tool_call` | HTTP POST to a tool endpoint with name + arguments | react-loop, tool-calling-pipeline |
| `human_review` | Human-in-the-loop approval gate with timeout/escalation | guardrail-validation |
| `noop` | Pass-through (captures output params without side effects) | react-loop, guardrail-validation |
| `set_state` | Write a value to session-scoped state | react-loop |
| `transform` | Transform context data using expressions | react-loop |

### `llm_call` params

| Field | Type | Default | Description |
|---|---|---|---|
| `provider` | string | `"openai"` | Provider: `openai`, `anthropic`, `gemini`, `deepseek`, `qwen`, `perplexity`, `groq`, `together`, `mistral`, `openrouter` |
| `providers` | array | — | Failover list of `{provider, api_key, model}` objects |
| `model` | string | per-provider | Model identifier |
| `messages` | array | `[]` | Chat messages (`{role, content}`) |
| `system` | string | — | System prompt |
| `temperature` | number | — | Sampling temperature |
| `max_tokens` | number | `4096` | Max output tokens |
| `tools` | array | — | Tool/function definitions |
| `tool_choice` | string/object | — | Tool selection strategy |
| `api_key` | string | — | API key (direct) |
| `api_key_env` | string | — | Env var name containing API key |
| `base_url` | string | per-provider | Override API base URL |

### `tool_call` params

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | **required** | Tool endpoint URL |
| `tool_name` | string | `"unknown"` | Name of the tool being invoked |
| `arguments` | object | `{}` | Arguments passed to the tool |
| `method` | string | `"POST"` | HTTP method |
| `headers` | object | `{}` | Extra HTTP headers |
| `timeout_ms` | u64 | `30000` | Request timeout in ms |

---

## Block types used

| Block | Where | Purpose |
|---|---|---|
| `loop` | react-loop | Iterates the observe→decide→act cycle until `agent_done` or max iterations |
| `router` | react-loop, guardrail-validation | Branches on LLM output (finish vs continue, safe vs unsafe, severity levels) |
| `for_each` | tool-calling-pipeline | Iterates over LLM-generated plan steps |
| `parallel` | multi-agent-delegation | Runs specialist sub-sequences concurrently |
| `sub_sequence` | multi-agent-delegation | Invokes specialist agent sequences |

---

## Running a pattern

```bash
# 1. Set provider and engine credentials
export OPENAI_API_KEY=sk-...
export ORCH8_API_KEY=replace-me
export ORCH8_URL=http://localhost:8080/api/v1

# 2. POST the sequence
curl -X POST "$ORCH8_URL/sequences" \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H 'x-tenant-id: demo' \
  -H 'Content-Type: application/json' \
  -d @react-loop.json

# 3. Start an instance with a task
curl -X POST "$ORCH8_URL/instances" \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H 'x-tenant-id: demo' \
  -H 'Content-Type: application/json' \
  -d '{
    "sequence_id": "<id from previous step>",
    "tenant_id": "demo",
    "namespace": "default",
    "context": {
      "data": {
        "task": "Find the CEO of Anthropic",
        "tool_dispatch_url": "http://localhost:3001/tools",
        "available_tools": ["web_search", "calculator"],
        "tool_schemas": []
      }
    }
  }'
```

---

## Composing patterns

Patterns nest — the `multi-agent-delegation` pattern is built on top of `tool-calling-pipeline` and could itself be a branch inside `guardrail-validation`. Use `sub_sequence` blocks to compose without duplicating block definitions.

---

## Key conventions

- **Template expressions**: `{{context.data.*}}` for instance context, `{{outputs.<block_id>.*}}` for prior step outputs
- **Router conditions**: plain string expressions evaluated against context and outputs (e.g. `"outputs.observe.content.action == finish"`)
- **Retry policy**: durations in milliseconds (`initial_backoff`, `max_backoff`)
- **Parallel branches**: array of arrays — each inner array is a branch of blocks
- **ForEach**: `collection` is a template path, `body` contains the blocks to iterate

---

## See also

- [API reference — Block Definition Reference](../API.md#block-definition-reference)
- [Architecture — Built-in Step Handlers](../ARCHITECTURE.md#built-in-step-handlers)
- [External Workers](../WORKERS.md) — how to implement custom handlers
- [Configuration](../CONFIGURATION.md) — `ORCH8_ENCRYPTION_KEY` for API key encryption at rest
