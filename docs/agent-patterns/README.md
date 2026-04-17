# Agent Patterns

Reference sequence definitions for common AI agent architectures. Each JSON file is a complete sequence you can POST to `/sequences` (after filling in handler names registered for your workers).

These are **starting points**, not finished products — the `llm_call`, tool, and guardrail handlers referenced here are not built-in. Register them via `@orch8/worker-sdk` or a native Rust handler, then adapt params (model, provider, tool schemas) to your setup.

---

## Patterns

| File | Pattern | When to use |
|---|---|---|
| [`react-loop.json`](react-loop.json) | ReAct (Reason → Act → Observe) loop with tool calling | Research tasks, multi-step lookups, anything requiring tool invocation with reasoning between each call |
| [`tool-calling-pipeline.json`](tool-calling-pipeline.json) | Linear tool-call pipeline | Known-structure workflows: LLM picks from a fixed tool set, result flows to next step |
| [`multi-agent-delegation.json`](multi-agent-delegation.json) | Router → specialist sub-sequences | Coordinator delegates to domain experts; use when routing logic is cheap and specialists are expensive |
| [`guardrail-validation.json`](guardrail-validation.json) | Pre + post LLM call validation | Regulated content, safety-critical outputs, PII scrubbing, structured-output enforcement |

---

## Handlers referenced

The patterns assume handlers like:

| Handler | Purpose | Typical implementation |
|---|---|---|
| `llm_call` | Invoke an LLM provider with messages | External worker wrapping OpenAI / Anthropic / Bedrock |
| `tool_lookup` | Fetch tool schemas for the current agent | External worker or static JSON lookup |
| `tool_execute` | Dispatch to a named tool (search, fetch, compute) | External worker router |
| `input_guardrail` / `output_guardrail` | Validate content against safety / schema rules | External worker or rule engine |

None of these ship with the engine. Register them before deploying a pattern.

---

## Running a pattern

```bash
# 1. Register handlers via your worker
# 2. POST the sequence
curl -X POST http://localhost:8080/sequences \
  -H 'Content-Type: application/json' \
  -d @react-loop.json

# 3. Start an instance with a task
curl -X POST http://localhost:8080/instances \
  -H 'Content-Type: application/json' \
  -d '{
    "sequence_id": "<id from previous step>",
    "tenant_id": "demo",
    "namespace": "default",
    "context": { "data": { "task": "Find the CEO of Anthropic" } }
  }'
```

---

## Composing patterns

Patterns nest — the `multi-agent-delegation` pattern is built on top of `tool-calling-pipeline` and could itself be a branch inside `guardrail-validation`. Use `sub_sequence` blocks to compose without duplicating block definitions.

---

## See also

- [API reference — Block Definition Reference](../API.md#block-definition-reference)
- [External Workers](../WORKERS.md) — how to implement `llm_call`, `tool_execute`, etc.
