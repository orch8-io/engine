# `llm_call` response cache

Opt in per step to reuse a previous answer for the same request. A hit costs no
tokens, returns in milliseconds, and still succeeds when a tenant budget is
exhausted.

```json
{
  "type": "step", "id": "summarize", "handler": "llm_call",
  "params": {
    "provider": "openai", "model": "gpt-5.6-luna", "api_key_env": "OPENAI_API_KEY",
    "messages": [{ "role": "user", "content": "Summarize: {{ context.data.doc }}" }],
    "cache": { "mode": "exact", "ttl": "6h" }
  }
}
```

## Options

| Field | Default | Meaning |
|---|---|---|
| `mode` | `"exact"` | `exact` or `semantic` (see below). |
| `ttl` | `3600` | Seconds, or `"90s"`, `"15m"`, `"6h"`, `"7d"`. Max 30 days. |
| `max_entry_bytes` | 256 KiB | Responses larger than this are not stored. Hard ceiling 1 MiB. |
| `allow_tool_calls` | `false` | Also cache turns whose answer is a tool call. |
| `similarity_threshold` | `0.95` | Semantic mode: minimum cosine similarity (0.5–1.0). |
| `embedding` | `{}` | Semantic mode: `embed` handler config (`model`, `base_url`, `api_key` / `api_key_env`, `timeout_ms`). |

`cache: false` (or omitting it) disables the cache.

## Cache key

The key is a SHA-256 over:

- the tenant,
- the normalized request: provider (or the whole `providers` failover list),
  the effective model (after default-model resolution), `system` and
  `messages` (after prompt rendering), sampling params, `tools`, `tool_choice`,
  `response_format`, `response_schema`, `base_url`, and so on,
- the resolved [prompt registry](PROMPTS.md) version (name, version, content hash).

These fields are excluded because they do not change the answer: `api_key`,
`api_key_env`, `stream`, the timeouts, `max_image_bytes`, and the `cache`
block itself. Rotating a key does not invalidate the cache. Changing the model,
the prompt version, a tool definition or the schema does.

## What is never cached

- **Errors.** Only successful outputs reach the store, and responses without an
  assistant message are rejected.
- **Dry-runs.** The cache is neither read nor written.
- **Tool-call turns** (`message.tool_calls` present), unless
  `allow_tool_calls: true`. In an agent loop the next turn depends on tool
  results, so replaying a stale tool call is rarely what you want.
- **Requests with artifact-backed images.** The key would name the artifact,
  not its bytes. Inline base64 images are hashed and cacheable.
- **Outputs over `max_entry_bytes`.**

The output records the outcome:

```json
"cache": { "hit": false, "mode": "exact", "stored": true, "skip_reason": null }
"cache": { "hit": true, "mode": "exact", "similarity": null,
           "cached_at": "…", "expires_at": "…", "key": "3b1f0c9e2a7d4b61" }
```

`skip_reason` is one of `tool_call_turn`, `too_large`, `empty_response`,
`store_failed`, `uncacheable_request`.

A cache hit returns the stored output unchanged, including its original
`usage`, plus the `cache` block and the current `prompt` resolution. Cache
read/write failures degrade to a miss: the cache never blocks a call.

## Semantic mode

`mode: "semantic"` first tries the exact key. On a miss, it embeds the request
text (system + message text) through the same OpenAI-compatible `/embeddings`
client as the `embed` / `memory_search` handlers. It then compares that vector
with unexpired entries in the same **partition**: the same tenant, provider,
model, tools, schema and prompt version, differing only in message text. The
most similar entry at or above `similarity_threshold` is returned, and the
output shows its `similarity`. The scan is brute-force cosine over at most the
200 newest entries in the partition, like agent memory: no vector database.

Semantic hits trade exactness for savings. Use them for FAQ-style or
paraphrase-heavy traffic, not where a single word changes the answer. If the
embedding call fails, the lookup degrades to exact-only.

## Security and retention

- Entries are tenant-scoped. The tenant is part of the key and of every storage
  query.
- With field encryption enabled (`ORCH8_ENCRYPTION_KEY`), `response` and
  `embedding` are encrypted at rest with AES-256-GCM. The ciphertext is bound to
  its `(tenant, key)` row, the same class of protection as `context.data`.
- Expired rows are ignored on read and deleted by the GC loop.
- `DELETE /llm-cache` purges a tenant's cache (tenant header, or `?tenant=` with
  the root key).

## Savings reporting

Every hit records a `usage_events` row with `kind = "llm_cache_hit"`, carrying
the tokens the original call used. These rows are not spend. They do not count
toward instance token budgets or tenant budgets. `GET /usage` reports them
separately:

```json
"cache_savings": { "hits": 42, "input_tokens": 51000, "output_tokens": 9000,
                   "saved_usd": 0.3525, "saved_is_complete": true }
```

Each hit also emits a `tracing` event `orch8.llm_cache.hit` (target
`orch8::llm_cache`) with the mode, model, saved tokens and similarity.
