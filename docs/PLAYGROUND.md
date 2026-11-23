# Browser playground (`orch8-wasm`)

> **Stability: experimental**, may change or be removed in any release.

`orch8-wasm` compiles a subset of Orch8 to `wasm32-unknown-unknown` so a
sequence can be validated, preflighted and dry-run entirely in the browser —
no server, no account, no network calls. The website's `/play` page loads it.

It is a **dry run**, not the engine. Use it to check a definition's shape and
control flow; use a real server (`orch8 preflight`, the local quick start, or
the embedded `orch8` crate / Node package) to see real execution.

## Why a subset

`orch8-engine` does not build for `wasm32-unknown-unknown`: it depends on the
tokio runtime, sqlx (PostgreSQL and SQLite drivers), reqwest, and wasmtime.
`packages/node-native` avoids that by running the real engine natively inside
Node. The browser has no such option, so `orch8-wasm` reuses only the parts
that are pure:

| Reused unchanged                                                                  | Source                                                                                            |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Strict decoder (`deserialize_sequence_strict`) and `SequenceDefinition::validate` | `orch8-types` (built with `default-features = false`, which drops the `sqlx` derives)             |
| Expression evaluator (router / loop / `when` / `assert` / `{{ }}` templates)      | `orch8-engine/src/expression.rs`, compiled in with `#[path]` so both builds share one source file |
| `ab_split` variant selection (SHA-256 of instance id + block id)                  | re-implemented byte-for-byte from `handlers/ab_split.rs`                                          |
| Preflight report types (`PreflightReport`, `Finding`)                             | `orch8-types`                                                                                     |

## JavaScript API

All functions take and return JSON strings and never throw; a failure is
`{"ok": false, "error": "..."}`.

| Export                                     | Returns                                                                                               |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `validate_sequence(json)`                  | `{ok, sequence, defaults_applied, warnings}` — the normalized definition as the server would store it |
| `preflight(json)`                          | a `PreflightReport` (same shape as `POST /sequences/preflight`)                                       |
| `run_dry(json, input_json, max_ticks)`     | a run result (below) with `input_json` as `context.data`                                              |
| `run_dry_with_options(json, options_json)` | same, with `{input, config, mocks, max_ticks, instance_id, start_at, timezone, seed, cancel_before, signals}` |
| `sequence_schema_version()`                | the sequence schema version this build accepts                                                        |

Server-assigned fields (`id`, `tenant_id`, `namespace`, `name`, `version`,
`created_at`) may be omitted; fixed placeholders are filled in and listed in
`defaults_applied`. Everything else goes through the server's strict decoder,
so unknown fields and invalid blocks are rejected exactly as the API would.

A run result identifies its `verification_scope` as `simulated_preview`.
Status describes the outcome of that simulation, including any mocked steps
or child sequences.

A run result contains `status` (`completed` | `failed` | `cancelled` | `waiting` | `tick_limit` | `operation_limit`),
`error`, `virtual_duration_ms`, `ticks`, `operations`, `timeline`, `outputs` (by block id),
`context`, `state` (instance KV), `would_call`, and `notes`. Each timeline
entry is `{step_id, path, kind, handler, started_at_virtual_ms,
finished_at_virtual_ms, status, attempt, output, error}`, where `status` is
`completed`, `mocked`, `failed`, `skipped`, `cancelled` (race losers) or
`decision` (router / loop / for_each / ab_split / race / try_catch / saga
bookkeeping). Repeated outputs, errors, and resolved call parameters share a
256 KiB detail budget. `trace_truncated` and per-entry `detail_omitted` flag
omitted details. The compact timeline and final outputs remain available.

`start_at` is an RFC 3339 instant and defaults to the Unix epoch. Expressions
see virtual time through `now()`, including time consumed by delays and mocked
work. Supply an unsigned integer `seed` to reproduce `random()` and `uuid()`
results and the branches that depend on them.

The `timezone` option defaults to UTC. `delay.fire_at_local` uses the step's
timezone override when supplied. `business_days_only` uses the instance
timezone and merges `delay.holidays` with `config.holidays`. Dates use
`YYYY-MM-DD`. Delay jitter draws from the seeded stream when a seed is set.

Set `cancel_before` to a block ID to cancel at its first visit. A protected
cancellation scope drains first. The sequence's `on_cancel` hooks run once
within the same execution budget.

Supply `signals` to simulate `wait_for_input`. Use fixtures such as
`{"at_ms":500,"signal_type":"custom:human_input:approve","payload":{"value":"yes"}}`.
Delivery times are measured from `start_at`. The preview retains early
answers until the matching gate opens and rejects answers after its deadline.
An unresolved gate without a deadline returns `waiting`. An empty list
enables waits with no supplied answers.

### Mocks

`mocks` maps a block id to a canned outcome, used instead of the handler:

```json
{
  "mocks": {
    "classify": { "output": { "label": "spam" }, "duration_ms": 1200 },
    "charge": { "error": "card declined" },
    "fetch": { "fail_attempts": 2, "output": { "ok": true } }
  }
}
```

A successful mock is checked against the producing step's `output_schema`.
An invalid output fails that step before another step can consume it. Set
`"output": null` to mock an explicit JSON null.

`fail_attempts` fails the first N attempts as retryable, so a step's `retry`
policy and backoff are exercised in virtual time.

## What the dry run models

- **Virtual time.** Step `delay.duration`, `sleep` `duration_ms`, retry
  backoff (`initial_backoff * multiplier^n`, capped at `max_backoff`), loop
  `poll_interval`, and mock `duration_ms` advance a millisecond counter.
  Nothing waits.
- **Local built-ins run for real:** `noop`, `log`, `sleep`, `fail`,
  `transform`, `assert`, `set_state`, `get_state`, `delete_state`,
  `merge_state` (state lives in an in-memory map).
- **Every other handler is recorded, not called:** `http_request`,
  `llm_call`, `tool_call`, `mcp_call`, `agent`, `embed`, memory, blob,
  `emit_event`, `send_signal`, `query_instance`, `human_review`,
  `wait_for_event`, `jev`, `self_modify`, custom worker handlers and
  `sub_sequence`. Each becomes a `would_call` entry with its resolved params
  and returns `{"dry_run": true, "handler": ...}` unless mocked.
- **Control flow:** `parallel` (branches start together; the block ends with
  the slowest), `race` (`first_to_resolve` / `first_to_succeed` by virtual
  finish time; losers marked `cancelled`), `loop` (condition, `break_on`,
  `max_iterations`, `continue_on_error`), `for_each` (binds `item_var` in
  `context.data`, then removes it), `router` (first truthy route, else
  `default`), `try_catch` (+ `finally_block`), `saga` (reverse-order
  compensation, then fail), `ab_split`, `cancellation_scope`, step `when`
  guards, and `on_failure` cleanup.
- **Tick budget.** Each step attempt costs one tick; `max_ticks` stops runaway
  loops (loops whose exit depends on real handler output never exit in a dry
  run) with `status: "tick_limit"`.
- **Operation budget.** Every block visit, loop/for-each iteration and step
  attempt also consumes an interpreter operation. A fixed 10,000-operation
  ceiling returns `status: "operation_limit"`, including for loops whose steps
  are all skipped or whose routes select no steps. This ceiling also applies
  when `max_ticks` is very large. Error handlers and compensation cannot catch
  either execution limit. Best-effort `on_failure` cleanup shares the ceiling
  and preserves the original failure status. `operations` reports the amount consumed. This is a
  dispatch limit; it does not bound the byte size of supplied input or outputs.

## Limits

- No durability, crash recovery, scheduler, leases, queues, workers, rate
  limits, circuit breakers, send windows, SLAs, deadlines, or live signals.
- Template pipe filters, fallback chains, and imported ASL templates use the
  engine's evaluator. An invalid filter argument fails the producing step.
- Offline preflight runs the same typed-dataflow compiler as the server.
- Interceptors, compensation receipts and lint rules are outside
  the preview. Output schemas are checked for supplied
  mocks.
- Step parameters and simulated handlers see context filtered through
  `context_access`. Workflow guards and routing still see the workflow
  context, as on the server.
- Preflight cannot see workers, credentials, plugins or providers: any
  non-local handler makes the `runtime_inventory` check `unknown` (never
  `pass`), so `overall` is `unknown` for most real workflows. That is correct —
  only a server can prove those.
- Step outputs are not merged into `context.data`; reference them as
  `outputs.<block_id>.*`, as on the server.

## Building

```bash
rustup target add wasm32-unknown-unknown
wasm_bindgen_version=$(awk '/^name = "wasm-bindgen"$/ { getline; gsub(/version = |"/, ""); print; exit }' Cargo.lock)
cargo install --locked wasm-bindgen-cli --version "$wasm_bindgen_version"
scripts/build-wasm-playground.sh            # writes ../web/public/wasm/orch8
scripts/build-wasm-playground.sh /tmp/orch8-playground  # or anywhere else
```

The script runs `wasm-opt -Oz` when binaryen is on `PATH`. Native tests:
`cargo test -p orch8-wasm`.

The CI differential test feeds seeded workflows and identical mocks to the
native contract runner and the WebAssembly preview. It compares terminal
state, executed leaf order, and the exact mocked outputs. A mismatch retains
the original and reduced workflow in the `preview-divergences` artifact.
