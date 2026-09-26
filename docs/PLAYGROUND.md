# Browser playground (`orch8-wasm`)

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

| Reused unchanged | Source |
| --- | --- |
| Strict decoder (`deserialize_sequence_strict`) and `SequenceDefinition::validate` | `orch8-types` (built with `default-features = false`, which drops the `sqlx` derives) |
| Expression evaluator (router / loop / `when` / `assert` / `{{ }}` templates) | `orch8-engine/src/expression.rs`, compiled in with `#[path]` so both builds share one source file |
| `ab_split` variant selection (SHA-256 of instance id + block id) | re-implemented byte-for-byte from `handlers/ab_split.rs` |
| Preflight report types (`PreflightReport`, `Finding`) | `orch8-types` |

## JavaScript API

All functions take and return JSON strings and never throw; a failure is
`{"ok": false, "error": "..."}`.

| Export | Returns |
| --- | --- |
| `validate_sequence(json)` | `{ok, sequence, defaults_applied, warnings}` — the normalized definition as the server would store it |
| `preflight(json)` | a `PreflightReport` (same shape as `POST /sequences/preflight`) |
| `run_dry(json, input_json, max_ticks)` | a run result (below) with `input_json` as `context.data` |
| `run_dry_with_options(json, options_json)` | same, with `{input, config, mocks, max_ticks, instance_id}` |
| `sequence_schema_version()` | the sequence schema version this build accepts |

Server-assigned fields (`id`, `tenant_id`, `namespace`, `name`, `version`,
`created_at`) may be omitted; fixed placeholders are filled in and listed in
`defaults_applied`. Everything else goes through the server's strict decoder,
so unknown fields and invalid blocks are rejected exactly as the API would.

A run result contains `status` (`completed` | `failed` | `tick_limit`),
`error`, `virtual_duration_ms`, `ticks`, `timeline`, `outputs` (by block id),
`context`, `state` (instance KV), `would_call`, and `notes`. Each timeline
entry is `{step_id, path, kind, handler, started_at_virtual_ms,
finished_at_virtual_ms, status, attempt, output, error}`, where `status` is
`completed`, `mocked`, `failed`, `skipped`, `cancelled` (race losers) or
`decision` (router / loop / for_each / ab_split / race / try_catch / saga
bookkeeping).

### Mocks

`mocks` maps a block id to a canned outcome, used instead of the handler:

```json
{
  "mocks": {
    "classify": { "output": { "label": "spam" }, "duration_ms": 1200 },
    "charge":   { "error": "card declined" },
    "fetch":    { "fail_attempts": 2, "output": { "ok": true } }
  }
}
```

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

## Limits

- No durability, crash recovery, scheduler, leases, queues, workers, rate
  limits, circuit breakers, send windows, SLAs, deadlines, signals, or cancel.
- `delay.fire_at_local` and `business_days_only` are calendar-dependent and
  ignored (only `duration` applies; a note is added).
- Template pipe filters (`{{ x | upper }}`) are engine-only
  (`template.rs` depends on engine error types) and are left unresolved with a
  note. Plain `{{ expression }}` templates resolve.
- Interceptors, `on_cancel`, compensation receipts, output-schema validation,
  context access filtering, typed-dataflow checks and lint rules are not run.
- `now()` in expressions reads the browser clock, not virtual time; `uuid()`
  and `random()` are real random values.
- Preflight cannot see workers, credentials, plugins or providers: any
  non-local handler makes the `runtime_inventory` check `unknown` (never
  `pass`), so `overall` is `unknown` for most real workflows. That is correct —
  only a server can prove those.
- Step outputs are not merged into `context.data`; reference them as
  `outputs.<block_id>.*`, as on the server.

## Building

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version <wasm-bindgen version in Cargo.lock>
scripts/build-wasm-playground.sh            # writes ../web/public/wasm/orch8
scripts/build-wasm-playground.sh <out-dir>  # or anywhere else
```

The script runs `wasm-opt -Oz` when binaryen is on `PATH`. Native tests:
`cargo test -p orch8-wasm`.
