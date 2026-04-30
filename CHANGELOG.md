# Changelog

All notable changes to the Orch8 Engine will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] — 2026-04-30

### Added
- **Template pipe filters:** `upper`, `lower`, `trim`, `abs`, `url_encode`, `base64`, `base64_decode`, `default()`, `truncate()`, `join()`, `split()`, `hash()`, `round()`, `replace()`.
- **Expression functions:** `now()`, `uuid()`, `random()`, `format_date()`, `day_of_week()`, `keys()`, `values()`, `contains()`, `starts_with()`, `ends_with()`, `sum()`, `avg()`, `min()`, `max()`, `first()`, `last()`, `slice()`, `sort()`, `unique()`, `count()`, `change_pct()`, `clamp()`.
- **Multi-argument function parser** in expression evaluator — functions accept comma-separated args.
- **Root variable shortcuts** in templates and expressions: `config.*`, `data.*`, `runtime.*` as shorthands for `context.config.*`, `context.data.*`, `context.runtime.*`.
- **Enhanced loop block:** `break_on` (expression-based early exit), `continue_on_error` (skip failed iterations), `poll_interval` (defer re-execution between iterations).
- **Static template validation** on sequence create — warns about unknown template roots, unknown pipe filters, unclosed expressions.
- **Instance KV state store:** `set_state`, `get_state`, `delete_state` built-in handlers with per-instance key-value persistence (`instance_kv_state` table).
- **`transform` built-in handler:** passes template-resolved params through as output — useful for reshaping data between steps without a custom handler.
- **`assert` built-in handler:** evaluates a condition expression and fails the step with a configurable message if falsy — enables guard clauses and invariant checks in sequences.
- **`merge_state` built-in handler:** batch-writes multiple key-value pairs to instance KV state in one step.
- **`state.*` template root:** access instance KV state in templates (e.g. `{{ state.color }}`), pre-fetched at param resolution time.
- **Step output caching:** `cache_key` field on `StepDef` — when set, step output is cached in instance KV state under `_cache:{key}` and served from cache on subsequent executions.
- Postgres migration `033_instance_kv_state.sql`.

### Fixed
- `for_each` collection resolution from template paths (e.g. `{{ context.data.items }}`).
- `emit_event` dedupe FK insert order — child instance created before dedupe row to satisfy foreign key constraint.

## [0.2.0] — 2026-04-29

### Added
- Centralized `orch8_types::auth` module for constant-time secret verification (`verify_secret_constant_time`, `precompute_secret_digest`, `verify_secret_against_digest`).
- Schema-per-instance support via `search_path` config and `DATABASE_SEARCH_PATH` env var.
- Partial index on `task_instances(updated_at) WHERE state = 'waiting'` for scheduler scan performance.
- E2E coverage for interceptors, templates, self-modify, signals, and circuit breaker.
- Expanded unit test coverage across api, engine, storage, and types.
- Externalized state TTL garbage collector with configurable sweep interval.
- `externalized_state` foreign-key cascade and shutdown-drain semantics.
- Per-instance required-field context preload (`RequiredFieldTree`) to avoid fetching externalized payloads unused by the current step.
- Transactional `create_instance(s)_externalized` and `update_instance_context_externalized` paths.
- Batch preload of externalized markers in the scheduler claim cycle.
- zstd compression for externalized payloads >= 1 KiB.
- `ExternalizationMode` config enum (`never`, `threshold { bytes }`, `always_outputs`) with default `threshold { bytes: 65536 }`.
- ActivePieces sidecar integration via the `ap://` handler prefix.
- Swiss operator-console dashboard visual pass (proportional bars, live timestamps, click-to-copy IDs).
- Criterion benchmarks and preload metrics.

### Fixed
- Architecture audit: 10 fixes with test coverage.
- Atomic worker completion with CAS guard, chunked batch lookup, and pause safety.
- Fallback to non-atomic transition when node not found in worker completion.
- Sequential cursor semantics, race subtree cancellation, and pause guard.
- Revert concurrent post-tick processing to sequential for correctness.
- Resolve externalization markers before step dispatch.
- Resolve externalization markers in `GET /instances/:id/outputs` responses.
- Enforce size ceiling and apply field filter policy on all dispatch paths.
- CI: pass `--insecure` to the server in the E2E harness.

### Changed
- Evaluator instance-caching optimization: only refetch from storage when a prior dispatch may have mutated context (Step, ForEach, TryCatch, or signal processing).
- Auth: deduplicated SHA-256 + constant-time comparison into shared `orch8_types::auth` module, removed per-crate `sha2`/`subtle` dependencies from `orch8-api` and `orch8-grpc`.
- Concurrency limits filtering uses borrowed keys (`&str` instead of `String`), eliminating HashMap allocation per tick.
- Eliminated DashMap write-lock contention on circuit breaker hot path.
- Hot-path optimizations and benchmark expansion.
- Performance batching and security hardening across scheduler, api, engine, and storage.

### Security
- Replaced all `format!`-based dynamic SQL with `sqlx::QueryBuilder` in SQLite storage (5 fixes).
- Fixed SQL injection vulnerability by removing `format!` from workers.rs queries.

## [0.1.0] — 2026-03-15

First public pre-release.

---

[Unreleased]: https://github.com/orch8-io/engine/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/orch8-io/engine/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/orch8-io/engine/releases/tag/v0.1.0
