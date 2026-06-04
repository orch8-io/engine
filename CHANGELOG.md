# Changelog

All notable changes to the Orch8 Engine will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Artifact HTTP endpoints**: `GET /instances/{id}/artifacts` lists an instance's artifacts (`{key, size, uri}`); `GET /artifacts/{key}` streams the raw bytes (optional `?content_type=` overrides the response type). Both are tenant-scoped via the owning instance — a cross-tenant request gets the same `404` as a missing one, so artifacts can't be probed across tenants. Unblocks a control-plane artifacts viewer.
- **LLM token-usage capture** (`usage_events`): `llm_call` (and `agent`, which calls it) records token usage after each successful call — normalizing the OpenAI (`prompt_tokens`/`completion_tokens`) and Anthropic (`input_tokens`/`output_tokens`) shapes — into structured `usage_events` rows (`tenant_id`, `instance_id`, `block_id`, `kind="llm_tokens"`, `model`, token counts). Best-effort: a recording failure never fails the step. `GET /usage?start=&end=` returns per-`(kind, model)` totals for the tenant (header-scoped callers see only their own). Unblocks a cost dashboard.
- **`mcp_call` handler**: native Model Context Protocol client over the Streamable HTTP transport. Performs the `initialize` → `notifications/initialized` → request handshake per call, supports `tools/call` and `tools/list`, parses both `application/json` and `text/event-stream` (SSE) responses, and honors `Mcp-Session-Id`. Because it runs as a durable step, MCP tool calls inherit retry, rate-limiting, circuit-breaker, and crash-recovery semantics — a tool reporting `isError: true` surfaces as `is_error` rather than failing the step.
- **`agent` handler**: a native, durable ReAct loop. Drives reason → act → observe over `llm_call` (10 providers + failover) and `tool_call`/`mcp_call` until the model stops requesting tools or `max_iterations` is hit. Tool errors become observations so the model can self-correct.
- **Agent memory handlers** (`embed`, `memory_store`, `memory_search`): durable, semantically-searchable memory built on the existing instance KV store. Cosine ranking runs in pure Rust — no pgvector/sqlite-vec extension — so memory also works offline on the mobile (SQLite) backend.
- **`mcp_call` named-server resolution**: a step may reference `server: "<name>"` instead of a raw `url`; the endpoint (and optional `token`/`headers`) is resolved from the read-only `context.config.mcp_servers` registry. Engine-local and offline — the managed multi-tenant catalog remains a cloud concern.
- **`agent` durable checkpointing**: after each completed iteration the loop persists its progress (conversation + iteration counter) to the instance KV, so a crash/retry resumes from the last completed iteration instead of re-running (and re-paying for) earlier turns.
- **`agent` tool auto-discovery**: when no `tools` schema is supplied and `tool_dispatch.type == "mcp"`, the agent calls `tools/list` and forwards the discovered schema to the model.
- **`agent` auto-memory** (`auto_memory`): semantically recalls prior memories for the goal and prepends them as context before the run, and stores the final answer after — wiring the memory handlers into the loop.
- **Durable artifacts**: a generic binary-blob primitive (`ArtifactRef` + `ResourceStore::{put,get,delete,list}_artifact`) backed by [`object_store`]. Bytes live out-of-band; a small JSON ref travels in step outputs (survives checkpoint/replay). Backends: **local filesystem**, **S3-compatible** (AWS S3 / Cloudflare R2 / MinIO), configured via `ORCH8_ARTIFACT_BACKEND` / `[artifacts]`. In-memory is intentionally not a production backend (it would break durability). Artifact bytes are **encrypted at rest (AES-256-GCM)** by the `EncryptingStorage` wrapper before reaching the object store — independent of bucket SSE. Encrypted blobs carry a magic header so reads are self-describing: any blob written *before* encryption was enabled is returned as-is rather than failing to decrypt, and `ArtifactRef.size` reports the plaintext length. Cleanup capability via `ResourceStore::delete_instance_artifacts(instance_id)` (list + delete); for S3/R2 also set a bucket lifecycle policy.
  - **Local backend durability caveat**: the local filesystem backend is durable **only on a persistent volume**. On ephemeral container filesystems (Cloud Run, k8s without a PVC) artifacts are lost on restart — the server now logs a startup `WARN` and **requires an absolute `artifacts.path`**. Use the S3 backend for cloud deployments.
  - **Automatic retention sweeper**: set `ORCH8_ARTIFACT_RETENTION_SECS` (`engine.artifact_retention_secs`, default `0` = disabled) to have the background GC delete the artifacts of instances that have been in a terminal state longer than the window. Bounded per tick, idempotent (an `_artifacts_gced` instance marker prevents re-scanning), and surfaced via `orch8_gc_artifacts_{deleted,errors}_total`. Disabled by default so artifacts are never deleted unless you opt in.
- **Binary I/O on `tool_call`**: `response_as: "artifact"` captures a (2xx) response body as a durable artifact; `body_artifact` (+ optional `upload: { mode: "multipart", … }`) uploads an artifact's bytes as the request body (raw or multipart). Generic — composes into image generation, asset fetch, and media upload without any provider-specific handler.
- **Dry-run mode**: pass `dry_run: true` when creating an instance (per-run, so *any* sequence can be run dry). Side-effecting steps — `tool_call`, `llm_call`, `mcp_call`, `agent`, `emit_event`, `send_signal`, `embed`/`memory_store`/`memory_search`, `self_modify`, and the Activepieces/gRPC plugins — run their **pure validation first** (URL/SSRF safety, API-key presence, `tool_name`, trigger resolution incl. tenant + target sequence, target-instance existence, block-definition validity) and only then skip the real effect, returning **one canonical envelope**: `{ "dry_run": true, "handler": <name>, "would": <skipped effect>, …<success-shape with empty values> }` so downstream templates resolve and a green dry-run actually means the step is well-formed. Control flow, templating and branching execute for real; `human_review` skips its outbound notification and, by default, keeps its pending/pause gate. Pass `dry_run_auto_approve: true` (alongside `dry_run`) to auto-resolve human gates with the default (first) choice so the simulation flows through and exercises post-gate steps. The flag lives in the per-instance `RuntimeContext` (queryable on the instance, logged at creation, survives checkpoint/replay) and propagates to spawned sub-sequence children, so nothing leaves the box. **Dry-run is part of the idempotency identity** — a real run reusing a dry-run's `idempotency_key` is *not* deduplicated to the simulation.
  - **Rollback caveat**: `dry_run` lives in the serialized instance context; rolling the binary back to a pre-dry-run version drops the flag (serde ignores unknown fields), so in-flight dry-run instances would execute their remaining steps **for real**. Drain dry-run instances before downgrading.
- **`blob_put` / `blob_get` handlers**: first-class step access to the durable-artifact primitive (previously reachable only via `tool_call`). `blob_put` stores `text` or `data` (base64) and returns an `ArtifactRef`; `blob_get` reads it back as `base64` or `utf8`. Size-guarded (`max_size_bytes`, default 25 MiB), dry-run aware, and accepts a `ref` as a bare key string, `{ "key": … }`, or the `{ "artifact": { … } }` wrapper that `blob_put`/`tool_call` emit — so bytes pass between steps via `{{outputs.<id>.artifact}}`. Makes binary pipelines (LLM → image → store → publish) expressible without a provider-specific handler. **Requires an artifact backend** to be configured (see *Durable artifacts*); fails permanently with `Unsupported` otherwise.
- **Signed outbound webhooks**: `webhooks.secret` (a redacted `SecretString`) signs every delivery Stripe/GitHub-style — `X-Orch8-Timestamp` + `X-Orch8-Signature: sha256=HMAC-SHA256(secret, "{timestamp}.{body}")` — so receivers can verify authenticity and reject replays. Mirrors the inbound trigger-secret model in the outbound direction; deliveries stay unsigned (unchanged) when no secret is set. Builds on the existing durable retry/backoff delivery path.
- **Per-tenant API keys**: admin-only endpoints (`POST /api-keys`, `GET /api-keys`, `DELETE /api-keys/{id}`) mint, list, and revoke scoped authentication keys. Each key is bound to exactly one tenant; the plaintext secret (shown once at creation) is never stored — only its SHA-256 hash is persisted. Per-tenant keys authenticate via the same `x-api-key` header but ignore `X-Tenant-Id` (the tenant is taken from the key record), preventing header spoofing across tenants.
- **`last_used_at` tracking for API keys**: every successful authentication with a per-tenant key updates `last_used_at` via a fire-and-forget async write, giving operators an audit trail for stale-key rotation.
- **gRPC per-tenant API key parity**: the gRPC interceptor (`Orch8GrpcService`) now resolves per-tenant keys by hash against storage, matching the HTTP middleware contract. Root key remains unscoped; per-tenant keys reject mismatched `x-tenant-id` metadata with `PermissionDenied`.

### Changed

- **`StorageError` taxonomy**: split the catch-all `Unsupported` into permanent `Unsupported` (not configured / not supported → maps to a non-retryable step error and HTTP 500) vs transient `Backend` (object-store network/throttle failure → retryable, HTTP 503). Fixes artifact misconfiguration retrying forever.
- `tool_call` `response_as:"artifact"` with a non-idempotent method requires an idempotent endpoint: if the artifact store fails *after* a successful request, the step retries and re-sends.
- **Secure-by-default tenant isolation**: `require_tenant_header` now defaults to `true` (was `false`). Disabling it still works for single-tenant deployments but the server logs a loud `WARN` at startup when auth is enabled.
- **Encryption fail-closed**: the server now refuses to start if no encryption key is configured, unless `--insecure` is explicitly passed. A missing key previously stored credentials and `context.data` in plaintext silently.
- **Unified step-dispatch internals**: the flat-sequence (`scheduler::step_exec`) and tree-evaluator (`handlers::step_block`) dispatch paths now share one `prepare_step` (context snapshot → templates → credentials → cache key, with its failure logging) and one `breaker_preflight` (circuit-breaker open/fallback/defer decision). The two ~150-line preambles were previously hand-mirrored and drifted; they are now single-sourced, with each path keeping only its own node-level vs instance-level failure transition.

### Fixed

- **Infinite retry on the fast path (never reached the DLQ)**: a retryable step failure in the flat-sequence scheduler deleted *all* of the block's outputs without persisting a retry marker, so `compute_attempt` reset to `0` every tick and `attempt >= max_attempts` was never true — the step retried forever and never failed the instance. The fast path now writes a `__retry__` marker (mirroring the tree evaluator) so the attempt counter advances and the instance fails after `max_attempts`. Retry markers are excluded from the completed-block set (SQLite + Postgres) so the block still re-executes, while the `__in_progress__` crash-recovery sentinel still counts as completed.
- **Response-body OOM**: `http_request` and `tool_call` checked `Content-Length` (trivially omitted by a chunked response) and then buffered the *entire* body before the post-read size check, so an endpoint streaming gigabytes could OOM the worker. Both now stream with a hard 10 MB cap that aborts mid-stream, bounding peak memory.
- **Silently stranded instances**: when no concurrency permit was available (or when re-arming a signalled instance), the scheduler dropped the result of the `Running → Scheduled` deferral write with `let _ = …`. If that write failed the row stayed `Running` and was never re-claimed, with no log. Both paths now `warn!` on failure so a stuck instance is observable.

### Performance

- **SLA deadline check**: `check_sla_deadlines` re-flattened the whole block tree (`flatten_blocks`) on every iteration of the evaluate loop (up to 200×). It now reuses the `block_map` the evaluator already builds once per evaluation — removing an O(blocks) allocation + hash per iteration on composite instances.
- **API-key auth hot path**: per-tenant API keys are now resolved through a process-wide short-TTL cache (`moka`, keyed by SHA-256 hash, 30 s), shared by the HTTP middleware and the gRPC interceptor. This collapses a per-request indexed `SELECT` — and, on the HTTP side, an unbounded per-request `last_used_at` write — into at most one read + one write per key per window. Revocation stays immediate: `revoke_api_key` evicts the cache entry (via `RETURNING key_hash`) in the same statement that flips the flag. The root key is also hashed once at startup (precomputed digest) instead of on every request.
- **Expression engine — borrow instead of clone**: the expression interpreter now threads `Cow<Value>` so path resolution returns a *borrow* into context/outputs. A value that is only inspected (`len`/`count`, comparison, truthiness) is no longer deep-cloned — `len(outputs.big_array)` on a 200-element array dropped from **57 µs to 0.57 µs (~100×)**. `runtime.*` access also stops serializing the entire `RuntimeContext` to JSON on every read (common scalar fields resolved directly from the typed struct; **−77%** on `runtime.attempt`).
- **Scheduler per-step writes**: each completed step on the fast path did a full `get_instance` read **plus** a whole-`context` rewrite just to bump `total_steps_executed`. Replaced with an atomic `increment_total_steps` (a single targeted `json_set` / `jsonb_set` UPDATE that returns the new count and leaves the rest of `context.data` untouched), removing ~2 DB round-trips and a full-context serialization per step.
- **Templated-step state fetch**: `resolve_templates_in_params` fetched and serialized the entire instance KV map for *every* templated step, even when no template referenced `{{ state.* }}`. The fetch is now gated on an actual `state` reference (conservative — no false negatives), skipping a DB round-trip for the common outputs-/context-only case.

### Security

- **SSRF via HTTP redirects**: the shared outbound HTTP client (`http_request`, `tool_call`, `agent`, `mcp_call`) followed redirects with reqwest's default policy, re-validating nothing — a public URL could `302 → http://169.254.169.254/…` and reach cloud-metadata/internal hosts. The client now re-checks every redirect hop and refuses redirects to non-`http(s)` schemes or private/loopback/link-local IP literals (hop count also capped).
- **`ORCH8_ALLOW_INTERNAL_URLS` fail-open**: the SSRF-bypass flag was enabled by `is_ok()` — true for any value, including an empty / `0` / `false` leftover var. It now requires an explicit truthy value (`1`/`true`).
- **Artifact IDOR in `tool_call`**: a `body_artifact.key` was fetched verbatim, so a workflow could read another instance's (potentially another tenant's) artifact via a sibling key. The key must now be prefixed by the current instance's id.
- **Artifact IDOR in `blob_get`**: `blob_get` now enforces the same instance-prefix ownership check as `tool_call`, rejecting cross-instance artifact reads before touching storage.
- **`api_key_env` secret exfiltration guard**: `llm_call` and `memory` handlers block workflow-controlled `api_key_env` from reading the engine's own secrets (`ORCH8_*` namespace) and well-known infrastructure env vars (`DATABASE_URL`, `AWS_SECRET_ACCESS_KEY`, `GITHUB_TOKEN`, etc.). Only legitimate provider keys (e.g. `OPENAI_API_KEY`) are allowed. The guard now also rejects any env var whose name *contains* a secret-shaped substring (`SECRET`, `PASSWORD`, `CREDENTIAL`, `PRIVATE_KEY`, `SESSION_TOKEN`), catching the long tail of third-party host secrets (`STRIPE_SECRET_KEY`, `TWILIO_AUTH_TOKEN`, …) an exact-match denylist can't enumerate.
- **Expression engine hardening**: `random()` with extreme `i64` bounds (`i64::MIN .. i64::MAX`) no longer panics from overflow — rewritten with `i128` arithmetic. `slice(arr, start, end)` with `start > end` now returns an empty array instead of panicking on an invalid Rust range.
- **Webhook future-timestamp window**: inbound webhook replay protection accepted timestamps up to 5 minutes in the *future* (symmetric `abs_diff`). Future skew is now capped at 60 s while the 5-minute past window is retained, shrinking the window for pre-dated captured payloads.

## [0.5.0] — 2026-05-20

### Added

- **Mobile sync**: offline-first engine with sync reporter, push notifications (APNs/FCM), SQLite storage, and UniFFI bindings for iOS/Android.
- **Push notifications**: new `orch8-push` crate with APNs and FCM providers for silent push delivery.
- **Mobile API endpoints**: `/mobile/sync`, `/mobile/devices/register`, `/mobile/devices`, `/mobile/approvals`, `/mobile/approvals/{id}/resolve`, `/mobile/status`, `/mobile/commands`.
- **Human review `allow_comment` parameter**: free-text comments alongside human decisions in `wait_for_input` steps.
- **Row Level Security**: preparation for tenant isolation at the database layer (policies to follow in v0.5.1).

### Fixed

- Circuit breaker DashMap write-lock optimization — reduced contention on hot path.
- Externalized state batch_save — removed unnecessary allocations.
- `complete_step` now enqueues human_input signal instead of direct context merge.
- Re-read instance context before updating step counter to preserve HITL mutations.
- Enable RLS on all engine tables in public schema (deferred to v0.5.1 pending policy definitions).

### Security

- Added tenant scoping to mobile sync endpoints (`create_command`, `resolve_approval`, `handle_sync`).
- Redacted resolved credentials from mobile command payloads.
- Fixed mutex poisoning recovery in push notification providers.

## [0.4.0] — 2026-05-10

### Changed

- Private newtype fields + unsigned integers for non-negative domain values.
- Eliminated String allocations on DashMap hot paths via custom hash key types.
- Used `sqlx::QueryBuilder` in SQLite storage for sequences list and signals batch delivery.

### Fixed

- Resolved all 25 RustSec advisories.
- Resolved clippy warnings (ref_as_ptr, cast, doc backticks, clone).
- Added `?mode=rwc` to SQLite URLs in init template and docs.
- E2E tests aligned with server validation rules.
- Codesmell audit: 22 fixes with comprehensive test coverage.
- Removed `format!`-based SQL string generation in PostgreSQL search_path handling.

### Security

- Replaced remaining `format!`-based dynamic SQL with `sqlx::QueryBuilder` in SQLite storage.

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

[Unreleased]: https://github.com/orch8-io/engine/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/orch8-io/engine/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/orch8-io/engine/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/orch8-io/engine/compare/v0.2.0...v0.3.1
[0.2.0]: https://github.com/orch8-io/engine/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/orch8-io/engine/releases/tag/v0.1.0
