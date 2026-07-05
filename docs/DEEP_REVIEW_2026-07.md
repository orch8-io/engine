# Deep Architecture Review — July 2026

Follow-up to `RUST_REVIEW.md` (mechanical pass). This review goes deeper: concurrency and
cancellation safety, transactional integrity, security surface, type-safety architecture,
mobile sync correctness, and build/test health. Five parallel deep reviews were run:
engine, storage, API/server/gRPC, types/CLI/facade, mobile/publisher/build.

**Workspace hygiene verified:** `cargo fmt --check` clean; `cargo clippy --workspace
--all-targets` zero warnings under `clippy::all = deny` + pedantic; edition 2024;
`unsafe_code = deny`; CI has MSRV job, cargo-deny, pinned action SHAs.

**Prior-review status:** `SecretString` unsafe zeroing → fixed (uses `zeroize`).
`unreachable!()` in `instances/bulk.rs` → fixed. Server startup `expect`s → fixed.
`tracing-subscriber` unused dep → removed. Still open: gRPC `block_in_place`
interceptor (reassessed as M-4), `stream_bus` expects, mobile/publisher
`reqwest::Client` builder expects (now `#[allow]`-annotated rather than fixed).
Prior §8 (clone reduction) can be closed as won't-fix: every step pays 5–10 storage
round-trips that dwarf any `serde_json::Value` clone; the Arc-wrapped `StepContext`
was the right and sufficient fix.

---

## Severity-ranked findings

### Critical

**C-1. Split-brain on long-running steps: stale reaper + sentinel-counts-as-completed corrupts data flow.**
`orch8-storage/src/postgres/misc.rs:93` resets any `running`/`waiting` instance whose
`updated_at` is older than `stale_instance_threshold_secs` (default 300s); nothing
touches `updated_at` while an in-process handler executes, and
`orch8-storage/src/postgres/outputs.rs:178-182` counts the `__in_progress__` sentinel
as completed. An `agent`/`llm_call` step running >5 min on node A gets reaped; node B
claims the instance, sees the sentinel as done, skips the block, and downstream steps
consume `{"_sentinel":true}` as real output — while node A keeps burning tokens and
races state transitions. This makes time-based recovery unsound for exactly the
long-running LLM workloads the product targets.
*Fix:* lease/heartbeat — touch `updated_at` (or a `claimed_by` + liveness check)
periodically while a handler permit is held, so the reaper only fires for dead nodes.
Effort: M.

**C-2. `EncryptingStorage` decorator silently reverts defaulted trait methods to no-ops.**
`orch8-storage/src/encrypting.rs` hand-implements ~150 pass-throughs but misses every
trait method that has a default body in `lib.rs`: `update_sequence_status` (default
`Ok(())`), the entire rollback-policy family (`lib.rs:1567-1631`), and
`update_instance_context_cas` (`lib.rs:282-290` — default calls plain
`update_instance_context` and unconditionally returns `Ok(true)`). Both concrete
backends implement these for real. With `ORCH8_ENCRYPTION_KEY` set: sequence
publish/unpublish silently stops persisting; rollback policies are created into the
void and never fire; the scheduler's CAS write (`scheduler/step_exec.rs:536`) becomes
last-write-wins → lost context updates under concurrent signal/step writes.
*Fix:* delete the default impls for these methods (the crate already has an explicit
"no default impl so it fails at compile time" convention — apply it consistently), or
add the missing overrides plus a trait-coverage test. Effort: M.

**C-3. "Encryption at rest" covers a minority of the sensitive surface.**
Only `task_instances.context.data`, credentials, trigger secrets, artifacts, and
mobile-command payloads are encrypted. Plaintext: `block_outputs.output` (LLM
responses, HTTP bodies — same data class as context), `worker_tasks.params/context`
(receives a *decrypted* context snapshot on every external-worker dispatch),
`signal_inbox` full contexts, `externalized_state`, `instance_kv_state`,
`checkpoints.checkpoint_data` (agent message history), `step_logs`. A DB
snapshot/backup leak exposes essentially all customer payload data despite
encryption being enabled — false assurance.
*Fix:* document the exact coverage contract prominently, then extend encryption to
`block_outputs.output`, `worker_tasks.params/context`, signal payloads,
`externalized_state.payload`, and checkpoints. Effort: L.

**C-4. Cross-tenant secret exfiltration via `/mobile/sync`** — `orch8-api/src/mobile_sync.rs:232-240`. *(verified in code)*
`handle_sync` fetches and acks pending commands by the client-supplied `req.device_id`
with **no check that the device belongs to the caller's tenant** — while its sibling
`create_command` (mobile_sync.rs:~560) performs exactly that ownership check and
documents why it's essential. A tenant-A key pulls tenant-B's command queue, whose
`step_result` payloads carry **resolved credentials** (line 200-212, "secrets are only
in transit"), and can ack/delete B's commands. Same gap on `register_device`
(upsert on device PK with no owner check → push-token takeover).
*Fix:* mirror `create_command`'s `get_mobile_device` + tenant-match guard in
`handle_sync` and `register_device`. Effort: S.

**Note on C-1 mechanism:** two reviewers disagreed on the sentinel's exact failure mode
— one found the `__in_progress__` sentinel *counted as completed* (block skipped,
downstream reads `{"_sentinel":true}`); the other found *nothing reads it* and the
side-effectful handler *re-executes*, with `attempt: u16::MAX`
(`scheduler/step_exec.rs:682`) corrupting `compute_attempt`. The SQL comment at
`outputs.rs:178` supports the skip reading. Both agree crash recovery around
long-running steps is unsound; **trace the memoization read path
(`param_resolve.rs:132`, `step.rs:73-76`, `execute_step_dry`) before implementing the
lease fix** so C-1 and the sentinel are fixed coherently.

### High

**H-1. `/metrics`, `/swagger-ui`, `/api-docs/openapi.json` unauthenticated** — `orch8-server/src/main.rs:145-158`.
`Router::layer` only wraps previously-registered routes; the metrics/Swagger merges
happen *after* the auth layers, so they're public. The adjacent comment asserts the
opposite. *Fix:* merge them before the auth `.layer(...)` calls (only health +
public webhooks belong after). Effort: S.

**H-2. Released migration edited in place → checksum failure blocks upgrades** — `migrations/010_add_concurrency_and_idempotency.sql`.
Modified in commit `05ed92a` (v0.5.0); `sqlx::migrate!` verifies checksums, so any
deployment that ran pre-v0.5.0 fails at boot with `VersionMismatch` — an
upgrade-blocking outage requiring manual `_sqlx_migrations` surgery. The header of
migration 030 shows 010 was retroactively rewritten before, too. *Fix:* revert 010 to
its originally-shipped bytes, ship changes as a new migration, add a CI check that
diffs migration files against their last-tagged hashes. Effort: S.

**H-3. gRPC `create_sequence` skips `SequenceDefinition::validate()`** — `orch8-grpc/src/service.rs:352-369`.
HTTP and facade paths validate; gRPC stores unvalidated definitions (duplicate block
IDs, >64 nesting, `max_iterations: 0` all pass). *Fix:* add the `validate()` call;
structurally, see S-1. Effort: S.

**H-4. Unbounded SubSequence recursion; parent budget dropped** — `orch8-engine/src/evaluator/dispatch.rs:185-270`.
A→A (or A→B→A) sub-sequence spawn has no depth guard and hardcodes `budget: None`;
one 10-line definition spawns child instances forever. *Fix:* depth counter in child
metadata with a cap (~16); propagate parent budget. Effort: M.

**H-5. No width/size caps in sequence validation** — `orch8-types/src/sequence.rs:675-690`.
Depth is capped at 64 but `max_iterations: u32::MAX`, 50k parallel branches, and
unbounded total block count all pass. *Fix:* caps in `validate()` (iterations ≤ 100k,
branches ≤ 256, blocks ≤ 5k). Effort: S.

**H-6. Deadline/SLA escalation and cleanup hooks run inline on the tick loop with no timeout** — `orch8-engine/src/scheduler.rs:268-305, 925-978, 1291`.
One escalation handler POSTing to a dead endpoint stalls all scheduling on the node.
*Fix:* `tokio::time::timeout` around every hook invocation; consider spawning sweeps
like `process_tick`. Effort: S.

**H-7. `StepTimeout` bypasses retry policy and try/catch** — `orch8-engine/src/handlers/step.rs:161,332`; `scheduler/step_exec.rs:851-895`; `handlers/step_block.rs:632-638`.
A step with `timeout` + `retry` that times out once fails the instance permanently;
a surrounding `TryCatch` never catches. *Fix:* map timeout to
`StepFailed { retryable: true }` at both dispatch sites. Effort: S.

**H-8. Cron firing is not atomic — duplicate instances after crash** — `orch8-engine/src/cron.rs:243-277`.
`create_instance` then `update_cron_fire_times` as separate writes, no idempotency
key. *Fix:* `idempotency_key = "cron:{schedule_id}:{fire_at}"` (dedupe machinery
exists) or a single transaction. Effort: S.

**H-9. Agent checkpoint `__agent__:{block_id}` never deleted — stale conversation resume** — `orch8-engine/src/handlers/agent.rs:143-155`.
Agent step inside a Loop restores the previous iteration's messages/counter; a pass
that ended at `max_iterations` returns instantly with zero LLM calls. *Fix:* delete
the KV key on successful completion; suffix with loop iteration when nested. Effort: S.

**H-10. Enabling encryption silently disables context externalization** — `orch8-storage/src/encrypting.rs:457-501`.
Context is encrypted into a single `"enc:v1:…"` string *before* the externalizing
layer runs; `externalize_fields` then sees a non-object and no-ops. Multi-MB contexts
get written inline on every step — TOAST bloat, slow claims, size-cap/GC machinery
never engages; no error, no log. *Fix:* invert the layering — externalize plaintext
fields first, encrypt each externalized payload and the residual data. Effort: M.

**H-11. SQLite rate-limit check leaks an open `BEGIN IMMEDIATE` on error** — `orch8-storage/src/sqlite/rate_limits.rs:23-72`.
Error path returns the connection to the pool mid-transaction holding the RESERVED
write lock; with `in_memory()`'s `max_connections(1)` the entire storage deadlocks
permanently. *Fix:* apply the existing `rollback_quiet` pattern; better, refactor all
six raw-tx sites onto one `with_immediate_tx` helper. Effort: S.

**H-12. SQLite backend has no forward schema migration path** — `orch8-storage/src/sqlite/schema.rs`.
Single `CREATE TABLE IF NOT EXISTS` blob; a file-backed DB (mobile/edge mode) created
by an older binary is missing columns added since → hard `Query` errors. The
`schema_versions` table exists but drives nothing. *Fix:* per-version `ALTER TABLE`
deltas at boot keyed on `schema_versions`, mirroring Postgres numbering. Effort: L.

**H-13. Tenant-scoped worker claims lock `task_instances` rows, interfering with the scheduler** — `orch8-storage/src/postgres/workers.rs:91-107`; `postgres/misc.rs:175-199`.
`FOR UPDATE SKIP LOCKED` without `OF wt` also locks joined `task_instances` rows, so
worker polls and the scheduler's claim query silently skip each other's rows.
*Fix:* `FOR UPDATE OF wt SKIP LOCKED` in both queries. Effort: S.

**H-14. Mobile: one poison manifest entry permanently bricks sync** — `orch8-mobile/src/sync.rs:491-494`.
A sequence that passes hash+signature but fails serde parse aborts sync before the
ETag persists — every device refetches and fails forever. *Fix:* per-entry errors
become `skipped`/`signature_failures`; fail sync only on manifest-level errors. Effort: S.

**H-15. Mobile: every manifest change re-downloads all sequences** — `orch8-mobile/src/sync.rs:408-507`.
The version short-circuit runs *after* downloading JSON + `.sig`; URLs are
content-addressed so the manifest hash suffices to skip. Dominant battery/network
cost of the SDK. *Fix:* compare version/sha256 before `download_sequence`. Effort: S.

**H-16. Mobile: server commands executed at-least-once with no idempotency** — `orch8-mobile/src/sync_reporter.rs:461-471, 555-578`.
Side effect runs before the ack row is written; ack reaches the server a sync later.
App killed in between → duplicate `start_workflow`/`human_input` on redelivery.
*Fix:* record + consult the ack before executing; require server dedup keys for
`start_workflow`. Effort: M.

**H-17. Mobile: telemetry auto-flush retry storm when offline** — `orch8-mobile/src/telemetry.rs:101-117`.
Past 80% buffer, every `record()` fires a synchronous failing POST (30s timeout), no
cooldown. *Fix:* `last_flush_attempt` + exponential cooldown; flush off-path. Effort: S.

**H-18. Engine death leaves a zombie API** — `orch8-server/src/main.rs:560-564`; `orch8-api/src/health.rs:31-36`.
Tick-loop exit only logs; `/health/ready` checks storage only. Pod stays "healthy",
accepts instances, executes nothing. *Fix:* cancel the shutdown token on engine exit
(let the orchestrator restart), or an engine-liveness flag consulted by readiness. Effort: S.

**H-19. Manifest replay resurrects deleted sequences** — `orch8-publisher/src/manifest.rs:41`; `orch8-mobile/src/sync.rs:827,1113,1164`. *(verified: `manifest_version` hardcoded to 1)*
The client verifies the manifest's Ed25519 signature but never checks monotonicity or
`generated_at` staleness, and the publisher stamps `manifest_version: 1` on every
manifest. A MITM (or CDN cache poisoning) can replay an old validly-signed manifest to
reinstall previously-removed (buggy/exploitable) sequences. *Fix:* monotonic
`manifest_version` in the publisher; client persists last-seen and rejects `<=`. Effort: S.

**H-20. Server→device command channel is unsigned; `sync_url` not https-validated** — `orch8-mobile/src/sync_reporter.rs:493-660`; `lib.rs:300-313`.
Sequences are Ed25519-verified but commands (`complete_step`/`cancel`/`start`) are
trusted from transport alone, and `sync_url` is used directly (accepts `http://`). MITM
can approve `wait_for_input` gates, start/cancel workflows, and read `sync_api_key` in
cleartext. *Fix:* hard-require https on `sync_url`; sign command payloads with the same
root-key chain as sequences. Effort: M. (Overlaps H-16 idempotency — fix together.)

**H-21. Transient storage errors become terminal `Failed`/DLQ** — engine tick treats a storage error mid-batch as step failure.
A 2-second Postgres failover DLQs an entire claimed batch. *Fix:* classify
storage/connection errors as retryable — reschedule, don't fail. Effort: S.

### Medium

**M-1. AES-GCM: no associated data (ciphertext relocatable) + unbounded key lifetime** — `orch8-types/src/encryption.rs:90-107, 160-170`.
No AAD binds ciphertext to tenant/instance/field — an attacker with DB write access
can transplant `enc:v1:` blobs between rows/tenants and they decrypt cleanly. NIST
caps random-nonce GCM at ~2³² invocations/key; this engine encrypts on nearly every
step write, so the bound is reachable under sustained load and nothing tracks usage.
*Fix:* `enc:v2:` with AAD = tenant/instance/field (v1 decrypt fallback for
migration); count encrypt calls and alert on budget; document rotation SLA. Effort: M.

**M-2. `decrypt_value` silently accepts plaintext (encryption-strip downgrade)** — `orch8-types/src/encryption.rs:118-123`.
Add `decrypt_value_strict()` for always-encrypted fields; metric on unexpected
plaintext. Effort: S.

**M-3. SSRF: `update_credential` does not re-validate `refresh_url`** — `orch8-api/src/credentials.rs:279-280`.
Create validates, update doesn't — PATCH to `http://169.254.169.254/...` then
server-side refresh. *Fix:* same `validate_public_url` guard on update. Related:
`validate_public_url` (`orch8-api/src/security.rs:13-50`) passes hostnames through
(DNS-rebinding window, no redirect guard) — resolve-and-pin in the outbound client.
Effort: S (+M for rebinding).

**M-4. gRPC transport: no TLS; blocking auth lookup in interceptor** — `orch8-server/src/main.rs:529-537`; `orch8-grpc/src/auth.rs:113-125`.
API keys travel plaintext on :50051; unique-bogus-key floods pin worker threads + DB
pool via `block_in_place`. *Fix:* async tower auth layer; `tls_config` support or a
loud non-loopback-without-TLS warning; bound in-flight cache-miss auth. Effort: M.

**M-5. No rate limiting; global concurrency cap sits before auth** — `orch8-server/src/main.rs:170-178`.
Anonymous flood to public endpoints starves authenticated traffic. *Fix:* per-tenant
limiter (e.g. `tower_governor`) inside the auth boundary; separate anonymous budget. Effort: M.

**M-6. `update_context` signal does a blind full-context overwrite (lost updates)** — `orch8-engine/src/signals.rs:172-177`.
Step paths use CAS; the signal path doesn't. *Fix:* merge data-section only, or
CAS-with-retry. Effort: M.

**M-7. Webhook delivery tasks untracked at shutdown — events lost despite outbox** — `orch8-engine/src/webhooks.rs:110`.
Bare `tokio::spawn`; aborted mid-flight at exit, never parked. *Fix:* `TaskTracker` +
flush, mirroring `circuit_breaker.rs`. Effort: S.

**M-8. Trigger listeners: config edits don't restart them; not joined at shutdown** — `orch8-engine/src/triggers.rs:104-152`.
Diff by slug only. *Fix:* include config hash in diff key; JoinSet. Effort: S/M.

**M-9. StreamBus TOCTOU evicts a channel a concurrent subscriber just joined** — `orch8-engine/src/stream_bus.rs:85-98, 128-137`.
SSE client connects but receives nothing. *Fix:* `DashMap::remove_if`. Effort: S.

**M-10. Cron `CancelPrevious` non-CAS state write, skips `on_cancel` hooks** — `orch8-engine/src/cron.rs:184-189`.
*Fix:* enqueue a cancel signal instead of direct state write. Effort: S.

**M-11. SLA/deadline sweeps only examine the first `batch_size` oldest instances** — `orch8-engine/src/scheduler.rs:762-771, 1043-1059`.
>256 waiting approvals → newer deadlines never fire; O(active) work per 100ms tick.
*Fix:* indexed `next_deadline_check_at` query, paginate, slower cadence. Effort: M.

**M-12. `max_concurrency` cap overshoots across engine nodes** — `orch8-storage/src/postgres/instances.rs:246, 273-335`.
Running-count SELECT inside the claim tx sees only committed rows; two nodes each
admit into the same free slots. *Fix:* `pg_advisory_xact_lock(hash(key))` per
distinct key before counting, or post-claim re-verify + demote. Effort: M.

**M-13. `acquire_manifest_lock` is a no-op on every backend, yet callers rely on it** — `orch8-storage/src/lib.rs:145-151`; caller `orch8-api/src/telemetry.rs:283`.
Concurrent manifest publishes interleave with zero contention errors. *Fix:*
implement via `pg_advisory_lock` (+ SQLite row lock), or delete the API. Effort: M.

**M-14. Encryption turns atomic `merge_context_data` into a 5-try CAS loop that can hard-fail** — `orch8-storage/src/encrypting.rs:358-397`.
Parallel branches / signal processing produce user-visible step failures only when
encryption is on — behavioral drift by config. *Fix:* jittered backoff + higher
bound; long-term per-field encryption of `data` values (also fixes H-10). Effort: M.

**M-15. PG pool lacks `max_lifetime` and server-side timeouts** — `orch8-storage/src/postgres/mod.rs:88-104`.
One hung statement pins a connection forever; stale connections survive failovers.
*Fix:* `statement_timeout`/`idle_in_transaction_session_timeout` in `after_connect`;
`max_lifetime` ~30min. Effort: S.

**M-16. Secret-bearing storage getters are not tenant-scoped** — `orch8-storage/src/lib.rs:1379, 1394, 1427`.
`get_credential(id)`, `get_trigger(slug)`, `get_plugin(name)` take no `TenantId`;
isolation depends on per-callsite API checks, and RLS is parked
(`migrations/039_enable_rls.sql.deferred`). One forgotten check = cross-tenant OAuth
token read. *Fix:* add `tenant_id` to the signatures so the compiler enforces
scoping; revisit the deferred RLS migration. Effort: M.

**M-17. Mobile: terminal instances re-reported every sync for 24h; sync blocks tick loop** — `orch8-mobile/src/sync_reporter.rs:249-264`; `tick_controller.rs:213-219`.
*Fix:* report on state transitions only; spawn `sync_once` with an in-flight guard. Effort: M.

**M-18. Mobile: sequence upsert is delete-then-create — non-atomic, orphans in-flight instances** — `orch8-mobile/src/sync.rs:659-685`.
New `SequenceId` orphans running instances. *Fix:* transactional, additive versioning;
pin running instances to old version. Effort: M.

**M-19. `required_handlers`/wait-info scan only top-level blocks** — `orch8-publisher/src/publish.rs:103-113`; `orch8-mobile/src/sync_reporter.rs:865-905`.
Handlers inside Parallel/Loop/Router/TryCatch missed → runtime failure instead of
sync-time skip; approvals inside containers lose prompt/choices. *Fix:* reuse the
existing `flatten_blocks`. Effort: S.

**M-20. `orch8-publisher/src/push.rs` is a broken duplicate of orch8-push** — FCM posts to project `_` with a non-token; APNs is a stub. *Fix:* delete `PushNotifier`, inject `orch8_push::PushProvider`. Effort: S.

**M-21. Push delivery: no retry; stale APNs JWT not invalidated on 403** — `orch8-push/src/apns.rs`, `fcm.rs`.
Up to 50 min of total push failure on clock skew. *Fix:* invalidate on 403 + retry
once; one bounded retry on 5xx; parse `EncodingKey` in `new()`. Effort: S.

**M-22. SigV4 skips URI/query canonicalization; uploads have no retry; `.sig`/`.json` ordering** — `orch8-publisher/src/cdn.rs:133-137`.
*Fix:* adopt `aws-sigv4` or canonicalize per spec; bounded retry; upload `.sig`
before `.json`. Effort: M.

**M-23. Mobile backoff has no actual jitter; 429/`Retry-After` ignored** — `orch8-mobile/src/sync.rs:169-227`.
Fleet-wide thundering herd after outages. *Fix:* real randomization + honor
`Retry-After`. Effort: S.

**M-24. Parallel/Race give no in-process concurrency — docs overpromise** — `orch8-engine/src/evaluator.rs` (one running step dispatched per pass).
Two 30s parallel HTTP steps take 60s; Race decided by branch order. *Fix:* `join_all`
over independent Running nodes bounded by the step semaphore, or document the
limitation. Effort: L.

**M-25. CLI/config paper cuts** — `orch8 init` writes config keys that don't exist
(silently ignored; `config validate` says valid) — `orch8-cli/src/commands/init.rs:78-80`;
generated API key written 0644 — `init.rs:138-159`; dev studio binds `0.0.0.0` with no
auth — `dev_server.rs:91`. *Fix:* correct template keys + unknown-key detection;
`mode(0o600)`; default `127.0.0.1` with `--host` opt-in. Effort: S each.

### Low (selected)

- **L-1.** HalfOpen circuit breaker admits unlimited concurrent probes; registry never evicts — `circuit_breaker.rs:230-244`. Single-probe CAS + idle eviction. S/M.
- **L-2.** Cron `Skip` with unparseable expression hot-loops every tick — `cron.rs:156-158`. Park after N failures. S.
- **L-3.** Crash-skipped blocks expose `{"_sentinel":true}` as permanent output with no marker — rewrite to explicit `_skipped_after_crash` + metric. S.
- **L-4.** Pending `human_input` signals on Paused instances re-processed every 100ms tick — `signals.rs:187-225`. S.
- **L-5.** API-key revocation lags up to 30s across nodes (process-local cache) — `api_key_cache.rs:43`. LISTEN/NOTIFY invalidation or document as SLA. M.
- **L-6.** `--insecure` couples auth-off with encryption-off — split flags. S.
- **L-7.** Dead duplicate match arm in `load_config` — `orch8-server/src/main.rs:609-618`. S.
- **L-8.** `TenantId::unchecked` is pub and used on untrusted gRPC input; `new` doesn't trim — `ids.rs:249-253`. S.
- **L-9.** Decoded key bytes not zeroized in `FieldEncryptor` — `encryption.rs:51-61`. S.
- **L-10.** `validate_https_url` (mobile) misses ULA/link-local/v4-mapped IPv6 + DNS rebinding — `orch8-mobile/src/lib.rs:209-221`. S.
- **L-11.** Dockerfile hand-rolled dep caching swallows errors (`|| true`); wasmtime built into server image by default — switch to cargo-chef; decide `wasm` default. M.
- **L-12.** deny.toml `multiple-versions = "warn"` is toothless — tree carries hashbrown×4, two RustCrypto generations. Deny + skip-list. S.
- **L-13.** CI: cargo-deny compiled from source each run; `cargo machete` not in CI; no coverage measurement; mobile bindings never exercised from Swift/Kotlin. S.
- **L-14.** Rate limiter is fixed-window; ARCHITECTURE.md claims sliding — boundary bursts admit 2×. Fix docs or two-bucket window. S.
- **L-15.** Mobile-sync timestamps round-trip second-precision TZ-less strings — same-second updates skipped/duplicated. RFC 3339 with fractional seconds. S.

### Structural investments

**S-1. Parse-don't-validate for `SequenceDefinition`** — `#[serde(try_from = "RawSequenceDefinition")]` running `validate()` inside `TryFrom`, so no transport can forget (H-3 proved one already did). `TenantId` already demonstrates the pattern. Effort: M.

**S-2. Narrow handler-facing storage facade** — handlers hold full `Arc<dyn StorageBackend>` and mutate instance state directly, forcing the scheduler to defensively re-read at 4+ sites. The ~150-method supertrait also makes full-surface decorators (`EncryptingStorage`, C-2) a standing liability. A narrowed trait removes a class of races and shrinks the decorator surface. Effort: L.

**S-3. Decompose `scheduler.rs`** (tick loop, admission, SLA sweeps, cleanup hooks, signal sweep are one module); split `expression.rs`/`template.rs` into lexer/parser/functions submodules (cohesive but 3000/2480 lines). Effort: M-L, mechanical.

**S-4. Feature-gate `sqlx`/`utoipa` in orch8-types** — currently every consumer (incl. UniFFI mobile builds) compiles tokio + rustls + two DB drivers for domain structs. ARCHITECTURE.md still claims "zero-dependency". Effort: M.

**S-5. Property-based testing + fuzzing for expression/template evaluators** — zero proptest/fuzz today; the evaluator executes server-supplied expressions on end-user devices. proptest no-panic/round-trip properties + cargo-fuzz targets + 60s CI smoke. Effort: M.

**S-6. Re-organize engine tests by subsystem** — 27k lines in session-named files (`engine_bugs_group_a.rs`, `*_coverage.rs`); 18 real `sleep` calls despite the ManualClock. Mechanical re-split; migrate sleeps. Effort: L, low-risk.

**S-7. Feature-gate the CLI's embedded dev server** — CLI currently compiles the whole engine+api+storage stack. Effort: M.

---

## Action plan

### Phase 0 — release blockers & security hotfixes (days, mostly S-effort)
1. **H-2** revert migration 010 to shipped bytes + new migration + CI hash check *(upgrade-blocking)*
2. **C-2** EncryptingStorage missing overrides / remove default impls + trait-coverage test *(silent data loss when encryption on)*
3. **H-1** metrics/Swagger auth ordering
4. **H-3** gRPC `validate()` call
5. **C-4** `/mobile/sync` + `register_device` tenant-ownership check *(cross-tenant credential leak)*
6. **M-3** `refresh_url` re-validation on update
7. **H-11** SQLite rate-limit tx rollback
8. **H-21** transient storage errors → retryable, not DLQ
9. **M-25** CLI: dev server bind 127.0.0.1, key file 0600, fix template keys

### Phase 1 — correctness of the core loop (1–2 weeks)
8. **C-1** lease/heartbeat for claimed instances — first trace the sentinel/memoization read path (see C-1 mechanism note) so the sentinel bug is fixed coherently with the lease
9. **H-6** timeouts around escalation/cleanup hooks
10. **H-7** StepTimeout → retryable classification
11. **H-8** cron idempotency key
12. **H-9** agent checkpoint cleanup
13. **H-13** `FOR UPDATE OF wt` in tenant-scoped claims
14. **H-18** engine-death → readiness/exit
15. **M-6** update_context CAS, **M-7** webhook TaskTracker, **M-9** StreamBus remove_if, **M-10** cron cancel-via-signal, **M-15** PG pool timeouts

### Phase 2 — encryption story made honest (1–2 weeks)
16. **H-10** externalize-then-encrypt layering
17. **C-3** extend coverage (block_outputs, worker_tasks, signals, checkpoints) — or document the contract as interim
18. **M-1/M-2** AES-GCM AAD v2 + strict decrypt + nonce budget
19. **M-14** encrypted merge_context backoff

### Phase 3 — mobile reliability (1 week)
20. **H-14** poison-manifest tolerance, **H-15** skip-before-download, **H-17** flush cooldown
21. **H-16** command idempotency, **M-17** transition-only reporting + non-blocking sync
22. **M-18** atomic versioned sequence upsert, **M-19** recursive handler scan, **M-23** real jitter
23. **M-20** delete broken PushNotifier, **M-21** push retry/JWT invalidation, **H-12** SQLite forward migrations (mobile deployment mode)
24. **H-19** manifest replay monotonicity, **H-20** signed command channel + https-only `sync_url`

### Phase 4 — hardening & structure (2–3 weeks, interleave)
24. **H-4/H-5** recursion depth + width caps, then **S-1** serde try_from
25. **M-4** async gRPC auth layer + TLS story, **M-5** per-tenant rate limiting
26. **M-11** indexed deadline sweeps, **M-12** concurrency-cap advisory lock, **M-13** real manifest lock
27. **M-16** tenant-scoped secret getters (+ revisit deferred RLS migration)
28. **S-5** proptest/fuzz for evaluators
29. **S-3** scheduler decomposition; **S-4** orch8-types feature-gating; **L-11/L-12/L-13** build/CI items
30. **M-24** Parallel/Race: decide concurrency vs. documentation; **M-22** SigV4 via `aws-sigv4`

Deliberately deferred: clone-reduction work (storage round-trips dwarf value clones —
prior review §8 closed as won't-fix), missing-docs backfill (tracked in
RUST_REVIEW.md §5), remaining Low items ride normal cadence.
