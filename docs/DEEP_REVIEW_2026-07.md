# Deep Architecture Review — July 2026

On 2026-07-05 a five-agent review of the `orch8.io/engine` workspace (engine,
storage, API/server/gRPC, types/CLI/facade, mobile/publisher/build) produced a
severity-ranked findings list — 4 Critical, 21 High, 25 Medium, several Low,
plus 7 Structural investments (S-1–S-7) — and a phased remediation plan. All
Critical, High, and Medium findings judged safe to fix without a larger
infrastructure investment have since been closed. This document is the
durable record of what was found, what was fixed, and what was deliberately
deferred.

The raw findings write-up itself is not checked in (it was a working
document, not a stable artifact); this file is the summary that replaces it.

## Remediation PRs

| PR | Scope | Merge commit |
|---|---|---|
| [#88](https://github.com/orch8-io/engine/pull/88) | 15 storage-layer findings | `c07d679` (merged into `main` as `40da554`) |
| [#90](https://github.com/orch8-io/engine/pull/90) | All remaining C/H/M/selected-L findings | `9305586` |

### #88 — storage-layer findings

Broad changes across storage/engine/api/mobile/publisher: full
`EncryptingStorage` trait coverage, a migration checksum-immutability CI
guard, tenant-scoped claim queries, AES-256-GCM AAD binding (`enc:v2:`
format), a SQLite forward-migration path, transactional rate-limit checks,
and a `mobile_sync` ownership check.

### #90 — remaining findings

| ID | Area | Finding | Fix |
|---|---|---|---|
| C-1 | engine | No lease/heartbeat on long-running steps — the stale-instance reaper could reclaim (and re-dispatch) a step still genuinely executing | Per-instance heartbeat task touches `updated_at` while a step is in flight; `recover_stale_instances` no longer mistakes "slow" for "dead" |
| H-3 | grpc | `create_sequence` skipped `SequenceDefinition::validate()` | Added the missing `.validate()` call |
| H-4 | engine | `SubSequence` had no recursion-depth cap and dropped the parent's `budget` | Added a depth cap + budget propagation on the child instance |
| H-5 | types | No width/size caps on sequence validation (branch count, loop iterations, total blocks) | Added `MAX_BRANCHES`, `MAX_ITERATIONS`, `MAX_TOTAL_BLOCKS` checks |
| H-7 | engine | `StepTimeout` bypassed retry policy and `TryCatch` at both dispatch sites | Normalizes into a retryable `StepFailed` before either dispatch path's match |
| H-8 | engine | Cron firing wasn't crash-atomic — a crash between instance-create and fire-time-advance could double-fire | Idempotency key derived from `(schedule_id, next_fire_at)`; conflict is swallowed, fire time still advances |
| H-9 | engine | Agent step's `__agent__:{block_id}` checkpoint was never deleted, so a `Loop` re-dispatching the same block id could resume a finished pass's exhausted state | Checkpoint is deleted on every terminal completion (`completed` and `max_iterations`) |
| H-15 | mobile | Sync always downloaded a sequence body + detached signature even when the local copy was already current | Version check moved before any network call |
| H-16 | mobile | Sync commands executed side effects before being marked delivered, so a redelivered command (lost ack) could double-fire | Commands are durably marked executed *before* running side effects; a duplicate delivery is a no-op |
| H-17 | mobile | Telemetry auto-flush had no cooldown — a failing endpoint was hammered on every recorded event once the buffer crossed 80% | Added a cooldown between flush attempts |
| M-2 | types | No strict decrypt path for fields that must always be encrypted | Added `FieldEncryptor::decrypt_value_strict` |
| M-3 | api | `refresh_url` was SSRF-validated on credential create but not update | Same validation now runs on update |
| M-6 | engine | `update_context` signal blindly overwrote the whole `ExecutionContext`, including engine-owned `runtime`/`audit` | Now merges only the signal's `data` keys into the existing context |
| M-7 | engine | Webhook delivery tasks were bare `tokio::spawn`s, not tracked for shutdown | Tracked via a `TaskTracker`; shutdown waits for in-flight deliveries |
| M-9 | engine | `StreamBus` subscribe/publish/drop had a TOCTOU that could orphan a fresh subscriber's channel | Switched to `DashMap::remove_if` for an atomic check-and-remove |
| M-20 | publisher | Unused, broken `PushNotifier` duplicate (APNs path was a stub that always errored) | Deleted; `orch8-push` is the real path |
| M-21 | push | No retry on transient push failures; no recovery from a rejected JWT/OAuth token | Added retry with backoff; 403 (APNs) / 401+403 (FCM) invalidates the cached token |
| M-23 | mobile | Retry backoff's "jitter" was a deterministic function of attempt number; 429 wasn't retried; `Retry-After` was ignored | Real random jitter; 429 now retried like 5xx; `Retry-After` honored when present |
| M-25 | cli | Scaffolded `orch8.toml` used field names (`max_instances_per_tick`, `stale_threshold_secs`) that don't exist on `SchedulerConfig` — silently no-op'd; dev server bound `0.0.0.0` | Corrected field names; dev server binds loopback only |
| L-6 | server | `--insecure` coupled "no auth" and "no encryption at rest" into one flag | Split into `--insecure-auth` / `--insecure-storage`; `--insecure` remains as shorthand for both |
| L-9 | types | Decoded encryption key bytes were left for a plain `Drop` instead of being zeroized | `FieldEncryptor::from_hex_key`/`with_old_key` zeroize the buffer after copying it into the cipher |

H-3, H-4, H-5, H-7, H-8, and M-10 (cron `CancelPrevious` now enqueues a signal
instead of writing `Cancelled` directly, so scoped-cancellation hooks still
run) were fixed earlier in the same branch, ahead of the items above.

## Deliberately deferred (disclosed, not silently dropped)

- gRPC TLS
- Full API rate limiting
- SigV4 (or equivalent) request signing
- Full Ed25519 signing of mobile command payloads (H-20's remaining portion)
- Mobile atomic upsert
- `Parallel` / `Race` block concurrency hardening
- M-4, M-5, M-8, M-11, M-17, M-18, M-22, M-24
- Most Low-severity items (L-1–L-13, except L-6 and L-9 above)
- All Structural investments (S-1–S-7)

## Verification

Every fix in #90 has a dedicated regression test. Each was confirmed to
**fail** against the pre-fix code, then the fix was restored and re-verified
green — including two genuinely racy concurrency bugs (`StreamBus`'s
`remove_if` fix and the webhook task-tracking fix), which were reproduced
under real thread parallelism before landing the fix.

Both PRs were verified with `cargo fmt --check`, `cargo clippy --workspace
--all-targets -- -D warnings`, and the full test suite (unit + integration,
SQLite and Postgres backends) — all green — plus GitHub Actions CI (MSRV,
Check & Lint, Unit Tests, E2E Tests, Docker Smoke Test) on both PRs before
merge.

Two pre-existing tests that had encoded the *old* buggy behavior as
intentional (`signal_update_context_replaces_instance_context`,
`signal_update_context_with_config_change`) were updated to assert the
corrected, review-mandated behavior instead.
