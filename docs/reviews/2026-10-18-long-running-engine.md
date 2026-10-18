# Long-running engine review — 2026-10-18

## Scope and method

Reviewed the scheduler's claim, heartbeat, retry, preemption, shutdown, and terminal paths; the Postgres, SQLite, and encrypted storage implementations; the shared lifecycle transition API; targeted storage contracts; ignored regressions; and CI/dependency checks. The repository has roughly 488 Rust source files and 174 SQL migrations, so this is a focused reliability review of the long-running execution path, not a line-by-line proof of the whole codebase.

## Fixed in this review

| Severity | Finding | Resolution |
| --- | --- | --- |
| Critical | A stale worker could use a state-only compare-and-swap after its lease was reclaimed, changing a replacement owner's `Running` instance to `Failed` or `Scheduled`. The heartbeat-then-update handback also had a check/write race. | Added a required atomic state-and-claim-token transition in both databases and the encrypted wrapper; used it in scheduler error, retry, handback, setup rollback, concurrency deferral, and preemption paths. |
| Critical | Terminal transitions from a claimed run could complete or fail a replacement owner's row and enqueue webhooks for the wrong run. | Added transactional claim-fenced terminal transitions with outbox rows and routed claimed scheduler and step execution paths through them. |
| High | A fixed/virtual clock could issue the same `claimed_at` token to consecutive owners, defeating fencing. A node clock ahead of Postgres could also delay stale-lease recovery. | Claim timestamps now advance at least one microsecond beyond the prior token. Postgres uses its own clock; SQLite uses wall time. Added a fixed-clock replacement-owner test for all four storage modes. |
| High | Rows explicitly created as `Running` or `Waiting` had no claim token, so claim-fenced transitions could not process them. | Canonical instance inserts now initialize the token from the row timestamp for active states. Added a backend contract for explicitly created `Running` rows. |
| Medium | `InstanceStore::conditional_update_instance_state` had an unconditional default that let new backends silently violate its compare-and-swap contract. | Made the method required for every backend implementation. |
| High | A governed memory record could shed `schema_version` and `content_sha256`, then be accepted as legacy shared memory after its content changed. | Governed records now require both fields. Truly ungoverned legacy records remain readable only from instance-local KV. |
| Medium | Six regression tests stayed ignored; five had already been fixed, while the memory test still exposed a bug. | Re-enabled tests for API capability grants, cross-tenant push acknowledgements, federation digest normalization, executor RPC exposure, native field collisions, and memory content commitment. |

Existing records that have a governance envelope but lack its version or content hash will now be unreadable. Recover them only from a trusted source; accepting the partial envelope would reopen the tampering bypass.

## Residual risks and recommended work

1. **Hook delivery under overload (high).** `HookTasks` drops cancel/escalation hooks when its 1,024-item in-memory queue is full and aborts remaining hooks when the shutdown grace expires (`orch8-engine/src/scheduler.rs`). A durable hook queue with retry/recovery is needed if these hooks are contractual. Until then, alert on `orch8_hooks_dropped_total` and exercise overload and restart cases in an integration test.
2. **In-flight writes after lease loss (high).** State and webhook transitions are fenced, but a handler already in progress can still save a block output, execution node, or context update after another worker reclaims the instance. Add a claim or run epoch to those write contracts and test a paused handler against reaper recovery.
3. **External effects during lease loss (high).** An already-running handler can also finish a remote effect after its heartbeat fails. The engine should pass a stable idempotency key to every external worker/connector and test recovery during a slow effect. At-least-once execution remains the contract until that is enforced end to end.
4. **Cross-backend failure testing (medium).** Add a fault-injection test that pauses a handler, reclaims its instance, and proves the original task cannot publish outputs or terminal events after the new claim begins. The storage contract now tests atomic state and outbox fencing, but does not simulate every in-flight handler side effect.
5. **Dependency advisory (medium).** The current audit reports [RUSTSEC-2026-0367](https://rustsec.org/advisories/RUSTSEC-2026-0367.html), [0368](https://rustsec.org/advisories/RUSTSEC-2026-0368.html), and [0369](https://rustsec.org/advisories/RUSTSEC-2026-0369.html) for `zip 8.6.0`, a build dependency of Swagger UI generation. `cargo deny check` passes with the repository's configured exceptions. Reassess when the upstream build dependency has a usable patched version; do not classify the advisory as an engine runtime exploit without an affected call path.

## Verification

The new `claim_fence` contract exercises a replacement owner with an unchanged scheduler clock, stale failure/reschedule attempts, a stale terminal webhook, and a valid current-owner terminal webhook on SQLite, Postgres, and both encrypted wrappers. CI should continue to run these legs with `DATABASE_URL` set. The six restored tests should remain enabled in the workspace suite.

Re-review triggers: a new storage backend; changes to claim or reaper SQL; lifecycle/step terminal paths; hook dispatch persistence; or the next `zip` dependency update.
