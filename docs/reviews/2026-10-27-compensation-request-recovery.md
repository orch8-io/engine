# Compensation creation after lost acknowledgements

A client that lost the creation response could not discover the committed run ID.
Retrying creation conflicted with the active run, even though forward execution
had already been durably fenced. Recovery now supports a persisted request UUID
and an indexed active-run lookup.

Creation derives a stable UUID from a versioned SHA-256 domain, length-prefixed
tenant, continuity ID and request UUID. Tenant authorization precedes replay.
The first request returns 201; the same intent returns 200 with the persisted
plan and current progress, including completed outcomes. Replaying an intent
never rebuilds its plan from newer evidence. Different intents retain active-run
conflict behavior. Malformed UUIDs and unknown creation fields return 422 before
writing. Callers must persist their intent before sending it and allocate a new
UUID only for a genuinely new compensation operation.

A lookup after admission failure recovers concurrent winners and committed writes
whose acknowledgement was lost. This also recovers an ambiguous commit within a
legacy creation call. Legacy retries can discover an active run through the new
GET endpoint or `orch8 execution compensation-active`. Terminal runs remain
available by known ID and keyed replay while their records are retained.

Both backends use their existing partial unique active-run index. Active and
known-ID reads share one decoder that checks the saved record against indexed
tenant, continuity, run ID, state and version. Corrupt records fail closed rather
than exposing foreign scope or a false completed outcome. The encryption wrapper
decrypts compensation parameters after this validation, and the fault-injection
forwarder covers the added method. Custom storage backends must implement the
active lookup; its default refuses unsupported recovery. No schema or dependency
change is required.

The isolated change passes 64 Rust tests: 48 storage cases on PostgreSQL, SQLite
and their encryption wrappers, 15 HTTP safety cases and a deterministic scoped
identity unit test. Corruption regression failed on all four variants before the
shared decoder fix. The final matrix covers all five metadata fields on both
lookup paths, concurrent keyed starts, lost response and commit acknowledgement,
completed replay, immutable saved-plan replay after evidence changes, active
lookup, tenant/continuity isolation and malformed request rejection.

Independent offline owners still require durable stop/outcome acknowledgement;
request deduplication does not establish distributed quiescence. Immutable
invocation-specific compensation rules and parameters, acknowledged archival
with retained receipts, and the broader mobile transfer work remain open.

All 129 TypeScript invariant, compensation and runtime E2E cases also pass
against the isolated server and PostgreSQL with no skips or failures. The CLI
recovery command builds and exposes the expected scoped command-line interface.

Strict all-target Clippy passes for storage, engine, API, gRPC, CLI and mobile;
formatting, TypeScript typecheck and documentation checks pass. CI and the
source-fence prerequisite must pass before this follow-up is merged.

The full API unit suite exposed a missing OpenAPI operation for active recovery.
That operation is now registered. Creation and active lookup use typed path
annotations with required scopes, a strict optional-request-ID body and durable
run response schemas; generic continuity registration preserves those typed
operations. All 367 API unit tests pass, including route inventory and generated
request/parameter/response contracts, alongside all 15 HTTP safety cases and
strict all-target API Clippy. Runtime handler bodies are unchanged.

The CI generated-client gate caught the expected OpenAPI fingerprint change. The representative Rust and JavaScript clients regenerate, compile/check and execute successfully; the regenerated contract fingerprint is updated and the complete generated-client gate passes locally.
