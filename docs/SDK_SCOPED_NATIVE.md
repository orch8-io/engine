# Scoped native SDK development API

The development Rust/UniFFI surface exposes `MobileAccount`, `MobileCatalog` and
`MobileCatalogRun` objects. Generated Swift and Kotlin bindings expose the same
typed objects and records. This API is not in the published v0.7.1 native binary.
Use generated bindings with the native library built from the same source.
The [common KMP facade](../packages/kmp/README.md#multiple-accounts-and-catalogs-development-sources)
wraps these objects through direct JVM/Android adapters and a protocol-4 Swift
bridge. Release artifacts and pins, RN/Flutter facade integration, scoped change
observation, account-owned worker/delegation and authenticated outcome reporting
remain rollout requirements. [ADR-0002](adr/0002-mobile-account-and-catalog-scopes.md) S1–S12 are not complete.

## Identity and lifetime

Call `engine.open_account` with an account key and the trusted signed-in issuer
and subject. Keys are bound permanently to that principal; refresh does not
change identity. Opening an active account returns its current session. Explicit
retirement followed by reopen creates a new session epoch.

Open catalogs with a stable catalog key, signed audience source, Ed25519 signing
authority, manifest URL and identity URL. Public endpoints require canonical
public HTTPS with no URL credentials, query or fragment. Bindings are immutable:
opening the same catalog with another authority, source or endpoint is refused.
Signed sync verifies a fresh nonce-bound identity response against the original
principal and catalog before downloading private content. Pass that operation's
token provider; switching a UI's selected account never changes a handle's owner.

The explicit `close_view` operation (`closeView` in Swift/Kotlin) closes a local
account/catalog/run view and its descendant views.
It does not cancel durable work or retire an account. Issued child views remain usable when temporary parent views are dropped.
Kotlin's generated `AutoCloseable.close()` disposes the native object; it does
not retire the account or fence already-issued children.
Views retain weak engine references;
releasing or shutting down the engine makes remaining views unavailable. Explicit
account `retire` durably fences that exact session and detaches its registered
callbacks from every catalog. An accepted retirement settles independently of its
observer; arbitrary host code can finish later and retains physical capacity until
it and its foreign references are actually destroyed.

## Dispatch and reads

1. Register catalog handlers. They receive `MobileCatalogCallbackContext` with
   engine-issued account, catalog, original execution epoch, run, block, attempt
   and effect ID. These fields never come from workflow JSON and are not bearer
   authorization. Use the effect ID when a host operation needs idempotency.
2. Sync the signed catalog with the matching principal's provider.
3. `prepare_run` freezes the selected definition and persists its request. Supply
   an operation key. An identical retry returns the same request; changing the
   request or reusing a retired execution's key is refused.
4. Retain the returned run, then call `start`. Dispatch is separately retryable.
   A materialization receipt prevents replay after native instance garbage
   collection. Existing foreground/background engine scheduling executes the run.
5. Read `snapshot` or bounded catalog `history`. Global legacy read, listener and
   reporting APIs do not expose scoped private data.

A snapshot returns only owned instance data. `pending_steps` comes from actual
waiting step nodes, with the current-step fallback only when no tree exists.
The snapshot's `session_epoch` describes the viewing session; history and callback
records retain the original execution epoch. Snapshot construction rechecks the
owned instance version after reading its tree and bounds retries under churn.
It is not a transaction snapshot spanning independent engine tree writes.

Pass the snapshot's exact `updated_at` to `complete_step`. A stale version or
nonpending step returns false without committing an answer. The backend checks
ownership, current execution epoch, instance version and pending step in the same
transaction that merges the answer and enqueues its signal. Cancellation returns
the native outcome, preserving terminal results. If concurrent materialization
wins a queued cancellation, no cancellation is claimed; retry the run operation.

History uses live keyset pagination with page sizes 1–128. Opaque cursor objects
are bound to account, catalog and viewing session and never authorize access.
Pages contain request metadata, not input or native context. `Unavailable` native
state means missing or inconsistent evidence; it is not an execution outcome.
Use `forget_cancelled_request` only to release a cancelled never-materialized
request. Materialized executions keep their durable effect evidence.

## Bounds

Native scoped request admission shares durable ceilings of 4096 records/64 MiB
globally, 1024 records/16 MiB per account, and 256 records/4 MiB per catalog.
Each input is at most 1 MiB. Cancelled requests remain charged until explicitly
forgotten. Existing physical handler, registration, token callback and execution
limits still apply. Reopening a session does not free an unsettled callback's
capacity. Catalog sync shares one coordinator per engine, with one operation per
account and two globally. Admission follows both the owned logical sync and the
physical token callback through foreign-reference destruction. Closing a view or
retiring/reopening its account cannot free the old account charge while a getter
or destructor is blocked; another account can use the remaining slot. This
proves one-engine fairness, not an independent-engine/process admission guarantee.
