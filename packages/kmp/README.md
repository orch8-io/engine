# Orch8 for Kotlin Multiplatform

`io.orch8:orch8-kmp` gives shared Kotlin code one coroutine/Flow API for the
embedded Orch8 workflow engine on Android and iOS. It does not contain its own
engine. It wraps the same native runtime the platform SDKs ship:

| Target        | What it calls                                                                                                   | How                                                                                                                   |
| ------------- | --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `androidMain` | `io.orch8:orch8-mobile` AAR ([`packages/android`](../android)), the UniFFI Kotlin bindings over the Rust engine | Direct Kotlin calls (`src/uniffiJvm`)                                                                                 |
| `iosMain`     | The `Orch8Mobile` Swift package ([`packages/swift`](../swift)), the UniFFI Swift bindings over the same engine  | A small Swift adapter you compile into the app ([`ios-bridge/Orch8KmpBridge.swift`](ios-bridge/Orch8KmpBridge.swift)) |
| `jvm`         | The checked-in generated bindings (`orch8-mobile/bindings/kotlin`)                                              | Used for tests and host tooling. At runtime it needs a host build of `liborch8_mobile`                                |

Status: **preview**, versioned with the SDK family (`0.7.1`). The API mirrors
the `MobileEngine` surface of the Swift and Android packages. See the
[API map](#api-map).

## Relation to `packages/android`

`packages/android` publishes the raw UniFFI surface: blocking calls, unsigned
integer types, and callback interfaces. This library adds the following on top
of it. It does not replace it:

- `suspend` functions that move every FFI call to `Dispatchers.IO`.
- `events: SharedFlow<EngineEvent>` in place of an `EngineListener`.
- `pendingSteps: StateFlow<List<PendingStep>>` for "what is waiting on the user".
- `observeInstance(id): Flow<InstanceSnapshot>`, which completes at a terminal state.
- `suspend` step handlers, with `Orch8HandlerException.Permanent`/`Retryable`.
- One `Orch8Exception(kind)` error model on both platforms, plus `EngineConfig.validate()`.

An Android-only app can keep using `packages/android` directly. Use this
library when the workflow code (handlers, UI state, sync policy) lives in a
shared KMP module.

## Setup

### Gradle

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven("https://raw.githubusercontent.com/orch8-io/maven/main")
    }
}

// shared/build.gradle.kts
kotlin {
    sourceSets {
        commonMain.dependencies {
            implementation("io.orch8:orch8-kmp:0.7.1")
        }
    }
}
```

> `io.orch8:orch8-kmp` is published to Orch8's Maven repository by the engine
> release workflow (`.github/workflows/mobile-distribution.yml`), starting
> with the first release after `0.7.1`. For `0.7.1` itself, build it locally
> with `gradle publishToMavenLocal -Porch8.kmp.targets=android,ios` from this
> directory and add `mavenLocal()`, or use
> `includeBuild("path/to/engine/packages/kmp")`.
>
> The published artifacts are `orch8-kmp` (root, Gradle module metadata),
> `orch8-kmp-android`, `orch8-kmp-iosarm64`, `orch8-kmp-iossimulatorarm64` and
> `orch8-kmp-iosx64`. The JVM target is only for this repository's tests and
> is not published.

The Android variant pulls in `io.orch8:orch8-mobile` (the AAR with
`liborch8_mobile.so` for all ABIs), so Android needs no extra setup.
Requirements: minSdk 24, JDK 17.

### iOS

The Rust engine reaches iOS through the Swift package, not through cinterop.
UniFFI's C ABI (RustBuffer and callback vtables) is meant to be driven by the
generated Swift code. Re-implementing it in Kotlin/Native would duplicate that
code and could drift from it. The bridge works like this instead:

1. Add the `Orch8Mobile` Swift package (`https://github.com/orch8-io/orch8-mobile-swift`,
   exact `0.7.1`) to the iOS app.
2. Link the `Orch8Kmp` framework (or your shared framework that depends on
   this library). The framework is static, and its base name is `Orch8Kmp`.
3. Copy [`ios-bridge/Orch8KmpBridge.swift`](ios-bridge/Orch8KmpBridge.swift)
   from the same checkout as the KMP library into the iOS app target. From the
   engine repository root, set your app source directory and framework name:

   ```bash
   APP_IOS_SOURCES="/absolute/path/to/your/iosApp/iosApp"
   sed 's/^import Orch8Kmp$/import Shared/' \
     packages/kmp/ios-bridge/Orch8KmpBridge.swift \
     > "$APP_IOS_SOURCES/Orch8KmpBridge.swift"
   ```

   `Shared` must match your shared framework's `baseName`; leave the import as
   `Orch8Kmp` when linking that framework directly. Add the copied Swift file to
   the app target. When developing against `main`, build matching KMP and native
   components rather than mixing unreleased bridge APIs with an older binary.

   The [release installer](ios-bridge/install-bridge.sh) downloads
   `Orch8KmpBridge-v<version>.swift`, verifies its SHA-256, and rewrites the
   framework import. Use it only for a release containing both bridge assets.
   Checked 2026-10-23: `v0.7.1` does **not** contain these assets, so its download
   path fails; the current release workflow will attach them to future releases.
   The bridge is source rather than a prebuilt package because its Kotlin
   protocols are exported by your app's shared framework.

4. Install the bridge once at launch, before any Kotlin code opens the engine:

```swift
import Orch8Kmp

@main
struct InspectorApp: App {
    init() {
        Orch8Ios.shared.install(factory: Orch8KmpBridgeFactory())
    }
    // ...
}
```

The adapter is a switch over one `call(method, argsJson)` entry point. It
returns a JSON envelope (`{"ok": ...}` or `{"error": {"kind", "message"}}`).
Kotlin/Native does not carry a Swift `throws` into Kotlin as a typed
exception, and Kotlin cannot see UniFFI's Swift records. Keeping the Swift
side to plain strings means all decoding, validation and error mapping run
in common Kotlin (`JsonBridgeBackend`), where the JVM test suite covers them.
Both sides check a protocol version (`ORCH8_BRIDGE_PROTOCOL = 5`) when the
engine opens, so a stale adapter fails immediately with a clear message.

## Swift observation

The common Kotlin Flow API remains available to shared Kotlin code. Swift uses
typed subscriptions; raw Flow exports are hidden because their collection
boundary only converts cancellation errors and can terminate the process on
ordinary engine errors. Use the following replacements in a matching development
framework (these APIs are not in published `0.7.1` artifacts):

| Shared Kotlin stream | Swift subscription | Observer callback |
| --- | --- | --- |
| `observeInstance(id)` | `subscribeInstance(instanceId:observer:pollIntervalMs:)` | `InstanceSnapshotObserver.onInstance` |
| `observeActiveInstances()` | `subscribeActiveInstances(observer:pollIntervalMs:)` | `ActiveInstancesObserver.onInstances` |
| `observeDelegation(id)` | `subscribeDelegation(delegationId:observer:pollIntervalMs:)` | `DelegationStatusObserver.onStatus` |
| `events` | `subscribeEvents(observer:)` | `EngineEventObserver.onEvent` |
| `pendingSteps` | `subscribePendingSteps(observer:)` | `PendingStepsObserver.onSteps` |

Observers also implement `onError(Orch8Exception)` and `onComplete()`. Callbacks
run on IO; dispatch UI changes to the main thread. Close the returned
`Orch8ObservationSubscription` when the screen leaves. Closing one subscription
keeps other subscriptions and workflow/delegation execution intact. Engine close
cancels root and scoped SDK subscriptions immediately, including subscriptions
sleeping between polls, and refuses new ones. Cancellation has no
error/completion callback; a previously accepted host callback may finish.
Instance/delegation streams complete on a known terminal state. Hot event and
pending-step streams attach before the subscription call returns; events do not
replay past deliveries, while pending steps include the current list.

These root subscriptions preserve legacy visibility and exclude scoped private
data. Use the catalog run's `subscribeSnapshots` for scoped UI observation.
Shared Kotlin Flow collectors keep their caller-owned coroutine lifecycle; cancel
them when their UI or application owner leaves.
Root engine lifetime still requires explicit application shutdown; wrapper GC
tests do not prove native backend/resource destruction on every platform.

## Usage (common code)

```kotlin
val engine = Orch8Engine.open(
    dbPath, // Android: Orch8Engine.open(context, config); iOS: Orch8Ios.defaultDatabasePath()
    EngineConfig(
        syncUrl = "https://api.example.com/mobile/sync",
        deviceId = deviceId,
        // No syncApiKey: the node authenticates with device sessions, see below.
    ),
)

engine.registerHandler("load_assignment") { _, input ->
    repository.assignmentJson(input) // suspend is fine; runs on the engine's worker thread
}
engine.loadSequence(fieldInspectionJson)
engine.resume()

val id = engine.start("field-inspection", buildJsonObject { put("site_id", "A-12") }, dedupKey = "insp:A-12")

// UI: react to steps waiting on the user
engine.pendingSteps.collect { steps -> render(steps) }

// Answer a wait_for_input gate: the choice is stored under `store_as`; data keys merge into context.data.
engine.answer(id, "capture_checklist", choice = "complete", data = buildJsonObject { put("checklist", checklist) })

// Status
engine.observeInstance(id).collect { snapshot -> showState(snapshot.state) }
```

Background windows (`BGTaskScheduler`, `WorkManager`):

```kotlin
val result = engine.runUntilIdle(maxTicks = 25, timeBudget = 20.seconds)
if (result.budgetExhausted) scheduleAnotherWindow()
```

On a silent push, call `engine.onPushReceived()`. The next tick then syncs
approvals and commands with the server.

## Multiple accounts and catalogs (development sources)

The scoped API requires a KMP framework, protocol-5 Swift adapter, generated
bindings and native library built from the same source revision. Published
`0.7.1` binaries do not implement it. On Android, build and stage the matching
`orch8-mobile` AAR in a Maven repository and pass `-Porch8.dist.repo=/path/to/repo`
when building KMP. Use a distinct development version for both artifacts through
`ORCH8_MOBILE_VERSION`; do not replace a published version in a shared cache.
CI compiles the Android adapter against its own commit's AAR with a unique
version, runs real workflows on the JVM and macOS native host, and compiles the
actual iOS framework and Swift adapter. Host tests do not replace device tests.

```kotlin
val account = engine.openAccount(AccountIdentity(accountKey, trustedIssuer, trustedSubject))
val catalog = account.openCatalog(CatalogConfiguration(
    catalogKey, source, signingAuthority, manifestUrl, identityUrl,
))
catalog.sync(accountTokenSource) // explicit, caller-owned provider for this operation
catalog.registerHandler("load_assignment", { context, _, input ->
    // Native-issued context identifies the original account/catalog/run/effect.
    repository.assignmentJson(input)
})
val run = catalog.prepareRun(CatalogRunRequest(
    workflow = "field-inspection", operationKey = "insp:A-12", inputJson = inputJson,
))
run.start() // retry prepareRun with the same key and identical request after a restart
val snapshot = run.snapshot()
if (snapshot != null && "capture_checklist" in snapshot.pendingSteps) {
    val accepted = run.answer(snapshot, "capture_checklist", "complete")
    if (!accepted) refreshScreen() // another answer or state transition won
}
val page = catalog.history(limit = 64)
page.next?.use { cursor -> render(catalog.history(cursor).items) }
```

Obtain the principal and catalog configuration from trusted application state;
workflow input cannot select either. A changed principal requires a different
account key or explicit retirement. Root `engine.resume()` / `runUntilIdle()`
still drive execution, while scoped reads, handlers, sync and answers use these
owned objects. Root legacy observers do not expose scoped runs. Collect
`run.observeSnapshots()` for owned UI state and use paginated `history()` for
metadata. The cold snapshot flow polls only while collected, emits distinct
values, applies collector backpressure and completes after a known terminal
snapshot. A `null` snapshot means the native record is absent; it does not prove
success or cancellation, including before materialization or after GC. Cancel
collection when the screen leaves or a prepared request is abandoned. Account
retirement and close fences propagate their typed errors. Poll intervals must
be finite and positive for both scoped and legacy observers.

Swift uses `run.subscribeSnapshots(observer:pollIntervalMs:)`, returning a
`CatalogSnapshotSubscription` to close when its screen leaves. Implement
`CatalogSnapshotObserver.onSnapshot`, `onError` and `onComplete`; callbacks run on
the IO dispatcher, so dispatch UI changes to the main thread. Ordinary errors
are delivered through `onError`, and closing a subscription cancels its task
without closing the run or cancelling durable work. A previously accepted host
callback may finish. The scoped raw `Flow` method is available in shared Kotlin
and hidden from Objective-C/Swift: Kotlin's raw `Flow.collect` export only
converts cancellation errors and can abort the process on ordinary exceptions.

`close()` disposes one object, so a run survives disposal of its parent views.
`closeView()` explicitly fences that view and its descendants. `account.retire()`
durably retires its epoch across catalogs. None of these operations deletes
workflow history. Keep the root engine alive for the application's runtime
lifetime; scoped objects do not keep a stopped engine running.

History reports request metadata separately from native execution state.
`CatalogNativeState.Unavailable` after native GC does not prove success or
failure. `forgetCancelledRequest()` only reclaims a cancelled request that was
never materialized. Scoped credentials, reporting, workers/delegation, native event listeners
and materialized-history reclamation remain separate work.

## Authenticating the device: device sessions (engine release after 0.7.1)

**Never ship an operator key (or any long-lived stored API key) in an app**:
anyone can extract it from the binary. The recommended flow:

1. Your **app backend** holds the operator key. After authenticating the user
   its own way, it mints a short-lived device session for this device's
   `deviceId` and `engine.nodeRuntimeId()` with
   `POST /runtimes/device-sessions`.
2. The **app** fetches that `dst_…` token from its backend through the token
   provider. `setTokenProvider` reserves native ownership before awaiting the
   first token, and the engine calls
   the same suspend function again whenever the control plane answers `401`
   (the session expired), then retries the request once.

Backend (Node, [`@orch8.io/sdk`](https://github.com/orch8-io/sdk-node)):

```typescript
import { Orch8Client } from "@orch8.io/sdk";

const orch8 = new Orch8Client({
  baseUrl: "https://api.example.com",
  tenantId: "acme",
  headers: { "x-api-key": process.env.ORCH8_OPERATOR_KEY! },
});

// POST /device-session  { deviceId, runtimeId }  (behind your own user auth)
app.post("/device-session", requireUser, async (req, res) => {
  const session = await orch8.createDeviceSession({
    deviceId: req.body.deviceId,
    runtimeId: req.body.runtimeId, // the app's engine.nodeRuntimeId()
    handlers: ["scan_document"], // handler allowlist; [] = delegation-only
    ttlSecs: 3600, // default 3600, max 86400
  });
  res.json({ token: session.token, expiresAt: session.expiresAt });
});
```

App (common code), **before** `registerNode`:

```kotlin
val runtimeId = engine.nodeRuntimeId()
engine.setTokenProvider {
    myBackend.deviceSession(deviceId = deviceId, runtimeId = runtimeId).token // suspend HTTP call
}
engine.registerNode(NodeCapabilities(hardware = listOf("camera")))
```

A device session is bound to its tenant, device, runtime and handler
allowlist and reaches only this device's own mobile, worker-lease and
delegation calls (see `docs/MOBILE_SDK.md`, "Authenticating a phone"). The
initial and refresh waits use `refreshTimeout` (default 30 s, finite and
positive). Eight unfinished SDK fetches share admission across token-source
instances. Timeout and cancellation stop waiting while non-cooperative work
retains its slot until actual completion. Exhausted admission starts no host
work. Late results cannot restore a closed source's cached token.

Native installation tickets fence initial completion and cancellation against
replacement through another alias. On iOS each native provider captures an
immutable callback source ID, so retired callbacks cannot invoke a replacement
fetcher. Protocol 4, matching native reservation APIs, and matching generated
bindings are required; protocol-1 and protocol-2 Swift bridges fail during open. These
unreleased APIs must be shipped with matching native SDK artifacts after 0.7.1.

Manifest sync also captures an immutable, per-call token-source ID. Concurrent
syncs, including anonymous calls, cannot borrow or clear another call's
credentials. A sync's return or failure retires only its own ID; callbacks after
retirement cannot invoke the source or resolve a successor. Shutdown retires all
manifest IDs. The supplied source stays caller-owned, so sync does not close a
source also used for a node or another call. This callback change requires the
protocol-5 Swift bridge and KMP framework to ship together.

`EngineConfig.syncApiKey` is the **legacy** path and not for production apps:
it still works, and the SDK logs a warning when the server reports the key is
operator-capable.

## Runtime node (engine release after 0.7.1)

The device can join the distributed-execution mesh as a runtime of kind
`mobile`; the Rust worker loop runs the handlers you registered for tasks the
server places on this device. These calls need the Orch8 engine release that
follows `0.7.1` (on iOS, an `Orch8KmpBridge.swift` from the same release).

```kotlin
engine.registerHandler("scan_document") { _, input ->
    val task = Orch8TaskContext.fromInput(input)          // null for local steps
    scanner.scan(input, idempotencyKey = task?.effectId)  // effectId: server idempotency key
}
engine.registerNode(NodeCapabilities(hardware = listOf("camera"), pushToken = fcmToken))
engine.startWorker(WorkerOptions(maxConcurrentTasks = 1))

// Silent push: id-only wake hint, the worker polls a leased task at once.
engine.onPushWake(taskId = data["task_id"], runtimeId = data["runtime_id"], reason = data["reason"])

// WorkManager / BGTask window: claims even while paused, returns when idle or out of budget.
val window = engine.runWorkerWindow(25.seconds)

engine.unregisterNode() // advertise draining
```

## Delegating from a phone-local workflow (engine release after 0.7.1)

A step of a workflow running on the device's own engine whose `$runtime`
places it on another runtime (`runtime_id` of another node, or
`runtime_kinds` without `mobile`) is handed to that runtime through the server
mailbox; the local instance parks and resumes exactly once with the result,
across disconnects and app kills. Handler `orch8.delegation` delegates the
server-side sequence `params.sequence_id` with `params.input`; any other
handler delegates just that step. Needs `registerNode` and a node credential
allowed to call the continuity API.

```kotlin
engine.registerNode(NodeCapabilities())
engine.startDelegation(DelegationOptions(tenantId = "acme")) // on every launch

// Explicit delegation from app code (no local step is parked):
val id = engine.delegate(
    DelegateRequest(
        instanceId = localInstanceId,
        destinationRuntimeId = desktopRuntimeId,
        subSequenceId = classifySequenceId,
        inputJson = """{"photo":{"id":"$photoId"}}""",
    ),
)
engine.observeDelegation(id).collect { status ->
    // PREPARING -> DELEGATED -> COMPLETED (outputJson) | FAILED (error) | ABANDONED
}
engine.listDelegations()   // journal, oldest first
engine.delegationStats()   // running, delegated, completed, failed, abandoned, resumed
engine.stopDelegation()    // pause; journaled delegations resume on the next start
```

`onPushReceived()` / `onPushWake(...)` advance pending delegations at once.

## Database location and backups

`Orch8Engine.open(context, ...)` (Android) uses `noBackupFilesDir`, which is
excluded from Auto Backup and device transfer. `Orch8Ios.defaultDatabasePath()`
(iOS) returns `<Application Support>/Orch8/orch8.db`, excluded from backup and
protected until first unlock. A database that already exists at the previous
default (`getDatabasePath(...)` / `<Documents>/orch8.db`) keeps being used, so
upgrades do not lose data. Pass your own path to override.

## API map

| `MobileEngine` (Swift / Android)                                                                                                                   | `Orch8Engine` (KMP)                                                                                                                    |
| -------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `init(dbPath:config:)` / `MobileEngine(dbPath, config)`                                                                                            | `Orch8Engine.open(dbPath, EngineConfig)`                                                                                               |
| `registerHandler(name, StepHandler)`                                                                                                               | `registerHandler(name) { stepName, inputJson -> outputJson }` (suspend)                                                                |
| `setListener(EngineListener)`                                                                                                                      | `events: SharedFlow<EngineEvent>`, `pendingSteps: StateFlow`                                                                           |
| `resume()` / `pause()` / `shutdown()`                                                                                                              | `resume()` / `pause()` / `close()`                                                                                                     |
| `tickOnce()` / `runUntilIdle(maxTicks, timeBudgetMs)`                                                                                              | `suspend tick()` / `suspend runUntilIdle(maxTicks, Duration)`                                                                          |
| `start` / `cancelInstance` / `getInstance` / `activeInstances` / `completeStep`                                                                    | same names, `suspend`, `JsonObject` overloads, plus `answer(id, step, choice, data)` for `wait_for_input` gates                        |
| `loadSequenceFromJson` / `loadSequencesFromUrl` / `loadedSequences`                                                                                | `loadSequence` / `loadSequencesFromUrl` / `loadedSequences`                                                                            |
| `sync(manifestUrl, TokenProvider?)`                                                                                                                | `sync(manifestUrl, Orch8TokenSource?)`                                                                                                 |
| `flushTelemetry` / `setDeviceContext` / `reportPowerState` / `onPushReceived`                                                                      | same, plus `PowerState.fromBattery(level, charging)`                                                                                   |
| `importContinuityCapsule` / `activateContinuityCapsule`                                                                                            | same                                                                                                                                   |
| `exportContinuityCapsule(..., signer)`                                                                                                             | Not wrapped. It needs a Secure Enclave/KeyStore signer, so call it from the platform SDK (same boundary as `@orch8.io/expo`)           |
| `nodeRuntimeId` / `registerNode` / `updateNodeStatus` / `unregisterNode`                                                                           | same names, `suspend`, common `NodeCapabilities` / `NodeRegistration` / `NodeConnectivity`                                             |
| `setTokenProvider(TokenProvider)`                                                                                                                  | `suspend setTokenProvider(refreshTimeout) { fetchToken() }` (suspend fetch of a device session)                                        |
| `startWorker(WorkerOptions)` / `stopWorker` / `runWorkerWindow(timeBudgetMs)` / `workerStats`                                                      | `suspend startWorker(WorkerOptions)` / `stopWorker()` / `runWorkerWindow(Duration)` / `workerStats()`                                  |
| `startDelegation(DelegationOptions)` / `stopDelegation` / `delegate(DelegateRequest)` / `delegationStatus` / `listDelegations` / `delegationStats` | same names, `suspend`, common types (`pollInterval` / `ttl` as `Duration`, `DelegationState` enum), plus `observeDelegation(id): Flow` |
| `onPushWake(envelopeJson)` / `enableBuiltin(name)`                                                                                                 | same, plus `onPushWake(taskId, runtimeId, reason)`                                                                                     |
| Swift-only `DistributedWorkerClient`, `TrustedDeviceHandoffCoordinator`                                                                            | Not wrapped. Use `packages/swift` directly                                                                                             |

## Development

The common tests run on the JVM target, which also type-checks
`src/uniffiJvm` against the real generated bindings. You don't need an
Android SDK or a Mac for that:

```bash
# from packages/kmp (Gradle 8.9 + JDK 17), or via Docker:
docker run --rm -v "$PWD/../..":/work -w /work/packages/kmp gradle:8.9-jdk17 \
  gradle --no-daemon jvmTest -Porch8.kmp.targets=jvm
```

A full build (`gradle build`) needs the Android SDK (compileSdk 35) and, for
the iOS targets, macOS with Xcode 16. The Gradle wrapper JAR is not checked
in. Run `gradle wrapper --gradle-version 8.9` once to create `./gradlew`.

To execute the real JVM/native installation tests too, pass
`-Porch8.native.lib.path=/absolute/path/to/the/native/library/directory` to
`jvmTest`. Without that property those two tests are explicitly skipped. The
test task fingerprints the native library so a Rust change invalidates cached
results. Mobile CI runs this path against the host library built for the same
commit, compiles `linkDebugFrameworkIosSimulatorArm64`, and checks the actual
Swift/iOS bridge against that framework with `scripts/check-kmp-apple-bridge.sh`.


Acknowledged scoped history can be archived explicitly:

```kotlin
val receipt = run.archiveAfterAcknowledgement("backend-upload:receipt-id")
```

Call this only after the app/backend has acknowledged the run. The reference is
trusted host input, not a credential verified by the engine. Only its SHA-256
digest is retained. The operation atomically clears staged input and releases
hot admission quota, while retaining the operation key and materialization
receipt. An identical prepare retry returns the same archived run; start is
refused, including after native GC. A different request under that key is refused.
An identical acknowledgement retry returns the original receipt, while changing
its reference is refused. Active and never-materialized runs return null.

History reports `ARCHIVED` and `CatalogNativeState.Recorded` for an outcome
observed at archive time. If native GC already removed the run, the archive
outcome stays null and history reports `Unavailable`. A receipt is not a native
snapshot or an event stream. Native context, execution trees and effect evidence
remain subject to their existing GC lifecycle; archival does not erase those.

Compact receipts have separate shared ceilings: 65,536 globally, 32,768 per
account and 16,384 per catalog, and 64/32/16 MiB of encoded receipts respectively.
Each encoded receipt is at most 4 KiB. Existing compact keys are never silently
evicted: archive exhaustion returns the typed resource-limit error and leaves
staged input/admission unchanged. These are bounded retention ceilings, not a
promise of unlimited on-device execution history. Raw operation keys and frozen
request metadata remain in their original bounded request rows.
