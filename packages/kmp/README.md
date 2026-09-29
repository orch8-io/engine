# Orch8 for Kotlin Multiplatform

`io.orch8:orch8-kmp` gives shared Kotlin code one coroutine/Flow API for the
embedded Orch8 workflow engine on Android and iOS. It does not contain its own
engine. It wraps the same native runtime the platform SDKs ship:

| Target | What it calls | How |
|---|---|---|
| `androidMain` | `io.orch8:orch8-mobile` AAR ([`packages/android`](../android)), the UniFFI Kotlin bindings over the Rust engine | Direct Kotlin calls (`src/uniffiJvm`) |
| `iosMain` | The `Orch8Mobile` Swift package ([`packages/swift`](../swift)), the UniFFI Swift bindings over the same engine | A small Swift adapter you compile into the app ([`ios-bridge/Orch8KmpBridge.swift`](ios-bridge/Orch8KmpBridge.swift)) |
| `jvm` | The checked-in generated bindings (`orch8-mobile/bindings/kotlin`) | Used for tests and host tooling. At runtime it needs a host build of `liborch8_mobile` |

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
3. Install [`ios-bridge/Orch8KmpBridge.swift`](ios-bridge/Orch8KmpBridge.swift)
   into the app target with
   [`ios-bridge/install-bridge.sh`](ios-bridge/install-bridge.sh):

   ```bash
   bash install-bridge.sh iosApp/iosApp Shared <version>
   ```

   It downloads `Orch8KmpBridge-v<version>.swift` from the engine release,
   verifies its SHA-256 against the published `.sha256`, and rewrites
   `import Orch8Kmp` to your shared framework's module name (`Shared` here;
   omit it if you link `Orch8Kmp` directly). The bridge ships as a source
   file rather than a Swift package or pod on purpose: it implements Kotlin
   protocols (`Orch8JsonBridgeFactory`) exported by *your* framework, whose
   module name is only known in your project. A prebuilt package would link
   a second Kotlin/Native runtime, and its types would not be the ones your
   shared code checks for.
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
Both sides check a protocol version (`ORCH8_BRIDGE_PROTOCOL = 1`) when the
engine opens, so a stale adapter fails immediately with a clear message.

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

## Authenticating the device: device sessions (engine release after 0.7.1)

**Never ship an operator key (or any long-lived stored API key) in an app**:
anyone can extract it from the binary. The recommended flow:

1. Your **app backend** holds the operator key. After authenticating the user
   its own way, it mints a short-lived device session for this device's
   `deviceId` and `engine.nodeRuntimeId()` with
   `POST /runtimes/device-sessions`.
2. The **app** fetches that `dst_…` token from its backend through the token
   provider. `setTokenProvider` awaits the first token, and the engine calls
   the same suspend function again whenever the control plane answers `401`
   (the session expired), then retries the request once.

Backend (Node, [`@orch8.io/sdk`](https://github.com/orch8-io/sdk-node)):

```typescript
import { Orch8Client } from "@orch8.io/sdk";

const orch8 = new Orch8Client({ baseUrl: "https://api.example.com", tenantId: "acme",
  headers: { "x-api-key": process.env.ORCH8_OPERATOR_KEY! } });

// POST /device-session  { deviceId, runtimeId }  (behind your own user auth)
app.post("/device-session", requireUser, async (req, res) => {
  const session = await orch8.createDeviceSession({
    deviceId: req.body.deviceId,
    runtimeId: req.body.runtimeId,          // the app's engine.nodeRuntimeId()
    handlers: ["scan_document"],            // handler allowlist; [] = delegation-only
    ttlSecs: 3600,                          // default 3600, max 86400
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
refresh runs on the engine's background thread, bounded by `refreshTimeout`
(default 30 s). On iOS it needs an `Orch8KmpBridge.swift` from the same
release; an older bridge answers `setTokenProvider` with `INVALID_INPUT`.

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

## API map

| `MobileEngine` (Swift / Android) | `Orch8Engine` (KMP) |
|---|---|
| `init(dbPath:config:)` / `MobileEngine(dbPath, config)` | `Orch8Engine.open(dbPath, EngineConfig)` |
| `registerHandler(name, StepHandler)` | `registerHandler(name) { stepName, inputJson -> outputJson }` (suspend) |
| `setListener(EngineListener)` | `events: SharedFlow<EngineEvent>`, `pendingSteps: StateFlow` |
| `resume()` / `pause()` / `shutdown()` | `resume()` / `pause()` / `close()` |
| `tickOnce()` / `runUntilIdle(maxTicks, timeBudgetMs)` | `suspend tick()` / `suspend runUntilIdle(maxTicks, Duration)` |
| `start` / `cancelInstance` / `getInstance` / `activeInstances` / `completeStep` | same names, `suspend`, `JsonObject` overloads, plus `answer(id, step, choice, data)` for `wait_for_input` gates |
| `loadSequenceFromJson` / `loadSequencesFromUrl` / `loadedSequences` | `loadSequence` / `loadSequencesFromUrl` / `loadedSequences` |
| `sync(manifestUrl, TokenProvider?)` | `sync(manifestUrl, Orch8TokenSource?)` |
| `flushTelemetry` / `setDeviceContext` / `reportPowerState` / `onPushReceived` | same, plus `PowerState.fromBattery(level, charging)` |
| `importContinuityCapsule` / `activateContinuityCapsule` | same |
| `exportContinuityCapsule(..., signer)` | Not wrapped. It needs a Secure Enclave/KeyStore signer, so call it from the platform SDK (same boundary as `@orch8.io/expo`) |
| `nodeRuntimeId` / `registerNode` / `updateNodeStatus` / `unregisterNode` | same names, `suspend`, common `NodeCapabilities` / `NodeRegistration` / `NodeConnectivity` |
| `setTokenProvider(TokenProvider)` | `suspend setTokenProvider(refreshTimeout) { fetchToken() }` (suspend fetch of a device session) |
| `startWorker(WorkerOptions)` / `stopWorker` / `runWorkerWindow(timeBudgetMs)` / `workerStats` | `suspend startWorker(WorkerOptions)` / `stopWorker()` / `runWorkerWindow(Duration)` / `workerStats()` |
| `startDelegation(DelegationOptions)` / `stopDelegation` / `delegate(DelegateRequest)` / `delegationStatus` / `listDelegations` / `delegationStats` | same names, `suspend`, common types (`pollInterval` / `ttl` as `Duration`, `DelegationState` enum), plus `observeDelegation(id): Flow` |
| `onPushWake(envelopeJson)` / `enableBuiltin(name)` | same, plus `onPushWake(taskId, runtimeId, reason)` |
| Swift-only `DistributedWorkerClient`, `TrustedDeviceHandoffCoordinator` | Not wrapped. Use `packages/swift` directly |

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
