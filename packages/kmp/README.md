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

> The `orch8-kmp` artifact is not published yet. Until it is, build it locally
> with `gradle publishToMavenLocal` from this directory and add `mavenLocal()`,
> or use `includeBuild("path/to/engine/packages/kmp")`.

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
   into the app target. If your shared module re-exports this library under
   another framework name, change `import Orch8Kmp` to match.
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
        syncApiKey = apiKey,
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
