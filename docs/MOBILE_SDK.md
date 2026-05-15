# Orch8 Mobile SDK

Server-configurable workflows running on-device. Update onboarding flows, promotions, and feature journeys without app store deployments.

## Architecture

```
┌─────────────────────────────┐
│         Host App            │
│  (Swift / Kotlin / RN)      │
├─────────────────────────────┤
│     UniFFI Bindings          │
├─────────────────────────────┤
│     orch8-mobile (Rust)      │
│  ┌──────────┬──────────┐    │
│  │ Scheduler│  Sync    │    │
│  │          │Orchestr. │    │
│  ├──────────┼──────────┤    │
│  │  SQLite Storage     │    │
│  └─────────────────────┘    │
└─────────────────────────────┘
```

The SDK embeds the full orch8 engine compiled as a native library. Sequences are synced from your server, verified with Ed25519 signatures, stored in a local SQLite database, and executed entirely on-device.

## Installation

### iOS (Swift Package Manager)

Add the package dependency in Xcode or `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/orch8-io/orch8-mobile-swift", from: "0.1.0"),
]
```

Or use the local path during development:

```swift
.package(path: "../packages/swift")
```

**Requirements:** iOS 15+, Xcode 15+.

### Android (Gradle)

Add the AAR dependency:

```kotlin
dependencies {
    implementation("io.orch8:orch8-mobile:0.1.0")
}
```

Or use a local project reference during development:

```kotlin
implementation(project(":orch8-mobile"))
```

**Requirements:** Android API 24+ (Android 7.0), JDK 17.

## Quick Start

### 1. Initialize the Engine

**Swift:**
```swift
import Orch8Mobile

let config = MobileEngineConfig(
    tickIntervalMs: 100,
    maxConcurrentSteps: 4,
    maxStepsPerInstance: 1000,
    maxConcurrentInstances: 10,
    maxTickDurationMs: 5000,
    maxInstanceLifetimeSecs: 86400,
    maxStoredSequences: 50,
    maxSequenceSizeBytes: 1_048_576,
    handlerTimeoutMs: 30000,
    operationTimeoutMs: 10000,
    telemetryEnabled: true,
    environment: "production",
    rootPublicKey: "<base64-ed25519-public-key>",
    sdkVersion: "0.1.0"
)

let engine = try MobileEngine(
    dbPath: FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        .appendingPathComponent("orch8.db").path,
    config: config
)
```

**Kotlin:**
```kotlin
import io.orch8.mobile.*

val config = MobileEngineConfig(
    tickIntervalMs = 100u,
    maxConcurrentSteps = 4u,
    maxStepsPerInstance = 1000u,
    maxConcurrentInstances = 10u,
    maxTickDurationMs = 5000u,
    maxInstanceLifetimeSecs = 86400u,
    maxStoredSequences = 50u,
    maxSequenceSizeBytes = 1_048_576u,
    handlerTimeoutMs = 30000u,
    operationTimeoutMs = 10000u,
    telemetryEnabled = true,
    environment = "production",
    rootPublicKey = "<base64-ed25519-public-key>",
    sdkVersion = "0.1.0"
)

val dbPath = context.getDatabasePath("orch8.db").absolutePath
val engine = MobileEngine(dbPath, config)
```

### 2. Register Step Handlers

Step handlers are native callbacks invoked when a workflow step executes. They receive JSON input and return JSON output.

**Swift:**
```swift
class ShowScreenHandler: StepHandler {
    func execute(stepName: String, input: String) throws -> String {
        let data = try JSONDecoder().decode(ScreenParams.self, from: input.data(using: .utf8)!)
        // Present UI, collect user response...
        return "{\"action\": \"continue\"}"
    }
}

try engine.registerHandler(name: "show_screen", handler: ShowScreenHandler())
```

**Kotlin:**
```kotlin
class ShowScreenHandler : StepHandler {
    override fun execute(stepName: String, input: String): String {
        val params = Json.decodeFromString<ScreenParams>(input)
        // Present UI, collect user response...
        return """{"action": "continue"}"""
    }
}

engine.registerHandler("show_screen", ShowScreenHandler())
```

### 3. Sync Sequences

Pull sequence definitions from your server. The manifest is Ed25519-signed to prevent tampering.

```swift
let result = try engine.sync(
    manifestUrl: "https://api.example.com/mobile/manifest.json",
    tokenProvider: nil  // or a TokenProvider for authenticated endpoints
)
print("Synced: \(result.added) added, \(result.updated) updated, \(result.removed) removed")
```

### 4. Start the Engine

```swift
// Resume the background tick loop
engine.resume()

// Start a workflow instance
let instanceId = try engine.start(
    sequenceName: "onboarding_v2",
    input: "{\"user_id\": \"abc123\"}",
    dedupKey: "onboarding:abc123"  // prevents duplicate instances
)
```

### 5. Listen for Events

```swift
class MyListener: EngineListener {
    func onInstanceCompleted(instanceId: String, output: String) {
        print("Completed: \(instanceId)")
    }

    func onInstanceFailed(instanceId: String, error: String) {
        print("Failed: \(instanceId): \(error)")
    }

    func onStepPending(instanceId: String, stepName: String, handler: String) {
        // A step needs user interaction — show the appropriate UI
        print("Pending: \(instanceId) step=\(stepName) handler=\(handler)")
    }
}

engine.setListener(listener: MyListener())
```

## API Reference

### MobileEngine

| Method | Description |
|--------|-------------|
| `new(dbPath, config)` | Create engine with SQLite database at path |
| `registerHandler(name, handler)` | Register a native step handler (before `resume()`) |
| `setListener(listener)` | Set the lifecycle event listener |
| `resume()` | Start the background tick loop |
| `pause()` | Pause the tick loop (waits for current tick) |
| `tickOnce()` | Execute a single tick manually |
| `start(sequenceName, input, dedupKey?)` | Start a workflow instance, returns instance ID |
| `cancelInstance(instanceId)` | Cancel a running instance |
| `getInstance(instanceId)` | Get instance state snapshot |
| `activeInstances()` | List all non-terminal instances |
| `completeStep(instanceId, stepName, output)` | Complete a step in Waiting state |
| `loadSequenceFromJson(json)` | Load a sequence directly (bypasses sync) |
| `loadedSequences()` | List locally stored sequences |
| `sync(manifestUrl, tokenProvider?)` | Sync sequences from remote manifest |
| `flushTelemetry(endpointUrl)` | Flush buffered telemetry events |
| `setDeviceContext(ctx)` | Set device info for telemetry |
| `shutdown()` | Shut down the engine |

### MobileEngineConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tickIntervalMs` | u64 | 100 | Tick loop interval |
| `maxConcurrentSteps` | u32 | 4 | Parallel step executions |
| `maxStepsPerInstance` | u32 | 1000 | Step limit per instance |
| `maxConcurrentInstances` | u32 | 10 | Active instance limit |
| `maxTickDurationMs` | u64 | 5000 | Tick budget |
| `maxInstanceLifetimeSecs` | u64 | 86400 | Auto-expire after 24h |
| `maxStoredSequences` | u32 | 50 | Local sequence limit |
| `maxSequenceSizeBytes` | u64 | 1MB | Max JSON size per sequence |
| `handlerTimeoutMs` | u64 | 30000 | Handler call timeout |
| `operationTimeoutMs` | u64 | 10000 | Sync API call timeout |
| `telemetryEnabled` | bool | true | Telemetry collection |
| `environment` | String | "production" | Target environment |
| `rootPublicKey` | String | "" | Ed25519 key for manifest verification |
| `sdkVersion` | String | crate version | SDK version for compatibility checks |

### StepHandler Protocol

```swift
protocol StepHandler {
    func execute(stepName: String, input: String) throws -> String
}
```

- `input`: JSON string with step parameters from the sequence definition
- Returns: JSON string merged into the execution context
- Throw `HandlerError.retryable(message:)` for transient failures (step retries)
- Throw `HandlerError.permanent(message:)` for fatal failures (instance fails)

### EngineListener Protocol

```swift
protocol EngineListener {
    func onInstanceCompleted(instanceId: String, output: String)
    func onInstanceFailed(instanceId: String, error: String)
    func onStepPending(instanceId: String, stepName: String, handler: String)
}
```

## Background Execution

### iOS

Use `BGProcessingTask` for long-running background sync:

```swift
import BackgroundTasks

func scheduleSync() {
    let request = BGProcessingTaskRequest(identifier: "io.orch8.sync")
    request.requiresNetworkConnectivity = true
    request.earliestBeginDate = Date(timeIntervalSinceNow: 3600)
    try? BGTaskScheduler.shared.submit(request)
}

BGTaskScheduler.shared.register(forTaskWithIdentifier: "io.orch8.sync", using: nil) { task in
    let syncTask = task as! BGProcessingTask
    let result = try? engine.sync(manifestUrl: manifestUrl, tokenProvider: nil)
    syncTask.setTaskCompleted(success: result != nil)
}
```

Call `engine.pause()` in `applicationDidEnterBackground` and `engine.resume()` in `applicationWillEnterForeground`.

### Android

Use `WorkManager` for periodic sync:

```kotlin
class Orch8SyncWorker(ctx: Context, params: WorkerParameters) : CoroutineWorker(ctx, params) {
    override suspend fun doWork(): Result {
        return try {
            engine.sync(manifestUrl, null)
            Result.success()
        } catch (e: Exception) {
            Result.retry()
        }
    }
}

val syncRequest = PeriodicWorkRequestBuilder<Orch8SyncWorker>(1, TimeUnit.HOURS)
    .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
    .build()
WorkManager.getInstance(context).enqueueUniquePeriodicWork("orch8-sync", KEEP, syncRequest)
```

Use `LifecycleObserver` to pause/resume:

```kotlin
class Orch8LifecycleObserver(private val engine: MobileEngine) : DefaultLifecycleObserver {
    override fun onStart(owner: LifecycleOwner) { engine.resume() }
    override fun onStop(owner: LifecycleOwner) { engine.pause() }
}
```

## Troubleshooting

### "sync not configured"
Set `rootPublicKey` in `MobileEngineConfig`. Generate a key pair with:
```bash
openssl genpkey -algorithm ed25519 -out private.pem
openssl pkey -in private.pem -pubout -outform DER | base64
```

### "handler timed out"
The default handler timeout is 30 seconds. For handlers that need user interaction, the step transitions to `Waiting` state. Use `completeStep()` when the user responds.

### "max concurrent instances reached"
Reduce active instances by calling `cancelInstance()` on stale ones, or increase `maxConcurrentInstances`.

### "sequence size exceeds limit"
Increase `maxSequenceSizeBytes` or reduce the sequence JSON size.

### Crash recovery
The engine sets a `dirty` flag when `pause()` times out. On the next `resume()`, it automatically recovers stale instances that were mid-execution when the app was killed.

### SQLite errors
The SDK uses WAL mode for concurrent reads. Ensure only one `MobileEngine` instance exists per database file.

## Building from Source

### iOS XCFramework

```bash
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
./scripts/build-xcframework.sh --release
```

Output: `packages/swift/Orch8Mobile.xcframework`

### Android AAR

```bash
rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
export ANDROID_NDK_HOME=/path/to/ndk
./scripts/build-android-aar.sh --release
```

Output: `packages/android/orch8-mobile/build/outputs/aar/orch8-mobile-release.aar`
