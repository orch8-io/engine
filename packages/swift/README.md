# Orch8 Mobile for Swift

Run server-configurable, durable workflows on iOS with the Orch8 `0.7.1`
engine embedded in your application.

## Swift Package Manager

```swift
dependencies: [
    .package(
        url: "https://github.com/orch8-io/orch8-mobile-swift",
        exact: "0.7.1"
    ),
]
```

Add the `Orch8Mobile` product to your application target, then:

```swift
import Orch8Mobile
```

Requires iOS 16 or newer and Xcode 16 or newer.

## CocoaPods

```ruby
pod 'Orch8Mobile', '0.7.1'
```

The package pins the immutable XCFramework from the Orch8 engine `v0.7.1`
release by SHA-256 checksum.

## Trusted-device handoff

`TrustedDeviceHandoffCoordinator` drives the retry-safe Cloud → iPhone and
iPhone → Cloud ownership transitions around the embedded engine. Your backend
must fetch the handoff envelope over an authenticated channel; never put the
capsule payload key, acceptance token, or signed grant in a push notification.
The coordinator is currently available from this source checkout and must be
included in the next tagged Swift package release; the published `0.7.1` tag
contains the underlying continuity engine but predates this convenience API.

```swift
let transport = try URLSessionTrustedDeviceHandoffTransport(
    baseUrl: URL(string: "https://api.example.com")!,
    headers: ["authorization": "Bearer \(deviceToken)"]
)
let handoffs = TrustedDeviceHandoffCoordinator(
    runtime: engine,
    transport: transport
)

// `envelope` was fetched after a silent push containing only a handoff ID.
let active = try await handoffs.receive(envelope)

// Use only inside an OS-granted foreground/background execution window.
let run = try await handoffs.runBackgroundWindow(timeBudgetMs: 20_000)
if run.hasPendingWork {
    scheduleAnotherBackgroundTask()
}

// The backend creates a cloud destination and returns this bounded plan.
let receipt = try await handoffs.returnToCloud(
    active: active,
    plan: returnPlan,
    signer: deviceCapsuleSigner
)
```

The receive operation is deliberately safe to retry as one unit. It imports
the capsule, claims server-side ownership, activates the local instance, and
records resume evidence. A return is allowed only while the local instance is
paused or waiting, preventing ownership transfer in the middle of an effect.

## Runtime node (remote worker)

`Orch8RuntimeNode` joins the distributed-execution mesh as a `mobile` runtime
and runs server-placed steps with your registered `StepHandler`s. The loop
itself runs in Rust (`MobileEngine.registerNode` / `startWorker`): it
heartbeats per lease, journals claims so tasks held when iOS kills the app are
released on the next launch, and polls immediately on push.

```swift
try engine.registerHandler(name: "scan_document", handler: ScanHandler())
let node = Orch8RuntimeNode(engine: engine)
try await node.setTokenProvider { try await backend.deviceSession(runtimeId: node.runtimeId()) }
try await node.join(capabilities: NodeCapabilities(hardware: ["camera"], pushToken: apnsToken))
try node.startWorker()

// AppDelegate silent push:
node.handlePush(userInfo: userInfo)
_ = try await node.runBackgroundWindow(seconds: 25)
```

Handlers get the task params plus `__orch8.effect_id`, the idempotency key to
forward to downstream APIs.

### Authenticating the node: device sessions

**Never ship an operator key (or any long-lived API key) in the app** — anyone
can extract it from the binary and act on your tenant. The recommended flow:

1. Your app backend holds the operator key.
2. After authenticating the user its own way, it mints a short-lived device
   session (`dst_…`) for this device's `deviceId` and `node.runtimeId()`,
   limited to the handlers the phone runs
   (`POST /api/v1/runtimes/device-sessions`).
3. The app fetches it through `Orch8RuntimeNode.setTokenProvider` **before**
   `join`. Every control-plane call (registration, worker leases, delegation,
   `/mobile/sync`) carries the token; on a `401` (expired session) the SDK
   awaits your closure again, off the main thread, and retries once.

```swift
// App
let node = Orch8RuntimeNode(engine: engine)
let runtimeId = try node.runtimeId()
try await node.setTokenProvider(refreshTimeout: 30) {
    var request = URLRequest(url: URL(string: "https://app.example.com/orch8/device-session")!)
    request.httpMethod = "POST"
    request.setValue("Bearer \(userSession)", forHTTPHeaderField: "authorization")
    request.setValue("application/json", forHTTPHeaderField: "content-type")
    request.httpBody = try JSONEncoder().encode(["deviceId": deviceId, "runtimeId": runtimeId])
    let (data, _) = try await URLSession.shared.data(for: request)
    return try JSONDecoder().decode(DeviceSession.self, from: data).token
}
try await node.join()
```

```ts
// Your backend (Node), with @orch8.io/sdk. The operator key never leaves it.
import { Orch8Client } from "@orch8.io/sdk";

const orch8 = new Orch8Client({
  baseUrl: "https://orch8.example.com",
  tenantId: "my-tenant",
  headers: { "x-api-key": process.env.ORCH8_OPERATOR_KEY! },
});

app.post("/orch8/device-session", requireUser, async (req, res) => {
  // Check that req.body.deviceId belongs to the signed-in user first.
  const session = await orch8.createDeviceSession({
    deviceId: req.body.deviceId,
    runtimeId: req.body.runtimeId,
    handlers: ["scan_document"], // [] = delegation-only phone
    ttlSecs: 3600, // default 3600, max 86400
  });
  res.json({ token: session.token, expiresAt: session.expiresAt });
});
```

The raw call is `POST /api/v1/runtimes/device-sessions` with
`{"device_id", "runtime_id", "handlers", "ttl_secs"}` and the operator key in
`x-api-key`; it answers `{"token", "device_id", "runtime_id", "expires_at",
"handlers"}`. A synchronous `TokenProvider` can be passed to
`node.setTokenProvider(_:)` (or `MobileEngine.setTokenProvider`) instead; its
`refreshToken()` runs on a background thread and may block on I/O.

`MobileEngineConfig.syncApiKey` is the **legacy** path and not for production
apps: a stored key in the app is extractable, and an operator-capable key makes
the SDK log a warning.

## Distributed work pickup (low level)

`DistributedWorkerClient` lets the phone participate as a leased worker
without moving the whole embedded execution. It advertises current runtime
facts during polling, uploads a selected file with a stable idempotency UUID,
and completes the task so the waiting Cloud workflow resumes. See
[`docs/MOBILE_SDK.md`](../../docs/MOBILE_SDK.md#capability-routed-distributed-work)
for the CUDA/region/browser requirement shape and a complete Swift example.
