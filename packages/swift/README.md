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

## Distributed work pickup

`DistributedWorkerClient` lets the phone participate as a leased worker
without moving the whole embedded execution. It advertises current runtime
facts during polling, uploads a selected file with a stable idempotency UUID,
and completes the task so the waiting Cloud workflow resumes. See
[`docs/MOBILE_SDK.md`](../../docs/MOBILE_SDK.md#capability-routed-distributed-work)
for the CUDA/region/browser requirement shape and a complete Swift example.
