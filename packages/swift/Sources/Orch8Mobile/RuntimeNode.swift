import Foundation

/// The phone as a runtime node in the Orch8 distributed-execution mesh.
///
/// A thin Swift wrapper over the Rust worker loop in `MobileEngine`:
/// `join` registers the device and its capabilities (re-advertised
/// automatically before the five-minute TTL), `startWorker` polls as kind
/// `mobile`, runs your registered `StepHandler`s, heartbeats per lease and
/// completes / fails / releases each task. Claims are journaled in the local
/// database, so a task held when iOS kills the app is released on the next
/// launch.
///
/// ```swift
/// try engine.registerHandler(name: "scan_document", handler: ScanHandler())
/// let node = Orch8RuntimeNode(engine: engine)
/// let runtimeId = try node.runtimeId()
/// try await node.setTokenProvider { try await myBackend.deviceSession(runtimeId: runtimeId) }
/// try await node.join(capabilities: NodeCapabilities(hardware: ["camera"], pushToken: apnsToken))
/// try node.startWorker()
/// ```
///
/// Handlers receive the task params plus a reserved `__orch8` object; pass
/// `__orch8.effect_id` to downstream APIs as the idempotency key.
public final class Orch8RuntimeNode: @unchecked Sendable {
    public let engine: MobileEngine

    public init(engine: MobileEngine) {
        self.engine = engine
    }

    /// Stable runtime UUID for this installation (the lease `worker_id`).
    public func runtimeId() throws -> String {
        try engine.nodeRuntimeId()
    }

    /// Authenticate every control-plane call (node registration, worker
    /// leases, delegation, sync) with device sessions from `fetch` instead of
    /// the legacy static `syncApiKey`.
    ///
    /// `fetch` should ask your backend for a short-lived device session
    /// (`POST /runtimes/device-sessions` with its operator key, for this
    /// device and `runtimeId()`). Native ownership is reserved before fetching;
    /// superseded completion cannot replace another alias's newer provider.
    /// It is awaited once now for the first token,
    /// and again whenever the control plane answers `401` (the request is
    /// then retried once). `refreshTimeout` bounds both waits. An initial
    /// failure throws; failed refresh leaves authorization unavailable or
    /// returns the original `401`. Eight unfinished Swift SDK fetches share
    /// process-wide admission across first fetches, refreshes, and retired
    /// providers. Exhaustion fails before invoking host code. Cancellation
    /// requests do not release capacity until host work actually returns.
    /// Call before `join`. Never ship an operator key in the app.
    public func setTokenProvider(
        refreshTimeout: TimeInterval = 30,
        _ fetch: @escaping @Sendable () async throws -> String
    ) async throws {
        let provider = AsyncTokenProvider(
            initialToken: "",
            timeout: refreshTimeout,
            fetch: fetch
        )
        try Task.checkCancellation()
        let engine = self.engine
        let installation = try engine.beginTokenProviderInstallation(provider: provider)
        do {
            try await withTaskCancellationHandler(operation: {
                let token = try await provider.fetchInitialToken()
                try Task.checkCancellation()
                guard try engine.completeTokenProviderInstallation(installationId: installation, token: token) else {
                    throw MobileError.Engine(message: "token provider installation superseded")
                }
                try Task.checkCancellation()
            }, onCancel: {
                _ = engine.cancelTokenProviderInstallation(installationId: installation)
            })
        } catch {
            _ = engine.cancelTokenProviderInstallation(installationId: installation)
            throw error
        }
    }

    /// Install a synchronous `TokenProvider` (see `MobileEngine.setTokenProvider`).
    /// `refreshToken()` runs on a background thread and may block on I/O.
    public func setTokenProvider(_ provider: TokenProvider) {
        engine.setTokenProvider(provider: provider)
    }

    /// Register the device + runtime capabilities. Safe to call on every
    /// launch; a second call updates the advertised facts. Runs off the
    /// caller's thread because it performs network I/O.
    @discardableResult
    public func join(capabilities: NodeCapabilities = NodeCapabilities()) async throws -> NodeRegistration {
        let engine = self.engine
        return try await Task.detached(priority: .utility) {
            try engine.registerNode(capabilities: capabilities)
        }.value
    }

    /// Report battery / connectivity changes (also refreshes liveness).
    public func updateStatus(connectivity: NodeConnectivity?, batteryPercent: UInt8?) async throws {
        let engine = self.engine
        try await Task.detached(priority: .utility) {
            try engine.updateNodeStatus(connectivity: connectivity, batteryPercent: batteryPercent)
        }.value
    }

    /// Start the background worker loop. Register handlers first.
    public func startWorker(_ options: WorkerOptions = WorkerOptions()) throws {
        try engine.startWorker(options: options)
    }

    public func stopWorker() {
        engine.stopWorker()
    }

    public func stats() -> WorkerStats {
        engine.workerStats()
    }

    /// Feed a silent-push payload (`userInfo`). Orch8 wake pushes are id-only
    /// hints (`task_id`, `runtime_id`, `reason`); the worker then polls for a
    /// leased task. Returns `false` for pushes addressed to another runtime or
    /// that carry no Orch8 fields.
    @discardableResult
    public func handlePush(userInfo: [AnyHashable: Any]) -> Bool {
        var envelope: [String: String] = [:]
        let source = (userInfo["orch8"] as? [AnyHashable: Any]) ?? userInfo
        for key in ["task_id", "runtime_id", "reason"] {
            if let value = source[key] as? String { envelope[key] = value }
        }
        guard !envelope.isEmpty,
              let data = try? JSONSerialization.data(withJSONObject: envelope),
              let json = String(data: data, encoding: .utf8) else {
            return false
        }
        return engine.onPushWake(envelopeJson: json)
    }

    /// Run the worker inside an OS-granted background window (BGTask or a
    /// push-wake handler's ~30 s). Claims tasks even while the app is paused,
    /// until idle or the budget elapses.
    public func runBackgroundWindow(seconds: TimeInterval) async throws -> WorkerWindowResult {
        let engine = self.engine
        let budget = UInt64(max(seconds, 0.001) * 1000)
        return try await Task.detached(priority: .utility) {
            try engine.runWorkerWindow(timeBudgetMs: budget)
        }.value
    }

    /// Leave the mesh (advertises `draining`, stops re-advertising).
    public func leave() {
        engine.unregisterNode()
    }
}
