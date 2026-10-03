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
/// try await node.setTokenProvider { try await myBackend.deviceSession(runtimeId: node.runtimeId()) }
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
    /// device and `runtimeId()`). It is awaited once now for the first token,
    /// and again whenever the control plane answers `401` (the request is
    /// then retried once); a refresh that throws, returns an empty token or
    /// exceeds `refreshTimeout` leaves the request failed with 401. Call it
    /// before `join`. Never ship an operator key in the app.
    public func setTokenProvider(
        refreshTimeout: TimeInterval = 30,
        _ fetch: @escaping @Sendable () async throws -> String
    ) async throws {
        let first = try await fetch()
        engine.setTokenProvider(provider: AsyncTokenProvider(
            initialToken: first,
            timeout: refreshTimeout,
            fetch: fetch
        ))
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

/// Adapts an async token closure to the synchronous UniFFI `TokenProvider`.
/// The engine calls `refreshToken()` on a Rust blocking thread, so waiting on
/// a semaphore there never blocks the main thread or a Swift executor.
final class AsyncTokenProvider: TokenProvider, @unchecked Sendable {
    private let lock = NSLock()
    private var token: String
    private let timeout: TimeInterval
    private let fetch: @Sendable () async throws -> String

    init(initialToken: String, timeout: TimeInterval, fetch: @escaping @Sendable () async throws -> String) {
        self.token = initialToken
        self.timeout = timeout
        self.fetch = fetch
    }

    func currentToken() -> String {
        lock.lock(); defer { lock.unlock() }
        return token
    }

    func refreshToken() throws -> String {
        let box = ResultBox()
        let semaphore = DispatchSemaphore(value: 0)
        let fetch = self.fetch
        Task.detached(priority: .utility) {
            do {
                box.set(.success(try await fetch()))
            } catch {
                box.set(.failure(error))
            }
            semaphore.signal()
        }
        guard semaphore.wait(timeout: .now() + max(timeout, 0.001)) == .success, let result = box.get() else {
            throw MobileError.Engine(message: "token provider timed out after \(timeout) s")
        }
        switch result {
        case .success(let fresh) where !fresh.isEmpty:
            lock.lock(); token = fresh; lock.unlock()
            return fresh
        case .success:
            throw MobileError.Engine(message: "token provider returned an empty token")
        case .failure(let error):
            throw MobileError.Engine(message: "token provider failed: \(error)")
        }
    }

    private final class ResultBox: @unchecked Sendable {
        private let lock = NSLock()
        private var value: Result<String, Error>?
        func set(_ result: Result<String, Error>) { lock.lock(); value = result; lock.unlock() }
        func get() -> Result<String, Error>? { lock.lock(); defer { lock.unlock() }; return value }
    }
}
