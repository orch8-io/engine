// Orch8KmpBridge.swift — Swift side of the Orch8 Kotlin Multiplatform bridge.
//
// Add this file to your iOS app target. The target must depend on:
//   - the `Orch8Mobile` Swift package (packages/swift, the UniFFI engine), and
//   - the `Orch8Kmp` framework produced by packages/kmp (or your shared KMP
//     module that re-exports it).
//
// At launch, before any Kotlin code opens the engine:
//
//     import Orch8Kmp
//     Orch8Ios.shared.install(factory: Orch8KmpBridgeFactory())
//
// Protocol version: 6 (must equal ORCH8_BRIDGE_PROTOCOL in
// src/commonMain/kotlin/io/orch8/kmp/bridge/JsonBridge.kt). Every call returns
// {"ok": value} or {"error": {"kind": "...", "message": "..."}}; all decoding,
// validation and error mapping happen in common Kotlin.

import Foundation
import CoreFoundation
import Orch8Kmp
import Orch8Mobile

private let bridgeProtocolVersion = 6

public final class Orch8KmpBridgeFactory: NSObject, Orch8JsonBridgeFactory {
    public func create() -> Orch8JsonBridge {
        Orch8KmpBridge()
    }
}

private struct BridgeFailure: Error {
    let kind: String
    let message: String
}

public final class Orch8KmpBridge: NSObject, Orch8JsonBridge, @unchecked Sendable {
    private let lock = NSLock()
    private var engine: MobileEngine?
    private var callbacks: Orch8BridgeCallbacks?

    public func setCallbacks(callbacks: Orch8BridgeCallbacks) {
        lock.lock()
        self.callbacks = callbacks
        let engine = self.engine
        lock.unlock()
        engine?.setListener(listener: ListenerAdapter(callbacks: callbacks))
    }

    public func openAccount(identityJson: String) -> Orch8ScopedReply {
        do {
            guard let engine = currentEngine() else { throw MobileError.Shutdown(message: "engine not open") }
            let identity = try Self.parseObject(identityJson)
            return NativeScopedReply.scope(.account(try engine.openAccount(identity: MobileAccountIdentity(
                accountKey: identity.string("accountKey"), issuer: identity.string("issuer"), subject: identity.string("subject")
            ))))
        } catch { return NativeScopedReply.failure(error) }
    }

    public func call(method: String, argsJson: String) -> String {
        do {
            let args = try Self.parseObject(argsJson)
            return Self.encode(["ok": try dispatch(method, args) ?? NSNull()])
        } catch let error as MobileError {
            return Self.errorEnvelope(Self.describe(error))
        } catch let error as SyncError {
            return Self.errorEnvelope(Self.describe(error))
        } catch let error as BridgeFailure {
            return Self.errorEnvelope((error.kind, error.message))
        } catch {
            return Self.errorEnvelope(("Engine", String(describing: error)))
        }
    }

    // MARK: - Dispatch

    private func dispatch(_ method: String, _ a: [String: Any]) throws -> Any? {
        if method == "open" {
            return try open(a)
        }
        guard let engine = currentEngine() else {
            throw BridgeFailure(kind: "Shutdown", message: "engine not open")
        }
        switch method {
        case "registerHandler":
            let name = try a.string("name")
            try engine.registerHandler(name: name, handler: StepHandlerAdapter(name: name, bridge: self))
            return nil
        case "resume":
            engine.resume(); return nil
        case "pause":
            engine.pause(); return nil
        case "shutdown":
            engine.shutdown()
            lock.lock()
            let retiredCallbacks = self.callbacks
            self.callbacks = nil
            self.engine = nil
            lock.unlock()
            withExtendedLifetime(retiredCallbacks) {}
            return nil
        case "onPushReceived":
            engine.onPushReceived(); return nil
        case "tickOnce":
            let r = try engine.tickOnce()
            return [
                "instancesAdvanced": Int(r.instancesAdvanced),
                "stepsExecuted": Int(r.stepsExecuted),
                "hasPendingWork": r.hasPendingWork,
            ]
        case "runUntilIdle":
            let r = try engine.runUntilIdle(
                maxTicks: UInt32(try a.uint("maxTicks")),
                timeBudgetMs: try a.uint("timeBudgetMs")
            )
            return [
                "ticksExecuted": Int(r.ticksExecuted),
                "instancesAdvanced": Int(r.instancesAdvanced),
                "stepsExecuted": Int(r.stepsExecuted),
                "hasPendingWork": r.hasPendingWork,
                "budgetExhausted": r.budgetExhausted,
            ]
        case "reportPowerState":
            engine.reportPowerState(state: try Self.powerState(try a.string("state")))
            return nil
        case "start":
            return try engine.start(
                sequenceName: try a.string("sequenceName"),
                input: try a.string("input"),
                dedupKey: a.optionalString("dedupKey")
            )
        case "cancelInstance":
            try engine.cancelInstance(instanceId: try a.string("instanceId")); return nil
        case "getInstance":
            let s = try engine.getInstance(instanceId: try a.string("instanceId"))
            return [
                "instanceId": s.instanceId,
                "sequenceName": s.sequenceName,
                "state": Self.stateWire(s.state),
                "context": s.context,
                "createdAt": s.createdAt,
                "updatedAt": s.updatedAt,
            ]
        case "activeInstances":
            return try engine.activeInstances().map { s in
                [
                    "instanceId": s.instanceId,
                    "sequenceName": s.sequenceName,
                    "state": Self.stateWire(s.state),
                    "createdAt": s.createdAt,
                ] as [String: Any]
            }
        case "completeStep":
            try engine.completeStep(
                instanceId: try a.string("instanceId"),
                stepName: try a.string("stepName"),
                output: try a.string("output")
            )
            return nil
        case "loadSequenceFromJson":
            try engine.loadSequenceFromJson(json: try a.string("json")); return nil
        case "loadSequencesFromUrl":
            return Int(try engine.loadSequencesFromUrl(url: try a.string("url")))
        case "loadedSequences":
            return try engine.loadedSequences().map { ["name": $0.name, "version": Int($0.version)] as [String: Any] }
        case "sync":
            let provider: TokenProvider?
            if let providerId = a.optionalString("providerId") {
                guard let callbacks = currentCallbacks() else {
                    throw BridgeFailure(kind: "Shutdown", message: "callbacks not installed")
                }
                provider = ManifestTokenAdapter(callbacks: callbacks, providerId: providerId)
            } else {
                provider = nil
            }
            let r = try engine.sync(manifestUrl: try a.string("manifestUrl"), tokenProvider: provider)
            return [
                "added": Int(r.added),
                "updated": Int(r.updated),
                "removed": Int(r.removed),
                "skipped": Int(r.skipped),
                "signatureFailures": Int(r.signatureFailures),
            ]
        case "flushTelemetry":
            let r = try engine.flushTelemetry(endpointUrl: try a.string("endpointUrl"))
            return ["sent": NSNumber(value: r.sent), "dropped": NSNumber(value: r.dropped)]
        case "setDeviceContext":
            engine.setDeviceContext(ctx: Orch8Mobile.DeviceContext(
                deviceId: try a.string("deviceId"),
                osName: try a.string("osName"),
                osVersion: try a.string("osVersion"),
                appVersion: try a.string("appVersion"),
                sdkVersion: try a.string("sdkVersion")
            ))
            return nil
        case "importContinuityCapsule":
            let r = try engine.importContinuityCapsule(
                capsuleJson: try a.string("capsuleJson"),
                payloadBase64: try a.string("payloadBase64"),
                payloadKeyBase64: try a.string("payloadKeyBase64"),
                destinationRuntimeId: try a.string("destinationRuntimeId"),
                destinationInstanceId: try a.string("destinationInstanceId")
            )
            return [
                "capsuleId": r.capsuleId,
                "continuityId": r.continuityId,
                "instanceId": r.instanceId,
                "sourceEpoch": NSNumber(value: r.sourceEpoch),
                "state": r.state,
            ]
        case "activateContinuityCapsule":
            try engine.activateContinuityCapsule(
                capsuleId: try a.string("capsuleId"),
                destinationRuntimeId: try a.string("destinationRuntimeId"),
                destinationInstanceId: try a.string("destinationInstanceId")
            )
            return nil
        // Runtime node / worker (Orch8Mobile after 0.7.1).
        case "nodeRuntimeId":
            return try engine.nodeRuntimeId()
        case "setTokenProvider":
            // Device sessions: the token lives in Kotlin (currentNodeToken /
            // refreshNodeToken), so a refresh runs the host's suspend fetch.
            guard let callbacks = currentCallbacks() else { throw BridgeFailure(kind: "Shutdown", message: "callbacks not installed") }
            engine.setTokenProvider(provider: NodeTokenAdapter(callbacks: callbacks, providerId: try a.string("providerId")))
            return nil
        case "beginTokenProviderInstallation":
            guard let callbacks = currentCallbacks() else { throw BridgeFailure(kind: "Shutdown", message: "callbacks not installed") }
            return try engine.beginTokenProviderInstallation(provider: NodeTokenAdapter(callbacks: callbacks, providerId: try a.string("providerId")))
        case "completeTokenProviderInstallation":
            return try engine.completeTokenProviderInstallation(installationId: try a.string("installationId"), token: try a.string("token"))
        case "cancelTokenProviderInstallation":
            return engine.cancelTokenProviderInstallation(installationId: try a.string("installationId"))
        case "registerNode":
            guard let c = a["capabilities"] as? [String: Any] else {
                throw BridgeFailure(kind: "InvalidInput", message: "missing capabilities")
            }
            let r = try engine.registerNode(capabilities: Orch8Mobile.NodeCapabilities(
                handlers: c["handlers"] as? [String] ?? [],
                regions: c["regions"] as? [String] ?? [],
                hardware: c["hardware"] as? [String] ?? [],
                plugins: c["plugins"] as? [String] ?? [],
                credentials: c["credentials"] as? [String] ?? [],
                offlineCapable: (c["offlineCapable"] as? Bool) ?? true,
                connectivity: try Self.connectivity(c.optionalString("connectivity")),
                batteryPercent: try c.optionalByte("batteryPercent"),
                platform: c.optionalString("platform") ?? "ios",
                pushToken: c.optionalString("pushToken"),
                appVersion: c.optionalString("appVersion"),
                apiBaseUrl: c.optionalString("apiBaseUrl"),
                capsuleSigningPublicKey: c.optionalString("capsuleSigningPublicKey")
            ))
            return [
                "runtimeId": r.runtimeId,
                "deviceId": r.deviceId,
                "handlers": r.handlers,
                "expiresAt": r.expiresAt,
            ] as [String: Any]
        case "updateNodeStatus":
            try engine.updateNodeStatus(
                connectivity: try Self.connectivity(a.optionalString("connectivity")),
                batteryPercent: try a.optionalByte("batteryPercent")
            )
            return nil
        case "unregisterNode":
            engine.unregisterNode(); return nil
        case "startWorker":
            try engine.startWorker(options: Orch8Mobile.WorkerOptions(
                maxConcurrentTasks: UInt32(clamping: try a.uint("maxConcurrentTasks")),
                idlePollIntervalMs: try a.uint("idlePollIntervalMs"),
                version: a.optionalString("version")
            ))
            return nil
        case "stopWorker":
            engine.stopWorker(); return nil
        case "runWorkerWindow":
            let r = try engine.runWorkerWindow(timeBudgetMs: try a.uint("timeBudgetMs"))
            return [
                "claimed": NSNumber(value: r.claimed),
                "completed": NSNumber(value: r.completed),
                "failed": NSNumber(value: r.failed),
                "stillRunning": Int(r.stillRunning),
                "budgetExhausted": r.budgetExhausted,
            ]
        case "workerStats":
            let s = engine.workerStats()
            return [
                "running": s.running,
                "inFlight": Int(s.inFlight),
                "claimed": NSNumber(value: s.claimed),
                "completed": NSNumber(value: s.completed),
                "failed": NSNumber(value: s.failed),
                "released": NSNumber(value: s.released),
                "lost": NSNumber(value: s.lost),
            ]
        case "onPushWake":
            return engine.onPushWake(envelopeJson: try a.string("envelopeJson"))
        case "enableBuiltin":
            try engine.enableBuiltin(name: try a.string("name")); return nil
        case "startDelegation":
            try engine.startDelegation(options: Orch8Mobile.DelegationOptions(
                tenantId: try a.string("tenantId"),
                pollIntervalMs: try a.uint("pollIntervalMs"),
                ttlSecs: UInt32(clamping: try a.uint("ttlSecs"))
            ))
            return nil
        case "stopDelegation":
            engine.stopDelegation(); return nil
        case "delegate":
            return try engine.delegate(request: Orch8Mobile.DelegateRequest(
                instanceId: try a.string("instanceId"),
                destinationRuntimeId: try a.string("destinationRuntimeId"),
                subSequenceId: try a.string("subSequenceId"),
                inputJson: a.optionalString("inputJson") ?? "{}"
            ))
        case "delegationStatus":
            return Self.delegationStatus(try engine.delegationStatus(delegationId: try a.string("delegationId")))
        case "listDelegations":
            return try engine.listDelegations().map(Self.delegationStatus)
        case "delegationStats":
            let s = engine.delegationStats()
            return [
                "running": s.running,
                "delegated": NSNumber(value: s.delegated),
                "completed": NSNumber(value: s.completed),
                "failed": NSNumber(value: s.failed),
                "abandoned": NSNumber(value: s.abandoned),
                "resumed": NSNumber(value: s.resumed),
            ]
        default:
            throw BridgeFailure(kind: "InvalidInput", message: "unknown bridge method \(method)")
        }
    }

    private func open(_ a: [String: Any]) throws -> Any {
        let remote = Int(try a.uint("protocol"))
        guard remote == bridgeProtocolVersion else {
            throw BridgeFailure(
                kind: "InvalidInput",
                message: "KMP speaks bridge protocol \(remote), Orch8KmpBridge.swift speaks \(bridgeProtocolVersion)"
            )
        }
        guard let c = a["config"] as? [String: Any] else {
            throw BridgeFailure(kind: "InvalidInput", message: "missing config")
        }
        let config = MobileEngineConfig(
            tickIntervalMs: try c.uint("tickIntervalMs"),
            maxConcurrentSteps: UInt32(try c.uint("maxConcurrentSteps")),
            maxStepsPerInstance: UInt32(try c.uint("maxStepsPerInstance")),
            maxConcurrentInstances: UInt32(try c.uint("maxConcurrentInstances")),
            maxTickDurationMs: try c.uint("maxTickDurationMs"),
            maxInstanceLifetimeSecs: try c.uint("maxInstanceLifetimeSecs"),
            maxStoredSequences: UInt32(try c.uint("maxStoredSequences")),
            maxSequenceSizeBytes: try c.uint("maxSequenceSizeBytes"),
            handlerTimeoutMs: try c.uint("handlerTimeoutMs"),
            operationTimeoutMs: try c.uint("operationTimeoutMs"),
            telemetryEnabled: (c["telemetryEnabled"] as? Bool) ?? true,
            telemetryUrl: try c.string("telemetryUrl"),
            environment: try c.string("environment"),
            rootPublicKey: try c.string("rootPublicKey"),
            sdkVersion: try c.string("sdkVersion"),
            memoryBudgetBytes: try c.uint("memoryBudgetBytes"),
            sequencesUrl: try c.string("sequencesUrl"),
            syncUrl: try c.string("syncUrl"),
            deviceId: try c.string("deviceId"),
            syncApiKey: try c.string("syncApiKey")
        )
        let engine = try MobileEngine(dbPath: try a.string("dbPath"), config: config)
        lock.lock()
        self.engine = engine
        lock.unlock()
        return ["protocol": bridgeProtocolVersion]
    }

    private func currentEngine() -> MobileEngine? {
        lock.lock(); defer { lock.unlock() }
        return engine
    }

    fileprivate func currentCallbacks() -> Orch8BridgeCallbacks? {
        lock.lock(); defer { lock.unlock() }
        return callbacks
    }

    // MARK: - Encoding

    fileprivate static func parseObject(_ json: String) throws -> [String: Any] {
        guard let data = json.data(using: .utf8),
              let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            throw BridgeFailure(kind: "InvalidInput", message: "bridge args must be a JSON object")
        }
        return object
    }

    fileprivate static func encode(_ object: [String: Any]) -> String {
        guard let data = try? JSONSerialization.data(withJSONObject: object),
              let text = String(data: data, encoding: .utf8)
        else {
            return #"{"error":{"kind":"Engine","message":"bridge could not encode result"}}"#
        }
        return text
    }

    fileprivate static func errorEnvelope(_ e: (String, String)) -> String {
        encode(["error": ["kind": e.0, "message": e.1]])
    }

    fileprivate static func describe(_ error: MobileError) -> (String, String) {
        switch error {
        case let .Engine(message): return ("Engine", message)
        case let .Storage(message): return ("Storage", message)
        case let .InvalidInput(message): return ("InvalidInput", message)
        case let .NotFound(message): return ("NotFound", message)
        case let .ResourceLimit(message): return ("ResourceLimit", message)
        case let .AlreadyExists(message): return ("AlreadyExists", message)
        case let .Shutdown(message): return ("Shutdown", message)
        }
    }

    fileprivate static func describe(_ error: SyncError) -> (String, String) {
        switch error {
        case let .Network(message): return ("Network", message)
        case let .SignatureInvalid(message): return ("SignatureInvalid", message)
        case let .InvalidManifest(message): return ("InvalidManifest", message)
        }
    }

    private static func delegationStatus(_ s: Orch8Mobile.DelegationStatus) -> [String: Any] {
        [
            "delegationId": s.delegationId,
            "state": s.state,
            "localInstanceId": s.localInstanceId,
            "blockId": s.blockId ?? NSNull(),
            "destinationRuntimeId": s.destinationRuntimeId ?? NSNull(),
            "outputJson": s.outputJson ?? NSNull(),
            "error": s.error ?? NSNull(),
        ]
    }

    fileprivate static func stateWire(_ state: InstanceStateKind) -> String {
        switch state {
        case .scheduled: return "scheduled"
        case .running: return "running"
        case .waiting: return "waiting"
        case .paused: return "paused"
        case .completed: return "completed"
        case .failed: return "failed"
        case .cancelled: return "cancelled"
        }
    }

    private static func powerState(_ wire: String) throws -> Orch8Mobile.PowerState {
        switch wire {
        case "charging": return .charging
        case "unplugged": return .unplugged
        case "lowBattery": return .lowBattery
        case "criticalBattery": return .criticalBattery
        default: throw BridgeFailure(kind: "InvalidInput", message: "unknown power state \(wire)")
        }
    }

    private static func connectivity(_ wire: String?) throws -> Orch8Mobile.NodeConnectivity? {
        switch wire {
        case nil: return nil
        case "offline": return .offline
        case "metered": return .metered
        case "wifi": return .wifi
        case "ethernet": return .ethernet
        case let other?: throw BridgeFailure(kind: "InvalidInput", message: "unknown connectivity \(other)")
        }
    }

    /// Decode a callback envelope produced by Kotlin (`BridgeCodec`).
    fileprivate static func unwrapCallback(_ envelope: String) -> Result<String, BridgeFailure> {
        guard let object = try? parseObject(envelope) else {
            return .failure(BridgeFailure(kind: "permanent", message: "malformed callback envelope"))
        }
        if let error = object["error"] as? [String: Any] {
            return .failure(BridgeFailure(
                kind: (error["kind"] as? String) ?? "retryable",
                message: (error["message"] as? String) ?? "callback failed"
            ))
        }
        return .success((object["ok"] as? String) ?? "{}")
    }
}

// MARK: - UniFFI callback adapters

private final class StepHandlerAdapter: StepHandler, @unchecked Sendable {
    private let name: String
    private weak var bridge: Orch8KmpBridge?

    init(name: String, bridge: Orch8KmpBridge) {
        self.name = name
        self.bridge = bridge
    }

    func execute(stepName: String, input: String) throws -> String {
        guard let callbacks = bridge?.currentCallbacks() else {
            throw HandlerError.Retryable(message: "Kotlin callbacks not installed yet")
        }
        let envelope = callbacks.executeHandler(handlerName: name, stepName: stepName, inputJson: input)
        switch Orch8KmpBridge.unwrapCallback(envelope) {
        case let .success(output):
            return output
        case let .failure(failure) where failure.kind == "permanent":
            throw HandlerError.Permanent(message: failure.message)
        case let .failure(failure):
            throw HandlerError.Retryable(message: failure.message)
        }
    }
}

private final class ListenerAdapter: EngineListener, @unchecked Sendable {
    private let callbacks: Orch8BridgeCallbacks

    init(callbacks: Orch8BridgeCallbacks) {
        self.callbacks = callbacks
    }

    func onInstanceCompleted(instanceId: String, output: String) {
        callbacks.onInstanceCompleted(instanceId: instanceId, output: output)
    }

    func onInstanceFailed(instanceId: String, error: String) {
        callbacks.onInstanceFailed(instanceId: instanceId, error: error)
    }

    func onStepPending(instanceId: String, stepName: String, handler: String) {
        callbacks.onStepPending(instanceId: instanceId, stepName: stepName, handler: handler)
    }
}

private final class ManifestTokenAdapter: TokenProvider, @unchecked Sendable {
    private let callbacks: Orch8BridgeCallbacks
    private let providerId: String

    init(callbacks: Orch8BridgeCallbacks, providerId: String) {
        self.callbacks = callbacks
        self.providerId = providerId
    }

    func currentToken() -> String {
        callbacks.currentManifestToken(providerId: providerId)
    }

    func refreshToken() throws -> String {
        switch Orch8KmpBridge.unwrapCallback(callbacks.refreshManifestToken(providerId: providerId)) {
        case let .success(token): return token
        case let .failure(failure): throw MobileError.Engine(message: failure.message)
        }
    }
}

/// The node credential (device sessions) for `MobileEngine.setTokenProvider`.
/// `refreshToken` runs on the engine's blocking thread, where waiting for the
/// Kotlin suspend fetch is fine.
private final class NodeTokenAdapter: TokenProvider, @unchecked Sendable {
    private let callbacks: Orch8BridgeCallbacks
    private let providerId: String

    init(callbacks: Orch8BridgeCallbacks, providerId: String) {
        self.callbacks = callbacks
        self.providerId = providerId
    }

    deinit { callbacks.releaseNodeTokenSource(providerId: providerId) }

    func currentToken() -> String {
        callbacks.currentNodeToken(providerId: providerId)
    }

    func refreshToken() throws -> String {
        switch Orch8KmpBridge.unwrapCallback(callbacks.refreshNodeToken(providerId: providerId)) {
        case let .success(token): return token
        case let .failure(failure): throw MobileError.Engine(message: failure.message)
        }
    }
}

// MARK: - Argument helpers

private extension Dictionary where Key == String, Value == Any {
    func string(_ key: String) throws -> String {
        guard let value = self[key] as? String else {
            throw BridgeFailure(kind: "InvalidInput", message: "missing string argument '\(key)'")
        }
        return value
    }

    func optionalString(_ key: String) -> String? {
        self[key] as? String
    }

    func optionalByte(_ key: String) throws -> UInt8? {
        guard let value = self[key], !(value is NSNull) else { return nil }
        guard let number = value as? NSNumber, (0...255).contains(number.intValue) else {
            throw BridgeFailure(kind: "InvalidInput", message: "'\(key)' must be 0...255")
        }
        return number.uint8Value
    }

    func uint(_ key: String) throws -> UInt64 {
        guard let number = self[key] as? NSNumber, number.int64Value >= 0 else {
            throw BridgeFailure(kind: "InvalidInput", message: "missing non-negative number '\(key)'")
        }
        return number.uint64Value
    }
}

// MARK: - Owned scoped ports (protocol 6)

private enum NativeScopedNode {
    case account(MobileAccount)
    case catalog(MobileCatalog)
    case run(MobileCatalogRun)
    case cursor(MobileCatalogHistoryCursor)
}

private final class NativeScopedReply: NSObject, Orch8ScopedReply {
    private let json: String
    private let handle: Orch8ScopedBridge?
    init(_ value: Any? = nil, handle: Orch8ScopedBridge? = nil) {
        self.json = Orch8KmpBridge.encode(["ok": value ?? NSNull()])
        self.handle = handle
    }
    private init(error: (String, String)) {
        json = Orch8KmpBridge.errorEnvelope(error)
        handle = nil
    }
    func payloadJson() -> String { json }
    func resultHandle() -> Orch8ScopedBridge? { handle }
    static func failure(_ error: Error) -> NativeScopedReply {
        if let error = error as? MobileError { return NativeScopedReply(error: Orch8KmpBridge.describe(error)) }
        if let error = error as? SyncError { return NativeScopedReply(error: Orch8KmpBridge.describe(error)) }
        if let error = error as? BridgeFailure { return NativeScopedReply(error: (error.kind, error.message)) }
        return NativeScopedReply(error: ("Engine", String(describing: error)))
    }
    static func scope(_ node: NativeScopedNode) -> NativeScopedReply {
        let port = NativeScopedPort(node)
        return NativeScopedReply(port.descriptor(), handle: port)
    }
}

/// Owns a native view or cursor, which itself retains only weak engine identity.
/// No global handle table or reference to the root engine bridge is captured.
private final class NativeScopedPort: NSObject, Orch8ScopedBridge, @unchecked Sendable {
    private let lock = NSLock()
    private var node: NativeScopedNode?
    init(_ node: NativeScopedNode) { self.node = node }
    private func snapshotNode() -> NativeScopedNode? {
        lock.lock(); defer { lock.unlock() }; return node
    }
    func descriptor() -> [String: Any] {
        switch snapshotNode() {
        case let .account(account): return ["kind": "account", "accountId": account.accountId(), "sessionEpoch": account.sessionEpoch()]
        case let .catalog(catalog): return ["kind": "catalog", "accountId": catalog.accountId(), "catalogId": catalog.catalogId(), "sessionEpoch": catalog.sessionEpoch()]
        case let .run(run): return ["kind": "run", "accountId": run.accountId(), "catalogId": run.catalogId(), "sessionEpoch": run.sessionEpoch(), "instanceId": run.instanceId()]
        case .cursor: return ["kind": "cursor"]
        case nil: return ["kind": "disposed"]
        }
    }
    func call(method: String, argsJson: String, reference: Orch8ScopedBridge?) -> Orch8ScopedReply {
        if method == "dispose" {
            lock.lock(); let retired = node; node = nil; lock.unlock()
            withExtendedLifetime(retired) {}
            return NativeScopedReply()
        }
        do {
            guard let node = snapshotNode() else { throw MobileError.Shutdown(message: "scope view disposed") }
            let args = try Orch8KmpBridge.parseObject(argsJson)
            switch (node, method) {
            case let (.account(account), "openCatalog"):
                return NativeScopedReply.scope(.catalog(try account.openCatalog(configuration: MobileCatalogConfiguration(
                    catalogKey: args.string("catalogKey"), source: args.string("source"), signingAuthority: args.string("signingAuthority"),
                    manifestUrl: args.string("manifestUrl"), identityUrl: args.string("identityUrl")
                ))))
            case let (.account(account), "retire"): try account.retire()
            case let (.account(account), "closeView"): account.closeView()
            case let (.catalog(catalog), "closeView"): catalog.closeView()
            case let (.run(run), "closeView"): run.closeView()
            case let (.catalog(catalog), "prepareRun"):
                var version: Int32?
                if let value = args["version"], !(value is NSNull) {
                    guard let number = value as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID(),
                          number.doubleValue == Double(number.int64Value),
                          let positive = Int32(exactly: number.int64Value), positive > 0
                    else { throw BridgeFailure(kind: "InvalidInput", message: "invalid workflow version") }
                    version = positive
                }
                return NativeScopedReply.scope(.run(try catalog.prepareRun(request: MobileCatalogRunRequest(
                    workflow: args.string("workflow"), version: version, inputJson: args.string("inputJson"), operationKey: args.string("operationKey")
                ))))
            case let (.catalog(catalog), "run"):
                return NativeScopedReply.scope(.run(try catalog.run(instanceId: args.string("instanceId"))))
            case let (.catalog(catalog), "history"):
                guard let number = args["limit"] as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID(),
                      number.doubleValue == Double(number.int64Value), (1...128).contains(number.int64Value)
                else { throw BridgeFailure(kind: "InvalidInput", message: "invalid history page size") }
                var cursor: MobileCatalogHistoryCursor?
                if let reference {
                    guard let port = reference as? NativeScopedPort, case let .cursor(value) = port.snapshotNode()
                    else { throw BridgeFailure(kind: "InvalidInput", message: "invalid cursor object") }
                    cursor = value
                }
                let page = try catalog.history(cursor: cursor, limit: UInt32(number.int64Value))
                return NativeScopedReply(["items": page.items.map(Self.summary), "hasNext": page.next != nil],
                    handle: page.next.map { NativeScopedPort(.cursor($0)) })
            case let (.run(run), "start"): try run.start()
            case let (.run(run), "snapshot"):
                guard let value = try run.snapshot() else { return NativeScopedReply() }
                return NativeScopedReply([
                    "instanceId": value.instanceId, "accountId": value.accountId, "catalogId": value.catalogId,
                    "sessionEpoch": value.sessionEpoch, "state": Orch8KmpBridge.stateWire(value.state),
                    "dataJson": value.dataJson, "pendingSteps": value.pendingSteps,
                    "createdAt": value.createdAt, "updatedAt": value.updatedAt,
                ])
            case let (.run(run), "cancel"):
                return NativeScopedReply(try run.cancel().map(Orch8KmpBridge.stateWire))
            case let (.run(run), "completeStep"):
                return NativeScopedReply(try run.completeStep(expectedUpdatedAt: args.string("expectedUpdatedAt"),
                    stepName: args.string("stepName"), outputJson: args.string("outputJson")))
            case let (.run(run), "archiveAfterAcknowledgement"):
                guard let value = try run.archiveAfterAcknowledgement(acknowledgement: args.string("acknowledgement")) else { return NativeScopedReply() }
                return NativeScopedReply([
                    "instanceId": value.instanceId, "accountId": value.accountId, "catalogId": value.catalogId,
                    "acknowledgementDigest": value.acknowledgementDigest, "archivedAt": value.archivedAt,
                    "outcome": value.outcome.map(Orch8KmpBridge.stateWire) as Any? ?? NSNull(),
                ])
            case let (.run(run), "forgetCancelledRequest"):
                return NativeScopedReply(try run.forgetCancelledRequest())
            default: throw BridgeFailure(kind: "InvalidInput", message: "unknown scoped operation")
            }
            return NativeScopedReply()
        } catch { return NativeScopedReply.failure(error) }
    }
    func registerHandler(name: String, timeoutMs: Int64, handler: Orch8ScopedHandler) -> String {
        do {
            guard case let .catalog(catalog) = snapshotNode() else { throw MobileError.Shutdown(message: "catalog view disposed") }
            guard timeoutMs > 0 else { throw BridgeFailure(kind: "InvalidInput", message: "invalid handler timeout") }
            try catalog.registerHandler(name: name, handler: ScopedHandlerAdapter(handler), timeoutMs: UInt64(timeoutMs))
            return NativeScopedReply().payloadJson()
        } catch { return NativeScopedReply.failure(error).payloadJson() }
    }
    func sync(tokens: Orch8ScopedTokenSource) -> String {
        do {
            guard case let .catalog(catalog) = snapshotNode() else { throw MobileError.Shutdown(message: "catalog view disposed") }
            let value = try catalog.sync(tokenProvider: ScopedTokenAdapter(tokens))
            return NativeScopedReply(["added": value.added, "updated": value.updated, "removed": value.removed,
                "skipped": value.skipped, "signatureFailures": value.signatureFailures]).payloadJson()
        } catch { return NativeScopedReply.failure(error).payloadJson() }
    }
    private static func summary(_ value: MobileCatalogRunSummary) -> [String: Any] {
        let state: [String: Any]
        switch value.nativeState {
        case .notMaterialized: state = ["kind": "not_materialized"]
        case .unavailable: state = ["kind": "unavailable"]
        case let .present(state: native): state = ["kind": "present", "state": Orch8KmpBridge.stateWire(native)]
        case let .recorded(state: native): state = ["kind": "recorded", "state": Orch8KmpBridge.stateWire(native)]
        }
        let request: String
        switch value.requestState { case .prepared: request = "prepared"; case .cancelled: request = "cancelled"; case .archived: request = "archived" }
        return ["instanceId": value.instanceId, "accountId": value.accountId, "catalogId": value.catalogId,
            "ownerEpoch": value.ownerEpoch, "workflow": value.workflow, "version": value.version,
            "sequenceId": value.sequenceId, "createdAt": value.createdAt, "requestState": request, "nativeState": state]
    }
}

private final class ScopedHandlerAdapter: MobileCatalogStepHandler, @unchecked Sendable {
    private let handler: Orch8ScopedHandler
    init(_ handler: Orch8ScopedHandler) { self.handler = handler }
    func execute(context: MobileCatalogCallbackContext, stepName: String, input: String) throws -> String {
        let json = Orch8KmpBridge.encode([
            "accountId": context.accountId, "catalogId": context.catalogId, "ownerEpoch": context.ownerEpoch,
            "runId": context.runId, "blockId": context.blockId, "attempt": context.attempt, "effectId": context.effectId,
        ])
        switch Orch8KmpBridge.unwrapCallback(handler.execute(contextJson: json, stepName: stepName, inputJson: input)) {
        case let .success(output): return output
        case let .failure(error) where error.kind == "permanent": throw HandlerError.Permanent(message: error.message)
        case let .failure(error): throw HandlerError.Retryable(message: error.message)
        }
    }
}
private final class ScopedTokenAdapter: TokenProvider, @unchecked Sendable {
    private let tokens: Orch8ScopedTokenSource
    init(_ tokens: Orch8ScopedTokenSource) { self.tokens = tokens }
    func currentToken() -> String { tokens.cachedToken() }
    func refreshToken() throws -> String {
        switch Orch8KmpBridge.unwrapCallback(tokens.refreshEnvelope()) {
        case let .success(token): return token
        case let .failure(error): throw MobileError.Engine(message: error.message)
        }
    }
}
