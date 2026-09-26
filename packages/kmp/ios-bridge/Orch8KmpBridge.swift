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
// Protocol version: 1 (must equal ORCH8_BRIDGE_PROTOCOL in
// src/commonMain/kotlin/io/orch8/kmp/bridge/JsonBridge.kt). Every call returns
// {"ok": value} or {"error": {"kind": "...", "message": "..."}}; all decoding,
// validation and error mapping happen in common Kotlin.

import Foundation
import Orch8Kmp
import Orch8Mobile

private let bridgeProtocolVersion = 1

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
            lock.lock(); self.engine = nil; lock.unlock()
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
            let useTokens = (a["useTokenSource"] as? Bool) ?? false
            let provider: TokenProvider? = useTokens ? TokenAdapter(bridge: self) : nil
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
            engine.setDeviceContext(ctx: DeviceContext(
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

    private static func encode(_ object: [String: Any]) -> String {
        guard let data = try? JSONSerialization.data(withJSONObject: object),
              let text = String(data: data, encoding: .utf8)
        else {
            return #"{"error":{"kind":"Engine","message":"bridge could not encode result"}}"#
        }
        return text
    }

    private static func errorEnvelope(_ e: (String, String)) -> String {
        encode(["error": ["kind": e.0, "message": e.1]])
    }

    private static func describe(_ error: MobileError) -> (String, String) {
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

    private static func describe(_ error: SyncError) -> (String, String) {
        switch error {
        case let .Network(message): return ("Network", message)
        case let .SignatureInvalid(message): return ("SignatureInvalid", message)
        case let .InvalidManifest(message): return ("InvalidManifest", message)
        }
    }

    private static func stateWire(_ state: InstanceStateKind) -> String {
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

    private static func powerState(_ wire: String) throws -> PowerState {
        switch wire {
        case "charging": return .charging
        case "unplugged": return .unplugged
        case "lowBattery": return .lowBattery
        case "criticalBattery": return .criticalBattery
        default: throw BridgeFailure(kind: "InvalidInput", message: "unknown power state \(wire)")
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

private final class TokenAdapter: TokenProvider, @unchecked Sendable {
    private weak var bridge: Orch8KmpBridge?

    init(bridge: Orch8KmpBridge) {
        self.bridge = bridge
    }

    func currentToken() -> String {
        bridge?.currentCallbacks()?.currentToken() ?? ""
    }

    func refreshToken() throws -> String {
        guard let callbacks = bridge?.currentCallbacks() else {
            throw MobileError.Engine(message: "Kotlin callbacks not installed")
        }
        switch Orch8KmpBridge.unwrapCallback(callbacks.refreshToken()) {
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

    func uint(_ key: String) throws -> UInt64 {
        guard let number = self[key] as? NSNumber, number.int64Value >= 0 else {
            throw BridgeFailure(kind: "InvalidInput", message: "missing non-negative number '\(key)'")
        }
        return number.uint64Value
    }
}
