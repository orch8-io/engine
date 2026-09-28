import Flutter
import UIKit
import Orch8Mobile

/// Flutter bridge over the Orch8Mobile UniFFI surface.
///
/// Threading: engine calls run on `workQueue` and deliver results on the main
/// thread. A step handler blocks its engine thread while Dart runs
/// `executeStep` on the platform thread, so no engine call may run on the
/// main thread.
public class Orch8FlutterPlugin: NSObject, FlutterPlugin {
    private let lock = NSLock()
    private var _engine: MobileEngine?
    private var handlerTimeoutMs: UInt64 = 30_000
    private var eventSink: FlutterEventSink?
    fileprivate var channel: FlutterMethodChannel?
    private let workQueue = DispatchQueue(label: "io.orch8.flutter.work", qos: .utility, attributes: .concurrent)

    private var engine: MobileEngine? {
        lock.lock(); defer { lock.unlock() }
        return _engine
    }

    public static func register(with registrar: FlutterPluginRegistrar) {
        let channel = FlutterMethodChannel(name: "io.orch8/mobile", binaryMessenger: registrar.messenger())
        let eventChannel = FlutterEventChannel(name: "io.orch8/events", binaryMessenger: registrar.messenger())

        let instance = Orch8FlutterPlugin()
        instance.channel = channel
        registrar.addMethodCallDelegate(instance, channel: channel)
        eventChannel.setStreamHandler(instance)
    }

    // MARK: - Dispatch

    public func handle(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
        let args = call.arguments as? [String: Any] ?? [:]
        if call.method == "initialize" {
            handleInitialize(args, result: result)
            return
        }
        guard let engine = engine else {
            result(FlutterError(code: "ENGINE_NOT_INITIALIZED", message: "Call initialize() first.", details: nil))
            return
        }
        workQueue.async {
            let reply: Any?
            do {
                reply = try self.dispatch(call.method, args, engine)
            } catch let error as InvalidArgument {
                reply = FlutterError(code: "INVALID_ARGUMENT", message: error.message, details: nil)
            } catch {
                reply = FlutterError(code: "ORCH8_ERROR", message: "\(error)", details: nil)
            }
            DispatchQueue.main.async { result(reply) }
        }
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    private func dispatch(_ method: String, _ a: [String: Any], _ engine: MobileEngine) throws -> Any? {
        switch method {
        case "registerHandler":
            lock.lock(); let timeout = handlerTimeoutMs; lock.unlock()
            try engine.registerHandler(
                name: try a.string("name"),
                handler: FlutterStepHandler(plugin: self, timeoutMs: timeout)
            )
            return nil
        case "resume":
            engine.resume(); return nil
        case "pause":
            engine.pause(); return nil
        case "shutdown":
            lock.lock(); _engine = nil; lock.unlock()
            engine.shutdown()
            return nil
        case "tickOnce":
            let r = try engine.tickOnce()
            return [
                "instancesAdvanced": Int(r.instancesAdvanced),
                "stepsExecuted": Int(r.stepsExecuted),
                "hasPendingWork": r.hasPendingWork,
            ]
        case "runUntilIdle":
            let r = try engine.runUntilIdle(
                maxTicks: UInt32(clamping: try a.uint("maxTicks")),
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
            let state: PowerState
            switch a["state"] as? String {
            case "charging": state = .charging
            case "lowBattery": state = .lowBattery
            case "criticalBattery": state = .criticalBattery
            default: state = .unplugged
            }
            engine.reportPowerState(state: state)
            return nil
        case "start":
            return try engine.start(
                sequenceName: try a.string("sequenceName"),
                input: a["input"] as? String ?? "{}",
                dedupKey: a["dedupKey"] as? String
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
            return Int(try engine.loadSequencesFromUrl(url: a["url"] as? String ?? ""))
        case "loadedSequences":
            return try engine.loadedSequences().map { ["name": $0.name, "version": Int($0.version)] as [String: Any] }
        case "sync":
            let provider: TokenProvider? = (a["token"] as? String).map { StaticTokenProvider(token: $0) }
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
            return ["sent": Int(clamping: r.sent), "dropped": Int(clamping: r.dropped)]
        case "onPushReceived":
            engine.onPushReceived(); return nil

        // Runtime node / worker (Orch8Mobile after 0.7.1).
        case "nodeRuntimeId":
            return try engine.nodeRuntimeId()
        case "registerNode":
            let r = try engine.registerNode(capabilities: NodeCapabilities(
                handlers: a["handlers"] as? [String] ?? [],
                regions: a["regions"] as? [String] ?? [],
                hardware: a["hardware"] as? [String] ?? [],
                plugins: a["plugins"] as? [String] ?? [],
                credentials: a["credentials"] as? [String] ?? [],
                offlineCapable: a["offlineCapable"] as? Bool ?? true,
                connectivity: Self.connectivity(a["connectivity"] as? String),
                batteryPercent: (a["batteryPercent"] as? NSNumber).map { UInt8(clamping: $0.intValue) },
                platform: a["platform"] as? String ?? "ios",
                pushToken: a["pushToken"] as? String,
                appVersion: a["appVersion"] as? String,
                apiBaseUrl: a["apiBaseUrl"] as? String,
                capsuleSigningPublicKey: a["capsuleSigningPublicKey"] as? String
            ))
            return [
                "runtimeId": r.runtimeId,
                "deviceId": r.deviceId,
                "handlers": r.handlers,
                "expiresAt": r.expiresAt,
            ] as [String: Any]
        case "updateNodeStatus":
            try engine.updateNodeStatus(
                connectivity: Self.connectivity(a["connectivity"] as? String),
                batteryPercent: (a["batteryPercent"] as? NSNumber).map { UInt8(clamping: $0.intValue) }
            )
            return nil
        case "unregisterNode":
            engine.unregisterNode(); return nil
        case "startWorker":
            try engine.startWorker(options: WorkerOptions(
                maxConcurrentTasks: UInt32(clamping: (a["maxConcurrentTasks"] as? NSNumber)?.intValue ?? 1),
                idlePollIntervalMs: (a["idlePollIntervalMs"] as? NSNumber)?.uint64Value ?? 15000,
                version: a["version"] as? String
            ))
            return nil
        case "stopWorker":
            engine.stopWorker(); return nil
        case "runWorkerWindow":
            let r = try engine.runWorkerWindow(timeBudgetMs: try a.uint("timeBudgetMs"))
            return [
                "claimed": Int(clamping: r.claimed),
                "completed": Int(clamping: r.completed),
                "failed": Int(clamping: r.failed),
                "stillRunning": Int(r.stillRunning),
                "budgetExhausted": r.budgetExhausted,
            ]
        case "workerStats":
            let s = engine.workerStats()
            return [
                "running": s.running,
                "inFlight": Int(s.inFlight),
                "claimed": Int(clamping: s.claimed),
                "completed": Int(clamping: s.completed),
                "failed": Int(clamping: s.failed),
                "released": Int(clamping: s.released),
                "lost": Int(clamping: s.lost),
            ]
        case "onPushWake":
            return engine.onPushWake(envelopeJson: try a.string("envelopeJson"))
        case "enableBuiltin":
            try engine.enableBuiltin(name: try a.string("name")); return nil
        default:
            return FlutterMethodNotImplemented
        }
    }

    private func handleInitialize(_ args: [String: Any], result: @escaping FlutterResult) {
        let dbPath = args["dbPath"] as? String
            ?? FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
                .appendingPathComponent("orch8.db").path

        func u64(_ key: String, _ fallback: UInt64) -> UInt64 {
            guard let n = args[key] as? NSNumber, n.int64Value >= 0 else { return fallback }
            return n.uint64Value
        }
        func u32(_ key: String, _ fallback: UInt32) -> UInt32 {
            guard let n = args[key] as? NSNumber, n.int64Value >= 0 else { return fallback }
            return UInt32(clamping: n.uint64Value)
        }

        let config = MobileEngineConfig(
            tickIntervalMs: u64("tickIntervalMs", 100),
            maxConcurrentSteps: u32("maxConcurrentSteps", 4),
            maxStepsPerInstance: u32("maxStepsPerInstance", 1000),
            maxConcurrentInstances: u32("maxConcurrentInstances", 10),
            maxTickDurationMs: u64("maxTickDurationMs", 5000),
            maxInstanceLifetimeSecs: u64("maxInstanceLifetimeSecs", 86400),
            maxStoredSequences: u32("maxStoredSequences", 50),
            maxSequenceSizeBytes: u64("maxSequenceSizeBytes", 1_048_576),
            handlerTimeoutMs: u64("handlerTimeoutMs", 30000),
            operationTimeoutMs: u64("operationTimeoutMs", 10000),
            telemetryEnabled: args["telemetryEnabled"] as? Bool ?? true,
            telemetryUrl: args["telemetryUrl"] as? String ?? "",
            environment: args["environment"] as? String ?? "production",
            rootPublicKey: args["rootPublicKey"] as? String ?? "",
            sdkVersion: args["sdkVersion"] as? String ?? "0.7.1",
            memoryBudgetBytes: u64("memoryBudgetBytes", 0),
            sequencesUrl: args["sequencesUrl"] as? String ?? "",
            syncUrl: args["syncUrl"] as? String ?? "",
            deviceId: args["deviceId"] as? String ?? "",
            syncApiKey: args["syncApiKey"] as? String ?? ""
        )

        workQueue.async {
            let reply: Any?
            do {
                let engine = try MobileEngine(dbPath: dbPath, config: config)
                engine.setListener(listener: FlutterEngineListener(plugin: self))
                self.lock.lock()
                let previous = self._engine
                self._engine = engine
                self.handlerTimeoutMs = config.handlerTimeoutMs
                self.lock.unlock()
                previous?.shutdown()
                reply = nil
            } catch {
                reply = FlutterError(code: "INIT_ERROR", message: "\(error)", details: nil)
            }
            DispatchQueue.main.async { result(reply) }
        }
    }

    fileprivate func sendEvent(_ event: [String: Any]) {
        DispatchQueue.main.async { self.eventSink?(event) }
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

    private static func connectivity(_ wire: String?) -> NodeConnectivity? {
        switch wire {
        case "offline": return .offline
        case "metered": return .metered
        case "wifi": return .wifi
        case "ethernet": return .ethernet
        default: return nil
        }
    }
}

extension Orch8FlutterPlugin: FlutterStreamHandler {
    public func onListen(withArguments arguments: Any?, eventSink: @escaping FlutterEventSink) -> FlutterError? {
        self.eventSink = eventSink
        return nil
    }

    public func onCancel(withArguments arguments: Any?) -> FlutterError? {
        eventSink = nil
        return nil
    }
}

/// Runs the Dart handler through `executeStep` on the platform thread and
/// blocks the engine thread until it answers or `handlerTimeoutMs` elapses.
final class FlutterStepHandler: StepHandler, @unchecked Sendable {
    private weak var plugin: Orch8FlutterPlugin?
    private let timeoutMs: UInt64

    init(plugin: Orch8FlutterPlugin, timeoutMs: UInt64) {
        self.plugin = plugin
        self.timeoutMs = timeoutMs
    }

    func execute(stepName: String, input: String) throws -> String {
        guard let channel = plugin?.channel else {
            throw HandlerError.Retryable(message: "Flutter channel not attached")
        }
        let done = DispatchSemaphore(value: 0)
        let box = ResultBox()
        DispatchQueue.main.async {
            channel.invokeMethod("executeStep", arguments: ["stepName": stepName, "input": input]) { reply in
                box.set(reply)
                done.signal()
            }
        }
        let millis = Int(min(timeoutMs, UInt64(Int32.max)))
        guard done.wait(timeout: .now() + .milliseconds(millis)) == .success else {
            throw HandlerError.Retryable(message: "Dart handler '\(stepName)' timed out after \(timeoutMs) ms")
        }
        switch box.get() {
        case let output as String:
            return output
        case let error as FlutterError:
            let message = error.message ?? error.code
            if error.code == "PERMANENT" { throw HandlerError.Permanent(message: message) }
            throw HandlerError.Retryable(message: message)
        case let value where (value as AnyObject) === FlutterMethodNotImplemented:
            throw HandlerError.Retryable(message: "Dart side has no executeStep handler")
        default:
            return "{}"
        }
    }
}

private final class ResultBox: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Any?
    func set(_ v: Any?) { lock.lock(); value = v; lock.unlock() }
    func get() -> Any? { lock.lock(); defer { lock.unlock() }; return value }
}

final class StaticTokenProvider: TokenProvider, @unchecked Sendable {
    private let token: String
    init(token: String) { self.token = token }
    func currentToken() -> String { token }
    func refreshToken() -> String { token }
}

final class FlutterEngineListener: EngineListener, @unchecked Sendable {
    private weak var plugin: Orch8FlutterPlugin?

    init(plugin: Orch8FlutterPlugin) {
        self.plugin = plugin
    }

    func onInstanceCompleted(instanceId: String, output: String) {
        plugin?.sendEvent([
            "type": "instanceCompleted",
            "instanceId": instanceId,
            "output": output,
        ])
    }

    func onInstanceFailed(instanceId: String, error: String) {
        plugin?.sendEvent([
            "type": "instanceFailed",
            "instanceId": instanceId,
            "error": error,
        ])
    }

    func onStepPending(instanceId: String, stepName: String, handler: String) {
        plugin?.sendEvent([
            "type": "stepPending",
            "instanceId": instanceId,
            "stepName": stepName,
            "handler": handler,
        ])
    }
}

private struct InvalidArgument: Error {
    let message: String
}

private extension Dictionary where Key == String, Value == Any {
    func string(_ key: String) throws -> String {
        guard let value = self[key] as? String else {
            throw InvalidArgument(message: "missing string argument '\(key)'")
        }
        return value
    }

    func uint(_ key: String) throws -> UInt64 {
        guard let number = self[key] as? NSNumber, number.int64Value >= 0 else {
            throw InvalidArgument(message: "missing non-negative number '\(key)'")
        }
        return number.uint64Value
    }
}
