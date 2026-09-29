import Foundation
import React
import Orch8Mobile

/// React Native bridge over the Orch8Mobile UniFFI surface.
///
/// Threading: engine calls that can run step handlers (`tickOnce`,
/// `runUntilIdle`, `runWorkerWindow`, …) execute on `workQueue`, never on the
/// module's method queue, because a handler blocks its engine thread until JS
/// answers through `resolveStep`, which arrives on the method queue.
@objc(Orch8Module)
class Orch8Module: RCTEventEmitter {
    private let lock = NSLock()
    private var _engine: MobileEngine?
    private var handlerTimeoutMs: UInt64 = 30_000
    private let workQueue = DispatchQueue(label: "io.orch8.reactnative.work", qos: .utility, attributes: .concurrent)
    let pending = PendingHandlerCalls()

    private var engine: MobileEngine? {
        lock.lock(); defer { lock.unlock() }
        return _engine
    }

    override func supportedEvents() -> [String] {
        [
            "orch8:instanceCompleted",
            "orch8:instanceFailed",
            "orch8:stepPending",
            "orch8:executeStep",
        ]
    }

    @objc static override func requiresMainQueueSetup() -> Bool { false }

    // MARK: - Helpers

    private func withEngine(
        _ reject: @escaping RCTPromiseRejectBlock,
        _ body: (MobileEngine) throws -> Void
    ) {
        guard let engine = engine else {
            reject("ENGINE_NOT_INITIALIZED", "Call initialize() first.", nil)
            return
        }
        do {
            try body(engine)
        } catch {
            reject("ORCH8_ERROR", "\(error)", error)
        }
    }

    /// Runs a potentially blocking engine call off the method queue.
    private func background(
        _ reject: @escaping RCTPromiseRejectBlock,
        _ body: @escaping (MobileEngine) throws -> Void
    ) {
        guard let engine = engine else {
            reject("ENGINE_NOT_INITIALIZED", "Call initialize() first.", nil)
            return
        }
        workQueue.async {
            do {
                try body(engine)
            } catch {
                reject("ORCH8_ERROR", "\(error)", error)
            }
        }
    }

    private static func u64(_ config: NSDictionary, _ key: String, _ fallback: UInt64) -> UInt64 {
        guard let n = config[key] as? NSNumber, n.doubleValue >= 0 else { return fallback }
        return n.uint64Value
    }

    private static func u32(_ config: NSDictionary, _ key: String, _ fallback: UInt32) -> UInt32 {
        guard let n = config[key] as? NSNumber, n.doubleValue >= 0 else { return fallback }
        return n.uint32Value
    }

    private static func stateKind(_ state: InstanceStateKind) -> String {
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

    private static func connectivity(_ value: String?) -> NodeConnectivity? {
        switch value {
        case "offline": return .offline
        case "metered": return .metered
        case "wifi": return .wifi
        case "ethernet": return .ethernet
        default: return nil
        }
    }

    // MARK: - Lifecycle

    @objc(initialize:resolver:rejecter:)
    func initialize(
        _ config: NSDictionary,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        let dbPath = config["dbPath"] as? String
            ?? FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
                .appendingPathComponent("orch8.db").path

        let cfg = MobileEngineConfig(
            tickIntervalMs: Self.u64(config, "tickIntervalMs", 500),
            maxConcurrentSteps: Self.u32(config, "maxConcurrentSteps", 4),
            maxStepsPerInstance: Self.u32(config, "maxStepsPerInstance", 1000),
            maxConcurrentInstances: Self.u32(config, "maxConcurrentInstances", 10),
            maxTickDurationMs: Self.u64(config, "maxTickDurationMs", 5000),
            maxInstanceLifetimeSecs: Self.u64(config, "maxInstanceLifetimeSecs", 86400),
            maxStoredSequences: Self.u32(config, "maxStoredSequences", 50),
            maxSequenceSizeBytes: Self.u64(config, "maxSequenceSizeBytes", 1_048_576),
            handlerTimeoutMs: Self.u64(config, "handlerTimeoutMs", 30000),
            operationTimeoutMs: Self.u64(config, "operationTimeoutMs", 10000),
            telemetryEnabled: config["telemetryEnabled"] as? Bool ?? true,
            telemetryUrl: config["telemetryUrl"] as? String ?? "",
            environment: config["environment"] as? String ?? "production",
            rootPublicKey: config["rootPublicKey"] as? String ?? "",
            sdkVersion: config["sdkVersion"] as? String ?? "0.7.1",
            memoryBudgetBytes: Self.u64(config, "memoryBudgetBytes", 0),
            sequencesUrl: config["sequencesUrl"] as? String ?? "",
            syncUrl: config["syncUrl"] as? String ?? "",
            deviceId: config["deviceId"] as? String ?? "",
            syncApiKey: config["syncApiKey"] as? String ?? ""
        )

        workQueue.async {
            do {
                let engine = try MobileEngine(dbPath: dbPath, config: cfg)
                engine.setListener(listener: RNEngineListener(module: self))
                self.lock.lock()
                let previous = self._engine
                self._engine = engine
                self.handlerTimeoutMs = cfg.handlerTimeoutMs
                self.lock.unlock()
                previous?.shutdown()
                resolve(nil)
            } catch {
                reject("INIT_ERROR", "\(error)", error)
            }
        }
    }

    @objc(shutdown:rejecter:)
    func shutdown(
        _ resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        lock.lock()
        let engine = _engine
        _engine = nil
        lock.unlock()
        pending.failAll("engine shut down")
        workQueue.async {
            engine?.shutdown()
            resolve(nil)
        }
    }

    @objc(resume:rejecter:)
    func resume(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        withEngine(reject) { $0.resume(); resolve(nil) }
    }

    @objc(pause:rejecter:)
    func pause(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { $0.pause(); resolve(nil) }
    }

    @objc(tickOnce:rejecter:)
    func tickOnce(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { engine in
            let r = try engine.tickOnce()
            resolve([
                "instancesAdvanced": r.instancesAdvanced,
                "stepsExecuted": r.stepsExecuted,
                "hasPendingWork": r.hasPendingWork,
            ])
        }
    }

    @objc(runUntilIdle:timeBudgetMs:resolver:rejecter:)
    func runUntilIdle(
        _ maxTicks: NSNumber,
        timeBudgetMs: NSNumber,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { engine in
            let r = try engine.runUntilIdle(maxTicks: maxTicks.uint32Value, timeBudgetMs: timeBudgetMs.uint64Value)
            resolve([
                "ticksExecuted": r.ticksExecuted,
                "instancesAdvanced": r.instancesAdvanced,
                "stepsExecuted": r.stepsExecuted,
                "hasPendingWork": r.hasPendingWork,
                "budgetExhausted": r.budgetExhausted,
            ])
        }
    }

    @objc(reportPowerState:resolver:rejecter:)
    func reportPowerState(
        _ state: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        withEngine(reject) { engine in
            let ps: PowerState
            switch state {
            case "charging": ps = .charging
            case "lowBattery": ps = .lowBattery
            case "criticalBattery": ps = .criticalBattery
            default: ps = .unplugged
            }
            engine.reportPowerState(state: ps)
            resolve(nil)
        }
    }

    // MARK: - Handlers

    @objc(registerHandler:resolver:rejecter:)
    func registerHandler(
        _ name: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        lock.lock(); let timeout = handlerTimeoutMs; lock.unlock()
        withEngine(reject) { engine in
            try engine.registerHandler(name: name, handler: RNStepHandler(module: self, timeoutMs: timeout))
            resolve(nil)
        }
    }

    /// JS answer for an `orch8:executeStep` event.
    @objc(resolveStep:output:error:permanent:)
    func resolveStep(_ requestId: String, output: String?, error: String?, permanent: Bool) {
        pending.resolve(requestId, output: output, error: error, permanent: permanent)
    }

    // MARK: - Instances

    @objc(start:input:dedupKey:resolver:rejecter:)
    func start(
        _ sequenceName: String,
        input: String,
        dedupKey: String?,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { resolve(try $0.start(sequenceName: sequenceName, input: input, dedupKey: dedupKey)) }
    }

    @objc(cancelInstance:resolver:rejecter:)
    func cancelInstance(
        _ instanceId: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { try $0.cancelInstance(instanceId: instanceId); resolve(nil) }
    }

    @objc(getInstance:resolver:rejecter:)
    func getInstance(
        _ instanceId: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { engine in
            let s = try engine.getInstance(instanceId: instanceId)
            resolve([
                "instanceId": s.instanceId,
                "sequenceName": s.sequenceName,
                "state": Self.stateKind(s.state),
                "context": s.context,
                "createdAt": s.createdAt,
                "updatedAt": s.updatedAt,
            ])
        }
    }

    @objc(activeInstances:rejecter:)
    func activeInstances(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { engine in
            resolve(try engine.activeInstances().map { i in
                [
                    "instanceId": i.instanceId,
                    "sequenceName": i.sequenceName,
                    "state": Self.stateKind(i.state),
                    "createdAt": i.createdAt,
                ]
            })
        }
    }

    @objc(completeStep:stepName:output:resolver:rejecter:)
    func completeStep(
        _ instanceId: String,
        stepName: String,
        output: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { try $0.completeStep(instanceId: instanceId, stepName: stepName, output: output); resolve(nil) }
    }

    // MARK: - Sequences, sync, telemetry

    @objc(loadSequenceFromJson:resolver:rejecter:)
    func loadSequenceFromJson(
        _ json: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { try $0.loadSequenceFromJson(json: json); resolve(nil) }
    }

    @objc(loadSequencesFromUrl:resolver:rejecter:)
    func loadSequencesFromUrl(
        _ url: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { resolve(try $0.loadSequencesFromUrl(url: url)) }
    }

    @objc(loadedSequences:rejecter:)
    func loadedSequences(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { engine in
            resolve(try engine.loadedSequences().map { ["name": $0.name, "version": $0.version] })
        }
    }

    @objc(sync:token:resolver:rejecter:)
    func sync(
        _ manifestUrl: String,
        token: String?,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { engine in
            let provider: TokenProvider? = token.map { StaticTokenProvider(token: $0) }
            let r = try engine.sync(manifestUrl: manifestUrl, tokenProvider: provider)
            resolve([
                "added": r.added,
                "updated": r.updated,
                "removed": r.removed,
                "skipped": r.skipped,
                "signatureFailures": r.signatureFailures,
            ])
        }
    }

    @objc(flushTelemetry:resolver:rejecter:)
    func flushTelemetry(
        _ endpointUrl: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { engine in
            let r = try engine.flushTelemetry(endpointUrl: endpointUrl)
            resolve(["sent": r.sent, "dropped": r.dropped])
        }
    }

    @objc(setDeviceContext:resolver:rejecter:)
    func setDeviceContext(
        _ ctx: NSDictionary,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        withEngine(reject) { engine in
            engine.setDeviceContext(ctx: DeviceContext(
                deviceId: ctx["deviceId"] as? String ?? "",
                osName: ctx["osName"] as? String ?? "ios",
                osVersion: ctx["osVersion"] as? String ?? "",
                appVersion: ctx["appVersion"] as? String ?? "",
                sdkVersion: ctx["sdkVersion"] as? String ?? "react-native"
            ))
            resolve(nil)
        }
    }

    @objc(onPushReceived:rejecter:)
    func onPushReceived(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        withEngine(reject) { $0.onPushReceived(); resolve(nil) }
    }

    // MARK: - Runtime node / worker

    @objc(nodeRuntimeId:rejecter:)
    func nodeRuntimeId(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { resolve(try $0.nodeRuntimeId()) }
    }

    @objc(registerNode:resolver:rejecter:)
    func registerNode(
        _ caps: NSDictionary,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        let capabilities = NodeCapabilities(
            handlers: caps["handlers"] as? [String] ?? [],
            regions: caps["regions"] as? [String] ?? [],
            hardware: caps["hardware"] as? [String] ?? [],
            plugins: caps["plugins"] as? [String] ?? [],
            credentials: caps["credentials"] as? [String] ?? [],
            offlineCapable: caps["offlineCapable"] as? Bool ?? true,
            connectivity: Self.connectivity(caps["connectivity"] as? String),
            batteryPercent: (caps["batteryPercent"] as? NSNumber).map { UInt8(clamping: $0.intValue) },
            platform: caps["platform"] as? String,
            pushToken: caps["pushToken"] as? String,
            appVersion: caps["appVersion"] as? String,
            apiBaseUrl: caps["apiBaseUrl"] as? String,
            capsuleSigningPublicKey: caps["capsuleSigningPublicKey"] as? String
        )
        background(reject) { engine in
            let r = try engine.registerNode(capabilities: capabilities)
            resolve([
                "runtimeId": r.runtimeId,
                "deviceId": r.deviceId,
                "handlers": r.handlers,
                "expiresAt": r.expiresAt,
            ])
        }
    }

    @objc(updateNodeStatus:batteryPercent:resolver:rejecter:)
    func updateNodeStatus(
        _ connectivity: String?,
        batteryPercent: NSNumber?,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { engine in
            try engine.updateNodeStatus(
                connectivity: Self.connectivity(connectivity),
                batteryPercent: batteryPercent.map { UInt8(clamping: $0.intValue) }
            )
            resolve(nil)
        }
    }

    @objc(unregisterNode:rejecter:)
    func unregisterNode(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { $0.unregisterNode(); resolve(nil) }
    }

    @objc(startWorker:resolver:rejecter:)
    func startWorker(
        _ options: NSDictionary,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        let opts = WorkerOptions(
            maxConcurrentTasks: (options["maxConcurrentTasks"] as? NSNumber)?.uint32Value ?? 1,
            idlePollIntervalMs: (options["idlePollIntervalMs"] as? NSNumber)?.uint64Value ?? 15000,
            version: options["version"] as? String
        )
        background(reject) { try $0.startWorker(options: opts); resolve(nil) }
    }

    @objc(stopWorker:rejecter:)
    func stopWorker(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { $0.stopWorker(); resolve(nil) }
    }

    @objc(runWorkerWindow:resolver:rejecter:)
    func runWorkerWindow(
        _ timeBudgetMs: NSNumber,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { engine in
            let r = try engine.runWorkerWindow(timeBudgetMs: timeBudgetMs.uint64Value)
            resolve([
                "claimed": r.claimed,
                "completed": r.completed,
                "failed": r.failed,
                "stillRunning": r.stillRunning,
                "budgetExhausted": r.budgetExhausted,
            ])
        }
    }

    @objc(workerStats:rejecter:)
    func workerStats(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        withEngine(reject) { engine in
            let s = engine.workerStats()
            resolve([
                "running": s.running,
                "inFlight": s.inFlight,
                "claimed": s.claimed,
                "completed": s.completed,
                "failed": s.failed,
                "released": s.released,
                "lost": s.lost,
            ])
        }
    }

    @objc(onPushWake:resolver:rejecter:)
    func onPushWake(
        _ envelopeJson: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        withEngine(reject) { resolve($0.onPushWake(envelopeJson: envelopeJson)) }
    }

    @objc(enableBuiltin:resolver:rejecter:)
    func enableBuiltin(
        _ name: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        withEngine(reject) { try $0.enableBuiltin(name: name); resolve(nil) }
    }

    // MARK: - Delegation from phone-local workflows

    private static func delegationStatus(_ s: DelegationStatus) -> [String: Any] {
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

    @objc(startDelegation:resolver:rejecter:)
    func startDelegation(
        _ options: NSDictionary,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        let opts = DelegationOptions(
            tenantId: options["tenantId"] as? String ?? "",
            pollIntervalMs: Self.u64(options, "pollIntervalMs", 2000),
            ttlSecs: Self.u32(options, "ttlSecs", 600)
        )
        background(reject) { try $0.startDelegation(options: opts); resolve(nil) }
    }

    @objc(stopDelegation:rejecter:)
    func stopDelegation(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { $0.stopDelegation(); resolve(nil) }
    }

    @objc(delegate:resolver:rejecter:)
    func delegate(
        _ request: NSDictionary,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        let req = DelegateRequest(
            instanceId: request["instanceId"] as? String ?? "",
            destinationRuntimeId: request["destinationRuntimeId"] as? String ?? "",
            subSequenceId: request["subSequenceId"] as? String ?? "",
            inputJson: request["inputJson"] as? String ?? "{}"
        )
        background(reject) { resolve(try $0.delegate(request: req)) }
    }

    @objc(delegationStatus:resolver:rejecter:)
    func delegationStatus(
        _ delegationId: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        background(reject) { engine in
            resolve(Self.delegationStatus(try engine.delegationStatus(delegationId: delegationId)))
        }
    }

    @objc(listDelegations:rejecter:)
    func listDelegations(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        background(reject) { engine in
            resolve(try engine.listDelegations().map(Self.delegationStatus))
        }
    }

    @objc(delegationStats:rejecter:)
    func delegationStats(_ resolve: @escaping RCTPromiseResolveBlock, rejecter reject: @escaping RCTPromiseRejectBlock) {
        withEngine(reject) { engine in
            let s = engine.delegationStats()
            resolve([
                "running": s.running,
                "delegated": s.delegated,
                "completed": s.completed,
                "failed": s.failed,
                "abandoned": s.abandoned,
                "resumed": s.resumed,
            ])
        }
    }
}

// MARK: - Handler bridge

/// Outstanding native-to-JS handler calls, keyed by request id.
final class PendingHandlerCalls: @unchecked Sendable {
    struct Outcome {
        let output: String?
        let error: String?
        let permanent: Bool
    }

    private final class Slot {
        let semaphore = DispatchSemaphore(value: 0)
        var outcome: Outcome?
    }

    private let lock = NSLock()
    private var slots: [String: Slot] = [:]

    func open(_ id: String) {
        lock.lock(); slots[id] = Slot(); lock.unlock()
    }

    func wait(_ id: String, timeoutMs: UInt64) -> Outcome? {
        lock.lock(); let slot = slots[id]; lock.unlock()
        guard let slot else { return nil }
        let result = slot.semaphore.wait(timeout: .now() + .milliseconds(Int(min(timeoutMs, UInt64(Int32.max)))))
        lock.lock()
        slots.removeValue(forKey: id)
        let outcome = slot.outcome
        lock.unlock()
        return result == .success ? outcome : nil
    }

    func resolve(_ id: String, output: String?, error: String?, permanent: Bool) {
        lock.lock()
        guard let slot = slots[id], slot.outcome == nil else { lock.unlock(); return } // timed out, unknown, or answered
        slot.outcome = Outcome(output: output, error: error, permanent: permanent)
        lock.unlock()
        slot.semaphore.signal()
    }

    func failAll(_ message: String) {
        lock.lock()
        let open = slots.values.filter { $0.outcome == nil }
        for slot in open { slot.outcome = Outcome(output: nil, error: message, permanent: false) }
        lock.unlock()
        for slot in open { slot.semaphore.signal() }
    }
}

/// Emits `orch8:executeStep` and blocks the engine thread until JS answers
/// with `resolveStep` or `handlerTimeoutMs` elapses (retryable failure).
final class RNStepHandler: StepHandler, @unchecked Sendable {
    private weak var module: Orch8Module?
    private let timeoutMs: UInt64

    init(module: Orch8Module, timeoutMs: UInt64) {
        self.module = module
        self.timeoutMs = timeoutMs
    }

    func execute(stepName: String, input: String) throws -> String {
        guard let module else { throw HandlerError.Retryable(message: "React Native module released") }
        let requestId = UUID().uuidString
        module.pending.open(requestId)
        module.sendEvent(withName: "orch8:executeStep", body: [
            "requestId": requestId,
            "stepName": stepName,
            "input": input,
        ])
        guard let outcome = module.pending.wait(requestId, timeoutMs: timeoutMs) else {
            throw HandlerError.Retryable(message: "JS handler '\(stepName)' timed out after \(timeoutMs) ms")
        }
        if let error = outcome.error {
            throw outcome.permanent
                ? HandlerError.Permanent(message: error)
                : HandlerError.Retryable(message: error)
        }
        return outcome.output ?? "{}"
    }
}

final class StaticTokenProvider: TokenProvider, @unchecked Sendable {
    private let token: String
    init(token: String) { self.token = token }
    func currentToken() -> String { token }
    func refreshToken() -> String { token }
}

final class RNEngineListener: EngineListener, @unchecked Sendable {
    private weak var module: Orch8Module?

    init(module: Orch8Module) {
        self.module = module
    }

    func onInstanceCompleted(instanceId: String, output: String) {
        module?.sendEvent(withName: "orch8:instanceCompleted", body: [
            "instanceId": instanceId,
            "output": output,
        ])
    }

    func onInstanceFailed(instanceId: String, error: String) {
        module?.sendEvent(withName: "orch8:instanceFailed", body: [
            "instanceId": instanceId,
            "error": error,
        ])
    }

    func onStepPending(instanceId: String, stepName: String, handler: String) {
        module?.sendEvent(withName: "orch8:stepPending", body: [
            "instanceId": instanceId,
            "stepName": stepName,
            "handler": handler,
        ])
    }
}
