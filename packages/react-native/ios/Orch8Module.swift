import Foundation
import Orch8Mobile

@objc(Orch8Module)
class Orch8Module: RCTEventEmitter {
    private var engine: MobileEngine?
    private var hasListeners = false

    override func supportedEvents() -> [String] {
        [
            "orch8:instanceCompleted",
            "orch8:instanceFailed",
            "orch8:stepPending",
            "orch8:executeStep",
        ]
    }

    override func startObserving() { hasListeners = true }
    override func stopObserving() { hasListeners = false }

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
            tickIntervalMs: (config["tickIntervalMs"] as? NSNumber)?.uint64Value ?? 100,
            maxConcurrentSteps: (config["maxConcurrentSteps"] as? NSNumber)?.uint32Value ?? 4,
            maxStepsPerInstance: (config["maxStepsPerInstance"] as? NSNumber)?.uint32Value ?? 1000,
            maxConcurrentInstances: (config["maxConcurrentInstances"] as? NSNumber)?.uint32Value ?? 10,
            maxTickDurationMs: (config["maxTickDurationMs"] as? NSNumber)?.uint64Value ?? 5000,
            maxInstanceLifetimeSecs: (config["maxInstanceLifetimeSecs"] as? NSNumber)?.uint64Value ?? 86400,
            maxStoredSequences: (config["maxStoredSequences"] as? NSNumber)?.uint32Value ?? 50,
            maxSequenceSizeBytes: (config["maxSequenceSizeBytes"] as? NSNumber)?.uint64Value ?? 1_048_576,
            handlerTimeoutMs: (config["handlerTimeoutMs"] as? NSNumber)?.uint64Value ?? 30000,
            operationTimeoutMs: (config["operationTimeoutMs"] as? NSNumber)?.uint64Value ?? 10000,
            telemetryEnabled: config["telemetryEnabled"] as? Bool ?? true,
            environment: config["environment"] as? String ?? "production",
            rootPublicKey: config["rootPublicKey"] as? String ?? "",
            sdkVersion: config["sdkVersion"] as? String ?? "0.1.0"
        )

        do {
            engine = try MobileEngine(dbPath: dbPath, config: cfg)

            let listener = RNEngineListener(module: self)
            engine?.setListener(listener: listener)

            resolve(nil)
        } catch {
            reject("INIT_ERROR", error.localizedDescription, error)
        }
    }

    @objc(resume:rejecter:)
    func resume(
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        engine?.resume()
        resolve(nil)
    }

    @objc(pause:rejecter:)
    func pause(
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        engine?.pause()
        resolve(nil)
    }

    @objc(start:input:dedupKey:resolver:rejecter:)
    func start(
        _ sequenceName: String,
        input: String,
        dedupKey: String?,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        do {
            let instanceId = try engine?.start(
                sequenceName: sequenceName,
                input: input,
                dedupKey: dedupKey
            )
            resolve(instanceId)
        } catch {
            reject("START_ERROR", error.localizedDescription, error)
        }
    }

    @objc(sync:token:resolver:rejecter:)
    func sync(
        _ manifestUrl: String,
        token: String?,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        do {
            let result = try engine?.sync(
                manifestUrl: manifestUrl,
                tokenProvider: nil
            )
            resolve([
                "added": result?.added ?? 0,
                "updated": result?.updated ?? 0,
                "removed": result?.removed ?? 0,
                "skipped": result?.skipped ?? 0,
            ])
        } catch {
            reject("SYNC_ERROR", error.localizedDescription, error)
        }
    }

    @objc(shutdown:rejecter:)
    func shutdown(
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        engine?.shutdown()
        engine = nil
        resolve(nil)
    }

    @objc static override func requiresMainQueueSetup() -> Bool { false }
}

class RNEngineListener: EngineListener {
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
