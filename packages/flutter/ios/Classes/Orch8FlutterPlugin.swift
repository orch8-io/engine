import Flutter
import UIKit
import Orch8Mobile

public class Orch8FlutterPlugin: NSObject, FlutterPlugin {
    private var engine: MobileEngine?
    private var eventSink: FlutterEventSink?
    private var channel: FlutterMethodChannel?

    public static func register(with registrar: FlutterPluginRegistrar) {
        let channel = FlutterMethodChannel(name: "io.orch8/mobile", binaryMessenger: registrar.messenger())
        let eventChannel = FlutterEventChannel(name: "io.orch8/events", binaryMessenger: registrar.messenger())

        let instance = Orch8FlutterPlugin()
        instance.channel = channel
        registrar.addMethodCallDelegate(instance, channel: channel)
        eventChannel.setStreamHandler(instance)
    }

    public func handle(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
        switch call.method {
        case "initialize":
            handleInitialize(call.arguments as? [String: Any] ?? [:], result: result)
        case "resume":
            engine?.resume()
            result(nil)
        case "pause":
            engine?.pause()
            result(nil)
        case "start":
            handleStart(call.arguments as? [String: Any] ?? [:], result: result)
        case "sync":
            handleSync(call.arguments as? [String: Any] ?? [:], result: result)
        case "shutdown":
            engine?.shutdown()
            engine = nil
            result(nil)
        default:
            result(FlutterMethodNotImplemented)
        }
    }

    private func handleInitialize(_ args: [String: Any], result: FlutterResult) {
        let dbPath = args["dbPath"] as? String
            ?? FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
                .appendingPathComponent("orch8.db").path

        let config = MobileEngineConfig(
            tickIntervalMs: (args["tickIntervalMs"] as? NSNumber)?.uint64Value ?? 100,
            maxConcurrentSteps: (args["maxConcurrentSteps"] as? NSNumber)?.uint32Value ?? 4,
            maxStepsPerInstance: (args["maxStepsPerInstance"] as? NSNumber)?.uint32Value ?? 1000,
            maxConcurrentInstances: (args["maxConcurrentInstances"] as? NSNumber)?.uint32Value ?? 10,
            maxTickDurationMs: (args["maxTickDurationMs"] as? NSNumber)?.uint64Value ?? 5000,
            maxInstanceLifetimeSecs: (args["maxInstanceLifetimeSecs"] as? NSNumber)?.uint64Value ?? 86400,
            maxStoredSequences: (args["maxStoredSequences"] as? NSNumber)?.uint32Value ?? 50,
            maxSequenceSizeBytes: (args["maxSequenceSizeBytes"] as? NSNumber)?.uint64Value ?? 1_048_576,
            handlerTimeoutMs: (args["handlerTimeoutMs"] as? NSNumber)?.uint64Value ?? 30000,
            operationTimeoutMs: (args["operationTimeoutMs"] as? NSNumber)?.uint64Value ?? 10000,
            telemetryEnabled: args["telemetryEnabled"] as? Bool ?? true,
            environment: args["environment"] as? String ?? "production",
            rootPublicKey: args["rootPublicKey"] as? String ?? "",
            sdkVersion: args["sdkVersion"] as? String ?? "0.1.0"
        )

        do {
            engine = try MobileEngine(dbPath: dbPath, config: config)

            engine?.setListener(listener: FlutterEngineListener(plugin: self))

            result(nil)
        } catch {
            result(FlutterError(code: "INIT_ERROR", message: error.localizedDescription, details: nil))
        }
    }

    private func handleStart(_ args: [String: Any], result: FlutterResult) {
        do {
            let instanceId = try engine?.start(
                sequenceName: args["sequenceName"] as? String ?? "",
                input: args["input"] as? String ?? "{}",
                dedupKey: args["dedupKey"] as? String
            )
            result(instanceId)
        } catch {
            result(FlutterError(code: "START_ERROR", message: error.localizedDescription, details: nil))
        }
    }

    private func handleSync(_ args: [String: Any], result: FlutterResult) {
        do {
            let syncResult = try engine?.sync(
                manifestUrl: args["manifestUrl"] as? String ?? "",
                tokenProvider: nil
            )
            result([
                "added": syncResult?.added ?? 0,
                "updated": syncResult?.updated ?? 0,
                "removed": syncResult?.removed ?? 0,
                "skipped": syncResult?.skipped ?? 0,
            ])
        } catch {
            result(FlutterError(code: "SYNC_ERROR", message: error.localizedDescription, details: nil))
        }
    }

    fileprivate func sendEvent(_ event: [String: Any]) {
        eventSink?(event)
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

private class FlutterEngineListener: EngineListener {
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
