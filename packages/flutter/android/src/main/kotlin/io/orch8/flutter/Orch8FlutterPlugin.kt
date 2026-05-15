package io.orch8.flutter

import io.flutter.embedding.engine.plugins.FlutterPlugin
import io.flutter.plugin.common.EventChannel
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel
import io.orch8.mobile.*

class Orch8FlutterPlugin : FlutterPlugin, MethodChannel.MethodCallHandler, EventChannel.StreamHandler {
    private var channel: MethodChannel? = null
    private var eventChannel: EventChannel? = null
    private var eventSink: EventChannel.EventSink? = null
    private var engine: MobileEngine? = null

    override fun onAttachedToEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        channel = MethodChannel(binding.binaryMessenger, "io.orch8/mobile").also {
            it.setMethodCallHandler(this)
        }
        eventChannel = EventChannel(binding.binaryMessenger, "io.orch8/events").also {
            it.setStreamHandler(this)
        }
    }

    override fun onDetachedFromEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        channel?.setMethodCallHandler(null)
        channel = null
        eventChannel?.setStreamHandler(null)
        eventChannel = null
    }

    override fun onMethodCall(call: MethodCall, result: MethodChannel.Result) {
        when (call.method) {
            "initialize" -> handleInitialize(call, result)
            "resume" -> { engine?.resume(); result.success(null) }
            "pause" -> { engine?.pause(); result.success(null) }
            "start" -> handleStart(call, result)
            "sync" -> handleSync(call, result)
            "shutdown" -> { engine?.shutdown(); engine = null; result.success(null) }
            else -> result.notImplemented()
        }
    }

    override fun onListen(arguments: Any?, events: EventChannel.EventSink?) {
        eventSink = events
    }

    override fun onCancel(arguments: Any?) {
        eventSink = null
    }

    private fun handleInitialize(call: MethodCall, result: MethodChannel.Result) {
        try {
            val args = call.arguments as Map<*, *>
            val dbPath = args["dbPath"] as? String ?: "orch8.db"

            val cfg = MobileEngineConfig(
                tickIntervalMs = (args["tickIntervalMs"] as? Number)?.toLong()?.toULong() ?: 100u,
                maxConcurrentSteps = (args["maxConcurrentSteps"] as? Number)?.toInt()?.toUInt() ?: 4u,
                maxStepsPerInstance = (args["maxStepsPerInstance"] as? Number)?.toInt()?.toUInt() ?: 1000u,
                maxConcurrentInstances = (args["maxConcurrentInstances"] as? Number)?.toInt()?.toUInt() ?: 10u,
                maxTickDurationMs = (args["maxTickDurationMs"] as? Number)?.toLong()?.toULong() ?: 5000u,
                maxInstanceLifetimeSecs = (args["maxInstanceLifetimeSecs"] as? Number)?.toLong()?.toULong() ?: 86400u,
                maxStoredSequences = (args["maxStoredSequences"] as? Number)?.toInt()?.toUInt() ?: 50u,
                maxSequenceSizeBytes = (args["maxSequenceSizeBytes"] as? Number)?.toLong()?.toULong() ?: 1_048_576u,
                handlerTimeoutMs = (args["handlerTimeoutMs"] as? Number)?.toLong()?.toULong() ?: 30000u,
                operationTimeoutMs = (args["operationTimeoutMs"] as? Number)?.toLong()?.toULong() ?: 10000u,
                telemetryEnabled = args["telemetryEnabled"] as? Boolean ?: true,
                environment = args["environment"] as? String ?: "production",
                rootPublicKey = args["rootPublicKey"] as? String ?: "",
                sdkVersion = args["sdkVersion"] as? String ?: "0.1.0",
            )

            engine = MobileEngine(dbPath, cfg)

            engine?.setListener(object : EngineListener {
                override fun onInstanceCompleted(instanceId: String, output: String) {
                    eventSink?.success(mapOf(
                        "type" to "instanceCompleted",
                        "instanceId" to instanceId,
                        "output" to output,
                    ))
                }

                override fun onInstanceFailed(instanceId: String, error: String) {
                    eventSink?.success(mapOf(
                        "type" to "instanceFailed",
                        "instanceId" to instanceId,
                        "error" to error,
                    ))
                }

                override fun onStepPending(instanceId: String, stepName: String, handler: String) {
                    eventSink?.success(mapOf(
                        "type" to "stepPending",
                        "instanceId" to instanceId,
                        "stepName" to stepName,
                        "handler" to handler,
                    ))
                }
            })

            result.success(null)
        } catch (e: Exception) {
            result.error("INIT_ERROR", e.message, null)
        }
    }

    private fun handleStart(call: MethodCall, result: MethodChannel.Result) {
        try {
            val args = call.arguments as Map<*, *>
            val instanceId = engine?.start(
                args["sequenceName"] as String,
                args["input"] as? String ?: "{}",
                args["dedupKey"] as? String,
            )
            result.success(instanceId)
        } catch (e: Exception) {
            result.error("START_ERROR", e.message, null)
        }
    }

    private fun handleSync(call: MethodCall, result: MethodChannel.Result) {
        try {
            val args = call.arguments as Map<*, *>
            val syncResult = engine?.sync(args["manifestUrl"] as String, null)
            result.success(mapOf(
                "added" to (syncResult?.added ?: 0),
                "updated" to (syncResult?.updated ?: 0),
                "removed" to (syncResult?.removed ?: 0),
                "skipped" to (syncResult?.skipped ?: 0),
            ))
        } catch (e: Exception) {
            result.error("SYNC_ERROR", e.message, null)
        }
    }
}
