package io.orch8.flutter

import android.content.Context
import android.os.Handler
import android.os.Looper
import io.flutter.embedding.engine.plugins.FlutterPlugin
import io.flutter.plugin.common.EventChannel
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel
import io.orch8.mobile.DelegateRequest
import io.orch8.mobile.DelegationOptions
import io.orch8.mobile.DelegationStatus
import io.orch8.mobile.EngineListener
import io.orch8.mobile.HandlerException
import io.orch8.mobile.InstanceStateKind
import io.orch8.mobile.MobileEngine
import io.orch8.mobile.MobileEngineConfig
import io.orch8.mobile.NodeCapabilities
import io.orch8.mobile.NodeConnectivity
import io.orch8.mobile.PowerState
import io.orch8.mobile.StepHandler
import io.orch8.mobile.TokenProvider
import io.orch8.mobile.WorkerOptions
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Flutter bridge over the Orch8Mobile UniFFI surface.
 *
 * Threading: engine calls run on [work] and reply on the platform thread. A
 * step handler blocks its engine thread while Dart runs `executeStep` on the
 * platform thread, so no engine call may run on the platform thread.
 *
 * UniFFI maps Rust u8/u32/u64 to Kotlin UByte/UInt/ULong; Dart ints arrive as
 * Int or Long, so conversions happen here.
 */
class Orch8FlutterPlugin : FlutterPlugin, MethodChannel.MethodCallHandler, EventChannel.StreamHandler {
    private var channel: MethodChannel? = null
    private var eventChannel: EventChannel? = null
    private var eventSink: EventChannel.EventSink? = null
    private var appContext: Context? = null

    @Volatile private var engine: MobileEngine? = null

    @Volatile private var handlerTimeoutMs: Long = 30_000
    private val main = Handler(Looper.getMainLooper())
    private val work: ExecutorService = Executors.newCachedThreadPool { r ->
        Thread(r, "orch8-flutter-work").apply { isDaemon = true }
    }

    override fun onAttachedToEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        appContext = binding.applicationContext
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

    override fun onListen(arguments: Any?, events: EventChannel.EventSink?) {
        eventSink = events
    }

    override fun onCancel(arguments: Any?) {
        eventSink = null
    }

    private fun emit(event: Map<String, Any?>) {
        main.post { eventSink?.success(event) }
    }

    override fun onMethodCall(call: MethodCall, result: MethodChannel.Result) {
        val args = (call.arguments as? Map<*, *>) ?: emptyMap<Any, Any>()
        if (call.method == "initialize") {
            handleInitialize(args, result)
            return
        }
        val eng = engine
        if (eng == null) {
            result.error("ENGINE_NOT_INITIALIZED", "Call initialize() first.", null)
            return
        }
        work.execute {
            try {
                val reply = dispatch(call.method, args, eng)
                main.post { if (reply === NotImplemented) result.notImplemented() else result.success(reply) }
            } catch (e: IllegalArgumentException) {
                main.post { result.error("INVALID_ARGUMENT", e.message, null) }
            } catch (e: Exception) {
                main.post { result.error("ORCH8_ERROR", e.message ?: e.toString(), null) }
            }
        }
    }

    private object NotImplemented

    private fun Map<*, *>.str(key: String): String? = this[key] as? String

    private fun Map<*, *>.requireStr(key: String): String =
        str(key) ?: throw IllegalArgumentException("missing string argument '$key'")

    private fun Map<*, *>.long(key: String): Long? = (this[key] as? Number)?.toLong()?.takeIf { it >= 0 }

    private fun Map<*, *>.requireLong(key: String): Long =
        long(key) ?: throw IllegalArgumentException("missing non-negative number '$key'")

    private fun Map<*, *>.strings(key: String): List<String> =
        (this[key] as? List<*>)?.filterIsInstance<String>() ?: emptyList()

    private fun connectivity(value: String?): NodeConnectivity? = when (value) {
        "offline" -> NodeConnectivity.OFFLINE
        "metered" -> NodeConnectivity.METERED
        "wifi" -> NodeConnectivity.WIFI
        "ethernet" -> NodeConnectivity.ETHERNET
        else -> null
    }

    private fun battery(value: Any?): UByte? = (value as? Number)?.toInt()?.coerceIn(0, 100)?.toUByte()

    private fun stateWire(state: InstanceStateKind): String = when (state) {
        InstanceStateKind.SCHEDULED -> "scheduled"
        InstanceStateKind.RUNNING -> "running"
        InstanceStateKind.WAITING -> "waiting"
        InstanceStateKind.PAUSED -> "paused"
        InstanceStateKind.COMPLETED -> "completed"
        InstanceStateKind.FAILED -> "failed"
        InstanceStateKind.CANCELLED -> "cancelled"
    }

    private fun dispatch(method: String, a: Map<*, *>, eng: MobileEngine): Any? = when (method) {
        "registerHandler" -> {
            eng.registerHandler(a.requireStr("name"), FlutterStepHandler(this, handlerTimeoutMs))
            null
        }
        "resume" -> eng.resume().let { null }
        "pause" -> eng.pause().let { null }
        "shutdown" -> {
            engine = null
            eng.shutdown()
            null
        }
        "tickOnce" -> eng.tickOnce().let {
            mapOf(
                "instancesAdvanced" to it.instancesAdvanced.toInt(),
                "stepsExecuted" to it.stepsExecuted.toInt(),
                "hasPendingWork" to it.hasPendingWork,
            )
        }
        "runUntilIdle" -> eng.runUntilIdle(a.requireLong("maxTicks").toUInt(), a.requireLong("timeBudgetMs").toULong()).let {
            mapOf(
                "ticksExecuted" to it.ticksExecuted.toInt(),
                "instancesAdvanced" to it.instancesAdvanced.toInt(),
                "stepsExecuted" to it.stepsExecuted.toInt(),
                "hasPendingWork" to it.hasPendingWork,
                "budgetExhausted" to it.budgetExhausted,
            )
        }
        "reportPowerState" -> {
            eng.reportPowerState(
                when (a.str("state")) {
                    "charging" -> PowerState.CHARGING
                    "lowBattery" -> PowerState.LOW_BATTERY
                    "criticalBattery" -> PowerState.CRITICAL_BATTERY
                    else -> PowerState.UNPLUGGED
                },
            )
            null
        }
        "start" -> eng.start(a.requireStr("sequenceName"), a.str("input") ?: "{}", a.str("dedupKey"))
        "cancelInstance" -> eng.cancelInstance(a.requireStr("instanceId")).let { null }
        "getInstance" -> eng.getInstance(a.requireStr("instanceId")).let {
            mapOf(
                "instanceId" to it.instanceId,
                "sequenceName" to it.sequenceName,
                "state" to stateWire(it.state),
                "context" to it.context,
                "createdAt" to it.createdAt,
                "updatedAt" to it.updatedAt,
            )
        }
        "activeInstances" -> eng.activeInstances().map {
            mapOf(
                "instanceId" to it.instanceId,
                "sequenceName" to it.sequenceName,
                "state" to stateWire(it.state),
                "createdAt" to it.createdAt,
            )
        }
        "completeStep" -> {
            eng.completeStep(a.requireStr("instanceId"), a.requireStr("stepName"), a.requireStr("output"))
            null
        }
        "loadSequenceFromJson" -> eng.loadSequenceFromJson(a.requireStr("json")).let { null }
        "loadSequencesFromUrl" -> eng.loadSequencesFromUrl(a.str("url") ?: "").toInt()
        "loadedSequences" -> eng.loadedSequences().map { mapOf("name" to it.name, "version" to it.version) }
        "sync" -> eng.sync(a.requireStr("manifestUrl"), a.str("token")?.let { StaticTokenProvider(it) }).let {
            mapOf(
                "added" to it.added.toInt(),
                "updated" to it.updated.toInt(),
                "removed" to it.removed.toInt(),
                "skipped" to it.skipped.toInt(),
                "signatureFailures" to it.signatureFailures.toInt(),
            )
        }
        "flushTelemetry" -> eng.flushTelemetry(a.requireStr("endpointUrl")).let {
            mapOf("sent" to it.sent.toLong(), "dropped" to it.dropped.toLong())
        }
        "onPushReceived" -> eng.onPushReceived().let { null }

        // Runtime node / worker (orch8-mobile after 0.7.1).
        "nodeRuntimeId" -> eng.nodeRuntimeId()
        "registerNode" -> eng.registerNode(
            NodeCapabilities(
                handlers = a.strings("handlers"),
                regions = a.strings("regions"),
                hardware = a.strings("hardware"),
                plugins = a.strings("plugins"),
                credentials = a.strings("credentials"),
                offlineCapable = a["offlineCapable"] as? Boolean ?: true,
                connectivity = connectivity(a.str("connectivity")),
                batteryPercent = battery(a["batteryPercent"]),
                platform = a.str("platform") ?: "android",
                pushToken = a.str("pushToken"),
                appVersion = a.str("appVersion"),
                apiBaseUrl = a.str("apiBaseUrl"),
                capsuleSigningPublicKey = a.str("capsuleSigningPublicKey"),
            ),
        ).let {
            mapOf(
                "runtimeId" to it.runtimeId,
                "deviceId" to it.deviceId,
                "handlers" to it.handlers,
                "expiresAt" to it.expiresAt,
            )
        }
        "updateNodeStatus" -> {
            eng.updateNodeStatus(connectivity(a.str("connectivity")), battery(a["batteryPercent"]))
            null
        }
        "unregisterNode" -> eng.unregisterNode().let { null }
        "startWorker" -> {
            eng.startWorker(
                WorkerOptions(
                    maxConcurrentTasks = (a.long("maxConcurrentTasks") ?: 1L).toUInt(),
                    idlePollIntervalMs = (a.long("idlePollIntervalMs") ?: 15_000L).toULong(),
                    version = a.str("version"),
                ),
            )
            null
        }
        "stopWorker" -> eng.stopWorker().let { null }
        "runWorkerWindow" -> eng.runWorkerWindow(a.requireLong("timeBudgetMs").toULong()).let {
            mapOf(
                "claimed" to it.claimed.toLong(),
                "completed" to it.completed.toLong(),
                "failed" to it.failed.toLong(),
                "stillRunning" to it.stillRunning.toInt(),
                "budgetExhausted" to it.budgetExhausted,
            )
        }
        "workerStats" -> eng.workerStats().let {
            mapOf(
                "running" to it.running,
                "inFlight" to it.inFlight.toInt(),
                "claimed" to it.claimed.toLong(),
                "completed" to it.completed.toLong(),
                "failed" to it.failed.toLong(),
                "released" to it.released.toLong(),
                "lost" to it.lost.toLong(),
            )
        }
        "onPushWake" -> eng.onPushWake(a.requireStr("envelopeJson"))
        "enableBuiltin" -> eng.enableBuiltin(a.requireStr("name")).let { null }

        // Delegation from phone-local workflows (orch8-mobile after 0.7.1).
        "startDelegation" -> {
            eng.startDelegation(
                DelegationOptions(
                    tenantId = a.requireStr("tenantId"),
                    pollIntervalMs = (a.long("pollIntervalMs") ?: 2_000L).toULong(),
                    ttlSecs = (a.long("ttlSecs") ?: 600L).toUInt(),
                ),
            )
            null
        }
        "stopDelegation" -> eng.stopDelegation().let { null }
        "delegate" -> eng.delegate(
            DelegateRequest(
                instanceId = a.requireStr("instanceId"),
                destinationRuntimeId = a.requireStr("destinationRuntimeId"),
                subSequenceId = a.requireStr("subSequenceId"),
                inputJson = a.str("inputJson") ?: "{}",
            ),
        )
        "delegationStatus" -> delegationStatusMap(eng.delegationStatus(a.requireStr("delegationId")))
        "listDelegations" -> eng.listDelegations().map(::delegationStatusMap)
        "delegationStats" -> eng.delegationStats().let {
            mapOf(
                "running" to it.running,
                "delegated" to it.delegated.toLong(),
                "completed" to it.completed.toLong(),
                "failed" to it.failed.toLong(),
                "abandoned" to it.abandoned.toLong(),
                "resumed" to it.resumed.toLong(),
            )
        }
        else -> NotImplemented
    }

    private fun delegationStatusMap(s: DelegationStatus): Map<String, Any?> = mapOf(
        "delegationId" to s.delegationId,
        "state" to s.state,
        "localInstanceId" to s.localInstanceId,
        "blockId" to s.blockId,
        "destinationRuntimeId" to s.destinationRuntimeId,
        "outputJson" to s.outputJson,
        "error" to s.error,
    )

    private fun handleInitialize(args: Map<*, *>, result: MethodChannel.Result) {
        val dbPath = args.str("dbPath")
            ?: appContext?.getDatabasePath("orch8.db")?.absolutePath
            ?: "orch8.db"
        fun ulong(key: String, default: ULong) = args.long(key)?.toULong() ?: default
        fun uint(key: String, default: UInt) = args.long(key)?.toUInt() ?: default
        val cfg = MobileEngineConfig(
            tickIntervalMs = ulong("tickIntervalMs", 100uL),
            maxConcurrentSteps = uint("maxConcurrentSteps", 4u),
            maxStepsPerInstance = uint("maxStepsPerInstance", 1000u),
            maxConcurrentInstances = uint("maxConcurrentInstances", 10u),
            maxTickDurationMs = ulong("maxTickDurationMs", 5000uL),
            maxInstanceLifetimeSecs = ulong("maxInstanceLifetimeSecs", 86400uL),
            maxStoredSequences = uint("maxStoredSequences", 50u),
            maxSequenceSizeBytes = ulong("maxSequenceSizeBytes", 1_048_576uL),
            handlerTimeoutMs = ulong("handlerTimeoutMs", 30000uL),
            operationTimeoutMs = ulong("operationTimeoutMs", 10000uL),
            telemetryEnabled = args["telemetryEnabled"] as? Boolean ?: true,
            telemetryUrl = args.str("telemetryUrl") ?: "",
            environment = args.str("environment") ?: "production",
            rootPublicKey = args.str("rootPublicKey") ?: "",
            sdkVersion = args["sdkVersion"] as? String ?: "0.7.1",
            memoryBudgetBytes = ulong("memoryBudgetBytes", 0uL),
            sequencesUrl = args.str("sequencesUrl") ?: "",
            syncUrl = args.str("syncUrl") ?: "",
            deviceId = args.str("deviceId") ?: "",
            syncApiKey = args.str("syncApiKey") ?: "",
        )
        work.execute {
            try {
                val eng = MobileEngine(dbPath, cfg)
                eng.setListener(FlutterEngineListener(::emit))
                val previous = engine
                handlerTimeoutMs = cfg.handlerTimeoutMs.toLong()
                engine = eng
                previous?.shutdown()
                main.post { result.success(null) }
            } catch (e: Exception) {
                main.post { result.error("INIT_ERROR", e.message ?: e.toString(), null) }
            }
        }
    }

    /**
     * Runs the Dart handler through `executeStep` on the platform thread and
     * blocks the engine thread until it answers or `handlerTimeoutMs` elapses.
     */
    private class FlutterStepHandler(
        private val plugin: Orch8FlutterPlugin,
        private val timeoutMs: Long,
    ) : StepHandler {
        private sealed class Reply {
            class Ok(val value: Any?) : Reply()
            class Err(val code: String, val message: String?) : Reply()
            object Missing : Reply()
        }

        override fun execute(stepName: String, input: String): String {
            val channel = plugin.channel ?: throw HandlerException.Retryable("Flutter channel not attached")
            val done = CountDownLatch(1)
            val reply = AtomicReference<Reply>()
            plugin.main.post {
                channel.invokeMethod(
                    "executeStep",
                    mapOf("stepName" to stepName, "input" to input),
                    object : MethodChannel.Result {
                        override fun success(result: Any?) {
                            reply.set(Reply.Ok(result)); done.countDown()
                        }

                        override fun error(errorCode: String, errorMessage: String?, errorDetails: Any?) {
                            reply.set(Reply.Err(errorCode, errorMessage)); done.countDown()
                        }

                        override fun notImplemented() {
                            reply.set(Reply.Missing); done.countDown()
                        }
                    },
                )
            }
            if (!done.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw HandlerException.Retryable("Dart handler '$stepName' timed out after $timeoutMs ms")
            }
            return when (val r = reply.get()) {
                is Reply.Ok -> r.value as? String ?: "{}"
                is Reply.Err ->
                    if (r.code == "PERMANENT") {
                        throw HandlerException.Permanent(r.message ?: r.code)
                    } else {
                        throw HandlerException.Retryable(r.message ?: r.code)
                    }
                else -> throw HandlerException.Retryable("Dart side has no executeStep handler")
            }
        }
    }
}

private class StaticTokenProvider(private val token: String) : TokenProvider {
    override fun currentToken(): String = token

    override fun refreshToken(): String = token
}

private class FlutterEngineListener(private val emit: (Map<String, Any?>) -> Unit) : EngineListener {
    override fun onInstanceCompleted(instanceId: String, output: String) =
        emit(mapOf("type" to "instanceCompleted", "instanceId" to instanceId, "output" to output))

    override fun onInstanceFailed(instanceId: String, error: String) =
        emit(mapOf("type" to "instanceFailed", "instanceId" to instanceId, "error" to error))

    override fun onStepPending(instanceId: String, stepName: String, handler: String) =
        emit(mapOf("type" to "stepPending", "instanceId" to instanceId, "stepName" to stepName, "handler" to handler))
}
