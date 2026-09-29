package io.orch8.reactnative

import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.facebook.react.bridge.ReadableArray
import com.facebook.react.bridge.ReadableMap
import com.facebook.react.bridge.WritableMap
import com.facebook.react.modules.core.DeviceEventManagerModule
import io.orch8.mobile.DelegateRequest
import io.orch8.mobile.DelegationOptions
import io.orch8.mobile.DelegationStatus
import io.orch8.mobile.DeviceContext
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
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * React Native bridge over the Orch8Mobile UniFFI surface.
 *
 * Threading: engine calls that can run step handlers (tickOnce, runUntilIdle,
 * runWorkerWindow, ...) run on [work], never on the native-modules thread,
 * because a handler blocks its engine thread until JS answers through
 * [resolveStep], which arrives on the native-modules thread.
 *
 * UniFFI maps Rust u8/u32/u64 to Kotlin UByte/UInt/ULong; JS numbers arrive as
 * Double, so conversions happen at this boundary.
 */
class Orch8Module(reactContext: ReactApplicationContext) :
    ReactContextBaseJavaModule(reactContext) {

    @Volatile private var engine: MobileEngine? = null
    @Volatile private var handlerTimeoutMs: Long = 30_000
    private val work: ExecutorService = Executors.newCachedThreadPool { r ->
        Thread(r, "orch8-rn-work").apply { isDaemon = true }
    }
    internal val pending = PendingHandlerCalls()

    override fun getName(): String = "Orch8Module"

    // -- Helpers ------------------------------------------------------------

    private inline fun withEngine(promise: Promise, body: (MobileEngine) -> Unit) {
        val eng = engine
        if (eng == null) {
            promise.reject("ENGINE_NOT_INITIALIZED", "Call initialize() first.")
            return
        }
        try {
            body(eng)
        } catch (e: Exception) {
            promise.reject("ORCH8_ERROR", e.message ?: e.toString(), e)
        }
    }

    private fun background(promise: Promise, body: (MobileEngine) -> Unit) {
        val eng = engine
        if (eng == null) {
            promise.reject("ENGINE_NOT_INITIALIZED", "Call initialize() first.")
            return
        }
        work.execute {
            try {
                body(eng)
            } catch (e: Exception) {
                promise.reject("ORCH8_ERROR", e.message ?: e.toString(), e)
            }
        }
    }

    private fun ReadableMap.num(key: String): Double? =
        if (hasKey(key) && !isNull(key)) getDouble(key) else null

    private fun ReadableMap.str(key: String): String? =
        if (hasKey(key) && !isNull(key)) getString(key) else null

    private fun ReadableMap.uint(key: String, default: UInt): UInt =
        num(key)?.takeIf { it >= 0 }?.toLong()?.toUInt() ?: default

    private fun ReadableMap.ulong(key: String, default: ULong): ULong =
        num(key)?.takeIf { it >= 0 }?.toLong()?.toULong() ?: default

    private fun ReadableMap.strings(key: String): List<String> {
        if (!hasKey(key) || isNull(key)) return emptyList()
        val arr: ReadableArray = getArray(key) ?: return emptyList()
        return (0 until arr.size()).mapNotNull { arr.getString(it) }
    }

    private fun batteryByte(value: Double?): UByte? =
        value?.toInt()?.coerceIn(0, 100)?.toUByte()

    private fun connectivity(value: String?): NodeConnectivity? = when (value) {
        "offline" -> NodeConnectivity.OFFLINE
        "metered" -> NodeConnectivity.METERED
        "wifi" -> NodeConnectivity.WIFI
        "ethernet" -> NodeConnectivity.ETHERNET
        else -> null
    }

    private fun stateKind(state: InstanceStateKind): String = when (state) {
        InstanceStateKind.SCHEDULED -> "scheduled"
        InstanceStateKind.RUNNING -> "running"
        InstanceStateKind.WAITING -> "waiting"
        InstanceStateKind.PAUSED -> "paused"
        InstanceStateKind.COMPLETED -> "completed"
        InstanceStateKind.FAILED -> "failed"
        InstanceStateKind.CANCELLED -> "cancelled"
    }

    internal fun sendEvent(name: String, params: WritableMap) {
        reactApplicationContext
            .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter::class.java)
            .emit(name, params)
    }

    // Required by NativeEventEmitter on Android.
    @Suppress("UNUSED_PARAMETER")
    @ReactMethod fun addListener(eventName: String) = Unit

    @Suppress("UNUSED_PARAMETER")
    @ReactMethod fun removeListeners(count: Double) = Unit

    // -- Lifecycle ------------------------------------------------------------

    @ReactMethod
    fun initialize(config: ReadableMap, promise: Promise) {
        val dbPath = config.str("dbPath")
            ?: reactApplicationContext.getDatabasePath("orch8.db").absolutePath
        val cfg = MobileEngineConfig(
            tickIntervalMs = config.ulong("tickIntervalMs", 500uL),
            maxConcurrentSteps = config.uint("maxConcurrentSteps", 4u),
            maxStepsPerInstance = config.uint("maxStepsPerInstance", 1000u),
            maxConcurrentInstances = config.uint("maxConcurrentInstances", 10u),
            maxTickDurationMs = config.ulong("maxTickDurationMs", 5000uL),
            maxInstanceLifetimeSecs = config.ulong("maxInstanceLifetimeSecs", 86400uL),
            maxStoredSequences = config.uint("maxStoredSequences", 50u),
            maxSequenceSizeBytes = config.ulong("maxSequenceSizeBytes", 1_048_576uL),
            handlerTimeoutMs = config.ulong("handlerTimeoutMs", 30000uL),
            operationTimeoutMs = config.ulong("operationTimeoutMs", 10000uL),
            telemetryEnabled = if (config.hasKey("telemetryEnabled")) config.getBoolean("telemetryEnabled") else true,
            telemetryUrl = config.str("telemetryUrl") ?: "",
            environment = config.str("environment") ?: "production",
            rootPublicKey = config.str("rootPublicKey") ?: "",
            sdkVersion = config.str("sdkVersion") ?: "0.7.1",
            memoryBudgetBytes = config.ulong("memoryBudgetBytes", 0uL),
            sequencesUrl = config.str("sequencesUrl") ?: "",
            syncUrl = config.str("syncUrl") ?: "",
            deviceId = config.str("deviceId") ?: "",
            syncApiKey = config.str("syncApiKey") ?: "",
        )
        work.execute {
            try {
                val eng = MobileEngine(dbPath, cfg)
                eng.setListener(RNEngineListener(this))
                val previous = engine
                handlerTimeoutMs = cfg.handlerTimeoutMs.toLong()
                engine = eng
                previous?.shutdown()
                promise.resolve(null)
            } catch (e: Exception) {
                promise.reject("INIT_ERROR", e.message ?: e.toString(), e)
            }
        }
    }

    @ReactMethod
    fun shutdown(promise: Promise) {
        val eng = engine
        engine = null
        pending.failAll("engine shut down")
        work.execute {
            eng?.shutdown()
            promise.resolve(null)
        }
    }

    @ReactMethod
    fun resume(promise: Promise) = withEngine(promise) { it.resume(); promise.resolve(null) }

    @ReactMethod
    fun pause(promise: Promise) = background(promise) { it.pause(); promise.resolve(null) }

    @ReactMethod
    fun tickOnce(promise: Promise) = background(promise) { eng ->
        val r = eng.tickOnce()
        promise.resolve(Arguments.createMap().apply {
            putDouble("instancesAdvanced", r.instancesAdvanced.toDouble())
            putDouble("stepsExecuted", r.stepsExecuted.toDouble())
            putBoolean("hasPendingWork", r.hasPendingWork)
        })
    }

    @ReactMethod
    fun runUntilIdle(maxTicks: Double, timeBudgetMs: Double, promise: Promise) = background(promise) { eng ->
        val r = eng.runUntilIdle(maxTicks.toLong().toUInt(), timeBudgetMs.toLong().toULong())
        promise.resolve(Arguments.createMap().apply {
            putDouble("ticksExecuted", r.ticksExecuted.toDouble())
            putDouble("instancesAdvanced", r.instancesAdvanced.toDouble())
            putDouble("stepsExecuted", r.stepsExecuted.toDouble())
            putBoolean("hasPendingWork", r.hasPendingWork)
            putBoolean("budgetExhausted", r.budgetExhausted)
        })
    }

    @ReactMethod
    fun reportPowerState(state: String, promise: Promise) = withEngine(promise) { eng ->
        eng.reportPowerState(
            when (state) {
                "charging" -> PowerState.CHARGING
                "lowBattery" -> PowerState.LOW_BATTERY
                "criticalBattery" -> PowerState.CRITICAL_BATTERY
                else -> PowerState.UNPLUGGED
            },
        )
        promise.resolve(null)
    }

    // -- Handlers -------------------------------------------------------------

    @ReactMethod
    fun registerHandler(name: String, promise: Promise) = withEngine(promise) { eng ->
        eng.registerHandler(name, RNStepHandler(this, handlerTimeoutMs))
        promise.resolve(null)
    }

    /** JS answer for an `orch8:executeStep` event. */
    @ReactMethod
    fun resolveStep(requestId: String, output: String?, error: String?, permanent: Boolean) {
        pending.resolve(requestId, output, error, permanent)
    }

    // -- Instances ------------------------------------------------------------

    @ReactMethod
    fun start(sequenceName: String, input: String, dedupKey: String?, promise: Promise) =
        background(promise) { promise.resolve(it.start(sequenceName, input, dedupKey)) }

    @ReactMethod
    fun cancelInstance(instanceId: String, promise: Promise) =
        background(promise) { it.cancelInstance(instanceId); promise.resolve(null) }

    @ReactMethod
    fun getInstance(instanceId: String, promise: Promise) = background(promise) { eng ->
        val s = eng.getInstance(instanceId)
        promise.resolve(Arguments.createMap().apply {
            putString("instanceId", s.instanceId)
            putString("sequenceName", s.sequenceName)
            putString("state", stateKind(s.state))
            putString("context", s.context)
            putString("createdAt", s.createdAt)
            putString("updatedAt", s.updatedAt)
        })
    }

    @ReactMethod
    fun activeInstances(promise: Promise) = background(promise) { eng ->
        val arr = Arguments.createArray()
        for (i in eng.activeInstances()) {
            arr.pushMap(Arguments.createMap().apply {
                putString("instanceId", i.instanceId)
                putString("sequenceName", i.sequenceName)
                putString("state", stateKind(i.state))
                putString("createdAt", i.createdAt)
            })
        }
        promise.resolve(arr)
    }

    @ReactMethod
    fun completeStep(instanceId: String, stepName: String, output: String, promise: Promise) =
        background(promise) { it.completeStep(instanceId, stepName, output); promise.resolve(null) }

    // -- Sequences, sync, telemetry -------------------------------------------

    @ReactMethod
    fun loadSequenceFromJson(json: String, promise: Promise) =
        background(promise) { it.loadSequenceFromJson(json); promise.resolve(null) }

    @ReactMethod
    fun loadSequencesFromUrl(url: String, promise: Promise) =
        background(promise) { promise.resolve(it.loadSequencesFromUrl(url).toDouble()) }

    @ReactMethod
    fun loadedSequences(promise: Promise) = background(promise) { eng ->
        val arr = Arguments.createArray()
        for (seq in eng.loadedSequences()) {
            arr.pushMap(Arguments.createMap().apply {
                putString("name", seq.name)
                putInt("version", seq.version)
            })
        }
        promise.resolve(arr)
    }

    @ReactMethod
    fun sync(manifestUrl: String, token: String?, promise: Promise) = background(promise) { eng ->
        val r = eng.sync(manifestUrl, token?.let { StaticTokenProvider(it) })
        promise.resolve(Arguments.createMap().apply {
            putDouble("added", r.added.toDouble())
            putDouble("updated", r.updated.toDouble())
            putDouble("removed", r.removed.toDouble())
            putDouble("skipped", r.skipped.toDouble())
            putDouble("signatureFailures", r.signatureFailures.toDouble())
        })
    }

    @ReactMethod
    fun flushTelemetry(endpointUrl: String, promise: Promise) = background(promise) { eng ->
        val r = eng.flushTelemetry(endpointUrl)
        promise.resolve(Arguments.createMap().apply {
            putDouble("sent", r.sent.toDouble())
            putDouble("dropped", r.dropped.toDouble())
        })
    }

    @ReactMethod
    fun setDeviceContext(ctx: ReadableMap, promise: Promise) = withEngine(promise) { eng ->
        eng.setDeviceContext(
            DeviceContext(
                deviceId = ctx.str("deviceId") ?: "",
                osName = ctx.str("osName") ?: "android",
                osVersion = ctx.str("osVersion") ?: "",
                appVersion = ctx.str("appVersion") ?: "",
                sdkVersion = ctx.str("sdkVersion") ?: "react-native",
            ),
        )
        promise.resolve(null)
    }

    @ReactMethod
    fun onPushReceived(promise: Promise) = withEngine(promise) { it.onPushReceived(); promise.resolve(null) }

    // -- Runtime node / worker ------------------------------------------------

    @ReactMethod
    fun nodeRuntimeId(promise: Promise) = background(promise) { promise.resolve(it.nodeRuntimeId()) }

    @ReactMethod
    fun registerNode(caps: ReadableMap, promise: Promise) {
        val capabilities = NodeCapabilities(
            handlers = caps.strings("handlers"),
            regions = caps.strings("regions"),
            hardware = caps.strings("hardware"),
            plugins = caps.strings("plugins"),
            credentials = caps.strings("credentials"),
            offlineCapable = if (caps.hasKey("offlineCapable")) caps.getBoolean("offlineCapable") else true,
            connectivity = connectivity(caps.str("connectivity")),
            batteryPercent = batteryByte(caps.num("batteryPercent")),
            platform = caps.str("platform") ?: "android",
            pushToken = caps.str("pushToken"),
            appVersion = caps.str("appVersion"),
            apiBaseUrl = caps.str("apiBaseUrl"),
            capsuleSigningPublicKey = caps.str("capsuleSigningPublicKey"),
        )
        background(promise) { eng ->
            val r = eng.registerNode(capabilities)
            promise.resolve(Arguments.createMap().apply {
                putString("runtimeId", r.runtimeId)
                putString("deviceId", r.deviceId)
                putArray("handlers", Arguments.fromList(r.handlers))
                putString("expiresAt", r.expiresAt)
            })
        }
    }

    @ReactMethod
    fun updateNodeStatus(connectivity: String?, batteryPercent: Double?, promise: Promise) =
        background(promise) { eng ->
            eng.updateNodeStatus(connectivity(connectivity), batteryByte(batteryPercent))
            promise.resolve(null)
        }

    @ReactMethod
    fun unregisterNode(promise: Promise) = background(promise) { it.unregisterNode(); promise.resolve(null) }

    @ReactMethod
    fun startWorker(options: ReadableMap, promise: Promise) {
        val opts = WorkerOptions(
            maxConcurrentTasks = options.uint("maxConcurrentTasks", 1u),
            idlePollIntervalMs = options.ulong("idlePollIntervalMs", 15000uL),
            version = options.str("version"),
        )
        background(promise) { it.startWorker(opts); promise.resolve(null) }
    }

    @ReactMethod
    fun stopWorker(promise: Promise) = background(promise) { it.stopWorker(); promise.resolve(null) }

    @ReactMethod
    fun runWorkerWindow(timeBudgetMs: Double, promise: Promise) = background(promise) { eng ->
        val r = eng.runWorkerWindow(timeBudgetMs.toLong().toULong())
        promise.resolve(Arguments.createMap().apply {
            putDouble("claimed", r.claimed.toDouble())
            putDouble("completed", r.completed.toDouble())
            putDouble("failed", r.failed.toDouble())
            putDouble("stillRunning", r.stillRunning.toDouble())
            putBoolean("budgetExhausted", r.budgetExhausted)
        })
    }

    @ReactMethod
    fun workerStats(promise: Promise) = withEngine(promise) { eng ->
        val s = eng.workerStats()
        promise.resolve(Arguments.createMap().apply {
            putBoolean("running", s.running)
            putDouble("inFlight", s.inFlight.toDouble())
            putDouble("claimed", s.claimed.toDouble())
            putDouble("completed", s.completed.toDouble())
            putDouble("failed", s.failed.toDouble())
            putDouble("released", s.released.toDouble())
            putDouble("lost", s.lost.toDouble())
        })
    }

    @ReactMethod
    fun onPushWake(envelopeJson: String, promise: Promise) =
        withEngine(promise) { promise.resolve(it.onPushWake(envelopeJson)) }

    @ReactMethod
    fun enableBuiltin(name: String, promise: Promise) =
        withEngine(promise) { it.enableBuiltin(name); promise.resolve(null) }

    // -- Delegation from phone-local workflows -------------------------------

    private fun delegationStatusMap(s: DelegationStatus): WritableMap = Arguments.createMap().apply {
        putString("delegationId", s.delegationId)
        putString("state", s.state)
        putString("localInstanceId", s.localInstanceId)
        putString("blockId", s.blockId)
        putString("destinationRuntimeId", s.destinationRuntimeId)
        putString("outputJson", s.outputJson)
        putString("error", s.error)
    }

    @ReactMethod
    fun startDelegation(options: ReadableMap, promise: Promise) {
        val opts = DelegationOptions(
            tenantId = options.str("tenantId") ?: "",
            pollIntervalMs = options.ulong("pollIntervalMs", 2000uL),
            ttlSecs = options.uint("ttlSecs", 600u),
        )
        background(promise) { it.startDelegation(opts); promise.resolve(null) }
    }

    @ReactMethod
    fun stopDelegation(promise: Promise) = background(promise) { it.stopDelegation(); promise.resolve(null) }

    @ReactMethod
    fun delegate(request: ReadableMap, promise: Promise) {
        val req = DelegateRequest(
            instanceId = request.str("instanceId") ?: "",
            destinationRuntimeId = request.str("destinationRuntimeId") ?: "",
            subSequenceId = request.str("subSequenceId") ?: "",
            inputJson = request.str("inputJson") ?: "{}",
        )
        background(promise) { promise.resolve(it.delegate(req)) }
    }

    @ReactMethod
    fun delegationStatus(delegationId: String, promise: Promise) = background(promise) { eng ->
        promise.resolve(delegationStatusMap(eng.delegationStatus(delegationId)))
    }

    @ReactMethod
    fun listDelegations(promise: Promise) = background(promise) { eng ->
        val arr = Arguments.createArray()
        eng.listDelegations().forEach { arr.pushMap(delegationStatusMap(it)) }
        promise.resolve(arr)
    }

    @ReactMethod
    fun delegationStats(promise: Promise) = withEngine(promise) { eng ->
        val s = eng.delegationStats()
        promise.resolve(Arguments.createMap().apply {
            putBoolean("running", s.running)
            putDouble("delegated", s.delegated.toDouble())
            putDouble("completed", s.completed.toDouble())
            putDouble("failed", s.failed.toDouble())
            putDouble("abandoned", s.abandoned.toDouble())
            putDouble("resumed", s.resumed.toDouble())
        })
    }

    override fun invalidate() {
        pending.failAll("React Native context invalidated")
        work.shutdown()
        super.invalidate()
    }
}

/** Outstanding native-to-JS handler calls, keyed by request id. */
internal class PendingHandlerCalls {
    class Outcome(val output: String?, val error: String?, val permanent: Boolean)

    private class Slot {
        val latch = CountDownLatch(1)

        @Volatile var outcome: Outcome? = null
    }

    private val slots = ConcurrentHashMap<String, Slot>()

    fun open(id: String) {
        slots[id] = Slot()
    }

    fun await(id: String, timeoutMs: Long): Outcome? {
        val slot = slots[id] ?: return null
        val done = slot.latch.await(timeoutMs, TimeUnit.MILLISECONDS)
        slots.remove(id)
        return if (done) slot.outcome else null
    }

    fun resolve(id: String, output: String?, error: String?, permanent: Boolean) {
        val slot = slots[id] ?: return // timed out or unknown
        synchronized(slot) {
            if (slot.outcome != null) return
            slot.outcome = Outcome(output, error, permanent)
        }
        slot.latch.countDown()
    }

    fun failAll(message: String) {
        for (id in slots.keys.toList()) resolve(id, null, message, false)
    }
}

/**
 * Emits `orch8:executeStep` and blocks the engine thread until JS answers with
 * `resolveStep` or `handlerTimeoutMs` elapses (retryable failure).
 */
private class RNStepHandler(
    private val module: Orch8Module,
    private val timeoutMs: Long,
) : StepHandler {
    override fun execute(stepName: String, input: String): String {
        val requestId = UUID.randomUUID().toString()
        module.pending.open(requestId)
        module.sendEvent("orch8:executeStep", Arguments.createMap().apply {
            putString("requestId", requestId)
            putString("stepName", stepName)
            putString("input", input)
        })
        val outcome = module.pending.await(requestId, timeoutMs)
            ?: throw HandlerException.Retryable("JS handler '$stepName' timed out after $timeoutMs ms")
        outcome.error?.let { message ->
            throw if (outcome.permanent) HandlerException.Permanent(message) else HandlerException.Retryable(message)
        }
        return outcome.output ?: "{}"
    }
}

private class StaticTokenProvider(private val token: String) : TokenProvider {
    override fun currentToken(): String = token

    override fun refreshToken(): String = token
}

private class RNEngineListener(private val module: Orch8Module) : EngineListener {
    override fun onInstanceCompleted(instanceId: String, output: String) {
        module.sendEvent("orch8:instanceCompleted", Arguments.createMap().apply {
            putString("instanceId", instanceId)
            putString("output", output)
        })
    }

    override fun onInstanceFailed(instanceId: String, error: String) {
        module.sendEvent("orch8:instanceFailed", Arguments.createMap().apply {
            putString("instanceId", instanceId)
            putString("error", error)
        })
    }

    override fun onStepPending(instanceId: String, stepName: String, handler: String) {
        module.sendEvent("orch8:stepPending", Arguments.createMap().apply {
            putString("instanceId", instanceId)
            putString("stepName", stepName)
            putString("handler", handler)
        })
    }
}
