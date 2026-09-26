package io.orch8.kmp.internal

import io.orch8.kmp.BackgroundRunResult
import io.orch8.kmp.ContinuityImportResult
import io.orch8.kmp.DeviceContext
import io.orch8.kmp.EngineConfig
import io.orch8.kmp.EngineEvent
import io.orch8.kmp.FlushResult
import io.orch8.kmp.InstanceSnapshot
import io.orch8.kmp.InstanceState
import io.orch8.kmp.InstanceSummary
import io.orch8.kmp.Orch8ErrorKind
import io.orch8.kmp.Orch8Exception
import io.orch8.kmp.Orch8HandlerException
import io.orch8.kmp.Orch8TokenSource
import io.orch8.kmp.PowerState
import io.orch8.kmp.SequenceInfo
import io.orch8.kmp.SyncResult
import io.orch8.kmp.TickResult
import io.orch8.kmp.bridge.ORCH8_BRIDGE_PROTOCOL
import io.orch8.kmp.bridge.Orch8BridgeCallbacks
import io.orch8.kmp.bridge.Orch8JsonBridge
import io.orch8.kmp.lenientJson
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put

/** Encodes/decodes the bridge envelope. Pure; shared by backend and callbacks. */
internal object BridgeCodec {
    fun ok(value: JsonElement = JsonNull): String = buildJsonObject { put("ok", value) }.toString()

    fun error(kind: String, message: String): String =
        buildJsonObject {
            put("error", buildJsonObject {
                put("kind", kind)
                put("message", message)
            })
        }.toString()

    /** Returns the `ok` value or throws the decoded [Orch8Exception]. */
    fun unwrap(method: String, envelope: String): JsonElement {
        val root = try {
            lenientJson.parseToJsonElement(envelope).jsonObject
        } catch (e: Exception) {
            throw Orch8Exception(Orch8ErrorKind.ENGINE, "bridge returned malformed envelope for $method", e)
        }
        root["error"]?.let { err ->
            val obj = err as? JsonObject
            val kind = obj?.get("kind")?.jsonPrimitive?.contentOrNull ?: "unknown"
            val message = obj?.get("message")?.jsonPrimitive?.contentOrNull ?: "unknown bridge error"
            throw Orch8Exception(Orch8ErrorKind.fromWire(kind), message)
        }
        if (!root.containsKey("ok")) {
            throw Orch8Exception(Orch8ErrorKind.ENGINE, "bridge envelope for $method has neither ok nor error")
        }
        return root.getValue("ok")
    }

    fun configJson(config: EngineConfig): JsonObject = buildJsonObject {
        put("tickIntervalMs", config.tickIntervalMs)
        put("maxConcurrentSteps", config.maxConcurrentSteps)
        put("maxStepsPerInstance", config.maxStepsPerInstance)
        put("maxConcurrentInstances", config.maxConcurrentInstances)
        put("maxTickDurationMs", config.maxTickDurationMs)
        put("maxInstanceLifetimeSecs", config.maxInstanceLifetimeSecs)
        put("maxStoredSequences", config.maxStoredSequences)
        put("maxSequenceSizeBytes", config.maxSequenceSizeBytes)
        put("handlerTimeoutMs", config.handlerTimeoutMs)
        put("operationTimeoutMs", config.operationTimeoutMs)
        put("telemetryEnabled", config.telemetryEnabled)
        put("telemetryUrl", config.telemetryUrl)
        put("environment", config.environment)
        put("rootPublicKey", config.rootPublicKey)
        put("sdkVersion", config.sdkVersion)
        put("memoryBudgetBytes", config.memoryBudgetBytes)
        put("sequencesUrl", config.sequencesUrl)
        put("syncUrl", config.syncUrl)
        put("deviceId", config.deviceId)
        put("syncApiKey", config.syncApiKey)
    }

    fun powerStateWire(state: PowerState): String = when (state) {
        PowerState.CHARGING -> "charging"
        PowerState.UNPLUGGED -> "unplugged"
        PowerState.LOW_BATTERY -> "lowBattery"
        PowerState.CRITICAL_BATTERY -> "criticalBattery"
    }

    // --- decoding helpers -------------------------------------------------

    private fun JsonElement.obj(method: String): JsonObject =
        this as? JsonObject ?: throw malformed(method, "expected object")

    private fun JsonObject.str(method: String, key: String): String =
        (this[key] as? JsonPrimitive)?.contentOrNull ?: throw malformed(method, "missing string '$key'")

    private fun JsonObject.int(method: String, key: String): Int =
        (this[key] as? JsonPrimitive)?.intOrNull ?: throw malformed(method, "missing int '$key'")

    private fun JsonObject.long(method: String, key: String): Long =
        (this[key] as? JsonPrimitive)?.longOrNull ?: throw malformed(method, "missing number '$key'")

    private fun JsonObject.bool(method: String, key: String): Boolean =
        (this[key] as? JsonPrimitive)?.booleanOrNull ?: throw malformed(method, "missing bool '$key'")

    private fun malformed(method: String, detail: String) =
        Orch8Exception(Orch8ErrorKind.ENGINE, "bridge result for $method malformed: $detail")

    fun tick(v: JsonElement): TickResult = v.obj("tickOnce").let {
        TickResult(
            instancesAdvanced = it.int("tickOnce", "instancesAdvanced"),
            stepsExecuted = it.int("tickOnce", "stepsExecuted"),
            hasPendingWork = it.bool("tickOnce", "hasPendingWork"),
        )
    }

    fun background(v: JsonElement): BackgroundRunResult = v.obj("runUntilIdle").let {
        val m = "runUntilIdle"
        BackgroundRunResult(
            ticksExecuted = it.int(m, "ticksExecuted"),
            instancesAdvanced = it.int(m, "instancesAdvanced"),
            stepsExecuted = it.int(m, "stepsExecuted"),
            hasPendingWork = it.bool(m, "hasPendingWork"),
            budgetExhausted = it.bool(m, "budgetExhausted"),
        )
    }

    fun snapshot(v: JsonElement): InstanceSnapshot = v.obj("getInstance").let {
        val m = "getInstance"
        InstanceSnapshot(
            instanceId = it.str(m, "instanceId"),
            sequenceName = it.str(m, "sequenceName"),
            state = InstanceState.fromWire(it.str(m, "state")),
            contextJson = it.str(m, "context"),
            createdAt = it.str(m, "createdAt"),
            updatedAt = it.str(m, "updatedAt"),
        )
    }

    fun summaries(v: JsonElement): List<InstanceSummary> =
        (v as? JsonArray ?: throw malformed("activeInstances", "expected array")).map { e ->
            val m = "activeInstances"
            val o = e.obj(m)
            InstanceSummary(
                instanceId = o.str(m, "instanceId"),
                sequenceName = o.str(m, "sequenceName"),
                state = InstanceState.fromWire(o.str(m, "state")),
                createdAt = o.str(m, "createdAt"),
            )
        }

    fun sequences(v: JsonElement): List<SequenceInfo> =
        (v as? JsonArray ?: throw malformed("loadedSequences", "expected array")).map { e ->
            val o = e.obj("loadedSequences")
            SequenceInfo(o.str("loadedSequences", "name"), o.int("loadedSequences", "version"))
        }

    fun sync(v: JsonElement): SyncResult = v.obj("sync").let {
        SyncResult(
            added = it.int("sync", "added"),
            updated = it.int("sync", "updated"),
            removed = it.int("sync", "removed"),
            skipped = it.int("sync", "skipped"),
            signatureFailures = it.int("sync", "signatureFailures"),
        )
    }

    fun flush(v: JsonElement): FlushResult = v.obj("flushTelemetry").let {
        FlushResult(sent = it.long("flushTelemetry", "sent"), dropped = it.long("flushTelemetry", "dropped"))
    }

    fun continuityImport(v: JsonElement): ContinuityImportResult = v.obj("importContinuityCapsule").let {
        val m = "importContinuityCapsule"
        ContinuityImportResult(
            capsuleId = it.str(m, "capsuleId"),
            continuityId = it.str(m, "continuityId"),
            instanceId = it.str(m, "instanceId"),
            sourceEpoch = it.long(m, "sourceEpoch"),
            state = it.str(m, "state"),
        )
    }
}

/**
 * [EngineBackend] over a Swift-implemented [Orch8JsonBridge]. Lives in common
 * code so the whole protocol is tested on the JVM with a fake bridge.
 */
internal class JsonBridgeBackend private constructor(
    private val bridge: Orch8JsonBridge,
) : EngineBackend {
    private val handlers = HashMap<String, BlockingStepHandler>()
    private var sink: (EngineEvent) -> Unit = {}

    // Only read while a sync() call is in flight; sync calls are serialized by the engine.
    private var activeTokens: Orch8TokenSource? = null

    private val callbacks = object : Orch8BridgeCallbacks {
        override fun executeHandler(handlerName: String, stepName: String, inputJson: String): String {
            val handler = handlers[handlerName]
                ?: return BridgeCodec.error("permanent", "no handler registered for '$handlerName'")
            return try {
                BridgeCodec.ok(JsonPrimitive(handler.execute(stepName, inputJson)))
            } catch (e: Orch8HandlerException.Permanent) {
                BridgeCodec.error("permanent", e.message ?: "permanent handler failure")
            } catch (e: Exception) {
                BridgeCodec.error("retryable", e.message ?: "retryable handler failure")
            }
        }

        override fun onInstanceCompleted(instanceId: String, output: String) =
            sink(EngineEvent.InstanceCompleted(instanceId, output))

        override fun onInstanceFailed(instanceId: String, error: String) =
            sink(EngineEvent.InstanceFailed(instanceId, error))

        override fun onStepPending(instanceId: String, stepName: String, handler: String) =
            sink(EngineEvent.StepPending(instanceId, stepName, handler))

        override fun currentToken(): String = activeTokens?.currentToken().orEmpty()

        override fun refreshToken(): String {
            val tokens = activeTokens ?: return BridgeCodec.error("invalid_input", "no token source")
            return try {
                BridgeCodec.ok(JsonPrimitive(tokens.refreshToken()))
            } catch (e: Orch8Exception) {
                BridgeCodec.error(e.kind.wire, e.message ?: "token refresh failed")
            } catch (e: Exception) {
                BridgeCodec.error("network", e.message ?: "token refresh failed")
            }
        }
    }

    private fun call(method: String, args: JsonObjectBuilder.() -> Unit = {}): JsonElement =
        BridgeCodec.unwrap(method, bridge.call(method, buildJsonObject(args).toString()))

    override fun registerHandler(name: String, handler: BlockingStepHandler) {
        call("registerHandler") { put("name", name) }
        handlers[name] = handler
    }

    override fun setListener(sink: (EngineEvent) -> Unit) {
        this.sink = sink
    }

    override fun resume() {
        call("resume")
    }

    override fun pause() {
        call("pause")
    }

    override fun shutdown() {
        call("shutdown")
    }

    override fun tickOnce(): TickResult = BridgeCodec.tick(call("tickOnce"))

    override fun runUntilIdle(maxTicks: Int, timeBudgetMs: Long): BackgroundRunResult =
        BridgeCodec.background(
            call("runUntilIdle") {
                put("maxTicks", maxTicks)
                put("timeBudgetMs", timeBudgetMs)
            },
        )

    override fun reportPowerState(state: PowerState) {
        call("reportPowerState") { put("state", BridgeCodec.powerStateWire(state)) }
    }

    override fun onPushReceived() {
        call("onPushReceived")
    }

    override fun start(sequenceName: String, inputJson: String, dedupKey: String?): String {
        val result = call("start") {
            put("sequenceName", sequenceName)
            put("input", inputJson)
            put("dedupKey", dedupKey)
        }
        return (result as? JsonPrimitive)?.contentOrNull
            ?: throw Orch8Exception(Orch8ErrorKind.ENGINE, "bridge start returned no instance id")
    }

    override fun cancelInstance(instanceId: String) {
        call("cancelInstance") { put("instanceId", instanceId) }
    }

    override fun getInstance(instanceId: String): InstanceSnapshot =
        BridgeCodec.snapshot(call("getInstance") { put("instanceId", instanceId) })

    override fun activeInstances(): List<InstanceSummary> = BridgeCodec.summaries(call("activeInstances"))

    override fun completeStep(instanceId: String, stepName: String, outputJson: String) {
        call("completeStep") {
            put("instanceId", instanceId)
            put("stepName", stepName)
            put("output", outputJson)
        }
    }

    override fun loadSequenceFromJson(json: String) {
        call("loadSequenceFromJson") { put("json", json) }
    }

    override fun loadSequencesFromUrl(url: String): Int =
        call("loadSequencesFromUrl") { put("url", url) }.jsonPrimitive.intOrNull
            ?: throw Orch8Exception(Orch8ErrorKind.ENGINE, "bridge loadSequencesFromUrl returned no count")

    override fun loadedSequences(): List<SequenceInfo> = BridgeCodec.sequences(call("loadedSequences"))

    override fun sync(manifestUrl: String, tokens: Orch8TokenSource?): SyncResult {
        activeTokens = tokens
        try {
            return BridgeCodec.sync(
                call("sync") {
                    put("manifestUrl", manifestUrl)
                    put("useTokenSource", tokens != null)
                },
            )
        } finally {
            activeTokens = null
        }
    }

    override fun flushTelemetry(endpointUrl: String): FlushResult =
        BridgeCodec.flush(call("flushTelemetry") { put("endpointUrl", endpointUrl) })

    override fun setDeviceContext(ctx: DeviceContext) {
        call("setDeviceContext") {
            put("deviceId", ctx.deviceId)
            put("osName", ctx.osName)
            put("osVersion", ctx.osVersion)
            put("appVersion", ctx.appVersion)
            put("sdkVersion", ctx.sdkVersion)
        }
    }

    override fun importContinuityCapsule(
        capsuleJson: String,
        payloadBase64: String,
        payloadKeyBase64: String,
        destinationRuntimeId: String,
        destinationInstanceId: String,
    ): ContinuityImportResult = BridgeCodec.continuityImport(
        call("importContinuityCapsule") {
            put("capsuleJson", capsuleJson)
            put("payloadBase64", payloadBase64)
            put("payloadKeyBase64", payloadKeyBase64)
            put("destinationRuntimeId", destinationRuntimeId)
            put("destinationInstanceId", destinationInstanceId)
        },
    )

    override fun activateContinuityCapsule(capsuleId: String, destinationRuntimeId: String, destinationInstanceId: String) {
        call("activateContinuityCapsule") {
            put("capsuleId", capsuleId)
            put("destinationRuntimeId", destinationRuntimeId)
            put("destinationInstanceId", destinationInstanceId)
        }
    }

    companion object {
        /** Open the engine through [bridge] and install the callbacks. */
        fun open(bridge: Orch8JsonBridge, dbPath: String, config: EngineConfig): JsonBridgeBackend {
            val backend = JsonBridgeBackend(bridge)
            val result = backend.call("open") {
                put("protocol", ORCH8_BRIDGE_PROTOCOL)
                put("dbPath", dbPath)
                put("config", BridgeCodec.configJson(config))
            }
            val remoteProtocol = (result as? JsonObject)?.get("protocol")?.jsonPrimitive?.intOrNull
            if (remoteProtocol != ORCH8_BRIDGE_PROTOCOL) {
                throw Orch8Exception(
                    Orch8ErrorKind.INVALID_INPUT,
                    "Swift bridge speaks protocol $remoteProtocol, KMP expects $ORCH8_BRIDGE_PROTOCOL; " +
                        "update Orch8KmpBridge.swift to the version shipped with this library",
                )
            }
            bridge.setCallbacks(backend.callbacks)
            return backend
        }
    }
}
