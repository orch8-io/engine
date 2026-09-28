package io.orch8.kmp

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.longOrNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// Runtime node / worker types. The underlying calls need the Orch8 engine
// release after 0.7.1 (io.orch8:orch8-mobile / Orch8Mobile with
// register_node, start_worker, ...).

/** Network facts a node advertises. Mirrors `NodeConnectivity`. */
enum class NodeConnectivity(val wire: String) {
    OFFLINE("offline"),
    METERED("metered"),
    WIFI("wifi"),
    ETHERNET("ethernet"),
}

/**
 * What this device offers the distributed-execution mesh. Every field is
 * optional; `handlers` empty means every handler registered with
 * [Orch8Engine.registerHandler].
 */
data class NodeCapabilities(
    val handlers: List<String> = emptyList(),
    val regions: List<String> = emptyList(),
    /** Free-form hardware facts (`camera`, `nfc`, …). `device:<deviceId>` is always added. */
    val hardware: List<String> = emptyList(),
    val plugins: List<String> = emptyList(),
    /** Credential binding *names* available on the device (never secrets). */
    val credentials: List<String> = emptyList(),
    val offlineCapable: Boolean = true,
    val connectivity: NodeConnectivity? = null,
    /** 0..100. */
    val batteryPercent: Int? = null,
    /** `ios` / `android`; inferred natively when null. */
    val platform: String? = null,
    /** APNs/FCM token used for id-only wake-up hints. */
    val pushToken: String? = null,
    val appVersion: String? = null,
    /** Overrides the API base derived from `EngineConfig.syncUrl`. */
    val apiBaseUrl: String? = null,
    val capsuleSigningPublicKey: String? = null,
) {
    init {
        require(batteryPercent == null || batteryPercent in 0..100) { "batteryPercent must be in 0..100" }
    }
}

/** Result of [Orch8Engine.registerNode]. */
data class NodeRegistration(
    val runtimeId: String,
    val deviceId: String,
    val handlers: List<String>,
    val expiresAt: String,
)

/** Options for [Orch8Engine.startWorker]. */
data class WorkerOptions(
    /** Remote tasks executed concurrently on the device. */
    val maxConcurrentTasks: Int = 1,
    /** Poll cadence while idle, before power-state scaling. */
    val idlePollInterval: Duration = 15.seconds,
    val version: String? = null,
) {
    init {
        require(maxConcurrentTasks > 0) { "maxConcurrentTasks must be positive" }
        require(idlePollInterval.isPositive()) { "idlePollInterval must be positive" }
    }
}

/** Worker counters since the engine was opened. */
data class WorkerStats(
    val running: Boolean,
    val inFlight: Int,
    val claimed: Long,
    val completed: Long,
    val failed: Long,
    val released: Long,
    val lost: Long,
)

/** Result of [Orch8Engine.runWorkerWindow]. */
data class WorkerWindowResult(
    val claimed: Long,
    val completed: Long,
    val failed: Long,
    val stillRunning: Int,
    val budgetExhausted: Boolean,
)

/**
 * Remote-task metadata the worker loop adds to a handler's params as the
 * reserved `__orch8` member. Absent for steps started on-device.
 */
data class Orch8TaskContext(
    /**
     * The server's deterministic idempotency key for the step's effect. Send
     * it to downstream APIs (for example as `Idempotency-Key`). Null against
     * servers that predate the distributed-execution contract.
     */
    val effectId: String?,
    val taskId: String?,
    val instanceId: String?,
    val blockId: String?,
    val attempt: Int?,
    val runtimeId: String?,
    val continuityEpoch: Long?,
    val resumeCheckpoint: JsonElement?,
) {
    companion object {
        /** Parse `__orch8` from a handler's `inputJson`; null for local steps or non-object params. */
        fun fromInput(inputJson: String): Orch8TaskContext? {
            val root = runCatching { lenientJson.parseToJsonElement(inputJson) }.getOrNull() as? JsonObject
                ?: return null
            val meta = root["__orch8"] as? JsonObject ?: return null
            fun str(key: String) = (meta[key] as? JsonPrimitive)?.takeIf { it.isString }?.contentOrNull
            fun num(key: String) = (meta[key] as? JsonPrimitive)?.takeUnless { it.isString }?.longOrNull
            return Orch8TaskContext(
                effectId = str("effect_id"),
                taskId = str("task_id"),
                instanceId = str("instance_id"),
                blockId = str("block_id"),
                attempt = num("attempt")?.toInt(),
                runtimeId = str("runtime_id"),
                continuityEpoch = num("continuity_epoch"),
                resumeCheckpoint = meta["resume_checkpoint"]?.takeUnless { it is JsonNull },
            )
        }
    }
}
