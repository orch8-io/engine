package io.orch8.kmp

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject

/** Lifecycle state of an on-device workflow instance. Mirrors `InstanceStateKind`. */
enum class InstanceState(val wire: String) {
    SCHEDULED("scheduled"),
    RUNNING("running"),
    WAITING("waiting"),
    PAUSED("paused"),
    COMPLETED("completed"),
    FAILED("failed"),
    CANCELLED("cancelled"),
    ;

    /** True once the instance can no longer make progress. */
    val isTerminal: Boolean
        get() = this == COMPLETED || this == FAILED || this == CANCELLED

    companion object {
        fun fromWire(value: String): InstanceState =
            entries.firstOrNull { it.wire.equals(value, ignoreCase = true) || it.name.equals(value, ignoreCase = true) }
                ?: throw Orch8Exception(Orch8ErrorKind.INVALID_INPUT, "unknown instance state: $value")
    }
}

/** Summary returned by [Orch8Engine.activeInstances]. */
data class InstanceSummary(
    val instanceId: String,
    val sequenceName: String,
    val state: InstanceState,
    val createdAt: String,
)

/** Full snapshot returned by [Orch8Engine.getInstance]. */
data class InstanceSnapshot(
    val instanceId: String,
    val sequenceName: String,
    val state: InstanceState,
    /** Raw execution-context JSON as stored by the engine. */
    val contextJson: String,
    val createdAt: String,
    val updatedAt: String,
) {
    /**
     * The execution context parsed as a JSON object, or an empty object when
     * the engine returned something that is not a JSON object.
     */
    val context: JsonObject
        get() = runCatching { lenientJson.parseToJsonElement(contextJson).jsonObject }
            .getOrElse { JsonObject(emptyMap()) }
}

/** Result of a single foreground tick. */
data class TickResult(
    val instancesAdvanced: Int,
    val stepsExecuted: Int,
    val hasPendingWork: Boolean,
)

/** Result of a bounded background window (`runUntilIdle`). */
data class BackgroundRunResult(
    val ticksExecuted: Int,
    val instancesAdvanced: Int,
    val stepsExecuted: Int,
    val hasPendingWork: Boolean,
    /** Work remains; schedule another OS background opportunity instead of spinning. */
    val budgetExhausted: Boolean,
)

/** Result of a signed-manifest sync. */
data class SyncResult(
    val added: Int,
    val updated: Int,
    val removed: Int,
    val skipped: Int,
    val signatureFailures: Int,
)

/** Result of a telemetry flush. */
data class FlushResult(val sent: Long, val dropped: Long)

/** A sequence stored in the local database. */
data class SequenceInfo(val name: String, val version: Int)

/** Device facts attached to telemetry batches. */
data class DeviceContext(
    val deviceId: String,
    val osName: String,
    val osVersion: String,
    val appVersion: String,
    val sdkVersion: String = ORCH8_KMP_VERSION,
)

/** Result of importing a portable continuity capsule into local quarantine. */
data class ContinuityImportResult(
    val capsuleId: String,
    val continuityId: String,
    val instanceId: String,
    val sourceEpoch: Long,
    val state: String,
)

/** Version of the Orch8 engine the KMP wrapper was released against. */
const val ORCH8_KMP_VERSION: String = "0.7.1"

internal val lenientJson: Json = Json {
    ignoreUnknownKeys = true
    isLenient = true
}
