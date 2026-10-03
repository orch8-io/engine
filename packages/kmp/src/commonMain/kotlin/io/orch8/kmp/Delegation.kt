package io.orch8.kmp

import kotlinx.serialization.json.JsonObject
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// Delegation from phone-local workflows. The underlying calls need the Orch8
// engine release after 0.7.1 (start_delegation, delegate, ...).

/** Maximum lifetime of a delegation and its grant accepted by the control plane. */
val MAX_DELEGATION_TTL: Duration = 86_400.seconds

/** Options for [Orch8Engine.startDelegation]. */
data class DelegationOptions(
    /** Tenant of the node credential; every continuity call is scoped to it. */
    val tenantId: String,
    /** How often pending delegations are advanced and polled. Push wakes advance them immediately. */
    val pollInterval: Duration = 2.seconds,
    /**
     * Lifetime of each grant and delegation (whole seconds, at most one day).
     * A destination that has not reported by then fails the delegation and
     * the parked step follows its retry policy.
     */
    val ttl: Duration = 600.seconds,
) {
    init {
        require(tenantId.isNotBlank()) { "tenantId must not be blank" }
        require(pollInterval.isPositive()) { "pollInterval must be positive" }
        require(ttl >= 1.seconds && ttl <= MAX_DELEGATION_TTL) { "ttl must be between 1s and 86400s" }
    }
}

/**
 * An explicit delegation of a server-side sub-sequence ([Orch8Engine.delegate]).
 * No local step is parked.
 */
data class DelegateRequest(
    /** Local parent instance the delegation belongs to (must exist). */
    val instanceId: String,
    /** Destination runtime id (a live registration of the same tenant). */
    val destinationRuntimeId: String,
    /** Server-side sequence the destination runs. */
    val subSequenceId: String,
    /** Explicit input handed to the sub-sequence; must be a JSON object. */
    val inputJson: String = "{}",
) {
    init {
        require(instanceId.isNotBlank()) { "instanceId must not be blank" }
        require(destinationRuntimeId.isNotBlank()) { "destinationRuntimeId must not be blank" }
        require(subSequenceId.isNotBlank()) { "subSequenceId must not be blank" }
        require(runCatching { lenientJson.parseToJsonElement(inputJson) }.getOrNull() is JsonObject) {
            "inputJson must be a JSON object"
        }
    }
}

/** Where a delegation stands. Mirrors `DelegationStatus.state`. */
enum class DelegationState(val wire: String) {
    /** Journaled, not yet accepted by the control plane. */
    PREPARING("preparing"),

    /** In the destination's mailbox or running there. */
    DELEGATED("delegated"),
    COMPLETED("completed"),
    FAILED("failed"),

    /** Never placed before its deadline. */
    ABANDONED("abandoned"),
    ;

    val isTerminal: Boolean
        get() = this == COMPLETED || this == FAILED || this == ABANDONED

    companion object {
        fun fromWire(value: String): DelegationState =
            entries.firstOrNull { it.wire.equals(value, ignoreCase = true) }
                ?: throw Orch8Exception(Orch8ErrorKind.ENGINE, "unknown delegation state: $value")
    }
}

/** A delegation as journaled on this device. */
data class DelegationStatus(
    val delegationId: String,
    val state: DelegationState,
    val localInstanceId: String,
    /** The parked local step, for delegations made by a sequence. */
    val blockId: String?,
    val destinationRuntimeId: String?,
    /** The destination's reported output (JSON), once completed. */
    val outputJson: String?,
    val error: String?,
)

/** Counters of the delegation pump (zeros while it is not running). */
data class DelegationStats(
    val running: Boolean,
    /** Delegations accepted by the control plane. */
    val delegated: Long,
    val completed: Long,
    val failed: Long,
    val abandoned: Long,
    /** Parked local steps resumed with an outcome (exactly once each). */
    val resumed: Long,
)
