package io.orch8.kmp

/**
 * Engine configuration. Field names and defaults match `MobileEngineConfig`
 * in docs/MOBILE_SDK.md; numeric fields use signed Kotlin types in common
 * code and are converted to the unsigned FFI types by each platform.
 */
data class EngineConfig(
    val tickIntervalMs: Long = 500,
    val maxConcurrentSteps: Int = 4,
    val maxStepsPerInstance: Int = 1000,
    val maxConcurrentInstances: Int = 10,
    val maxTickDurationMs: Long = 5_000,
    val maxInstanceLifetimeSecs: Long = 86_400,
    val maxStoredSequences: Int = 50,
    val maxSequenceSizeBytes: Long = 1_048_576,
    val handlerTimeoutMs: Long = 30_000,
    val operationTimeoutMs: Long = 10_000,
    val telemetryEnabled: Boolean = true,
    val telemetryUrl: String = "",
    val environment: String = "production",
    val rootPublicKey: String = "",
    val sdkVersion: String = ORCH8_KMP_VERSION,
    val memoryBudgetBytes: Long = 0,
    val sequencesUrl: String = "",
    val syncUrl: String = "",
    val deviceId: String = "",
    val syncApiKey: String = "",
) {
    /**
     * Fail fast on values the engine would reject (or silently misbehave on)
     * so the error surfaces at construction on every platform identically.
     * The engine still performs its own, stricter URL checks (public host,
     * port 443) when it is constructed.
     */
    fun validate(): EngineConfig {
        fun positive(name: String, value: Long) {
            if (value <= 0) invalid("$name must be greater than zero (got $value)")
        }
        positive("tickIntervalMs", tickIntervalMs)
        positive("maxConcurrentSteps", maxConcurrentSteps.toLong())
        positive("maxStepsPerInstance", maxStepsPerInstance.toLong())
        positive("maxConcurrentInstances", maxConcurrentInstances.toLong())
        positive("maxTickDurationMs", maxTickDurationMs)
        positive("maxInstanceLifetimeSecs", maxInstanceLifetimeSecs)
        positive("maxStoredSequences", maxStoredSequences.toLong())
        positive("maxSequenceSizeBytes", maxSequenceSizeBytes)
        positive("handlerTimeoutMs", handlerTimeoutMs)
        positive("operationTimeoutMs", operationTimeoutMs)
        if (memoryBudgetBytes < 0) invalid("memoryBudgetBytes must be >= 0 (0 = unlimited)")
        if (environment != "production" && environment != "staging") {
            invalid("environment must be \"production\" or \"staging\" (got \"$environment\")")
        }
        requireHttpsOrEmpty("telemetryUrl", telemetryUrl)
        requireHttpsOrEmpty("sequencesUrl", sequencesUrl)
        requireHttpsOrEmpty("syncUrl", syncUrl)
        if (syncUrl.isNotEmpty() && deviceId.isBlank()) {
            invalid("deviceId is required when syncUrl is set")
        }
        return this
    }

    /** True when the bidirectional status/approval/command channel is configured. */
    val serverSyncEnabled: Boolean
        get() = syncUrl.isNotEmpty()

    override fun toString(): String =
        // Never print the sync API key.
        "EngineConfig(environment=$environment, syncUrl=$syncUrl, deviceId=$deviceId, " +
            "syncApiKey=${if (syncApiKey.isEmpty()) "<unset>" else "<redacted>"}, " +
            "tickIntervalMs=$tickIntervalMs, maxConcurrentInstances=$maxConcurrentInstances)"

    private fun requireHttpsOrEmpty(name: String, value: String) {
        if (value.isNotEmpty() && !value.startsWith("https://")) {
            invalid("$name must be an https:// URL (got \"$value\")")
        }
    }

    private fun invalid(message: String): Nothing =
        throw Orch8Exception(Orch8ErrorKind.INVALID_INPUT, message)
}
