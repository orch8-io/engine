package io.orch8.kmp

/**
 * Error categories shared by every platform. They mirror the Rust
 * `MobileError` and `SyncError` variants so an error means the same thing on
 * Android and iOS.
 */
enum class Orch8ErrorKind(val wire: String) {
    ENGINE("engine"),
    STORAGE("storage"),
    INVALID_INPUT("invalid_input"),
    NOT_FOUND("not_found"),
    RESOURCE_LIMIT("resource_limit"),
    ALREADY_EXISTS("already_exists"),
    SHUTDOWN("shutdown"),
    NETWORK("network"),
    SIGNATURE_INVALID("signature_invalid"),
    INVALID_MANIFEST("invalid_manifest"),

    /** The platform reported an error this wrapper version does not know. */
    UNKNOWN("unknown"),
    ;

    companion object {
        /**
         * Parse a wire name. Accepts snake_case (`not_found`), UniFFI case
         * names (`NotFound`) and enum names (`NOT_FOUND`).
         */
        fun fromWire(value: String): Orch8ErrorKind {
            val normalized = value.trim()
                .replace(Regex("([a-z0-9])([A-Z])"), "$1_$2")
                .lowercase()
            return entries.firstOrNull { it.wire == normalized } ?: UNKNOWN
        }
    }
}

/** Any failure reported by the embedded engine. */
class Orch8Exception(
    val kind: Orch8ErrorKind,
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause) {
    /**
     * Whether retrying the same call later can plausibly succeed. Invalid
     * input, missing records and signature failures are not retryable.
     */
    val isRetryable: Boolean
        get() = when (kind) {
            Orch8ErrorKind.NETWORK,
            Orch8ErrorKind.RESOURCE_LIMIT,
            Orch8ErrorKind.STORAGE,
            -> true
            else -> false
        }

    override fun toString(): String = "Orch8Exception(${kind.wire}): $message"
}

/**
 * Throw from an [Orch8StepHandler] to control how the engine treats the
 * failure. Any other exception is treated as [Retryable].
 */
sealed class Orch8HandlerException(message: String, cause: Throwable? = null) : Exception(message, cause) {
    /** Transient failure: the step is retried per its retry policy. */
    class Retryable(message: String, cause: Throwable? = null) : Orch8HandlerException(message, cause)

    /** Fatal failure: the instance fails. */
    class Permanent(message: String, cause: Throwable? = null) : Orch8HandlerException(message, cause)
}
