package io.orch8.kmp.bridge

/**
 * String-only bridge implemented in Swift over the `Orch8Mobile` Swift
 * package (see `packages/kmp/ios-bridge/Orch8KmpBridge.swift`).
 *
 * Why strings: Kotlin/Native exports Kotlin interfaces to Swift as
 * Objective-C protocols, but Swift `throws` does not cross into Kotlin as a
 * typed exception, and UniFFI's Swift records are not visible to Kotlin. A
 * single `call(method, argsJson)` entry point with a JSON envelope keeps the
 * Swift side to a thin switch and lets all decoding, validation and error
 * mapping live in common Kotlin, where it is unit-tested.
 *
 * Envelope returned by every call:
 * - success: `{"ok": <value>}` (value may be `null`)
 * - failure: `{"error": {"kind": "<MobileError case>", "message": "..."}}`
 */
interface Orch8JsonBridge {
    fun call(method: String, argsJson: String): String

    /** Installed once, right after a successful `open` call. */
    fun setCallbacks(callbacks: Orch8BridgeCallbacks)
}

/** Creates a fresh, unopened bridge. Installed from Swift at app launch. */
interface Orch8JsonBridgeFactory {
    fun create(): Orch8JsonBridge
}

/** Engine → Kotlin callbacks. Implemented in Kotlin, invoked by the Swift bridge. */
interface Orch8BridgeCallbacks {
    /**
     * Run a registered handler. Returns an envelope: `{"ok": "<output json>"}`
     * or `{"error": {"kind": "retryable"|"permanent", "message": "..."}}`.
     */
    fun executeHandler(handlerName: String, stepName: String, inputJson: String): String

    fun onInstanceCompleted(instanceId: String, output: String)

    fun onInstanceFailed(instanceId: String, error: String)

    fun onStepPending(instanceId: String, stepName: String, handler: String)

    fun currentToken(): String

    /** Envelope: `{"ok": "<token>"}` or `{"error": {...}}`. */
    fun refreshToken(): String
}

/** Bumped when the method set or payload shapes change incompatibly. */
const val ORCH8_BRIDGE_PROTOCOL: Int = 1
