package io.orch8.kmp.bridge

/**
 * An owned native scope object. Replies transfer child objects directly, keeping
 * their lifetime independent of the engine bridge and of temporary parent views.
 * Payloads use the existing JSON success/error envelope for cross-language errors.
 */
interface Orch8ScopedBridge {
    fun call(method: String, argsJson: String, reference: Orch8ScopedBridge?): Orch8ScopedReply
    fun registerHandler(name: String, timeoutMs: Long, handler: Orch8ScopedHandler): String
    fun sync(tokens: Orch8ScopedTokenSource): String
}
interface Orch8ScopedReply {
    fun payloadJson(): String
    fun resultHandle(): Orch8ScopedBridge?
}
interface Orch8ScopedHandler {
    fun execute(contextJson: String, stepName: String, inputJson: String): String
}
interface Orch8ScopedTokenSource {
    fun currentToken(): String
    /** Existing success/error envelope, with a string token on success. */
    fun refreshToken(): String
}
