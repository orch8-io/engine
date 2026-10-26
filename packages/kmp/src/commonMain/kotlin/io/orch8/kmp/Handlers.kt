package io.orch8.kmp

import kotlinx.coroutines.CancellationException

/**
 * A native step handler. Receives the step's resolved params as JSON and
 * returns JSON that is merged into the execution context.
 *
 * The engine calls handlers from its blocking worker pool; the wrapper runs
 * this suspend function to completion on that thread, bounded by
 * `EngineConfig.handlerTimeoutMs`. Throw [Orch8HandlerException.Permanent] to
 * fail the instance; any other exception is retried.
 */
fun interface Orch8StepHandler {
    suspend fun execute(stepName: String, inputJson: String): String
}

/** Supplies (and refreshes) the bearer token used for authenticated manifest sync. */
interface Orch8TokenSource {
    @Throws(Orch8Exception::class, CancellationException::class)
    fun currentToken(): String

    /** Called after a 401/403. Throw [Orch8Exception] if no fresh token is available. */
    @Throws(Orch8Exception::class, CancellationException::class)
    fun refreshToken(): String
}
