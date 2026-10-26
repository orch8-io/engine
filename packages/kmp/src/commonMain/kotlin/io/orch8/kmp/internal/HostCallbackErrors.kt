package io.orch8.kmp.internal

import io.orch8.kmp.Orch8Exception
import io.orch8.kmp.Orch8HandlerException
import io.orch8.kmp.Orch8TokenSource

/** Unknown host exceptions can override message with another failing callback. */
internal fun Throwable.hostFailureMessage(fallback: String): String = when (this) {
    is Orch8Exception -> message ?: fallback
    is Orch8HandlerException.Permanent -> message ?: fallback
    is Orch8HandlerException.Retryable -> message ?: fallback
    // Exact standard classes have a known constructor-backed message getter.
    // An `is` check would also admit a host subclass overriding that getter.
    else -> when (this::class) {
        Exception::class, RuntimeException::class, IllegalStateException::class,
        IllegalArgumentException::class -> message ?: fallback
        else -> fallback
    }
}

/** Cached-token callbacks cannot throw across their nonthrowing native port. */
internal fun Orch8TokenSource.cachedTokenOrEmpty(): String = try { currentToken() } catch (_: Exception) { "" }
