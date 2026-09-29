package io.orch8.kmp

import io.orch8.kmp.internal.runHandlerBlocking
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import kotlin.concurrent.Volatile
import kotlin.time.Duration

/**
 * The node credential behind [Orch8Engine.setTokenProvider]: caches the
 * current device-session token and, when the engine reports a `401`, runs the
 * host's suspend `fetchToken` to completion on the engine's blocking thread
 * (never the UI thread), bounded by [timeout].
 */
internal class DeviceSessionTokens(
    initial: String,
    private val fetchToken: suspend () -> String,
    private val timeout: Duration,
) : Orch8TokenSource {
    @Volatile
    private var token: String = initial

    override fun currentToken(): String = token

    override fun refreshToken(): String {
        val fresh = try {
            runHandlerBlocking { withTimeout(timeout) { fetchToken() } }
        } catch (e: TimeoutCancellationException) {
            throw Orch8Exception(Orch8ErrorKind.NETWORK, "token provider timed out after $timeout", e)
        } catch (e: CancellationException) {
            throw Orch8Exception(Orch8ErrorKind.ENGINE, "token provider cancelled: ${e.message}", e)
        } catch (e: Orch8Exception) {
            throw e
        } catch (e: Exception) {
            throw Orch8Exception(Orch8ErrorKind.NETWORK, e.message ?: "token provider failed", e)
        }
        if (fresh.isBlank()) {
            throw Orch8Exception(Orch8ErrorKind.INVALID_INPUT, "token provider returned an empty token")
        }
        token = fresh
        return fresh
    }
}
