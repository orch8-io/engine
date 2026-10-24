package io.orch8.kmp

import io.orch8.kmp.internal.platformIoDispatcher
import io.orch8.kmp.internal.runHandlerBlocking
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.TimeSource

/** Counts physical SDK fetches, including work from retired token sources. */
internal class TokenFetchBudget(capacity: Int = 8) {
    private val slots = Semaphore(capacity)
    val availablePermits: Int get() = slots.availablePermits
    fun tryAcquire(): Boolean = slots.tryAcquire()
    fun release() = slots.release()
    companion object { val shared = TokenFetchBudget() }
}

/** Initial fetch and native refresh share a flight and physical admission. */
internal class DeviceSessionTokens(
    initial: String,
    private val fetchToken: suspend () -> String,
    private val timeout: Duration,
    dispatcher: CoroutineDispatcher = platformIoDispatcher,
    private val budget: TokenFetchBudget = TokenFetchBudget.shared,
) : Orch8TokenSource {
    private class Flight(owner: Job) {
        val parent = Job(owner)
        val result = CompletableDeferred<String>(parent)
        val started = TimeSource.Monotonic.markNow()
    }

    private data class CachedToken(val token: String, val closed: Boolean = false)
    private val cached = MutableStateFlow(CachedToken(initial))
    private val owner = SupervisorJob()
    private val scope = CoroutineScope(owner + dispatcher)
    private val state = Mutex()
    private var active: Flight? = null

    init { require(timeout.isFinite() && timeout.isPositive()) { "refreshTimeout must be finite and greater than zero" } }
    override fun currentToken(): String = cached.value.token
    fun close() {
        cached.value = CachedToken("", closed = true)
        owner.cancel()
    }

    suspend fun initialToken(): String = try {
        fetch(initial = true)
    } catch (e: Orch8Exception) {
        if (e.kind == Orch8ErrorKind.INVALID_INPUT) throw IllegalArgumentException("fetchToken returned an empty token", e)
        throw e
    }

    override fun refreshToken(): String = try {
        runHandlerBlocking { fetch() }
    } catch (e: CancellationException) {
        throw Orch8Exception(Orch8ErrorKind.ENGINE, "token provider cancelled", e)
    }

    private fun timeoutError() = Orch8Exception(Orch8ErrorKind.NETWORK, "token provider timed out")

    private suspend fun fetch(initial: Boolean = false): String {
        currentCoroutineContext().ensureActive()
        var created = false
        val flight = state.withLock {
            val snapshot = cached.value
            if (snapshot.closed || !owner.isActive) throw CancellationException("token source retired")
            if (initial && snapshot.token.isNotBlank()) return snapshot.token
            active?.let {
                if (it.parent.isCompleted) active = null
                else {
                    if (it.parent.isCancelled) throw timeoutError()
                    return@withLock it
                }
            }
            if (!budget.tryAcquire()) throw Orch8Exception(Orch8ErrorKind.RESOURCE_LIMIT, "token fetch capacity exhausted")
            Flight(owner).also { active = it; created = true }
        }
        if (created) start(flight)
        try {
            val remaining = timeout - flight.started.elapsedNow()
            if (!remaining.isPositive()) throw timeoutError()
            return withTimeout(remaining) { flight.result.await() }
        } catch (e: Throwable) {
            // Cancelling a wait does not finish non-cooperative child work.
            flight.parent.cancel()
            if (e is TimeoutCancellationException) throw timeoutError()
            throw e
        }
    }

    private fun start(flight: Flight) {
        val physical = try {
            scope.launch(flight.parent + CoroutineExceptionHandler { _, cause ->
                // Host cancellation handlers must not escape through a native
                // callback or print their potentially sensitive error message.
                flight.result.completeExceptionally(Orch8Exception(Orch8ErrorKind.ENGINE, "token fetch cancellation failed", cause))
            }) {
                var outcome = try {
                    currentCoroutineContext().ensureActive()
                    if (flight.started.elapsedNow() >= timeout) throw timeoutError()
                    // Isolate host children/completion handlers: neither the
                    // token nor physical admission can precede their finish.
                    val fresh = coroutineScope { fetchToken() }
                    currentCoroutineContext().ensureActive()
                    if (fresh.isBlank()) Result.failure(Orch8Exception(Orch8ErrorKind.INVALID_INPUT, "token provider returned an empty token"))
                    else Result.success(fresh)
                } catch (e: CancellationException) {
                    Result.failure(e)
                } catch (e: Throwable) {
                    Result.failure(Orch8Exception(Orch8ErrorKind.NETWORK, "token provider failed", e))
                }
                withContext(NonCancellable) {
                    state.withLock {
                        if (flight.parent.isCancelled) outcome = Result.failure(CancellationException("token fetch abandoned"))
                        else if (flight.started.elapsedNow() >= timeout) outcome = Result.failure(timeoutError())
                        if (active === flight) {
                            outcome.getOrNull()?.let { fresh ->
                                val previous = cached.getAndUpdate { if (it.closed) it else it.copy(token = fresh) }
                                if (previous.closed) outcome = Result.failure(CancellationException("token source retired"))
                            }
                            active = null
                        }
                    }
                }
                outcome.fold(flight.result::complete, flight.result::completeExceptionally)
            }
        } catch (e: Throwable) {
            flight.parent.cancel()
            budget.release()
            throw e
        }
        physical.invokeOnCompletion { cause ->
            if (!flight.result.isCompleted) flight.result.completeExceptionally(cause ?: CancellationException("token fetch ended"))
            flight.parent.complete()
            budget.release()
        }
    }
}
