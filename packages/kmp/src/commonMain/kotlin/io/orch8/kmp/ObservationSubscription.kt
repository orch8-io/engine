package io.orch8.kmp

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.yield
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/** Nonthrowing callbacks run on IO; dispatch UI changes to the main thread. */
interface Orch8ObservationObserver {
    fun onError(error: Orch8Exception)
    fun onComplete()
}
interface InstanceSnapshotObserver : Orch8ObservationObserver { fun onInstance(snapshot: InstanceSnapshot) }
interface ActiveInstancesObserver : Orch8ObservationObserver { fun onInstances(instances: List<InstanceSummary>) }
interface DelegationStatusObserver : Orch8ObservationObserver { fun onStatus(status: DelegationStatus) }
interface EngineEventObserver : Orch8ObservationObserver { fun onEvent(event: EngineEvent) }
interface PendingStepsObserver : Orch8ObservationObserver { fun onSteps(steps: List<PendingStep>) }

/** Stops this observation without cancelling workflow/delegation work. */
class Orch8ObservationSubscription internal constructor(private val stop: () -> Unit) : AutoCloseable {
    override fun close() = stop()
}

internal fun pollingInterval(milliseconds: Long): Duration = milliseconds.milliseconds.also {
    require(it.isFinite() && it.isPositive()) { "pollIntervalMs must be finite and greater than zero" }
}

// Identity equality avoids host equals calls when retiring callback references.
private class ObserverDispatch<T>(val lifecycle: Orch8ObservationObserver, val emit: (T) -> Unit)

internal fun <T> subscribeFlow(
    source: Flow<T>,
    observer: Orch8ObservationObserver,
    emit: (T) -> Unit,
    io: CoroutineDispatcher,
    owner: Job? = null,
    startImmediately: Boolean = false,
    accountId: String? = null,
    budget: ObservationBudget = ObservationBudget.shared,
): () -> Unit {
    // Reserve before retaining a callback or scheduling a native snapshot read.
    val reservation = budget.reserve(owner, accountId)
    val slot = MutableStateFlow<ObserverDispatch<T>?>(ObserverDispatch(observer, emit))
    val lifetime = SupervisorJob(owner)
    val coroutineStart = if (startImmediately) CoroutineStart.UNDISPATCHED else CoroutineStart.DEFAULT
    val task = try {
        CoroutineScope(io + lifetime).launch(start = coroutineStart) {
            try {
                source.collect {
                    // Hot collectors attach before subscribe returns. Yield before
                    // delivery so initial StateFlow values still reach callbacks
                    // through the configured dispatcher, not the caller's thread.
                    if (startImmediately) yield()
                    currentCoroutineContext().ensureActive()
                    slot.value?.emit?.invoke(it)
                }
                currentCoroutineContext().ensureActive()
                slot.value?.lifecycle?.onComplete()
            } catch (_: CancellationException) {
                // Explicit cancellation has no terminal delivery.
            } catch (error: Exception) {
                if (currentCoroutineContext().isActive) {
                    // An arbitrary callback exception may override message with
                    // another host getter. Keep its cause without invoking it.
                    val reported = error as? Orch8Exception
                        ?: Orch8Exception(Orch8ErrorKind.ENGINE, "observation failed", error)
                    try { slot.value?.lifecycle?.onError(reported) } catch (_: Exception) { }
                }
            } finally {
                slot.value = null
            }
        }
    } catch (error: Throwable) {
        try { slot.value = null; lifetime.cancel() }
        finally { reservation.close() }
        throw error
    }
    // Completion follows actual synchronous callback return, even when its
    // engine/observer was cancelled while the host was blocked.
    task.invokeOnCompletion {
        try { slot.value = null; lifetime.cancel() }
        finally { reservation.close() }
    }
    return {
        slot.value = null
        lifetime.cancel()
    }
}
