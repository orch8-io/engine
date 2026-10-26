package io.orch8.kmp

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlin.time.Duration

/** Callbacks run on the IO dispatcher. Dispatch UI changes to the main thread. */
interface CatalogSnapshotObserver {
    fun onSnapshot(snapshot: CatalogRunSnapshot?)
    fun onError(error: Orch8Exception)
    fun onComplete()
}

/**
 * Owns polling without retaining the engine. Closing stops the polling task and
 * releases the observer. An already accepted host callback may finish afterward.
 * Disposing a subscription does not close its run view or cancel durable work.
 */
class CatalogSnapshotSubscription internal constructor(private val stop: () -> Unit) : AutoCloseable {
    override fun close() = stop()
}

// Identity equality avoids invoking a host observer's equals during retirement.
private class SnapshotObserverSlot(val observer: CatalogSnapshotObserver)

internal fun subscribeCatalogSnapshots(
    run: Orch8CatalogRun,
    observer: CatalogSnapshotObserver,
    interval: Duration,
    io: CoroutineDispatcher,
): CatalogSnapshotSubscription {
    val slot = MutableStateFlow<SnapshotObserverSlot?>(SnapshotObserverSlot(observer))
    val lifetime = SupervisorJob()
    CoroutineScope(io + lifetime).launch {
        try {
            run.observeSnapshots(interval).collect { slot.value?.observer?.onSnapshot(it) }
            slot.value?.observer?.onComplete()
        } catch (_: CancellationException) {
            // Explicit cancellation has no error or completion delivery.
        } catch (error: Exception) {
            val reported = error as? Orch8Exception
                ?: Orch8Exception(Orch8ErrorKind.ENGINE, error.message ?: "snapshot observer failed", error)
            // Observer code is a nonthrowing foreign boundary, including its
            // error callback. A broken callback must not become an unhandled
            // Kotlin/Native coroutine exception and terminate the process.
            try { slot.value?.observer?.onError(reported) } catch (_: Exception) { }
        } finally {
            slot.value = null
            lifetime.cancel()
        }
    }
    return CatalogSnapshotSubscription {
        slot.value = null
        lifetime.cancel()
    }
}
