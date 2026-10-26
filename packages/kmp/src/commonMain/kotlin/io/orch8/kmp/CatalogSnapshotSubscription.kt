package io.orch8.kmp

import kotlinx.coroutines.Job
import kotlinx.coroutines.CoroutineDispatcher
import kotlin.time.Duration

/** Callbacks run on the IO dispatcher. Dispatch UI changes to the main thread. */
interface CatalogSnapshotObserver : Orch8ObservationObserver {
    fun onSnapshot(snapshot: CatalogRunSnapshot?)
}

/**
 * Owns polling without retaining the engine. Closing stops the polling task and
 * releases the observer. An already accepted host callback may finish afterward.
 * Engine close also stops SDK subscriptions immediately, including long delays.
 * Disposing a subscription does not close its run view or cancel durable work.
 */
class CatalogSnapshotSubscription internal constructor(private val stop: () -> Unit) : AutoCloseable {
    override fun close() = stop()
}

internal fun subscribeCatalogSnapshots(
    run: Orch8CatalogRun,
    observer: CatalogSnapshotObserver,
    interval: Duration,
    io: CoroutineDispatcher,
    owner: Job?,
): CatalogSnapshotSubscription = CatalogSnapshotSubscription(
    subscribeFlow(run.observeSnapshots(interval), observer, observer::onSnapshot, io, owner, accountId = run.accountId),
)
