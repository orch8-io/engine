package io.orch8.kmp

import io.orch8.kmp.internal.CatalogRunBackend
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

private class ObservedRunBackend : CatalogRunBackend {
    override val instanceId = "run-a"
    override val accountId = "account-a"
    override val catalogId = "catalog-a"
    override val sessionEpoch = 7L
    var reads = 0
    var closed = false
    var next: () -> CatalogRunSnapshot? = { snapshot(InstanceState.RUNNING) }
    fun snapshot(state: InstanceState) = CatalogRunSnapshot(instanceId, accountId, catalogId,
        sessionEpoch, state, "{}", emptyList(), "created", state.wire)
    override fun snapshot(): CatalogRunSnapshot? {
        if (closed) throw Orch8Exception(Orch8ErrorKind.SHUTDOWN, "view closed")
        reads++
        return next()
    }
    override fun start() = Unit
    override fun cancel(): InstanceState? = error("unexpected cancel")
    override fun completeStep(expectedUpdatedAt: String, stepName: String, outputJson: String): Boolean = error("unexpected answer")
    override fun archiveAfterAcknowledgement(acknowledgement: String): CatalogArchiveReceipt? = error("unexpected archive")
    override fun forgetCancelledRequest(): Boolean = error("unexpected cleanup")
    override fun closeView() { closed = true }
    override fun dispose() { closed = true }
}

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class ScopedObservationTest {
    @Test
    fun queuedAndChangedSnapshotsAreDistinctAndKnownTerminalStateEndsCollection() = runTest {
        val backend = ObservedRunBackend()
        val values = ArrayDeque(listOf(null, backend.snapshot(InstanceState.RUNNING),
            backend.snapshot(InstanceState.RUNNING), backend.snapshot(InstanceState.WAITING),
            backend.snapshot(InstanceState.COMPLETED)))
        backend.next = { values.removeFirst() }
        val run = Orch8CatalogRun(backend, StandardTestDispatcher(testScheduler))
        val flow = run.observeSnapshots(100.milliseconds)
        assertEquals(0, backend.reads) // construction starts no poll
        assertEquals(listOf(null, InstanceState.RUNNING, InstanceState.WAITING, InstanceState.COMPLETED),
            flow.toList().map { it?.state })
        assertEquals(5, backend.reads)
        assertEquals(400L, testScheduler.currentTime)
    }

    @Test
    fun slowCollectorDoesNotQueueSnapshotReadsAndCancellationStopsFurtherPolling() = runTest {
        val backend = ObservedRunBackend()
        backend.next = { backend.snapshot(if (backend.reads == 1) InstanceState.RUNNING else InstanceState.WAITING) }
        val run = Orch8CatalogRun(backend, StandardTestDispatcher(testScheduler))
        val collection = launch { run.observeSnapshots(100.milliseconds).onEach { delay(500) }.take(2).collect() }
        runCurrent()
        assertEquals(1, backend.reads)
        advanceTimeBy(499)
        runCurrent()
        assertEquals(1, backend.reads)
        advanceTimeBy(101)
        runCurrent()
        assertEquals(2, backend.reads)
        collection.cancelAndJoin()
        advanceTimeBy(10_000)
        runCurrent()
        assertEquals(2, backend.reads)
    }

    @Test
    fun missingNativeSnapshotIsObservableWithoutInventingAnOutcome() = runTest {
        val backend = ObservedRunBackend()
        backend.next = { null }
        val run = Orch8CatalogRun(backend, StandardTestDispatcher(testScheduler))
        val values = mutableListOf<CatalogRunSnapshot?>()
        val collection = launch { run.observeSnapshots(100.milliseconds).collect { values += it } }
        runCurrent()
        advanceTimeBy(300)
        runCurrent()
        assertEquals(listOf<CatalogRunSnapshot?>(null), values)
        assertEquals(true, collection.isActive)
        collection.cancelAndJoin()
        val reads = backend.reads
        advanceTimeBy(1000)
        runCurrent()
        assertEquals(reads, backend.reads)
    }

    @Test
    fun closeFenceReachesTheCollectorWithoutASecondEmission() = runTest {
        val backend = ObservedRunBackend()
        val run = Orch8CatalogRun(backend, StandardTestDispatcher(testScheduler))
        var emitted = 0
        val result = async {
            assertFailsWith<Orch8Exception> {
                run.observeSnapshots(100.milliseconds).collect { emitted++ }
            }
        }
        runCurrent()
        run.closeView()
        advanceTimeBy(100)
        runCurrent()
        assertEquals(Orch8ErrorKind.SHUTDOWN, result.await().kind)
        assertEquals(1, emitted)
    }

    @Test
    fun subscriptionCompletesOnceAndCloseBeforeSchedulingStartsNoRead() = runTest {
        val backend = ObservedRunBackend()
        backend.next = { backend.snapshot(InstanceState.COMPLETED) }
        val run = Orch8CatalogRun(backend, StandardTestDispatcher(testScheduler))
        var completed = 0
        var emitted = 0
        val observer = object : CatalogSnapshotObserver {
            override fun onSnapshot(snapshot: CatalogRunSnapshot?) { emitted++ }
            override fun onError(error: Orch8Exception) = error("unexpected error")
            override fun onComplete() { completed++ }
        }
        val cancelled = run.subscribeSnapshots(observer, 100)
        cancelled.close(); cancelled.close(); runCurrent()
        assertEquals(0, backend.reads)
        val active = run.subscribeSnapshots(observer, 100)
        runCurrent()
        assertEquals(1, emitted)
        assertEquals(1, completed)
        active.close()
        advanceTimeBy(1000); runCurrent()
        assertEquals(1, backend.reads)
    }

    @Test
    fun subscriptionReportsClosedViewAndQuarantinesBrokenErrorCallbacks() = runTest {
        val backend = ObservedRunBackend()
        val run = Orch8CatalogRun(backend, StandardTestDispatcher(testScheduler))
        val errors = mutableListOf<Orch8ErrorKind>()
        var snapshots = 0
        val subscription = run.subscribeSnapshots(object : CatalogSnapshotObserver {
            override fun onSnapshot(snapshot: CatalogRunSnapshot?) { snapshots++ }
            override fun onError(error: Orch8Exception) { errors += error.kind; error("broken error callback") }
            override fun onComplete() = error("unexpected completion")
        }, 100)
        runCurrent()
        run.closeView()
        advanceTimeBy(100); runCurrent()
        assertEquals(listOf(Orch8ErrorKind.SHUTDOWN), errors)
        assertEquals(1, snapshots)
        advanceTimeBy(1000); runCurrent()
        assertEquals(1, backend.reads)
        subscription.close()
    }

    @Test
    fun callbackFailureReportingDoesNotInvokeAnUntrustedExceptionMessageGetter() = runTest {
        val backend = ObservedRunBackend()
        val run = Orch8CatalogRun(backend, StandardTestDispatcher(testScheduler))
        var reported = 0
        val subscription = run.subscribeSnapshots(object : CatalogSnapshotObserver {
            override fun onSnapshot(snapshot: CatalogRunSnapshot?) {
                throw object : RuntimeException() {
                    override val message: String get() = error("host exception getter invoked")
                }
            }
            override fun onError(error: Orch8Exception) { assertEquals(Orch8ErrorKind.ENGINE, error.kind); reported++ }
            override fun onComplete() = error("unexpected completion")
        }, 100)
        runCurrent()
        assertEquals(1, reported)
        advanceTimeBy(1000); runCurrent()
        assertEquals(1, backend.reads)
        subscription.close()
    }

    @Test
    fun invalidIntervalsNeverReachTheBackend() = runTest {
        val backend = ObservedRunBackend()
        val run = Orch8CatalogRun(backend, StandardTestDispatcher(testScheduler))
        for (interval in listOf(Duration.ZERO, (-1).milliseconds, Duration.INFINITE)) {
            assertFailsWith<IllegalArgumentException> { run.observeSnapshots(interval).first() }
        }
        val observer = object : CatalogSnapshotObserver {
            override fun onSnapshot(snapshot: CatalogRunSnapshot?) = error("unexpected snapshot")
            override fun onError(error: Orch8Exception) = error("unexpected callback")
            override fun onComplete() = error("unexpected completion")
        }
        for (interval in listOf(0L, -1L, Long.MAX_VALUE)) {
            assertFailsWith<IllegalArgumentException> { run.subscribeSnapshots(observer, interval) }
        }
        runCurrent()
        assertEquals(0, backend.reads)
    }
}
