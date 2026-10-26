package io.orch8.kmp

import java.lang.ref.WeakReference
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class ScopedObserverOwnershipTest {
    private fun subscribe(run: Orch8CatalogRun): Pair<WeakReference<CatalogSnapshotObserver>, CatalogSnapshotSubscription> {
        val observer = object : CatalogSnapshotObserver {
            override fun onSnapshot(snapshot: CatalogRunSnapshot?) = Unit
            override fun onError(error: Orch8Exception) = error("unexpected error")
            override fun onComplete() = error("unexpected completion")
        }
        return WeakReference<CatalogSnapshotObserver>(observer) to run.subscribeSnapshots(observer, 86_400_000)
    }

    @Test
    fun engineCloseReleasesObserverWhileRunAndSubscriptionRemainRetained() = runTest {
        val backend = ObservedRunBackend()
        val engine = scopedEngine(backend, StandardTestDispatcher(testScheduler))
        val account = engine.openAccount(AccountIdentity("a", "issuer", "a"))
        val catalog = account.openCatalog(CatalogConfiguration("cat", "source", "authority", "https://example.com/manifest", "https://example.com/identity"))
        val run = catalog.prepareRun(CatalogRunRequest("flow", operationKey="key"))
        account.close(); catalog.close()
        val (observer, subscription) = subscribe(run)
        try {
            runCurrent()
            assertEquals(1, backend.reads)
            engine.close(); runCurrent()
            repeat(100) {
                if (observer.get() == null) return@repeat
                System.gc(); Thread.sleep(10)
            }
            assertNull(observer.get(), "shutdown retains an observer through its long polling delay")
            assertEquals(run.instanceId, backend.instanceId)
            assertEquals(false, backend.closed)
        } finally { subscription.close(); engine.close() }
    }
    private suspend fun detachedRun(): Pair<WeakReference<Orch8Engine>, Orch8CatalogRun> {
        val engine = scopedEngine(ObservedRunBackend(), Dispatchers.Unconfined)
        val account = engine.openAccount(AccountIdentity("a", "issuer", "a"))
        val catalog = account.openCatalog(CatalogConfiguration("cat", "source", "authority", "https://example.com/manifest", "https://example.com/identity"))
        val run = catalog.prepareRun(CatalogRunRequest("flow", operationKey="key"))
        account.close(); catalog.close()
        return WeakReference(engine) to run
    }

    @Test
    fun retainedScopeJobTokenDoesNotPinTheRootEngineWrapper() = runBlocking {
        val (engine, run) = detachedRun()
        try {
            repeat(100) {
                if (engine.get() == null) return@repeat
                System.gc(); Thread.sleep(10)
            }
            assertNull(engine.get(), "a scoped cancellation token pins the root engine wrapper")
            assertEquals("run-a", run.instanceId)
        } finally { run.close() }
    }

}
