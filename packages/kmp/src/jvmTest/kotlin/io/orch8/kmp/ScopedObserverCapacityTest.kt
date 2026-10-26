package io.orch8.kmp

import io.orch8.kmp.internal.CatalogRunBackend
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ScopedObserverCapacityTest {
    private suspend fun run(engine: Orch8Engine): Orch8CatalogRun {
        val account = engine.openAccount(AccountIdentity("a", "issuer", "a"))
        val catalog = account.openCatalog(CatalogConfiguration("cat", "source", "authority",
            "https://example.com/manifest", "https://example.com/identity"))
        return try { catalog.prepareRun(CatalogRunRequest("flow", operationKey = "key")) }
        finally { catalog.close(); account.close() }
    }

    @Test
    fun retiredEngineCallbacksKeepAccountCapacityWhileAnotherAccountCanObserve() = runBlocking {
        val first = scopedEngine(ObservedRunBackend(), Dispatchers.IO)
        val replacementBackend = ObservedRunBackend()
        val replacement = scopedEngine(replacementBackend, Dispatchers.IO)
        val entered = CountDownLatch(4)
        val release = CountDownLatch(1)
        val active = AtomicInteger()
        val subscriptions = mutableListOf<CatalogSnapshotSubscription>()
        val otherOwner = SupervisorJob()
        try {
            val original = run(first)
            repeat(4) {
                subscriptions += original.subscribeSnapshots(object : CatalogSnapshotObserver {
                    override fun onSnapshot(snapshot: CatalogRunSnapshot?) {
                        active.incrementAndGet(); entered.countDown()
                        try { check(release.await(15, TimeUnit.SECONDS)) }
                        finally { active.decrementAndGet() }
                    }
                    override fun onError(error: Orch8Exception) = Unit
                    override fun onComplete() = Unit
                }, 86_400_000)
            }
            assertTrue(entered.await(5, TimeUnit.SECONDS), "callbacks did not enter actual host threads")
            first.close()
            assertEquals(4, active.get(), "logical close interrupted the physical fixture")
            val next = run(replacement)
            val rejected = assertFailsWith<Orch8Exception> {
                subscriptions += next.subscribeSnapshots(object : CatalogSnapshotObserver {
                    override fun onSnapshot(snapshot: CatalogRunSnapshot?) = Unit
                    override fun onError(error: Orch8Exception) = Unit
                    override fun onComplete() = Unit
                })
            }
            assertEquals(Orch8ErrorKind.RESOURCE_LIMIT, rejected.kind)
            assertEquals(0, replacementBackend.reads, "refusal queued another native read")

            val otherBase = ObservedRunBackend()
            val other = Orch8CatalogRun(object : CatalogRunBackend by otherBase {
                override val accountId = "account-b"
                override fun snapshot() = otherBase.snapshot(InstanceState.COMPLETED).copy(accountId = accountId)
            }, Dispatchers.IO, otherOwner)
            val completed = CountDownLatch(1)
            subscriptions += other.subscribeSnapshots(object : CatalogSnapshotObserver {
                override fun onSnapshot(snapshot: CatalogRunSnapshot?) = Unit
                override fun onError(error: Orch8Exception) = Unit
                override fun onComplete() { completed.countDown() }
            })
            assertTrue(completed.await(5, TimeUnit.SECONDS), "one retired account blocked another account")
            release.countDown()
            withTimeout(5000) {
                while (active.get() != 0 || ObservationBudget.shared.availablePermits != 32) delay(1)
            }
            replacementBackend.next = { replacementBackend.snapshot(InstanceState.COMPLETED) }
            val resumed = CountDownLatch(1)
            subscriptions += next.subscribeSnapshots(object : CatalogSnapshotObserver {
                override fun onSnapshot(snapshot: CatalogRunSnapshot?) = Unit
                override fun onError(error: Orch8Exception) = Unit
                override fun onComplete() { resumed.countDown() }
            })
            assertTrue(resumed.await(5, TimeUnit.SECONDS), "account capacity did not return after physical completion")
        } finally {
            release.countDown()
            subscriptions.forEach { it.close() }
            first.close(); replacement.close(); otherOwner.cancel()
            withTimeout(5000) {
                while (active.get() != 0 || ObservationBudget.shared.availablePermits != 32) delay(1)
            }
        }
    }

    @Test
    fun concurrentSubscriptionsCannotOverbookOneAccount() = runBlocking {
        val base = ObservedRunBackend()
        val backend = object : CatalogRunBackend by base {
            override fun snapshot() = base.snapshot(InstanceState.RUNNING)
        }
        val owner = SupervisorJob()
        val observed = Orch8CatalogRun(backend, Dispatchers.IO, owner)
        val executor = java.util.concurrent.Executors.newFixedThreadPool(12)
        val start = CountDownLatch(1)
        val subscriptions = mutableListOf<CatalogSnapshotSubscription>()
        try {
            val attempts = List(12) {
                executor.submit(java.util.concurrent.Callable<CatalogSnapshotSubscription?> {
                    check(start.await(5, TimeUnit.SECONDS))
                    try {
                        observed.subscribeSnapshots(object : CatalogSnapshotObserver {
                            override fun onSnapshot(snapshot: CatalogRunSnapshot?) = Unit
                            override fun onError(error: Orch8Exception) = Unit
                            override fun onComplete() = Unit
                        }, 86_400_000)
                    } catch (error: Orch8Exception) {
                        if (error.kind != Orch8ErrorKind.RESOURCE_LIMIT) throw error
                        null
                    }
                })
            }
            start.countDown()
            attempts.mapNotNullTo(subscriptions) { it.get(5, TimeUnit.SECONDS) }
            assertEquals(4, subscriptions.size, "concurrent reserve overbooked the stable account")
        } finally {
            start.countDown(); subscriptions.forEach { it.close() }; owner.cancel(); executor.shutdownNow()
            withTimeout(5000) { while (ObservationBudget.shared.availablePermits != 32) delay(1) }
        }
    }

}
