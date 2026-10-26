package io.orch8.kmp

import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class ObservationBudgetTest {
    private open class Observer : Orch8ObservationObserver {
        override fun onError(error: Orch8Exception) = Unit
        override fun onComplete() = Unit
    }

    @Test
    fun admissionChecksEngineAccountAndProcessBeforeSchedulingAnyRead() = runTest {
        val budget = ObservationBudget(processLimit = 2, engineLimit = 1, accountLimit = 1)
        val io = StandardTestDispatcher(testScheduler)
        val owners = List(3) { SupervisorJob() }
        val stops = mutableListOf<() -> Unit>()
        var reads = 0
        fun subscribe(owner: Job, account: String): () -> Unit = subscribeFlow(
            flow { reads++; emit(1); awaitCancellation() }, Observer(), {}, io, owner,
            accountId = account, budget = budget)
        fun refused(owner: Job, account: String) {
            val error = assertFailsWith<Orch8Exception> { subscribe(owner, account) }
            assertEquals(Orch8ErrorKind.RESOURCE_LIMIT, error.kind)
        }
        try {
            stops += subscribe(owners[0], "a")
            refused(owners[0], "b") // engine bound, even with a different account
            refused(owners[1], "a") // same stable account on a different engine
            stops += subscribe(owners[1], "b")
            refused(owners[2], "c") // global bound across both owners
            assertEquals(0, reads)
            runCurrent()
            assertEquals(2, reads)
            stops[0](); stops[0](); runCurrent()
            assertEquals(1, budget.availablePermits)
            // Reuse the original engine/account: neither count may leak.
            stops += subscribe(owners[0], "a")
            runCurrent()
            assertEquals(3, reads)
        } finally {
            stops.forEach { it() }; owners.forEach { it.cancel() }; runCurrent()
        }
        assertEquals(2, budget.availablePermits)
    }

    @Test
    fun queuedCancellationAndThrowingCallbacksReturnCapacityExactlyOnce() = runTest {
        val budget = ObservationBudget(1, 1, 1)
        val io = StandardTestDispatcher(testScheduler)
        val owner = SupervisorJob()
        var reads = 0
        var errors = 0
        val observer = object : Observer() {
            override fun onError(error: Orch8Exception) { errors++; error("host error callback failed") }
            override fun onComplete() = error("host completion callback failed")
        }
        try {
            val queued = subscribeFlow(flow { reads++; emit(1) }, observer, {}, io, owner,
                accountId = "a", budget = budget)
            queued(); queued(); runCurrent()
            assertEquals(0, reads)
            assertEquals(1, budget.availablePermits)
            val failed = subscribeFlow(flowOf(1), observer, { error("host snapshot callback failed") }, io, owner,
                accountId = "a", budget = budget)
            runCurrent(); failed()
            assertEquals(1, errors)
            assertEquals(1, budget.availablePermits)
            val completed = subscribeFlow(flowOf(1), observer, {}, io, owner,
                accountId = "a", budget = budget)
            runCurrent(); completed()
            assertEquals(2, errors)
            assertEquals(1, budget.availablePermits)
        } finally { owner.cancel(); runCurrent() }
    }
}
