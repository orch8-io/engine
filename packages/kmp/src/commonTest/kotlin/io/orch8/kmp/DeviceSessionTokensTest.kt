package io.orch8.kmp

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class DeviceSessionTokensTest {
    @Test
    fun structuredChildrenMustFinishBeforeInitialPublication() = runTest {
        val budget = TokenFetchBudget(1)
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val source = DeviceSessionTokens("", {
            kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.currentCoroutineContext()).launch {
                entered.complete(Unit)
                withContext(NonCancellable) { release.await() }
            }
            "dst_premature"
        }, 5.seconds, UnconfinedTestDispatcher(testScheduler), budget)
        try {
            val initial = async(UnconfinedTestDispatcher(testScheduler)) { source.initialToken() }
            entered.await()
            assertEquals("", source.currentToken(), "structured host work still owns publication")
            assertEquals(false, initial.isCompleted)
            assertEquals(0, budget.availablePermits)
            release.complete(Unit)
            assertEquals("dst_premature", initial.await())
        } finally { release.complete(Unit); source.close(); runCurrent() }
        assertEquals(1, budget.availablePermits)
    }

    @Test
    fun timedOutAndRetiredSourcesKeepSharedPhysicalAdmissionUntilActualCompletion() = runTest {
        val budget = TokenFetchBudget(2)
        val gates = List(2) { CompletableDeferred<Unit>() }
        val entered = List(2) { CompletableDeferred<Unit>() }
        val sources = gates.indices.map { i ->
            DeviceSessionTokens("", {
                entered[i].complete(Unit)
                withContext(NonCancellable) { gates[i].await() }
                "dst_late"
            }, 50.milliseconds, UnconfinedTestDispatcher(testScheduler), budget)
        }
        try {
            val waits = sources.map { source -> async { runCatching { source.initialToken() } } }
            entered.forEach { it.await() }
            advanceTimeBy(51); runCurrent()
            waits.forEach { assertTrue(it.isCompleted); assertTrue(it.await().isFailure) }
            sources.forEach { it.close() }
            assertEquals(0, budget.availablePermits)
            var unexpected = 0
            repeat(128) { i ->
                val cached = if (i % 2 == 0) "dst_cached" else ""
                val refused = DeviceSessionTokens(cached, { unexpected++; "dst_wrong" }, 5.seconds, UnconfinedTestDispatcher(testScheduler), budget)
                val failure = assertFailsWith<Orch8Exception> { if (cached.isBlank()) refused.initialToken() else refused.refreshToken() }
                assertEquals(Orch8ErrorKind.RESOURCE_LIMIT, failure.kind)
                assertEquals(cached, refused.currentToken())
                refused.close()
            }
            assertEquals(0, unexpected)
            gates[0].complete(Unit); runCurrent()
            assertEquals(1, budget.availablePermits)
            assertEquals("", sources[0].currentToken(), "late completion must not restore a retired token")
            val recovery = DeviceSessionTokens("", { "dst_recovered" }, 5.seconds, UnconfinedTestDispatcher(testScheduler), budget)
            assertEquals("dst_recovered", recovery.initialToken())
            assertEquals(1, budget.availablePermits)
            recovery.close()
        } finally {
            gates.forEach { it.complete(Unit) }
            sources.forEach { it.close() }
            runCurrent()
        }
        assertEquals(2, budget.availablePermits)
    }

    @Test
    fun queuedCancellationReturnsAdmissionWithoutInvokingTheHost() = runTest {
        val budget = TokenFetchBudget(1)
        var calls = 0
        val source = DeviceSessionTokens("", { calls++; "dst_unexpected" }, 5.seconds, StandardTestDispatcher(testScheduler), budget)
        val waiting = async(UnconfinedTestDispatcher(testScheduler)) { source.initialToken() }
        assertEquals(0, budget.availablePermits)
        waiting.cancelAndJoin()
        runCurrent()
        assertEquals(0, calls)
        assertEquals(1, budget.availablePermits)
        source.close()
    }

    @Test
    fun simultaneousInitialWaitsShareOneHostFetch() = runTest {
        val budget = TokenFetchBudget(1)
        val release = CompletableDeferred<String>()
        var calls = 0
        val source = DeviceSessionTokens("", { calls++; release.await() }, 5.seconds, UnconfinedTestDispatcher(testScheduler), budget)
        try {
            val first = async(UnconfinedTestDispatcher(testScheduler)) { source.initialToken() }
            val second = async(UnconfinedTestDispatcher(testScheduler)) { source.initialToken() }
            assertEquals(1, calls)
            release.complete("dst_first")
            assertEquals("dst_first", first.await())
            assertEquals("dst_first", second.await())
            assertEquals("dst_first", source.initialToken(), "already bootstrapped initial token is reused")
            assertEquals(1, calls)
        } finally { release.complete("dst_cleanup"); source.close(); runCurrent() }
        assertEquals(1, budget.availablePermits)
    }

    @Test
    fun hostFailureAndBlankResultsReleaseAdmissionAndPreserveCachedTokens() = runTest {
        val budget = TokenFetchBudget(1)
        for (fetch in listOf<suspend () -> String>({ throw IllegalStateException("secret_host_detail") }, { " \t" })) {
            val source = DeviceSessionTokens("dst_cached", fetch, 5.seconds, UnconfinedTestDispatcher(testScheduler), budget)
            val error = assertFailsWith<Orch8Exception> { source.refreshToken() }
            assertTrue(!error.message.orEmpty().contains("secret_host_detail"))
            assertEquals("dst_cached", source.currentToken())
            assertEquals(1, budget.availablePermits)
            source.close()
        }
    }

    @Test
    fun failingHostCancellationHandlerCannotLeakCapacityOrEscapeTheSdkJob() = runTest {
        val budget = TokenFetchBudget(1)
        val entered = CompletableDeferred<Unit>()
        val source = DeviceSessionTokens("", {
            entered.complete(Unit)
            kotlinx.coroutines.suspendCancellableCoroutine<String> { continuation ->
                continuation.invokeOnCancellation { throw IllegalStateException("secret_cancellation_detail") }
            }
        }, 5.seconds, UnconfinedTestDispatcher(testScheduler), budget)
        val waiting = async { source.initialToken() }
        entered.await()
        waiting.cancelAndJoin()
        source.close()
        runCurrent()
        assertEquals(1, budget.availablePermits)
        assertEquals("", source.currentToken())
    }
}
