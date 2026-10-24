package io.orch8.kmp

import io.orch8.kmp.internal.openPlatformBackend
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.Assume.assumeTrue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class NativeTokenInstallationTest {
    @Test
    fun hostCompletionHandlersRetainAdmissionAndDelayPublication() = runBlocking {
        val budget = TokenFetchBudget(1)
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val source = DeviceSessionTokens("", {
            currentCoroutineContext()[Job]!!.invokeOnCompletion {
                entered.countDown()
                release.await(5, TimeUnit.SECONDS)
            }
            "dst_ready"
        }, 5.seconds, Dispatchers.IO, budget)
        try {
            val initial = async(Dispatchers.IO) { source.initialToken() }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            assertEquals("", source.currentToken())
            assertEquals(0, budget.availablePermits)
            release.countDown()
            assertEquals("dst_ready", withTimeout(5.seconds) { initial.await() })
            withTimeout(5.seconds) { while (budget.availablePermits != 1) delay(1) }
        } finally { release.countDown(); source.close() }
    }

    @Test
    fun realNativeOwnerRejectsOlderAsyncCompletionAcrossAliases() = runBlocking {
        assumeTrue("set orch8.native.lib.path to run native integration", System.getProperty("orch8.native.integration") == "true")
        val directory = Files.createTempDirectory("orch8-kmp-owner").toFile()
        val backend = openPlatformBackend(directory.resolve("node.db").path, EngineConfig())
        val first = Orch8Engine(backend, Dispatchers.IO)
        val second = Orch8Engine(backend, Dispatchers.IO)
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<String>()
        try {
            val older = async { runCatching { first.setTokenProvider { entered.complete(Unit); release.await() } } }
            withTimeout(5.seconds) { entered.await() }
            second.setTokenProvider { "dst_newer" }
            release.complete("dst_old")
            assertTrue(withTimeout(5.seconds) { older.await() }.isFailure)
            assertTrue(second.nodeRuntimeId().isNotBlank())
        } finally { release.complete("dst_cleanup"); first.close(); directory.deleteRecursively() }
    }

    @Test
    fun realNativeShutdownCancelsTheInitialWaitWhileHostWorkRetainsAdmission() = runBlocking {
        assumeTrue("set orch8.native.lib.path to run native integration", System.getProperty("orch8.native.integration") == "true")
        val directory = Files.createTempDirectory("orch8-kmp-shutdown").toFile()
        val engine = Orch8Engine.open(directory.resolve("node.db").path, EngineConfig())
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        try {
            val initial = async {
                runCatching { engine.setTokenProvider {
                    entered.complete(Unit)
                    withContext(NonCancellable) { release.await() }
                    "dst_late"
                } }
            }
            withTimeout(5.seconds) { entered.await() }
            engine.close()
            assertTrue(withTimeout(5.seconds) { initial.await() }.isFailure)
            assertEquals(7, TokenFetchBudget.shared.availablePermits)
            assertEquals(Orch8ErrorKind.SHUTDOWN, assertFailsWith<Orch8Exception> { engine.setTokenProvider { "dst_unexpected" } }.kind)
        } finally { release.complete(Unit); engine.close(); directory.deleteRecursively() }
    }
}
