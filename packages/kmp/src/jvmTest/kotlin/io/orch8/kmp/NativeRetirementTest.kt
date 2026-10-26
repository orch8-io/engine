package io.orch8.kmp

import com.sun.management.UnixOperatingSystemMXBean
import java.lang.management.ManagementFactory
import java.lang.ref.Reference
import java.nio.file.Files
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Assume.assumeTrue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.assertNotEquals
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

class NativeRetirementTest {
    private fun requireNative() = assumeTrue("set orch8.native.lib.path for native integration", System.getProperty("orch8.native.integration") == "true")
    private fun config() = EngineConfig(telemetryEnabled=false, operationTimeoutMs=3000)

    @Test
    fun retainedClosedFacadeDoesNotPreventReopeningItsDatabase() = runBlocking {
        requireNative()
        val directory=Files.createTempDirectory("orch8-kmp-retained-close-")
        val path=directory.resolve("node.db").toString()
        val original=Orch8Engine.open(path,config())
        try {
            val id=original.nodeRuntimeId()
            original.close()
            val replacement=Orch8Engine.open(path,config())
            try {
                assertEquals(id,replacement.nodeRuntimeId())
                // Keep the original facade live across replacement use.
                original.close()
            } finally {replacement.close()}
        } finally {original.close();directory.toFile().deleteRecursively()}
    }

    @Test
    fun retainedClosedFacadesDoNotAccumulateNativeDescriptors() {
        requireNative()
        val bean=ManagementFactory.getOperatingSystemMXBean()
        assumeTrue("requires Unix process descriptor accounting",bean is UnixOperatingSystemMXBean)
        bean as UnixOperatingSystemMXBean
        val directory=Files.createTempDirectory("orch8-kmp-native-retirement-")
        val retained=mutableListOf<Orch8Engine>()
        try {
            val warm=Orch8Engine.open(directory.resolve("warm.db").toString(),config())
            warm.close();retained+=warm
            val before=bean.openFileDescriptorCount
            assertTrue(before>=0,"descriptor accounting is unavailable")
            repeat(16) {index->
                val engine=Orch8Engine.open(directory.resolve("node-$index.db").toString(),config())
                engine.close();retained+=engine
            }
            // Shutdown may finish physical cleanup asynchronously. Retained
            // closed wrappers must not keep one runtime's descriptors each.
            val deadline=System.nanoTime()+5_000_000_000L
            while(bean.openFileDescriptorCount>before+8 && System.nanoTime()<deadline) Thread.sleep(20)
            val after=bean.openFileDescriptorCount
            println("Native retirement descriptors: before=$before after=$after retained=${retained.size}")
            Reference.reachabilityFence(retained)
            assertTrue(after>=0 && after<=before+8,"native descriptors accumulate with retained closed facades: before=$before after=$after")
            assertEquals(17,retained.size)
        } finally {retained.forEach {it.close()};directory.toFile().deleteRecursively()}
    }
    @Test
    fun callsAfterNativeDisposalStillReportTypedShutdown() = runBlocking {
        requireNative()
        val directory=Files.createTempDirectory("orch8-kmp-closed-api-")
        val engine=Orch8Engine.open(directory.resolve("node.db").toString(),config())
        try {
            engine.close();engine.close()
            val calls=listOf<suspend () -> Unit>(
                { engine.resume() }, { engine.pause() }, { engine.onPushReceived() },
                { engine.reportPowerState(PowerState.CHARGING) },
                { engine.tick() }, { engine.nodeRuntimeId() }, { engine.loadedSequences() },
                { engine.activeInstances() }, { engine.start("missing") },
                { engine.listDelegations() }, { engine.workerStats() }, { engine.delegationStats() },
            )
            for(call in calls) {
                val failure=assertFailsWith<Orch8Exception> { call() }
                assertEquals(Orch8ErrorKind.SHUTDOWN,failure.kind)
            }
        } finally {engine.close();directory.toFile().deleteRecursively()}
    }


    private fun workflow(name:String,handler:String) = """{"id":"${UUID.randomUUID()}","tenant_id":"mobile","namespace":"default","name":"$name","version":1,"blocks":[{"type":"step","id":"work","handler":"$handler","params":{}}],"created_at":"${Instant.now()}"}"""

    private suspend fun reopen(path:String):Orch8Engine {
        val deadline=System.nanoTime()+5_000_000_000L
        while(true) {
            try {return Orch8Engine.open(path,config())}
            catch(error:Orch8Exception) {
                if(error.kind!=Orch8ErrorKind.STORAGE || System.nanoTime()>=deadline) throw error
                delay(20)
            }
        }
    }

    @Test
    fun disposalAndRecoveryDoNotReplayAStillRunningExternalEffect() = runBlocking<Unit> {
        requireNative()
        val directory=Files.createTempDirectory("orch8-kmp-physical-retirement-")
        val path=directory.resolve("node.db").toString()
        val engine=Orch8Engine.open(path,config().copy(operationTimeoutMs=100,handlerTimeoutMs=50,maxTickDurationMs=50))
        val entered=CountDownLatch(1);val release=CountDownLatch(1);val exited=CountDownLatch(1)
        val oldCalls=AtomicInteger();val replayed=AtomicInteger();val controlCalls=AtomicInteger()
        var tick:Deferred<Result<TickResult>>?=null
        var replacement:Orch8Engine?=null
        try {
            engine.registerHandler("held",Orch8StepHandler {_,_->
                oldCalls.incrementAndGet();entered.countDown()
                try {
                    check(release.await(10,TimeUnit.SECONDS)) {"test callback gate expired"}
                    "{}"
                } finally {exited.countDown()}
            })
            engine.loadSequence(workflow("held","held"))
            val oldId=engine.start("held")
            tick=async(Dispatchers.IO) {runCatching {engine.tick()}}
            assertTrue(withContext(Dispatchers.IO) {entered.await(5,TimeUnit.SECONDS)},"native callback never entered")
            engine.close()
            val successor=reopen(path);replacement=successor
            assertEquals(1L,exited.count,"old callback must still be physically running")
            successor.registerHandler("held",Orch8StepHandler {_,_->replayed.incrementAndGet();"{}"})
            repeat(5) {successor.tick()}
            assertEquals(0,replayed.get(),"recovery redispatched an already accepted external effect")
            assertNotEquals(InstanceState.COMPLETED,successor.getInstance(oldId).state)
            successor.registerHandler("control",Orch8StepHandler {_,_->controlCalls.incrementAndGet();"{}"})
            successor.loadSequence(workflow("control","control"))
            val fresh=successor.start("control")
            withTimeout(5000) {
                while(successor.getInstance(fresh).state!=InstanceState.COMPLETED) {successor.tick();delay(5)}
            }
            assertEquals(1,controlCalls.get())
            release.countDown()
            assertTrue(withContext(Dispatchers.IO) {exited.await(5,TimeUnit.SECONDS)})
            val settled=withTimeout(5000) {tick.await()}
            assertTrue(settled.exceptionOrNull()?.let {it is Orch8Exception} != false,
                "an accepted native call leaked an untyped disposal error")
            repeat(5) {successor.tick()}
            assertEquals(1,oldCalls.get());assertEquals(0,replayed.get())
        } finally {
            release.countDown();engine.close();replacement?.close()
            withTimeout(5000) {tick?.await()}
            directory.toFile().deleteRecursively()
        }
    }
}
