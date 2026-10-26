package io.orch8.flutter

import io.orch8.mobile.MobileEngine
import io.orch8.mobile.MobileEngineConfig
import io.orch8.mobile.MobileException
import io.orch8.mobile.NoHandle
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.CountDownLatch
import kotlin.test.*

class NativeEngineLifecycleTest {
    private class Lane : AbstractExecutorService() {
        val tasks = ArrayDeque<Runnable>()
        var stopped = false
        override fun execute(task: Runnable) { check(!stopped); tasks.addLast(task) }
        override fun shutdown() { stopped = true }
        override fun shutdownNow(): MutableList<Runnable> = tasks.toMutableList().also { tasks.clear(); stopped = true }
        override fun isShutdown() = stopped
        override fun isTerminated() = stopped && tasks.isEmpty()
        override fun awaitTermination(timeout: Long, unit: TimeUnit) = isTerminated
        fun drain() { while(tasks.isNotEmpty()) tasks.removeFirst().run() }
    }
    private class Engine : MobileEngine(NoHandle) {
        var shutdowns=0;var disposals=0;var shutdownFailure:Exception?=null
        override fun shutdown() { shutdowns++;shutdownFailure?.let {throw it} }
        override fun close() { disposals++;super.close() }
    }
    private fun config() = MobileEngineConfig(
        500uL,4u,1000u,10u,5000uL,86400uL,50u,1048576uL,1000uL,3000uL,
        false,"","production","","0.7.1",0uL,"","","device","",
    )

    @Test fun latestQueuedInitializationWinsAndRetirementIsDeterministic() {
        val lane=Lane();val created=mutableListOf<String>();val owned=Engine()
        val owner=NativeEngineLifecycle(lane) {path,_->created+=path;owned}
        val replies=mutableListOf<Exception?>()
        owner.initialize("old",config(),{}, {_,_->}, {replies+=it})
        owner.initialize("new",config(),{}, {_,_->}, {replies+=it})
        lane.drain()
        assertEquals(listOf("new"),created)
        assertTrue(replies[0] is MobileException.Shutdown);assertNull(replies[1])
        assertSame(owned,owner.engine)
        val scope=owner.scopeOf(owned);assertTrue(scope.isCurrent())
        owner.shutdown(true,{}, {replies+=it});assertFalse(scope.isCurrent());assertNull(owner.engine)
        lane.drain();assertEquals(1,owned.shutdowns);assertEquals(1,owned.disposals)
    }

    @Test fun detachDuringConstructionDisposesCandidateAndDoesNotPublishIt() {
        val lane=Lane();val candidate=Engine();lateinit var owner:NativeEngineLifecycle
        owner=NativeEngineLifecycle(lane) {_,_->owner.shutdown(true,{},{});candidate}
        var configured=false;var failure:Exception?=null
        owner.initialize("db",config(),{}, {_,_->configured=true}, {failure=it})
        lane.drain()
        assertFalse(configured);assertNull(owner.engine);assertTrue(failure is MobileException.Shutdown)
        assertEquals(1,candidate.disposals)
    }

    @Test fun failedConfigurationDisposesCandidateAndAllowsAnotherInitialization() {
        val lane=Lane();val candidate=Engine();val failure=IllegalStateException("configuration failed")
        val replacement=Engine();val candidates=ArrayDeque(listOf(candidate,replacement))
        val owner=NativeEngineLifecycle(lane) {_,_->candidates.removeFirst()};var reply:Exception?=null
        owner.initialize("db",config(),{}, {_,_->throw failure}, {reply=it})
        lane.drain();assertSame(failure,reply);assertNull(owner.engine);assertEquals(1,candidate.disposals)
        owner.initialize("db",config(),{}, {_,_->}, {reply=it});lane.drain()
        assertNull(reply);assertSame(replacement,owner.engine)
        owner.shutdown(true,{},{});lane.drain();assertEquals(1,replacement.disposals)
    }

    @Test fun failedShutdownStillDisposesAndRepliesExactlyOnce() {
        val lane=Lane();val candidate=Engine();val owner=NativeEngineLifecycle(lane) {_,_->candidate}
        owner.initialize("db",config(),{}, {_,_->}, {});lane.drain()
        val failure=IllegalStateException("shutdown failed");candidate.shutdownFailure=failure
        val replies=mutableListOf<Exception?>()
        owner.shutdown(false,{}, {replies+=it});lane.drain()
        assertEquals(1,replies.size);assertSame(failure,replies[0]);assertEquals(1,candidate.disposals)
        assertNull(owner.engine)
        owner.shutdown(true,{},{});lane.drain()
    }

    @Test fun constructorsAndClosedCompletionsDoNotRunUnderTheStateLock() {
        val lane=Lane();lateinit var owner:NativeEngineLifecycle
        owner=NativeEngineLifecycle(lane) {_,_->
            val acquired=CountDownLatch(1)
            val thread=Thread {owner.engine;acquired.countDown()};thread.start()
            assertTrue(acquired.await(2,TimeUnit.SECONDS),"constructor holds state lock");thread.join()
            Engine()
        }
        owner.initialize("db",config(),{}, {_,_->}, {});lane.drain()
        owner.shutdown(true,{},{});lane.drain()
        var calls=0
        owner.initialize("late",config(),{}, {_,_->error("closed")}, {
            val acquired=CountDownLatch(1)
            val thread=Thread {owner.engine;acquired.countDown()};thread.start()
            assertTrue(acquired.await(2,TimeUnit.SECONDS),"completion holds state lock");thread.join();calls++
        })
        assertEquals(1,calls)
    }
}
