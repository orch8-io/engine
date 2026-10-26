package io.orch8.kmp

import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.delay
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import org.junit.Assume.assumeTrue
import kotlin.test.*

class ScopedFacadeNativeTest {
    private suspend fun withFixture(block: suspend (Orch8Engine, List<JsonObject>) -> Unit) {
        assumeTrue("set orch8.native.lib.path for native integration",System.getProperty("orch8.native.integration")=="true")
        val fixture=Path.of(checkNotNull(System.getProperty("orch8.native.fixture.path")) {"native integration requires a scoped fixture"})
        val directory=Files.createTempDirectory("orch8-kmp-scoped-facade-")
        val database=directory.resolve("node.db")
        Files.copy(fixture.resolve("scoped.db"),database)
        val cases=Json.parseToJsonElement(Files.readString(fixture.resolve("metadata.json"))).jsonObject["cases"]!!.jsonArray.map {it.jsonObject}
        val engine=Orch8Engine.open(database.toString(),EngineConfig(telemetryEnabled=false,operationTimeoutMs=10000))
        try { block(engine,cases) } finally {engine.close();directory.toFile().deleteRecursively()}
    }
    private fun JsonObject.text(name:String)=getValue(name).jsonPrimitive.content
    private fun JsonObject.identity()=AccountIdentity(text("accountKey"),text("issuer"),text("subject"))
    private fun JsonObject.configuration()=CatalogConfiguration(text("catalogKey"),text("source"),text("signingAuthority"),text("manifestUrl"),text("identityUrl"))
    private suspend fun advance(engine:Orch8Engine,run:Orch8CatalogRun,state:InstanceState,pending:Int?=null):CatalogRunSnapshot {
        repeat(80) {
            engine.tick();val snapshot=assertNotNull(run.snapshot())
            if(snapshot.state==state&&(pending==null||snapshot.pendingSteps.size==pending))return snapshot
            delay(5)
        }
        error("workflow failed to reach $state")
    }
    @Test
    fun permanentHostErrorCrossesTheRealNativeCallbackBoundaryWithoutPanicking() = runBlocking {
        withFixture { engine, cases ->
            val account = engine.openAccount(cases.first().identity())
            val catalog = account.openCatalog(cases.first().configuration())
            val entered = AtomicInteger()
            catalog.registerHandler("echo", Orch8CatalogStepHandler { _, _, _ ->
                entered.incrementAndGet()
                throw Orch8HandlerException.Permanent("typed host failure")
            }, 1000)
            val run = catalog.prepareRun(CatalogRunRequest("flow", operationKey = "foreign-permanent-error"))
            run.start()
            val waiting = advance(engine, run, InstanceState.WAITING, 2)
            assertTrue(run.answer(waiting, "left", "yes"))
            val remaining = advance(engine, run, InstanceState.WAITING, 1)
            assertTrue(run.answer(remaining, "right", "yes"))
            advance(engine, run, InstanceState.FAILED)
            repeat(3) { engine.tick() }
            assertEquals(2, entered.get())
        }
    }

    @Test
    fun fourScopedWorkflowsExecuteThroughTheCommonFacadeWithOwnedCallbacksAndVersionedApprovals()=runBlocking {
        withFixture {engine,cases->
            val contexts=ConcurrentLinkedQueue<CatalogCallbackContext>()
            val shared=Orch8CatalogStepHandler {context,_,_->contexts.add(context);"{\"ok\":true}"}
            val runs=cases.map {record->
                val account=engine.openAccount(record.identity())
                val catalog=account.openCatalog(record.configuration())
                catalog.registerHandler("echo",shared,1000)
                val run=catalog.prepareRun(CatalogRunRequest("flow","{\"account_id\":\"forged\"}","same-key"))
                account.close();run.start()
                Triple(catalog,run,record)
            }
            assertTrue(engine.activeInstances().isEmpty())
            for ((index,entry) in runs.withIndex()) {
                val (catalog,run,record)=entry
                val waiting=advance(engine,run,InstanceState.WAITING,2)
                assertEquals(record.text("accountId"),waiting.accountId)
                assertEquals(record.text("catalogId"),waiting.catalogId)
                val foreign=runs.first {it.second.instanceId!=run.instanceId}.second
                assertNull(catalog.run(foreign.instanceId).snapshot())
                assertFailsWith<Orch8Exception> {engine.getInstance(run.instanceId)}
                val first=if(index%2==0)"left"else"right"
                assertTrue(run.answer(waiting,first,"yes"))
                assertFalse(run.completeStep(waiting.updatedAt,first,"{\"value\":\"no\"}"))
                val next=advance(engine,run,InstanceState.WAITING,1)
                assertTrue(run.answer(next,if(first=="left")"right"else"left","yes"))
                val completed=advance(engine,run,InstanceState.COMPLETED)
                assertEquals("yes",completed.data.jsonObject["left"]!!.jsonPrimitive.content)
                assertEquals("yes",completed.data.jsonObject["right"]!!.jsonPrimitive.content)
                val history=catalog.history(limit=1)
                assertEquals(run.instanceId,history.items.single().instanceId)
                assertEquals(CatalogNativeState.Present(InstanceState.COMPLETED),history.items.single().nativeState)
                assertNull(history.next)
                val acknowledgement="uploaded-${run.instanceId}"
                val receipt=assertNotNull(run.archiveAfterAcknowledgement(acknowledgement))
                assertEquals(InstanceState.COMPLETED,receipt.outcome)
                assertEquals(run.instanceId,receipt.instanceId)
                assertEquals(receipt,run.archiveAfterAcknowledgement(acknowledgement))
                assertNull(catalog.run(foreign.instanceId).archiveAfterAcknowledgement("foreign"))
                assertEquals(CatalogRequestState.ARCHIVED,catalog.history(limit=1).items.single().requestState)
                assertEquals(CatalogNativeState.Recorded(InstanceState.COMPLETED),catalog.history(limit=1).items.single().nativeState)
                val duplicate=catalog.prepareRun(CatalogRunRequest("flow","{\"account_id\":\"forged\"}","same-key"))
                assertEquals(run.instanceId,duplicate.instanceId)
                assertFailsWith<Orch8Exception> { duplicate.start() }
                duplicate.close()
            }
            assertEquals(8,contexts.size)
            assertEquals(8,contexts.map {it.effectId}.toSet().size)
            contexts.forEach { context ->
                val owner=runs.single {it.second.instanceId==context.runId}.second
                assertEquals(owner.accountId,context.accountId);assertEquals(owner.catalogId,context.catalogId)
                assertEquals(owner.sessionEpoch,context.ownerEpoch)
            }
        }
    }
    @Test
    fun scopedObserversFollowRealWorkflowsWithoutMixingAccountsAndStopAtTerminal()=runBlocking {
        withFixture {engine,cases->
            val runs=listOf(cases.first(),cases.last()).map {record->
                val account=engine.openAccount(record.identity())
                val catalog=account.openCatalog(record.configuration())
                catalog.registerHandler("echo",Orch8CatalogStepHandler {_,_,_->"{}"},1000)
                val run=catalog.prepareRun(CatalogRunRequest("flow",operationKey="observed-flow"))
                account.close();catalog.close()
                assertNull(run.observeSnapshots(10.milliseconds).first())
                run
            }
            val streams=runs.map {run->async {withTimeout(10000) {run.observeSnapshots(10.milliseconds).toList()}}}
            runs.forEach {it.start()}
            for(run in runs) {
                val waiting=advance(engine,run,InstanceState.WAITING,2)
                assertTrue(run.answer(waiting,"left","yes"))
                val next=advance(engine,run,InstanceState.WAITING,1)
                assertTrue(run.answer(next,"right","yes"))
                advance(engine,run,InstanceState.COMPLETED)
            }
            streams.forEachIndexed {index,stream->
                val snapshots=stream.await().filterNotNull()
                assertEquals(InstanceState.COMPLETED,snapshots.last().state)
                assertTrue(snapshots.all {it.instanceId==runs[index].instanceId&&it.accountId==runs[index].accountId&&
                    it.catalogId==runs[index].catalogId&&it.sessionEpoch==runs[index].sessionEpoch})
            }
        }
    }

    @Test
    fun disposedParentsDoNotFenceIssuedViewsAndBrokenTokenGettersFailBeforeTransport()=runBlocking {
        withFixture {engine,cases->
            val record=cases.first();val account=engine.openAccount(record.identity())
            val catalog=account.openCatalog(record.configuration())
            val calls=AtomicInteger()
            val error=assertFailsWith<Orch8Exception> {catalog.sync(object:Orch8TokenSource {
                override fun currentToken():String {calls.incrementAndGet();error("broken cached getter")}
                override fun refreshToken():String=error("unexpected refresh")
            })}
            assertEquals(Orch8ErrorKind.INVALID_INPUT,error.kind);assertEquals(1,calls.get())
            val run=catalog.run(UUID.randomUUID().toString());account.close();catalog.close()
            assertNull(run.snapshot());run.close()
            assertEquals(Orch8ErrorKind.SHUTDOWN,assertFailsWith<Orch8Exception> {run.snapshot()}.kind)
            assertEquals(Orch8ErrorKind.SHUTDOWN,assertFailsWith<Orch8Exception> {run.completeStep("stale","left","{}")}.kind)
        }
    }
}
