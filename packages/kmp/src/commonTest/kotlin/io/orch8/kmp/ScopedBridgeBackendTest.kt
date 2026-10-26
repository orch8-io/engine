package io.orch8.kmp

import io.orch8.kmp.bridge.*
import io.orch8.kmp.internal.BlockingCatalogStepHandler
import io.orch8.kmp.internal.BridgeCodec
import io.orch8.kmp.internal.openJsonAccount
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.StandardTestDispatcher

private class ScopeReply(private val value: JsonElement = JsonNull, private val child: Orch8ScopedBridge? = null) : Orch8ScopedReply {
    override fun payloadJson() = BridgeCodec.ok(value)
    override fun resultHandle() = child
}
private class ScopePort(val kind: String) : Orch8ScopedBridge {
    var action: (String, JsonObject, Orch8ScopedBridge?) -> Orch8ScopedReply = { _,_,_ -> ScopeReply() }
    var handler: Orch8ScopedHandler? = null
    var tokens: Orch8ScopedTokenSource? = null
    override fun call(method: String, argsJson: String, reference: Orch8ScopedBridge?) =
        action(method, Json.parseToJsonElement(argsJson).jsonObject, reference)
    override fun registerHandler(name: String, timeoutMs: Long, handler: Orch8ScopedHandler): String {
        this.handler=handler;return BridgeCodec.ok()
    }
    override fun sync(tokens: Orch8ScopedTokenSource): String {
        this.tokens=tokens
        return BridgeCodec.ok(buildJsonObject { for (field in listOf("added","updated","removed","skipped","signatureFailures")) put(field,0) })
    }
}
private fun descriptor(kind: String) = buildJsonObject {
    put("kind",kind);put("accountId","account-a");put("sessionEpoch",7)
    if(kind!="account")put("catalogId","catalog-a")
    if(kind=="run")put("instanceId","run-a")
}
private class ScopeFixture {
    val account=ScopePort("account")
    val catalog=ScopePort("catalog")
    val run=ScopePort("run")
    val root=object:Orch8JsonBridge {
        override fun call(method:String,argsJson:String)=BridgeCodec.ok()
        override fun setCallbacks(callbacks:Orch8BridgeCallbacks){}
        override fun openAccount(identityJson:String):Orch8ScopedReply = ScopeReply(descriptor("account"),account)
    }
    init {
        account.action={method,_,_->if(method=="openCatalog")ScopeReply(descriptor("catalog"),catalog)else ScopeReply()}
        catalog.action={method,_,_->if(method=="prepareRun"||method=="run")ScopeReply(descriptor("run"),run)else ScopeReply()}
    }
    fun backend()=openJsonAccount(root,AccountIdentity("a","issuer","a"))
        .openCatalog(CatalogConfiguration("cat","source","key","https://example.com/manifest","https://example.com/identity"))
}
class ScopedBridgeBackendTest {
    @Test
    fun contextualCallbacksKeepNativeIdentityAndRejectForeignOrMalformedContextsBeforeHostEntry() {
        val fixture=ScopeFixture();val backend=fixture.backend();val contexts=mutableListOf<CatalogCallbackContext>()
        backend.registerHandler("echo",BlockingCatalogStepHandler { context,_,_->contexts+=context;"{}" },1000)
        val context=buildJsonObject {
            put("accountId","account-a");put("catalogId","catalog-a");put("ownerEpoch",7)
            put("runId","run-a");put("blockId","work");put("attempt",4294967295L);put("effectId","effect-a")
        }
        val handler=assertNotNull(fixture.catalog.handler)
        assertEquals(JsonPrimitive("{}"),BridgeCodec.unwrap("handler",handler.execute(context.toString(),"work","""{"accountId":"forged"}""")))
        assertEquals("account-a",contexts.single().accountId)
        assertEquals(4294967295L,contexts.single().attempt)
        for (bad in listOf(JsonObject(context+("accountId" to JsonPrimitive("foreign"))),JsonObject(context-("effectId")))) {
            val result=Json.parseToJsonElement(handler.execute(bad.toString(),"work","{}")).jsonObject
            assertEquals("permanent",result["error"]!!.jsonObject["kind"]!!.jsonPrimitive.content)
        }
        assertEquals(1,contexts.size)
    }
    @Test
    fun tokenAdaptersRemainBoundToTheirOwnSourceAfterEachLogicalSyncReturns() {
        val first=ScopeFixture();val second=ScopeFixture()
        fun tokens(value:String)=object:Orch8TokenSource {
            override fun currentToken()=value
            override fun refreshToken()="${value}_refreshed"
        }
        first.backend().sync(tokens("alpha"));second.backend().sync(tokens("beta"))
        assertEquals("alpha",assertNotNull(first.catalog.tokens).cachedToken())
        assertEquals("beta",assertNotNull(second.catalog.tokens).cachedToken())
        assertEquals(JsonPrimitive("alpha_refreshed"),BridgeCodec.unwrap("refresh",first.catalog.tokens!!.refreshEnvelope()))
        first.backend().sync(object:Orch8TokenSource {
            override fun currentToken():String=error("broken getter")
            override fun refreshToken():String=error("broken refresh")
        })
        assertEquals("",first.catalog.tokens!!.cachedToken())
        assertFailsWith<Orch8Exception> { BridgeCodec.unwrap("refresh",first.catalog.tokens!!.refreshEnvelope()) }
    }
    @Test
    fun historyTransfersAnOwnedCursorAndNeverSerializesItAsAuthority() {
        val fixture=ScopeFixture();val backend=fixture.backend();val cursor=ScopePort("cursor");var disposed=0
        cursor.action={method,_,_->if(method=="dispose")disposed++;ScopeReply()}
        val item=buildJsonObject {
            put("instanceId","run-a");put("accountId","account-a");put("catalogId","catalog-a");put("ownerEpoch",6)
            put("workflow","flow");put("version",1);put("sequenceId","sequence-a");put("createdAt","2026-01-01T00:00:00Z")
            put("requestState","prepared");put("nativeState",buildJsonObject {put("kind","unavailable")})
        }
        fixture.catalog.action={method,args,reference->
            assertEquals("history",method);assertEquals(setOf("limit"),args.keys)
            if(reference==null)ScopeReply(buildJsonObject {put("items",JsonArray(listOf(item)));put("hasNext",true)},cursor)
            else {assertSame(cursor,reference);ScopeReply(buildJsonObject {put("items",JsonArray(emptyList()));put("hasNext",false)})}
        }
        val first=backend.history(null,1)
        assertEquals(CatalogNativeState.Unavailable,first.items.single().nativeState)
        val next=assertNotNull(first.next)
        assertTrue(backend.history(next.backend,1).items.isEmpty());next.close();assertEquals(1,disposed)
    }
    @Test
    fun foreignSnapshotAndHistoryPayloadsAreRefusedBeforePublication() {
        val fixture=ScopeFixture();val backend=fixture.backend();val run=backend.run("run-a")
        fixture.run.action={_,_,_->ScopeReply(buildJsonObject {
            put("instanceId","run-a");put("accountId","foreign");put("catalogId","catalog-a");put("sessionEpoch",7)
        })}
        assertFailsWith<Orch8Exception> { run.snapshot() }
        fixture.catalog.action={_,_,_->ScopeReply(buildJsonObject {
            put("items",JsonArray(listOf(buildJsonObject {
                put("accountId","foreign");put("catalogId","catalog-a");put("ownerEpoch",7)
            })));put("hasNext",false)
        })}
        assertFailsWith<Orch8Exception> { backend.history(null,1) }
    }
    @Test
    fun versionedRepliesPreserveFalseAndTheExactCallerVersion() {
        val fixture=ScopeFixture();val run=fixture.backend().run("run-a")
        fixture.run.action={method,args,_->
            assertEquals("completeStep",method);assertEquals("original-version",args["expectedUpdatedAt"]!!.jsonPrimitive.content)
            assertEquals("left",args["stepName"]!!.jsonPrimitive.content);ScopeReply(JsonPrimitive(false))
        }
        assertFalse(run.completeStep("original-version","left","{}"))
    }
    @Test
    fun acknowledgedOpenIsClosedOnProtocolFailureWithoutMaskingTheOriginalError() {
        val calls=mutableListOf<String>()
        val bridge=object:Orch8JsonBridge {
            override fun setCallbacks(callbacks:Orch8BridgeCallbacks) = error("callbacks must not be installed")
            override fun openAccount(identityJson:String):Orch8ScopedReply = error("unused")
            override fun call(method:String,argsJson:String):String {
                calls+=method
                if(method=="open")return """{"ok":{"protocol":999}}"""
                error("cleanup failed")
            }
        }
        val error=assertFailsWith<Orch8Exception> {
            io.orch8.kmp.internal.JsonBridgeBackend.open(bridge,"/tmp/protocol-scope-test.db",EngineConfig())
        }
        assertEquals(Orch8ErrorKind.INVALID_INPUT,error.kind)
        assertEquals(listOf("open","shutdown"),calls)
    }

    @Test
    fun archiveReceiptsPreserveNullAndValidateOwnerOutcomeAndDigest() {
        val fixture=ScopeFixture();val run=fixture.backend().run("run-a")
        var response: JsonElement = JsonNull
        fixture.run.action={method,args,_->
            assertEquals("archiveAfterAcknowledgement",method)
            assertEquals("host-ack",args["acknowledgement"]!!.jsonPrimitive.content)
            ScopeReply(response)
        }
        assertNull(run.archiveAfterAcknowledgement("host-ack"))
        val valid=buildJsonObject {
            put("instanceId","run-a");put("accountId","account-a");put("catalogId","catalog-a")
            put("acknowledgementDigest","a".repeat(64));put("archivedAt","2026-01-01T00:00:00Z");put("outcome","completed")
        }
        response=valid
        assertEquals(InstanceState.COMPLETED,assertNotNull(run.archiveAfterAcknowledgement("host-ack")).outcome)
        response=JsonObject(valid+("outcome" to JsonNull))
        assertNull(assertNotNull(run.archiveAfterAcknowledgement("host-ack")).outcome)
        for (bad in listOf(
            JsonObject(valid+("accountId" to JsonPrimitive("foreign"))),
            JsonObject(valid+("instanceId" to JsonPrimitive("foreign"))),
            JsonObject(valid+("catalogId" to JsonPrimitive("foreign"))),
            JsonObject(valid+("outcome" to JsonPrimitive("waiting"))),
            JsonObject(valid+("acknowledgementDigest" to JsonPrimitive("invalid"))),
            JsonObject(valid-"outcome"),
        )) {
            response=bad
            assertFailsWith<Orch8Exception> { run.archiveAfterAcknowledgement("host-ack") }
        }
    }

    @Test
    fun archivedHistorySeparatesRecordedOutcomeFromUnknownOrLiveState() {
        val fixture=ScopeFixture();val backend=fixture.backend()
        var native: JsonObject = buildJsonObject { put("kind","recorded");put("state","completed") }
        var request="archived"
        fixture.catalog.action={_,_,_-> ScopeReply(buildJsonObject {
            put("items",JsonArray(listOf(buildJsonObject {
                put("instanceId","run-a");put("accountId","account-a");put("catalogId","catalog-a");put("ownerEpoch",6)
                put("workflow","flow");put("version",1);put("sequenceId","sequence-a");put("createdAt","2026-01-01T00:00:00Z")
                put("requestState",request);put("nativeState",native)
            })));put("hasNext",false)
        }) }
        assertEquals(CatalogRequestState.ARCHIVED,backend.history(null,1).items.single().requestState)
        assertEquals(CatalogNativeState.Recorded(InstanceState.COMPLETED),backend.history(null,1).items.single().nativeState)
        request="prepared"
        assertFailsWith<Orch8Exception> { backend.history(null,1) }
        request="archived";native=buildJsonObject {put("kind","unavailable")}
        assertEquals(CatalogNativeState.Unavailable,backend.history(null,1).items.single().nativeState)
        for (kind in listOf("present","recorded")) {
            native=buildJsonObject {put("kind",kind);put("state","waiting")}
            assertFailsWith<Orch8Exception> { backend.history(null,1) }
        }
    }

    @Test
    fun scopedHandlerAndTokenFailureEnvelopesNeverReadUnknownExceptionMessages() {
        val fixture=ScopeFixture();val backend=fixture.backend();val failure=ThrowingMessageException()
        backend.registerHandler("broken",BlockingCatalogStepHandler {_,_,_->throw failure},1000)
        val context=buildJsonObject {
            put("accountId","account-a");put("catalogId","catalog-a");put("ownerEpoch",7)
            put("runId","run-a");put("blockId","work");put("attempt",1);put("effectId","effect-a")
        }
        assertFailsWith<Orch8Exception> { BridgeCodec.unwrap("handler",fixture.catalog.handler!!.execute(context.toString(),"work","{}")) }
        backend.sync(object:Orch8TokenSource {
            override fun currentToken()="token"
            override fun refreshToken():String=throw failure
        })
        assertFailsWith<Orch8Exception> { BridgeCodec.unwrap("refresh",fixture.catalog.tokens!!.refreshEnvelope()) }
        assertEquals(0,failure.reads)
    }

    @Test
    fun scopedFacadePreservesKnownHandlerErrorsAndQuarantinesUnknownMessageGetters() = runTest {
        val fixture=ScopeFixture();val catalog=Orch8Catalog(fixture.backend(),StandardTestDispatcher(testScheduler))
        val unknown=ThrowingMessageException()
        val context=buildJsonObject {
            put("accountId","account-a");put("catalogId","catalog-a");put("ownerEpoch",7)
            put("runId","run-a");put("blockId","work");put("attempt",1);put("effectId","effect-a")
        }
        for ((failure,kind,message) in listOf(
            Triple(unknown,"retryable","catalog handler failed"),
            Triple(Orch8HandlerException.Permanent("known-permanent"),"permanent","known-permanent"),
            Triple(Orch8HandlerException.Retryable("known-retryable"),"retryable","known-retryable"),
        )) {
            catalog.registerHandler("probe",Orch8CatalogStepHandler {_,_,_->throw failure},1000)
            val error=Json.parseToJsonElement(fixture.catalog.handler!!.execute(context.toString(),"work","{}")).jsonObject["error"]!!.jsonObject
            assertEquals(kind,error["kind"]!!.jsonPrimitive.content)
            assertEquals(message,error["message"]!!.jsonPrimitive.content)
        }
        assertEquals(0,unknown.reads)
        catalog.close()
    }

}
