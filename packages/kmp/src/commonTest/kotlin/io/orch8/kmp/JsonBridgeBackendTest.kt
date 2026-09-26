package io.orch8.kmp

import io.orch8.kmp.bridge.Orch8BridgeCallbacks
import io.orch8.kmp.bridge.Orch8JsonBridge
import io.orch8.kmp.internal.BlockingStepHandler
import io.orch8.kmp.internal.BridgeCodec
import io.orch8.kmp.internal.JsonBridgeBackend
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** Scripted stand-in for the Swift bridge: records calls, replays envelopes. */
private class FakeJsonBridge(
    private val protocol: Int = 1,
) : Orch8JsonBridge {
    val calls = mutableListOf<Pair<String, JsonObject>>()
    val replies = mutableMapOf<String, String>()
    var installed: Orch8BridgeCallbacks? = null
    var onCall: (String) -> Unit = {}

    override fun call(method: String, argsJson: String): String {
        calls += method to Json.parseToJsonElement(argsJson).jsonObject
        onCall(method)
        if (method == "open") return """{"ok":{"protocol":$protocol}}"""
        return replies[method] ?: """{"ok":null}"""
    }

    override fun setCallbacks(callbacks: Orch8BridgeCallbacks) {
        installed = callbacks
    }
}

class JsonBridgeBackendTest {
    private fun open(bridge: FakeJsonBridge = FakeJsonBridge()) =
        bridge to JsonBridgeBackend.open(bridge, "/tmp/orch8.db", EngineConfig(deviceId = "dev-1"))

    @Test
    fun openSendsConfigAndInstallsCallbacks() {
        val (bridge, _) = open()
        val (method, args) = bridge.calls.single()
        assertEquals("open", method)
        assertEquals("/tmp/orch8.db", args["dbPath"]!!.jsonPrimitive.content)
        val config = args["config"]!!.jsonObject
        assertEquals("500", config["tickIntervalMs"]!!.jsonPrimitive.content)
        assertEquals("dev-1", config["deviceId"]!!.jsonPrimitive.content)
        assertNotNull(bridge.installed)
    }

    @Test
    fun protocolMismatchFailsFast() {
        val e = assertFailsWith<Orch8Exception> { open(FakeJsonBridge(protocol = 99)) }
        assertEquals(Orch8ErrorKind.INVALID_INPUT, e.kind)
    }

    @Test
    fun decodesTypedResults() {
        val (bridge, backend) = open()
        bridge.replies["getInstance"] =
            """{"ok":{"instanceId":"i1","sequenceName":"field-inspection","state":"waiting",
               "context":"{}","createdAt":"c","updatedAt":"u"}}"""
        bridge.replies["runUntilIdle"] =
            """{"ok":{"ticksExecuted":3,"instancesAdvanced":1,"stepsExecuted":2,"hasPendingWork":true,"budgetExhausted":false}}"""
        bridge.replies["activeInstances"] =
            """{"ok":[{"instanceId":"i1","sequenceName":"s","state":"running","createdAt":"c"}]}"""
        bridge.replies["start"] = """{"ok":"i1"}"""

        assertEquals(InstanceState.WAITING, backend.getInstance("i1").state)
        assertEquals(3, backend.runUntilIdle(25, 20_000).ticksExecuted)
        assertEquals(InstanceState.RUNNING, backend.activeInstances().single().state)
        assertEquals("i1", backend.start("s", "{}", null))
        val startArgs = bridge.calls.last { it.first == "start" }.second
        assertEquals("null", startArgs["dedupKey"].toString())
    }

    @Test
    fun errorEnvelopeBecomesTypedException() {
        val (bridge, backend) = open()
        bridge.replies["getInstance"] = BridgeCodec.error("NotFound", "instance i9 not found")
        val e = assertFailsWith<Orch8Exception> { backend.getInstance("i9") }
        assertEquals(Orch8ErrorKind.NOT_FOUND, e.kind)
        assertEquals("instance i9 not found", e.message)
    }

    @Test
    fun malformedResultsAreReportedNotCrashed() {
        val (bridge, backend) = open()
        bridge.replies["tickOnce"] = "definitely not json"
        assertEquals(Orch8ErrorKind.ENGINE, assertFailsWith<Orch8Exception> { backend.tickOnce() }.kind)
        bridge.replies["tickOnce"] = """{"ok":{"instancesAdvanced":1}}"""
        assertEquals(Orch8ErrorKind.ENGINE, assertFailsWith<Orch8Exception> { backend.tickOnce() }.kind)
        bridge.replies["tickOnce"] = """{"neither":1}"""
        assertFailsWith<Orch8Exception> { backend.tickOnce() }
    }

    @Test
    fun handlerCallbacksRouteToRegisteredHandlers() {
        val (bridge, backend) = open()
        backend.registerHandler("ok", BlockingStepHandler { step, input -> """{"step":"$step","in":$input}""" })
        backend.registerHandler("fatal", BlockingStepHandler { _, _ -> throw Orch8HandlerException.Permanent("bad data") })
        backend.registerHandler("flaky", BlockingStepHandler { _, _ -> error("offline") })
        val cb = bridge.installed!!

        val ok = Json.parseToJsonElement(cb.executeHandler("ok", "s1", "{}")).jsonObject
        assertEquals("""{"step":"s1","in":{}}""", ok["ok"]!!.jsonPrimitive.content)

        fun kindOf(envelope: String) =
            Json.parseToJsonElement(envelope).jsonObject["error"]!!.jsonObject["kind"]!!.jsonPrimitive.content
        assertEquals("permanent", kindOf(cb.executeHandler("fatal", "s", "{}")))
        assertEquals("retryable", kindOf(cb.executeHandler("flaky", "s", "{}")))
        assertEquals("permanent", kindOf(cb.executeHandler("missing", "s", "{}")))
    }

    @Test
    fun listenerCallbacksBecomeEvents() {
        val (bridge, backend) = open()
        val events = mutableListOf<EngineEvent>()
        backend.setListener { events += it }
        bridge.installed!!.onStepPending("i1", "supervisor_approval", "human_review")
        bridge.installed!!.onInstanceCompleted("i1", "{}")
        assertEquals(
            listOf(
                EngineEvent.StepPending("i1", "supervisor_approval", "human_review"),
                EngineEvent.InstanceCompleted("i1", "{}"),
            ),
            events,
        )
    }

    @Test
    fun tokenSourceIsOnlyVisibleDuringSync() {
        val (bridge, backend) = open()
        var refreshed = 0
        val tokens = object : Orch8TokenSource {
            override fun currentToken() = "t0"

            override fun refreshToken(): String = "t${++refreshed}"
        }
        bridge.replies["sync"] =
            """{"ok":{"added":1,"updated":0,"removed":0,"skipped":0,"signatureFailures":0}}"""
        var seenToken: String? = null
        var seenRefresh: String? = null
        bridge.onCall = { method ->
            if (method == "sync") {
                seenToken = bridge.installed!!.currentToken()
                seenRefresh = bridge.installed!!.refreshToken()
            }
        }

        assertEquals(1, backend.sync("https://api.example.com/manifest.json", tokens).added)
        assertEquals("t0", seenToken)
        assertEquals("t1", Json.parseToJsonElement(seenRefresh!!).jsonObject["ok"]!!.jsonPrimitive.content)
        // Outside sync the token source is gone.
        assertEquals("", bridge.installed!!.currentToken())
        assertTrue("error" in Json.parseToJsonElement(bridge.installed!!.refreshToken()).jsonObject)
        val args = bridge.calls.last { it.first == "sync" }.second
        assertTrue(args["useTokenSource"]!!.jsonPrimitive.content.toBoolean())
    }
}
