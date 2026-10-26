package io.orch8.kmp.internal

import io.orch8.kmp.*
import io.orch8.kmp.bridge.*
import kotlinx.serialization.json.*

private fun malformed(message: String): Nothing = throw Orch8Exception(Orch8ErrorKind.ENGINE, "scoped bridge: $message")
private fun JsonElement.objectValue(): JsonObject = this as? JsonObject ?: malformed("expected object")
private fun JsonObject.string(name: String): String = (get(name) as? JsonPrimitive)
    ?.takeIf { it.isString }?.content ?: malformed("missing string $name")
private fun JsonObject.number(name: String): Long = (get(name) as? JsonPrimitive)
    ?.takeIf { !it.isString }?.longOrNull ?: malformed("missing integer $name")
private fun JsonElement.booleanValue(): Boolean = (this as? JsonPrimitive)?.takeIf { !it.isString }?.booleanOrNull ?: malformed("expected boolean")
private fun payload(method: String, reply: Orch8ScopedReply): JsonElement = BridgeCodec.unwrap(method, reply.payloadJson())
private fun info(reply: Orch8ScopedReply, kind: String): Pair<Orch8ScopedBridge, JsonObject> {
    val data = payload("open $kind", reply).objectValue()
    if (data.string("kind") != kind) malformed("unexpected handle kind")
    return (reply.resultHandle() ?: malformed("missing native handle")) to data
}
private fun Orch8ScopedBridge.invoke(method: String, args: JsonObject = JsonObject(emptyMap()), reference: Orch8ScopedBridge? = null): Orch8ScopedReply =
    call(method, args.toString(), reference)

internal fun openJsonAccount(bridge: Orch8JsonBridge, identity: AccountIdentity): AccountBackend {
    val reply = bridge.openAccount(buildJsonObject { put("accountKey", identity.accountKey); put("issuer", identity.issuer); put("subject", identity.subject) }.toString())
    val (port, data) = info(reply, "account")
    return JsonAccountBackend(port, data)
}
private open class JsonScopedView(protected val port: Orch8ScopedBridge) : ScopedViewBackend {
    override fun closeView() { payload("closeView", port.invoke("closeView")) }
    override fun dispose() { payload("dispose", port.invoke("dispose")) }
}
private class JsonAccountBackend(port: Orch8ScopedBridge, data: JsonObject) : JsonScopedView(port), AccountBackend {
    override val accountId = data.string("accountId")
    override val sessionEpoch = data.number("sessionEpoch")
    override fun retire() { payload("retire", port.invoke("retire")) }
    override fun openCatalog(configuration: CatalogConfiguration): CatalogBackend {
        val (child, descriptor) = info(port.invoke("openCatalog", buildJsonObject {
            put("catalogKey", configuration.catalogKey); put("source", configuration.source)
            put("signingAuthority", configuration.signingAuthority); put("manifestUrl", configuration.manifestUrl)
            put("identityUrl", configuration.identityUrl)
        }), "catalog")
        if (descriptor.string("accountId") != accountId || descriptor.number("sessionEpoch") != sessionEpoch) malformed("catalog owner mismatch")
        return JsonCatalogBackend(child, descriptor)
    }
}
private class JsonCatalogBackend(port: Orch8ScopedBridge, data: JsonObject) : JsonScopedView(port), CatalogBackend {
    override val accountId = data.string("accountId")
    override val catalogId = data.string("catalogId")
    override val sessionEpoch = data.number("sessionEpoch")
    override fun registerHandler(name: String, handler: BlockingCatalogStepHandler, timeoutMs: Long) {
        val expectedAccount = accountId; val expectedCatalog = catalogId; val expectedEpoch = sessionEpoch
        BridgeCodec.unwrap("registerHandler", port.registerHandler(name, timeoutMs, object : Orch8ScopedHandler {
            override fun execute(contextJson: String, stepName: String, inputJson: String): String {
                val context = try {
                    val c = lenientJson.parseToJsonElement(contextJson).objectValue()
                    CatalogCallbackContext(c.string("accountId"), c.string("catalogId"), c.number("ownerEpoch"),
                        c.string("runId"), c.string("blockId"), c.number("attempt"), c.string("effectId")).also {
                        if (it.accountId != expectedAccount || it.catalogId != expectedCatalog || it.ownerEpoch != expectedEpoch ||
                            it.attempt !in 0..4294967295L || it.effectId.isBlank()) malformed("invalid callback identity")
                    }
                } catch (_: Exception) { return BridgeCodec.error("permanent", "invalid native callback identity") }
                return try { BridgeCodec.ok(JsonPrimitive(handler.execute(context, stepName, inputJson))) }
                catch (error: Orch8HandlerException.Permanent) { BridgeCodec.error("permanent", error.hostFailureMessage("catalog handler failed")) }
                catch (error: Exception) { BridgeCodec.error("retryable", error.hostFailureMessage("catalog handler failed")) }
            }
        }))
    }
    override fun sync(tokens: Orch8TokenSource): SyncResult {
        val result = BridgeCodec.unwrap("scoped sync", port.sync(object : Orch8ScopedTokenSource {
            override fun cachedToken(): String = tokens.cachedTokenOrEmpty()
            override fun refreshEnvelope(): String = try { BridgeCodec.ok(JsonPrimitive(tokens.refreshToken())) }
                catch (error: Exception) { BridgeCodec.error("Engine", error.hostFailureMessage("token refresh failed")) }
        })).objectValue()
        fun count(key: String): Int = result.number(key).let { if (it !in 0..Int.MAX_VALUE) malformed("invalid sync count") else it.toInt() }
        return SyncResult(count("added"), count("updated"), count("removed"), count("skipped"), count("signatureFailures"))
    }
    private fun runFrom(reply: Orch8ScopedReply): CatalogRunBackend {
        val (child, descriptor) = info(reply, "run")
        if (descriptor.string("accountId") != accountId || descriptor.string("catalogId") != catalogId || descriptor.number("sessionEpoch") != sessionEpoch)
            malformed("run owner mismatch")
        return JsonRunBackend(child, descriptor)
    }
    override fun prepareRun(request: CatalogRunRequest): CatalogRunBackend = runFrom(port.invoke("prepareRun", buildJsonObject {
        put("workflow", request.workflow); put("inputJson", request.inputJson); put("operationKey", request.operationKey)
        put("version", request.version?.let(::JsonPrimitive) ?: JsonNull)
    }))
    override fun run(instanceId: String): CatalogRunBackend = runFrom(port.invoke("run", buildJsonObject { put("instanceId", instanceId) }))
    override fun history(cursor: CatalogCursorBackend?, limit: Int): CatalogRunPage {
        val reference = when(cursor) {
            null -> null
            is JsonCursorBackend -> cursor.port
            else -> throw Orch8Exception(Orch8ErrorKind.INVALID_INPUT, "cursor belongs to another platform backend")
        }
        val reply = port.invoke("history", buildJsonObject { put("limit",limit) }, reference)
        val value = payload("history", reply).objectValue()
        val items = (value["items"] as? JsonArray ?: malformed("missing history items")).map { raw ->
            val item = raw.objectValue()
            val epoch = item.number("ownerEpoch")
            if (item.string("accountId") != accountId || item.string("catalogId") != catalogId || epoch !in 1..sessionEpoch)
                malformed("history owner mismatch")
            val version = item.number("version").let { if (it !in 1..Int.MAX_VALUE) malformed("invalid workflow version") else it.toInt() }
            val native = item["nativeState"]?.objectValue() ?: malformed("missing native state")
            val state = when(native.string("kind")) {
                "not_materialized" -> CatalogNativeState.NotMaterialized
                "unavailable" -> CatalogNativeState.Unavailable
                "present" -> CatalogNativeState.Present(InstanceState.fromWire(native.string("state")))
                "recorded" -> CatalogNativeState.Recorded(InstanceState.fromWire(native.string("state")).also { if (!it.isTerminal) malformed("nonterminal archive outcome") })
                else -> malformed("unknown native state")
            }
            val request = when(item.string("requestState")) { "prepared" -> CatalogRequestState.PREPARED; "cancelled" -> CatalogRequestState.CANCELLED; "archived" -> CatalogRequestState.ARCHIVED; else -> malformed("unknown request state") }
            if ((state is CatalogNativeState.Recorded && request != CatalogRequestState.ARCHIVED) ||
                (request == CatalogRequestState.ARCHIVED && state != CatalogNativeState.Unavailable && state !is CatalogNativeState.Recorded))
                malformed("inconsistent archive history")
            CatalogRunSummary(item.string("instanceId"), accountId, catalogId, epoch, item.string("workflow"), version,
                item.string("sequenceId"), item.string("createdAt"), request, state)
        }
        if (items.size > limit) malformed("oversized history page")
        val next = reply.resultHandle()
        if (value["hasNext"]?.booleanValue() != (next != null)) malformed("history cursor mismatch")
        return CatalogRunPage(items, next?.let { CatalogHistoryCursor(JsonCursorBackend(it)) })
    }
}
private class JsonCursorBackend(val port: Orch8ScopedBridge) : CatalogCursorBackend {
    override fun dispose() { payload("dispose cursor", port.invoke("dispose")) }
}
private class JsonRunBackend(port: Orch8ScopedBridge, data: JsonObject) : JsonScopedView(port), CatalogRunBackend {
    override val instanceId = data.string("instanceId")
    override val accountId = data.string("accountId")
    override val catalogId = data.string("catalogId")
    override val sessionEpoch = data.number("sessionEpoch")
    override fun start() { payload("start", port.invoke("start")) }
    override fun snapshot(): CatalogRunSnapshot? {
        val value = payload("snapshot", port.invoke("snapshot"))
        if (value == JsonNull) return null
        val snapshot = value.objectValue()
        if (snapshot.string("accountId") != accountId || snapshot.string("catalogId") != catalogId ||
            snapshot.number("sessionEpoch") != sessionEpoch || snapshot.string("instanceId") != instanceId) malformed("snapshot owner mismatch")
        val steps = (snapshot["pendingSteps"] as? JsonArray ?: malformed("missing pending steps")).map {
            (it as? JsonPrimitive)?.takeIf { it.isString }?.content ?: malformed("invalid pending step")
        }
        return CatalogRunSnapshot(instanceId,accountId,catalogId,sessionEpoch,InstanceState.fromWire(snapshot.string("state")),
            snapshot.string("dataJson"),steps,snapshot.string("createdAt"),snapshot.string("updatedAt"))
    }
    override fun cancel(): InstanceState? = payload("cancel",port.invoke("cancel")).let {
        if (it == JsonNull) null else InstanceState.fromWire((it as? JsonPrimitive)?.takeIf { it.isString }?.content ?: malformed("invalid native state"))
    }
    override fun completeStep(expectedUpdatedAt: String, stepName: String, outputJson: String): Boolean = payload("completeStep",port.invoke("completeStep",buildJsonObject {
        put("expectedUpdatedAt",expectedUpdatedAt);put("stepName",stepName);put("outputJson",outputJson)
    })).booleanValue()
    override fun archiveAfterAcknowledgement(acknowledgement: String): CatalogArchiveReceipt? {
        val value = payload("archiveAfterAcknowledgement", port.invoke("archiveAfterAcknowledgement", buildJsonObject {
            put("acknowledgement", acknowledgement)
        }))
        if (value == JsonNull) return null
        val receipt = value.objectValue()
        if (receipt.string("instanceId") != instanceId || receipt.string("accountId") != accountId || receipt.string("catalogId") != catalogId)
            malformed("archive owner mismatch")
        val outcome = when (val state = receipt["outcome"]) {
            JsonNull -> null
            is JsonPrimitive -> if (state.isString) InstanceState.fromWire(state.content).also { if (!it.isTerminal) malformed("nonterminal archive outcome") } else malformed("invalid archive outcome")
            else -> malformed("missing archive outcome")
        }
        val digest = receipt.string("acknowledgementDigest")
        if (digest.length != 64 || digest.any { it !in '0'..'9' && it !in 'a'..'f' }) malformed("invalid acknowledgement digest")
        return CatalogArchiveReceipt(instanceId,accountId,catalogId,digest,receipt.string("archivedAt"),outcome)
    }
    override fun forgetCancelledRequest(): Boolean = payload("forgetCancelledRequest",port.invoke("forgetCancelledRequest")).booleanValue()
}
