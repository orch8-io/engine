package io.orch8.reactnative

import io.orch8.mobile.MobileAccount
import io.orch8.mobile.MobileAccountIdentity
import io.orch8.mobile.MobileCatalog
import io.orch8.mobile.MobileCatalogCallbackContext
import io.orch8.mobile.MobileCatalogConfiguration
import io.orch8.mobile.MobileCatalogHistoryCursor
import io.orch8.mobile.MobileCatalogNativeState
import io.orch8.mobile.MobileCatalogRun
import io.orch8.mobile.MobileCatalogRunRequest
import io.orch8.mobile.MobileCatalogRunSummary
import io.orch8.mobile.MobileCatalogStepHandler
import io.orch8.mobile.MobileEngine
import io.orch8.mobile.MobileException
import io.orch8.mobile.HandlerException
import io.orch8.mobile.InstanceStateKind
import io.orch8.mobile.TokenProvider
import org.json.JSONArray
import org.json.JSONObject
import org.json.JSONTokener
import java.lang.ref.WeakReference
import java.util.UUID

internal interface MobileScopedCallbacks {
    val scopedPending: PendingHandlerCalls
    fun emitScoped(scope: NativeEngineLifecycle.Scope, name: String, body: Map<String, Any?>): Boolean
}

/** Native views retain weak engine identity; opaque IDs are never account selectors. */
internal class MobileScopedBridge {
    private sealed interface Node {
        fun close()
        class Account(val native: MobileAccount) : Node { override fun close() = native.close() }
        class Catalog(val native: MobileCatalog) : Node { override fun close() = native.close() }
        class Run(val native: MobileCatalogRun) : Node { override fun close() = native.close() }
        class Cursor(val native: MobileCatalogHistoryCursor, val account: String, val catalog: String, val epoch: Long) : Node { override fun close() = native.close() }
    }
    private data class Entry(val node: Node, val scope: NativeEngineLifecycle.Scope, val parent: String?, val account: String, val catalog: String?, val epoch: String)
    private val lock = Any()
    private val entries = mutableMapOf<String, Entry>()
    private var activeScope: NativeEngineLifecycle.Scope? = null
    fun prune() {
        val retired = synchronized(lock) {
            if (activeScope?.isCurrent() == true) return
            val stale = entries.filterValues { !it.scope.isCurrent() }
            stale.keys.forEach { entries.remove(it) }
            stale.values.toList()
        }
        retired.forEach { it.node.close() } // JNI cleanup never holds the table lock.
    }
    private fun insert(node: Node, scope: NativeEngineLifecycle.Scope, parent: String? = null): JSONObject {
        val id = UUID.randomUUID().toString()
        val result = JSONObject().put("handle", id)
        when (node) {
            is Node.Account -> result.put("kind", "account").put("accountId", node.native.accountId()).put("sessionEpoch", node.native.sessionEpoch().toString())
            is Node.Catalog -> result.put("kind", "catalog").put("accountId", node.native.accountId()).put("catalogId", node.native.catalogId()).put("sessionEpoch", node.native.sessionEpoch().toString())
            is Node.Run -> result.put("kind", "run").put("accountId", node.native.accountId()).put("catalogId", node.native.catalogId()).put("sessionEpoch", node.native.sessionEpoch().toString()).put("instanceId", node.native.instanceId())
            is Node.Cursor -> result.put("kind", "cursor").put("accountId", node.account).put("catalogId", node.catalog).put("sessionEpoch", node.epoch.toString())
        }
        val account = result.getString("accountId")
        val catalog = result.opt("catalogId") as? String
        val accepted = synchronized(lock) {
            if (entries.size >= 4096 || !scope.isCurrent() || entries.values.count { it.account == account } >= 1024 ||
                (catalog != null && entries.values.count { it.account == account && it.catalog == catalog } >= 512)) false
            else {
                entries[id] = Entry(node, scope, parent, account, catalog, result.getString("sessionEpoch"))
                activeScope = scope
                true
            }
        }
        if (!accepted) {
            node.close()
            if (!scope.isCurrent()) throw MobileException.Shutdown("engine retired")
            throw MobileException.ResourceLimit("native scoped view capacity exhausted or engine retired")
        }
        return result
    }
    private fun releaseTree(root: String) {
        val retired = synchronized(lock) {
            val keys = mutableSetOf(root)
            do {
                val previous = keys.size
                entries.forEach { (id, entry) -> if (entry.parent in keys) keys.add(id) }
            } while (previous != keys.size)
            val removed = entries.filterKeys { it in keys }
            removed.keys.forEach { entries.remove(it) }
            removed.values.toList()
        }
        retired.forEach { it.node.close() }
    }
    private fun releaseAccount(account: String, epoch: String) {
        val retired = synchronized(lock) {
            val removed = entries.filterValues { it.account == account && it.epoch == epoch }
            removed.keys.forEach { entries.remove(it) }
            removed.values.toList()
        }
        retired.forEach { it.node.close() }
    }
    private fun lookup(id: String): Entry {
        val entry = synchronized(lock) { entries[id] }
        if (entry == null || !entry.scope.isCurrent()) throw MobileException.Shutdown("scope view disposed or retired")
        return entry
    }
    fun call(engine: MobileEngine, scope: NativeEngineLifecycle.Scope, module: MobileScopedCallbacks, handle: String, method: String, argsJson: String): String {
        prune()
        if (argsJson.length > 4 * 1024 * 1024 || argsJson.toByteArray(Charsets.UTF_8).size > 4 * 1024 * 1024)
            throw MobileException.InvalidInput("scoped arguments must be a bounded JSON object")
        val args = try {
            val parser = JSONTokener(argsJson)
            val parsed = parser.nextValue()
            if (parsed !is JSONObject || parser.nextClean() != '\u0000') throw MobileException.InvalidInput("invalid JSON object")
            parsed
        } catch (_: Exception) { throw MobileException.InvalidInput("scoped arguments must be a JSON object") }
        val result: Any? = if (method == "dispose") {
            val retired = synchronized(lock) { entries.remove(handle) }
            retired?.node?.close()
            null
        } else if (handle.isEmpty() && method == "openAccount") {
            insert(Node.Account(engine.openAccount(MobileAccountIdentity(args.string("accountKey"), args.string("issuer"), args.string("subject")))), scope)
        } else {
            val entry = lookup(handle)
            if (entry.scope !== scope) throw MobileException.Shutdown("scope belongs to another engine")
            when (val node = entry.node) {
                is Node.Account -> when (method) {
                    "openCatalog" -> insert(Node.Catalog(node.native.openCatalog(MobileCatalogConfiguration(args.string("catalogKey"), args.string("source"), args.string("signingAuthority"), args.string("manifestUrl"), args.string("identityUrl")))), scope, handle)
                    "retire" -> { node.native.retire(); releaseAccount(entry.account, entry.epoch); null }
                    "closeView" -> { node.native.closeView(); releaseTree(handle); null }
                    else -> unknown()
                }
                is Node.Catalog -> when (method) {
                    "closeView" -> { node.native.closeView(); releaseTree(handle); null }
                    "prepareRun" -> insert(Node.Run(node.native.prepareRun(MobileCatalogRunRequest(args.string("workflow"), if (args.isNull("version")) null else args.positive("version", Int.MAX_VALUE.toLong()).toInt(), args.string("inputJson"), args.string("operationKey")))), scope, handle)
                    "run" -> insert(Node.Run(node.native.run(args.string("instanceId"))), scope, handle)
                    "registerHandler" -> {
                        val timeout = args.positive("timeoutMs", 9_007_199_254_740_991)
                        node.native.registerHandler(args.string("name"), MobileScopedHandler(module, scope, node.native, args.string("callbackKey")), timeout.toULong())
                        null
                    }
                    "sync" -> {
                        val value = node.native.sync(MobileScopedTokens(module, scope, args.string("providerKey"), args.string("initialToken")))
                        JSONObject().put("added", value.added.toLong()).put("updated", value.updated.toLong()).put("removed", value.removed.toLong()).put("skipped", value.skipped.toLong()).put("signatureFailures", value.signatureFailures.toLong())
                    }
                    "history" -> {
                        val cursor = if (args.isNull("cursor")) null else {
                            val cursorEntry = lookup(args.string("cursor"))
                            if (cursorEntry.scope !== scope) throw MobileException.InvalidInput("cursor belongs to another engine")
                            val candidate = cursorEntry.node as? Node.Cursor ?: throw MobileException.InvalidInput("invalid cursor")
                            if (candidate.account != node.native.accountId() || candidate.catalog != node.native.catalogId() || candidate.epoch != node.native.sessionEpoch())
                                throw MobileException.InvalidInput("cursor belongs to another catalog session")
                            candidate.native
                        }
                        val page = node.native.history(cursor, args.positive("limit", 128).toUInt())
                        JSONObject().put("items", JSONArray(page.items.map(::summary))).put("next", page.next?.let { insert(Node.Cursor(it, node.native.accountId(), node.native.catalogId(), node.native.sessionEpoch()), scope, handle) } ?: JSONObject.NULL)
                    }
                    else -> unknown()
                }
                is Node.Run -> when (method) {
                    "closeView" -> { node.native.closeView(); releaseTree(handle); null }
                    "start" -> { node.native.start(); null }
                    "snapshot" -> node.native.snapshot()?.let { JSONObject().put("instanceId", it.instanceId).put("accountId", it.accountId).put("catalogId", it.catalogId).put("sessionEpoch", it.sessionEpoch.toString()).put("state", state(it.state)).put("dataJson", it.dataJson).put("pendingSteps", JSONArray(it.pendingSteps)).put("createdAt", it.createdAt).put("updatedAt", it.updatedAt) }
                    "cancel" -> node.native.cancel()?.let(::state)
                    "completeStep" -> node.native.completeStep(args.string("expectedUpdatedAt"), args.string("stepName"), args.string("outputJson"))
                    "forgetCancelledRequest" -> node.native.forgetCancelledRequest()
                    "archiveAfterAcknowledgement" -> node.native.archiveAfterAcknowledgement(args.string("acknowledgement"))?.let { JSONObject().put("instanceId", it.instanceId).put("accountId", it.accountId).put("catalogId", it.catalogId).put("acknowledgementDigest", it.acknowledgementDigest).put("archivedAt", it.archivedAt).put("outcome", it.outcome?.let(::state) ?: JSONObject.NULL) }
                    else -> unknown()
                }
                is Node.Cursor -> if (method == "closeView") { releaseTree(handle); null } else unknown()
            }
        }
        return when (result) { null -> "null"; is String -> JSONObject.quote(result); else -> result.toString() }
    }
    private fun unknown(): Nothing = throw MobileException.InvalidInput("unknown scoped operation")
    private fun state(value: InstanceStateKind): String = value.name.lowercase(java.util.Locale.ROOT)
    private fun summary(value: MobileCatalogRunSummary): JSONObject {
        val native = when (val v = value.nativeState) {
            MobileCatalogNativeState.NotMaterialized -> JSONObject().put("kind", "not_materialized")
            MobileCatalogNativeState.Unavailable -> JSONObject().put("kind", "unavailable")
            is MobileCatalogNativeState.Present -> JSONObject().put("kind", "present").put("state", state(v.state))
            is MobileCatalogNativeState.Recorded -> JSONObject().put("kind", "recorded").put("state", state(v.state))
        }
        return JSONObject().put("instanceId", value.instanceId).put("accountId", value.accountId).put("catalogId", value.catalogId).put("ownerEpoch", value.ownerEpoch.toString()).put("workflow", value.workflow).put("version", value.version).put("sequenceId", value.sequenceId).put("createdAt", value.createdAt).put("requestState", value.requestState.name.lowercase(java.util.Locale.ROOT)).put("nativeState", native)
    }
}
private fun JSONObject.string(name: String): String = opt(name) as? String ?: throw MobileException.InvalidInput("missing string: $name")
private fun JSONObject.positive(name: String, maximum: Long): Long {
    val number = opt(name) as? Number ?: throw MobileException.InvalidInput("invalid positive integer: $name")
    val value = number.toLong()
    if (value !in 1..maximum || number.toDouble() != value.toDouble()) throw MobileException.InvalidInput("invalid positive integer: $name")
    return value
}
private class MobileScopedHandler(module: MobileScopedCallbacks, private val scope: NativeEngineLifecycle.Scope, catalog: MobileCatalog, private val key: String) : MobileCatalogStepHandler {
    private val module = WeakReference(module)
    private val account = catalog.accountId()
    private val catalogId = catalog.catalogId()
    private val epoch = catalog.sessionEpoch()
    override fun execute(context: MobileCatalogCallbackContext, stepName: String, input: String): String {
        val owner = module.get()?.takeIf { scope.isCurrent() } ?: throw HandlerException.Retryable("engine retired")
        if (context.accountId != account || context.catalogId != catalogId || context.ownerEpoch != epoch) throw HandlerException.Permanent("callback owner mismatch")
        val id = UUID.randomUUID().toString()
        owner.scopedPending.open(id)
        val payload = mapOf<String, Any?>(
            "callbackKey" to key, "requestId" to id, "stepName" to stepName, "inputJson" to input,
            "context" to mapOf("accountId" to context.accountId, "catalogId" to context.catalogId,
                "ownerEpoch" to context.ownerEpoch.toString(), "runId" to context.runId,
                "blockId" to context.blockId, "attempt" to context.attempt.toLong(), "effectId" to context.effectId)
        )
        if (!owner.emitScoped(scope, "orch8:scopedExecuteStep", payload)) { owner.scopedPending.cancel(id); throw HandlerException.Retryable("host runtime unavailable") }
        val reply = owner.scopedPending.awaitSettlement(id) ?: throw HandlerException.Retryable("catalog callback unavailable")
        if (!scope.isCurrent()) throw HandlerException.Retryable("engine retired")
        reply.error?.let { if (reply.permanent) throw HandlerException.Permanent(it) else throw HandlerException.Retryable(it) }
        return reply.output ?: "{}"
    }
}
private class MobileScopedTokens(module: MobileScopedCallbacks, private val scope: NativeEngineLifecycle.Scope, private val key: String, token: String) : TokenProvider {
    private val module = WeakReference(module)
    @Volatile private var token = token
    override fun currentToken(): String = if (scope.isCurrent()) token else ""
    override fun refreshToken(): String {
        val owner = module.get()?.takeIf { scope.isCurrent() } ?: throw MobileException.Shutdown("engine retired")
        val id = UUID.randomUUID().toString()
        owner.scopedPending.open(id)
        val payload = mapOf<String, Any?>("providerKey" to key, "requestId" to id)
        if (!owner.emitScoped(scope, "orch8:scopedRefreshToken", payload)) { owner.scopedPending.cancel(id); throw MobileException.Engine("host runtime unavailable") }
        val reply = owner.scopedPending.awaitSettlement(id) ?: throw MobileException.Engine("token callback unavailable")
        if (!scope.isCurrent()) throw MobileException.Shutdown("engine retired")
        reply.error?.let { throw MobileException.Engine(it) }
        val fresh = reply.output?.takeIf { it.isNotEmpty() } ?: throw MobileException.Engine("empty token")
        token = fresh
        return fresh
    }
}
