package io.orch8.kmp

import io.orch8.kmp.internal.AccountBackend
import io.orch8.kmp.internal.CatalogBackend
import io.orch8.kmp.internal.CatalogCursorBackend
import io.orch8.kmp.internal.CatalogRunBackend
import io.orch8.kmp.internal.BlockingCatalogStepHandler
import io.orch8.kmp.internal.runHandlerBlocking
import kotlin.native.HiddenFromObjC
import kotlin.experimental.ExperimentalObjCRefinement
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** Trusted signed-in identity. A stable account key cannot change its principal. */
data class AccountIdentity(val accountKey: String, val issuer: String, val subject: String)
data class CatalogConfiguration(
    val catalogKey: String, val source: String, val signingAuthority: String,
    val manifestUrl: String, val identityUrl: String,
)
data class CatalogRunRequest(
    val workflow: String, val inputJson: String = "{}", val operationKey: String,
    val version: Int? = null,
)
/** Native-issued delivery identity; never accepted as bearer authorization. */
data class CatalogCallbackContext(
    val accountId: String, val catalogId: String, val ownerEpoch: Long,
    val runId: String, val blockId: String, val attempt: Long, val effectId: String,
)
fun interface Orch8CatalogStepHandler {
    @Throws(Orch8HandlerException::class, CancellationException::class)
    suspend fun execute(context: CatalogCallbackContext, stepName: String, inputJson: String): String
}
data class CatalogRunSnapshot(
    val instanceId: String, val accountId: String, val catalogId: String, val sessionEpoch: Long,
    val state: InstanceState, val dataJson: String, val pendingSteps: List<String>,
    val createdAt: String, val updatedAt: String,
) {
    val data: JsonElement get() = lenientJson.parseToJsonElement(dataJson)
}
enum class CatalogRequestState { PREPARED, CANCELLED }
sealed interface CatalogNativeState {
    data object NotMaterialized : CatalogNativeState
    data class Present(val state: InstanceState) : CatalogNativeState
    data object Unavailable : CatalogNativeState
}
data class CatalogRunSummary(
    val instanceId: String, val accountId: String, val catalogId: String, val ownerEpoch: Long,
    val workflow: String, val version: Int, val sequenceId: String, val createdAt: String,
    val requestState: CatalogRequestState, val nativeState: CatalogNativeState,
)
/** Opaque position, not authority. Disposing a cursor does not alter history. */
class CatalogHistoryCursor internal constructor(internal val backend: CatalogCursorBackend) : AutoCloseable {
    override fun close() = backend.dispose()
}
data class CatalogRunPage(val items: List<CatalogRunSummary>, val next: CatalogHistoryCursor?)

/**
 * Local account view. [closeView] fences issued child views; [close] disposes only
 * this object. Durable retirement is explicit. Issued children outlive parent disposal.
 */
class Orch8Account internal constructor(private val backend: AccountBackend, private val io: CoroutineDispatcher) : AutoCloseable {
    val accountId: String = backend.accountId
    val sessionEpoch: Long = backend.sessionEpoch
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun openCatalog(configuration: CatalogConfiguration): Orch8Catalog = withContext(io) {
        Orch8Catalog(backend.openCatalog(configuration), io)
    }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun retire() = withContext(io) { backend.retire() }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    fun closeView() = backend.closeView()
    override fun close() = backend.dispose()
}

class Orch8Catalog internal constructor(private val backend: CatalogBackend, private val io: CoroutineDispatcher) : AutoCloseable {
    val accountId: String = backend.accountId
    val catalogId: String = backend.catalogId
    val sessionEpoch: Long = backend.sessionEpoch
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun sync(tokens: Orch8TokenSource): SyncResult = withContext(io) { backend.sync(tokens) }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun registerHandler(name: String, handler: Orch8CatalogStepHandler, timeoutMs: Long = 30_000) {
        require(name.isNotBlank() && timeoutMs > 0)
        withContext(io) {
            backend.registerHandler(name, BlockingCatalogStepHandler { context, step, input ->
                try { runHandlerBlocking { handler.execute(context, step, input) } }
                catch (error: Orch8HandlerException) { throw error }
                catch (error: Exception) { throw Orch8HandlerException.Retryable(error.message ?: "catalog handler failed", error) }
            }, timeoutMs)
        }
    }
    /** Persist before dispatch; the operation key safely recovers an identical retry. */
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun prepareRun(request: CatalogRunRequest): Orch8CatalogRun {
        require(request.workflow.isNotBlank() && request.operationKey.isNotBlank())
        return withContext(io) { Orch8CatalogRun(backend.prepareRun(request), io) }
    }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun run(instanceId: String): Orch8CatalogRun = withContext(io) {
        Orch8CatalogRun(backend.run(instanceId), io)
    }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun history(cursor: CatalogHistoryCursor? = null, limit: Int = 64): CatalogRunPage {
        require(limit in 1..128)
        return withContext(io) { backend.history(cursor?.backend, limit) }
    }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    fun closeView() = backend.closeView()
    override fun close() = backend.dispose()
}

class Orch8CatalogRun internal constructor(private val backend: CatalogRunBackend, private val io: CoroutineDispatcher) : AutoCloseable {
    val instanceId: String = backend.instanceId
    val accountId: String = backend.accountId
    val catalogId: String = backend.catalogId
    val sessionEpoch: Long = backend.sessionEpoch
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun start() = withContext(io) { backend.start() }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun snapshot(): CatalogRunSnapshot? = withContext(io) { backend.snapshot() }
    /**
     * Cold, backpressured snapshots of this owned run. A null value means no
     * native snapshot, not a successful or cancelled execution. Completes after
     * emitting a known terminal snapshot; cancel collection when the UI leaves.
     * Every poll rechecks native ownership. Closing/retiring the view propagates
     * its typed error. The flow retains this view, never the root engine.
     */
    @OptIn(ExperimentalObjCRefinement::class)
    @HiddenFromObjC
    fun observeSnapshots(pollInterval: Duration = 500.milliseconds): Flow<CatalogRunSnapshot?> = flow {
        require(pollInterval.isFinite() && pollInterval.isPositive()) { "pollInterval must be finite and greater than zero" }
        while (true) {
            val current = snapshot()
            emit(current)
            if (current?.state?.isTerminal == true) return@flow
            delay(pollInterval)
        }
    }.distinctUntilChanged()

    /** Swift-safe callbacks on the IO dispatcher; close the subscription when its UI leaves. */
    @Throws(Orch8Exception::class, IllegalArgumentException::class)
    fun subscribeSnapshots(observer: CatalogSnapshotObserver, pollIntervalMs: Long = 500): CatalogSnapshotSubscription {
        val interval = pollingInterval(pollIntervalMs)
        return subscribeCatalogSnapshots(this, observer, interval, io)
    }

    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun cancel(): InstanceState? = withContext(io) { backend.cancel() }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun completeStep(expectedUpdatedAt: String, stepName: String, outputJson: String): Boolean =
        withContext(io) { backend.completeStep(expectedUpdatedAt, stepName, outputJson) }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun answer(snapshot: CatalogRunSnapshot, stepName: String, choice: String, data: JsonObject = JsonObject(emptyMap())): Boolean {
        require(snapshot.instanceId == instanceId && snapshot.accountId == accountId &&
            snapshot.catalogId == catalogId && snapshot.sessionEpoch == sessionEpoch) { "snapshot belongs to another run or session" }
        require(choice.isNotBlank() && "value" !in data)
        return completeStep(snapshot.updatedAt, stepName, JsonObject(data + ("value" to JsonPrimitive(choice))).toString())
    }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    suspend fun forgetCancelledRequest(): Boolean = withContext(io) { backend.forgetCancelledRequest() }
    @Throws(Orch8Exception::class, IllegalArgumentException::class, CancellationException::class)
    fun closeView() = backend.closeView()
    override fun close() = backend.dispose()
}
