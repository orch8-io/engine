package io.orch8.kmp.internal

import io.orch8.kmp.CatalogCallbackContext
import io.orch8.kmp.CatalogConfiguration
import io.orch8.kmp.CatalogRunPage
import io.orch8.kmp.CatalogRunRequest
import io.orch8.kmp.CatalogRunSnapshot
import io.orch8.kmp.InstanceState
import io.orch8.kmp.Orch8TokenSource
import io.orch8.kmp.SyncResult

internal fun interface BlockingCatalogStepHandler {
    fun execute(context: CatalogCallbackContext, stepName: String, inputJson: String): String
}
internal interface ScopedViewBackend {
    fun closeView()
    fun dispose()
}
internal interface AccountBackend : ScopedViewBackend {
    val accountId: String
    val sessionEpoch: Long
    fun openCatalog(configuration: CatalogConfiguration): CatalogBackend
    fun retire()
}
internal interface CatalogBackend : ScopedViewBackend {
    val accountId: String
    val catalogId: String
    val sessionEpoch: Long
    fun registerHandler(name: String, handler: BlockingCatalogStepHandler, timeoutMs: Long)
    fun sync(tokens: Orch8TokenSource): SyncResult
    fun prepareRun(request: CatalogRunRequest): CatalogRunBackend
    fun run(instanceId: String): CatalogRunBackend
    fun history(cursor: CatalogCursorBackend?, limit: Int): CatalogRunPage
}
internal interface CatalogRunBackend : ScopedViewBackend {
    val instanceId: String
    val accountId: String
    val catalogId: String
    val sessionEpoch: Long
    fun start()
    fun snapshot(): CatalogRunSnapshot?
    fun cancel(): InstanceState?
    fun completeStep(expectedUpdatedAt: String, stepName: String, outputJson: String): Boolean
    fun forgetCancelledRequest(): Boolean
}
internal interface CatalogCursorBackend { fun dispose() }
