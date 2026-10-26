package io.orch8.kmp.internal

import io.orch8.kmp.CatalogArchiveReceipt
import io.orch8.kmp.AccountIdentity
import io.orch8.kmp.CatalogCallbackContext
import io.orch8.kmp.CatalogConfiguration
import io.orch8.kmp.CatalogHistoryCursor
import io.orch8.kmp.CatalogNativeState
import io.orch8.kmp.CatalogRequestState
import io.orch8.kmp.CatalogRunPage
import io.orch8.kmp.CatalogRunRequest
import io.orch8.kmp.CatalogRunSnapshot
import io.orch8.kmp.CatalogRunSummary
import io.orch8.kmp.InstanceState
import io.orch8.kmp.Orch8ErrorKind
import io.orch8.kmp.Orch8Exception
import io.orch8.kmp.Orch8HandlerException
import io.orch8.kmp.Orch8TokenSource
import io.orch8.kmp.SyncResult
import io.orch8.mobile.MobileAccount as UAccount
import io.orch8.mobile.MobileAccountIdentity as UIdentity
import io.orch8.mobile.MobileCatalog as UCatalog
import io.orch8.mobile.MobileCatalogConfiguration as UConfiguration
import io.orch8.mobile.MobileCatalogRun as URun
import io.orch8.mobile.MobileCatalogRunRequest as URequest
import io.orch8.mobile.MobileCatalogHistoryCursor as UCursor
import io.orch8.mobile.MobileCatalogNativeState as UNativeState
import io.orch8.mobile.MobileCatalogRequestState as URequestState
import io.orch8.mobile.MobileCatalogStepHandler as UHandler
import io.orch8.mobile.MobileCatalogCallbackContext as UContext
import io.orch8.mobile.HandlerException as UHandlerException

private inline fun <T> scopedErrors(block: () -> T): T = try { mapErrors(block) }
    catch (error: IllegalStateException) { throw Orch8Exception(Orch8ErrorKind.SHUTDOWN, "scope view disposed", error) }

internal fun AccountIdentity.toUniffi() = UIdentity(accountKey, issuer, subject)
internal class UniffiAccountBackend(private val native: UAccount) : AccountBackend {
    override val accountId = native.accountId()
    override val sessionEpoch = native.sessionEpoch()
    override fun openCatalog(configuration: CatalogConfiguration): CatalogBackend = scopedErrors {
        UniffiCatalogBackend(native.openCatalog(UConfiguration(configuration.catalogKey, configuration.source,
            configuration.signingAuthority, configuration.manifestUrl, configuration.identityUrl)))
    }
    override fun retire() = scopedErrors { native.retire() }
    override fun closeView() = scopedErrors { native.closeView() }
    override fun dispose() = native.close()
}
internal class UniffiCatalogBackend(private val native: UCatalog) : CatalogBackend {
    override val accountId = native.accountId()
    override val catalogId = native.catalogId()
    override val sessionEpoch = native.sessionEpoch()
    override fun registerHandler(name: String, handler: BlockingCatalogStepHandler, timeoutMs: Long) = scopedErrors {
        require(timeoutMs > 0)
        val expectedAccount = accountId
        val expectedCatalog = catalogId
        val expectedEpoch = sessionEpoch
        native.registerHandler(name, object : UHandler {
            override fun execute(context: UContext, stepName: String, input: String): String = try {
                if (context.accountId != expectedAccount || context.catalogId != expectedCatalog || context.ownerEpoch != expectedEpoch)
                    throw Orch8HandlerException.Permanent("callback belongs to another catalog session")
                handler.execute(CatalogCallbackContext(context.accountId, context.catalogId, context.ownerEpoch,
                    context.runId, context.blockId, context.attempt.toLong(), context.effectId), stepName, input)
            } catch (error: Orch8HandlerException.Permanent) {
                throw UHandlerException.Permanent(error.message ?: "permanent catalog handler failure")
            } catch (error: Exception) {
                throw UHandlerException.Retryable(error.message ?: "catalog handler failed")
            }
        }, timeoutMs.toULong())
    }
    override fun sync(tokens: Orch8TokenSource): SyncResult = scopedErrors {
        native.sync(tokens.toUniffi()).let { SyncResult(it.added.toInt(), it.updated.toInt(),
            it.removed.toInt(), it.skipped.toInt(), it.signatureFailures.toInt()) }
    }
    override fun prepareRun(request: CatalogRunRequest): CatalogRunBackend = scopedErrors {
        UniffiCatalogRunBackend(native.prepareRun(URequest(request.workflow, request.version, request.inputJson, request.operationKey)))
    }
    override fun run(instanceId: String): CatalogRunBackend = scopedErrors { UniffiCatalogRunBackend(native.run(instanceId)) }
    override fun history(cursor: CatalogCursorBackend?, limit: Int): CatalogRunPage = scopedErrors {
        require(limit in 1..128)
        val position = when (cursor) {
            null -> null
            is UniffiCursorBackend -> cursor.native
            else -> throw Orch8Exception(Orch8ErrorKind.INVALID_INPUT, "cursor belongs to another platform backend")
        }
        native.history(position, limit.toUInt()).let { page ->
            CatalogRunPage(page.items.map { item ->
                CatalogRunSummary(item.instanceId, item.accountId, item.catalogId, item.ownerEpoch,
                    item.workflow, item.version, item.sequenceId, item.createdAt,
                    when (item.requestState) { URequestState.PREPARED -> CatalogRequestState.PREPARED; URequestState.CANCELLED -> CatalogRequestState.CANCELLED; URequestState.ARCHIVED -> CatalogRequestState.ARCHIVED },
                    when (val state = item.nativeState) {
                        UNativeState.NotMaterialized -> CatalogNativeState.NotMaterialized
                        UNativeState.Unavailable -> CatalogNativeState.Unavailable
                        is UNativeState.Present -> CatalogNativeState.Present(InstanceState.fromWire(state.state.name))
                        is UNativeState.Recorded -> CatalogNativeState.Recorded(InstanceState.fromWire(state.state.name))
                    })
            }, page.next?.let { CatalogHistoryCursor(UniffiCursorBackend(it)) })
        }
    }
    override fun closeView() = scopedErrors { native.closeView() }
    override fun dispose() = native.close()
}
internal class UniffiCursorBackend(internal val native: UCursor) : CatalogCursorBackend {
    override fun dispose() = native.close()
}
internal class UniffiCatalogRunBackend(private val native: URun) : CatalogRunBackend {
    override val instanceId = native.instanceId()
    override val accountId = native.accountId()
    override val catalogId = native.catalogId()
    override val sessionEpoch = native.sessionEpoch()
    override fun start() = scopedErrors { native.start() }
    override fun snapshot(): CatalogRunSnapshot? = scopedErrors {
        native.snapshot()?.let { CatalogRunSnapshot(it.instanceId, it.accountId, it.catalogId,
            it.sessionEpoch, InstanceState.fromWire(it.state.name), it.dataJson, it.pendingSteps,
            it.createdAt, it.updatedAt) }
    }
    override fun cancel(): InstanceState? = scopedErrors { native.cancel()?.let { InstanceState.fromWire(it.name) } }
    override fun completeStep(expectedUpdatedAt: String, stepName: String, outputJson: String): Boolean =
        scopedErrors { native.completeStep(expectedUpdatedAt, stepName, outputJson) }
    override fun archiveAfterAcknowledgement(acknowledgement: String): CatalogArchiveReceipt? = scopedErrors {
        native.archiveAfterAcknowledgement(acknowledgement)?.let { CatalogArchiveReceipt(it.instanceId,it.accountId,it.catalogId,
            it.acknowledgementDigest,it.archivedAt,it.outcome?.let { state -> InstanceState.fromWire(state.name) }) }
    }
    override fun forgetCancelledRequest(): Boolean = scopedErrors { native.forgetCancelledRequest() }
    override fun closeView() = scopedErrors { native.closeView() }
    override fun dispose() = native.close()
}
