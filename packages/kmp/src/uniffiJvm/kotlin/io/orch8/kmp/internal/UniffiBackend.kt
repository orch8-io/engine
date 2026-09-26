// Compiled into androidMain (against the io.orch8:orch8-mobile AAR) and
// jvmMain (against orch8-mobile/bindings/kotlin). Both expose the same
// UniFFI-generated package, io.orch8.mobile.
package io.orch8.kmp.internal

import io.orch8.kmp.BackgroundRunResult
import io.orch8.kmp.ContinuityImportResult
import io.orch8.kmp.DeviceContext
import io.orch8.kmp.EngineConfig
import io.orch8.kmp.EngineEvent
import io.orch8.kmp.FlushResult
import io.orch8.kmp.InstanceSnapshot
import io.orch8.kmp.InstanceState
import io.orch8.kmp.InstanceSummary
import io.orch8.kmp.Orch8ErrorKind
import io.orch8.kmp.Orch8Exception
import io.orch8.kmp.Orch8HandlerException
import io.orch8.kmp.Orch8TokenSource
import io.orch8.kmp.PowerState
import io.orch8.kmp.SequenceInfo
import io.orch8.kmp.SyncResult
import io.orch8.kmp.TickResult
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import io.orch8.mobile.DeviceContext as UDeviceContext
import io.orch8.mobile.EngineListener as UEngineListener
import io.orch8.mobile.HandlerException as UHandlerException
import io.orch8.mobile.InstanceStateKind as UInstanceStateKind
import io.orch8.mobile.MobileEngine as UMobileEngine
import io.orch8.mobile.MobileEngineConfig as UMobileEngineConfig
import io.orch8.mobile.MobileException as UMobileException
import io.orch8.mobile.PowerState as UPowerState
import io.orch8.mobile.StepHandler as UStepHandler
import io.orch8.mobile.SyncException as USyncException
import io.orch8.mobile.TokenProvider as UTokenProvider

internal actual fun openPlatformBackend(dbPath: String, config: EngineConfig): EngineBackend =
    UniffiBackend(mapErrors { UMobileEngine(dbPath, config.toUniffi()) })

internal actual val platformIoDispatcher: CoroutineDispatcher = Dispatchers.IO

internal actual fun <T> runHandlerBlocking(block: suspend () -> T): T = runBlocking { block() }

/** [EngineBackend] over the UniFFI Kotlin bindings (JNA). */
internal class UniffiBackend(private val engine: UMobileEngine) : EngineBackend {
    override fun registerHandler(name: String, handler: BlockingStepHandler) = mapErrors {
        engine.registerHandler(
            name,
            object : UStepHandler {
                override fun execute(stepName: String, input: String): String =
                    try {
                        handler.execute(stepName, input)
                    } catch (e: Orch8HandlerException.Permanent) {
                        throw UHandlerException.Permanent(e.message ?: "permanent handler failure")
                    } catch (e: Exception) {
                        throw UHandlerException.Retryable(e.message ?: "retryable handler failure")
                    }
            },
        )
    }

    override fun setListener(sink: (EngineEvent) -> Unit) {
        engine.setListener(
            object : UEngineListener {
                override fun onInstanceCompleted(instanceId: String, output: String) =
                    sink(EngineEvent.InstanceCompleted(instanceId, output))

                override fun onInstanceFailed(instanceId: String, error: String) =
                    sink(EngineEvent.InstanceFailed(instanceId, error))

                override fun onStepPending(instanceId: String, stepName: String, handler: String) =
                    sink(EngineEvent.StepPending(instanceId, stepName, handler))
            },
        )
    }

    override fun resume() = engine.resume()

    override fun pause() = engine.pause()

    override fun shutdown() = engine.shutdown()

    override fun tickOnce(): TickResult = mapErrors {
        engine.tickOnce().let { TickResult(it.instancesAdvanced.toInt(), it.stepsExecuted.toInt(), it.hasPendingWork) }
    }

    override fun runUntilIdle(maxTicks: Int, timeBudgetMs: Long): BackgroundRunResult = mapErrors {
        engine.runUntilIdle(maxTicks.toUInt(), timeBudgetMs.toULong()).let {
            BackgroundRunResult(
                ticksExecuted = it.ticksExecuted.toInt(),
                instancesAdvanced = it.instancesAdvanced.toInt(),
                stepsExecuted = it.stepsExecuted.toInt(),
                hasPendingWork = it.hasPendingWork,
                budgetExhausted = it.budgetExhausted,
            )
        }
    }

    override fun reportPowerState(state: PowerState) = engine.reportPowerState(
        when (state) {
            PowerState.CHARGING -> UPowerState.CHARGING
            PowerState.UNPLUGGED -> UPowerState.UNPLUGGED
            PowerState.LOW_BATTERY -> UPowerState.LOW_BATTERY
            PowerState.CRITICAL_BATTERY -> UPowerState.CRITICAL_BATTERY
        },
    )

    override fun onPushReceived() = engine.onPushReceived()

    override fun start(sequenceName: String, inputJson: String, dedupKey: String?): String =
        mapErrors { engine.start(sequenceName, inputJson, dedupKey) }

    override fun cancelInstance(instanceId: String) = mapErrors { engine.cancelInstance(instanceId) }

    override fun getInstance(instanceId: String): InstanceSnapshot = mapErrors {
        engine.getInstance(instanceId).let {
            InstanceSnapshot(it.instanceId, it.sequenceName, it.state.toCommon(), it.context, it.createdAt, it.updatedAt)
        }
    }

    override fun activeInstances(): List<InstanceSummary> = mapErrors {
        engine.activeInstances().map { InstanceSummary(it.instanceId, it.sequenceName, it.state.toCommon(), it.createdAt) }
    }

    override fun completeStep(instanceId: String, stepName: String, outputJson: String) =
        mapErrors { engine.completeStep(instanceId, stepName, outputJson) }

    override fun loadSequenceFromJson(json: String) = mapErrors { engine.loadSequenceFromJson(json) }

    override fun loadSequencesFromUrl(url: String): Int = mapErrors { engine.loadSequencesFromUrl(url).toInt() }

    override fun loadedSequences(): List<SequenceInfo> = mapErrors {
        engine.loadedSequences().map { SequenceInfo(it.name, it.version) }
    }

    override fun sync(manifestUrl: String, tokens: Orch8TokenSource?): SyncResult = mapErrors {
        val provider = tokens?.let { source ->
            object : UTokenProvider {
                override fun currentToken(): String = source.currentToken()

                override fun refreshToken(): String =
                    try {
                        source.refreshToken()
                    } catch (e: Exception) {
                        throw UMobileException.Engine(e.message ?: "token refresh failed")
                    }
            }
        }
        engine.sync(manifestUrl, provider).let {
            SyncResult(
                added = it.added.toInt(),
                updated = it.updated.toInt(),
                removed = it.removed.toInt(),
                skipped = it.skipped.toInt(),
                signatureFailures = it.signatureFailures.toInt(),
            )
        }
    }

    override fun flushTelemetry(endpointUrl: String): FlushResult = mapErrors {
        engine.flushTelemetry(endpointUrl).let { FlushResult(it.sent.toLong(), it.dropped.toLong()) }
    }

    override fun setDeviceContext(ctx: DeviceContext) = engine.setDeviceContext(
        UDeviceContext(ctx.deviceId, ctx.osName, ctx.osVersion, ctx.appVersion, ctx.sdkVersion),
    )

    override fun importContinuityCapsule(
        capsuleJson: String,
        payloadBase64: String,
        payloadKeyBase64: String,
        destinationRuntimeId: String,
        destinationInstanceId: String,
    ): ContinuityImportResult = mapErrors {
        engine.importContinuityCapsule(
            capsuleJson,
            payloadBase64,
            payloadKeyBase64,
            destinationRuntimeId,
            destinationInstanceId,
        ).let { ContinuityImportResult(it.capsuleId, it.continuityId, it.instanceId, it.sourceEpoch.toLong(), it.state) }
    }

    override fun activateContinuityCapsule(capsuleId: String, destinationRuntimeId: String, destinationInstanceId: String) =
        mapErrors { engine.activateContinuityCapsule(capsuleId, destinationRuntimeId, destinationInstanceId) }
}

private fun UInstanceStateKind.toCommon(): InstanceState = when (this) {
    UInstanceStateKind.SCHEDULED -> InstanceState.SCHEDULED
    UInstanceStateKind.RUNNING -> InstanceState.RUNNING
    UInstanceStateKind.WAITING -> InstanceState.WAITING
    UInstanceStateKind.PAUSED -> InstanceState.PAUSED
    UInstanceStateKind.COMPLETED -> InstanceState.COMPLETED
    UInstanceStateKind.FAILED -> InstanceState.FAILED
    UInstanceStateKind.CANCELLED -> InstanceState.CANCELLED
}

internal fun EngineConfig.toUniffi(): UMobileEngineConfig = UMobileEngineConfig(
    tickIntervalMs = tickIntervalMs.toULong(),
    maxConcurrentSteps = maxConcurrentSteps.toUInt(),
    maxStepsPerInstance = maxStepsPerInstance.toUInt(),
    maxConcurrentInstances = maxConcurrentInstances.toUInt(),
    maxTickDurationMs = maxTickDurationMs.toULong(),
    maxInstanceLifetimeSecs = maxInstanceLifetimeSecs.toULong(),
    maxStoredSequences = maxStoredSequences.toUInt(),
    maxSequenceSizeBytes = maxSequenceSizeBytes.toULong(),
    handlerTimeoutMs = handlerTimeoutMs.toULong(),
    operationTimeoutMs = operationTimeoutMs.toULong(),
    telemetryEnabled = telemetryEnabled,
    telemetryUrl = telemetryUrl,
    environment = environment,
    rootPublicKey = rootPublicKey,
    sdkVersion = sdkVersion,
    memoryBudgetBytes = memoryBudgetBytes.toULong(),
    sequencesUrl = sequencesUrl,
    syncUrl = syncUrl,
    deviceId = deviceId,
    syncApiKey = syncApiKey,
)

/** Translate UniFFI exceptions to the common error model. */
internal inline fun <T> mapErrors(block: () -> T): T =
    try {
        block()
    } catch (e: UMobileException) {
        throw Orch8Exception(e.toKind(), e.message ?: e::class.simpleName.orEmpty(), e)
    } catch (e: USyncException) {
        throw Orch8Exception(e.toKind(), e.message ?: e::class.simpleName.orEmpty(), e)
    }

internal fun UMobileException.toKind(): Orch8ErrorKind = when (this) {
    is UMobileException.Engine -> Orch8ErrorKind.ENGINE
    is UMobileException.Storage -> Orch8ErrorKind.STORAGE
    is UMobileException.InvalidInput -> Orch8ErrorKind.INVALID_INPUT
    is UMobileException.NotFound -> Orch8ErrorKind.NOT_FOUND
    is UMobileException.ResourceLimit -> Orch8ErrorKind.RESOURCE_LIMIT
    is UMobileException.AlreadyExists -> Orch8ErrorKind.ALREADY_EXISTS
    is UMobileException.Shutdown -> Orch8ErrorKind.SHUTDOWN
}

internal fun USyncException.toKind(): Orch8ErrorKind = when (this) {
    is USyncException.Network -> Orch8ErrorKind.NETWORK
    is USyncException.SignatureInvalid -> Orch8ErrorKind.SIGNATURE_INVALID
    is USyncException.InvalidManifest -> Orch8ErrorKind.INVALID_MANIFEST
}
