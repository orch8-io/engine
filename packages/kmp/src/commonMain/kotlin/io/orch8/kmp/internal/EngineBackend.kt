package io.orch8.kmp.internal

import io.orch8.kmp.BackgroundRunResult
import io.orch8.kmp.ContinuityImportResult
import io.orch8.kmp.DeviceContext
import io.orch8.kmp.EngineConfig
import io.orch8.kmp.EngineEvent
import io.orch8.kmp.FlushResult
import io.orch8.kmp.InstanceSnapshot
import io.orch8.kmp.InstanceSummary
import io.orch8.kmp.Orch8TokenSource
import io.orch8.kmp.PowerState
import io.orch8.kmp.SequenceInfo
import io.orch8.kmp.SyncResult
import io.orch8.kmp.TickResult
import kotlinx.coroutines.CoroutineDispatcher

/**
 * Synchronous step handler as seen by a backend: called on an engine worker
 * thread, returns output JSON or throws `Orch8HandlerException`.
 */
internal fun interface BlockingStepHandler {
    fun execute(stepName: String, inputJson: String): String
}

/**
 * Platform port. Every method is a blocking FFI call; `Orch8Engine` moves them
 * off the caller's thread. Implementations must translate platform errors to
 * `Orch8Exception` so common code sees one error model.
 */
internal interface EngineBackend {
    fun registerHandler(name: String, handler: BlockingStepHandler)

    fun setListener(sink: (EngineEvent) -> Unit)

    fun resume()

    fun pause()

    fun shutdown()

    fun tickOnce(): TickResult

    fun runUntilIdle(maxTicks: Int, timeBudgetMs: Long): BackgroundRunResult

    fun reportPowerState(state: PowerState)

    fun onPushReceived()

    fun start(sequenceName: String, inputJson: String, dedupKey: String?): String

    fun cancelInstance(instanceId: String)

    fun getInstance(instanceId: String): InstanceSnapshot

    fun activeInstances(): List<InstanceSummary>

    fun completeStep(instanceId: String, stepName: String, outputJson: String)

    fun loadSequenceFromJson(json: String)

    fun loadSequencesFromUrl(url: String): Int

    fun loadedSequences(): List<SequenceInfo>

    fun sync(manifestUrl: String, tokens: Orch8TokenSource?): SyncResult

    fun flushTelemetry(endpointUrl: String): FlushResult

    fun setDeviceContext(ctx: DeviceContext)

    fun importContinuityCapsule(
        capsuleJson: String,
        payloadBase64: String,
        payloadKeyBase64: String,
        destinationRuntimeId: String,
        destinationInstanceId: String,
    ): ContinuityImportResult

    fun activateContinuityCapsule(
        capsuleId: String,
        destinationRuntimeId: String,
        destinationInstanceId: String,
    )
}

/**
 * Open the platform engine. Android/JVM: UniFFI Kotlin bindings.
 * iOS: the Swift bridge installed via `Orch8Ios.install`.
 */
internal expect fun openPlatformBackend(dbPath: String, config: EngineConfig): EngineBackend

/** Dispatcher for blocking FFI calls (Dispatchers.IO on every target). */
internal expect val platformIoDispatcher: CoroutineDispatcher

/** Run a suspend handler to completion on the current (engine worker) thread. */
internal expect fun <T> runHandlerBlocking(block: suspend () -> T): T
