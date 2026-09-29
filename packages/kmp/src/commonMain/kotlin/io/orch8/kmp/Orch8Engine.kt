package io.orch8.kmp

import io.orch8.kmp.internal.BlockingStepHandler
import io.orch8.kmp.internal.EngineBackend
import io.orch8.kmp.internal.openPlatformBackend
import io.orch8.kmp.internal.platformIoDispatcher
import io.orch8.kmp.internal.runHandlerBlocking
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The embedded Orch8 workflow engine with a coroutine/Flow API.
 *
 * One instance owns one SQLite database. Create it once per process (for
 * example in `Application.onCreate` or the iOS app delegate), register
 * handlers, then call [resume].
 *
 * ```kotlin
 * val engine = Orch8Engine.open(dbPath, EngineConfig(syncUrl = "...", deviceId = id))
 * engine.registerHandler("capture_photo") { _, input -> """{"ref":"file://..."}""" }
 * engine.loadSequence(sequenceJson)
 * engine.resume()
 * val id = engine.start("field-inspection", buildJsonObject { put("site", "A-12") })
 * engine.observeInstance(id).collect { println(it.state) }
 * ```
 */
class Orch8Engine internal constructor(
    private val backend: EngineBackend,
    private val ioDispatcher: CoroutineDispatcher = platformIoDispatcher,
) : AutoCloseable {
    private val _events = MutableSharedFlow<EngineEvent>(
        extraBufferCapacity = EVENT_BUFFER,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    private val _pendingSteps = MutableStateFlow<List<PendingStep>>(emptyList())

    /** Hot stream of lifecycle events. Late collectors do not receive past events. */
    val events: SharedFlow<EngineEvent> = _events.asSharedFlow()

    /** Steps currently waiting for the host, derived from [events]. */
    val pendingSteps: StateFlow<List<PendingStep>> = _pendingSteps.asStateFlow()

    init {
        backend.setListener { event ->
            _pendingSteps.update { PendingSteps.reduce(it, event) }
            _events.tryEmit(event)
        }
    }

    /** Register a native handler. Must be called before [resume]. */
    fun registerHandler(name: String, handler: Orch8StepHandler) {
        require(name.isNotBlank()) { "handler name must not be blank" }
        backend.registerHandler(name, handler.toBlocking())
    }

    /** Start the engine's own foreground tick loop. */
    fun resume() = backend.resume()

    /** Pause the foreground tick loop (waits for the current tick). */
    fun pause() = backend.pause()

    /** Execute one tick manually. */
    suspend fun tick(): TickResult = offload { backend.tickOnce() }

    /**
     * Drain work inside an OS-granted background window
     * (`BGTaskScheduler` / `WorkManager`). Never starts a persistent loop.
     */
    suspend fun runUntilIdle(maxTicks: Int = 25, timeBudget: Duration = 20.seconds): BackgroundRunResult {
        require(maxTicks > 0) { "maxTicks must be a positive integer" }
        require(timeBudget.isPositive()) { "timeBudget must be greater than zero" }
        return offload { backend.runUntilIdle(maxTicks, timeBudget.inWholeMilliseconds.coerceAtLeast(1)) }
    }

    fun reportPowerState(state: PowerState) = backend.reportPowerState(state)

    /** Call when a silent push arrives: forces a server sync on the next tick. */
    fun onPushReceived() = backend.onPushReceived()

    /** Start a workflow instance; returns its id. [dedupKey] prevents duplicates. */
    suspend fun start(sequenceName: String, input: JsonObject = JsonObject(emptyMap()), dedupKey: String? = null): String =
        startJson(sequenceName, input.toString(), dedupKey)

    suspend fun startJson(sequenceName: String, inputJson: String, dedupKey: String? = null): String {
        require(sequenceName.isNotBlank()) { "sequenceName must not be blank" }
        return offload { backend.start(sequenceName, inputJson, dedupKey) }
    }

    suspend fun cancelInstance(instanceId: String) {
        offload { backend.cancelInstance(instanceId) }
        _pendingSteps.update { PendingSteps.withoutInstance(it, instanceId) }
    }

    suspend fun getInstance(instanceId: String): InstanceSnapshot = offload { backend.getInstance(instanceId) }

    suspend fun activeInstances(): List<InstanceSummary> = offload { backend.activeInstances() }

    /**
     * Answer a `wait_for_input` gate. [choice] must be one of the step's
     * `choices` values (`"yes"`/`"no"` when none are declared); the engine
     * stores it under `store_as` and merges the top-level keys of [data]
     * into `context.data`. A payload without a valid choice is rejected by
     * the engine and the step keeps waiting.
     */
    suspend fun answer(
        instanceId: String,
        stepName: String,
        choice: String,
        data: JsonObject = JsonObject(emptyMap()),
    ) {
        require(choice.isNotBlank()) { "choice must not be blank" }
        require("value" !in data) { "data must not contain 'value'; pass it as choice" }
        completeStep(instanceId, stepName, JsonObject(data + ("value" to JsonPrimitive(choice))))
    }

    /**
     * Resume a step parked on `wait_for_input` with a raw payload. Prefer
     * [answer]; the payload must carry `"value"` set to a declared choice.
     */
    suspend fun completeStep(instanceId: String, stepName: String, output: JsonObject = JsonObject(emptyMap())) =
        completeStepJson(instanceId, stepName, output.toString())

    suspend fun completeStepJson(instanceId: String, stepName: String, outputJson: String) {
        offload { backend.completeStep(instanceId, stepName, outputJson) }
        _pendingSteps.update { PendingSteps.withoutStep(it, instanceId, stepName) }
    }

    /** Load a sequence definition directly (bypasses signed sync). */
    suspend fun loadSequence(json: String) = offload { backend.loadSequenceFromJson(json) }

    /** Load a JSON array of sequences; empty [url] uses `EngineConfig.sequencesUrl`. */
    suspend fun loadSequencesFromUrl(url: String = ""): Int = offload { backend.loadSequencesFromUrl(url) }

    suspend fun loadedSequences(): List<SequenceInfo> = offload { backend.loadedSequences() }

    /** Sync the Ed25519-signed sequence manifest. */
    suspend fun sync(manifestUrl: String, tokens: Orch8TokenSource? = null): SyncResult =
        offload { backend.sync(manifestUrl, tokens) }

    suspend fun flushTelemetry(endpointUrl: String): FlushResult = offload { backend.flushTelemetry(endpointUrl) }

    fun setDeviceContext(ctx: DeviceContext) = backend.setDeviceContext(ctx)

    suspend fun importContinuityCapsule(
        capsuleJson: String,
        payloadBase64: String,
        payloadKeyBase64: String,
        destinationRuntimeId: String,
        destinationInstanceId: String,
    ): ContinuityImportResult = offload {
        backend.importContinuityCapsule(
            capsuleJson,
            payloadBase64,
            payloadKeyBase64,
            destinationRuntimeId,
            destinationInstanceId,
        )
    }

    suspend fun activateContinuityCapsule(capsuleId: String, destinationRuntimeId: String, destinationInstanceId: String) =
        offload { backend.activateContinuityCapsule(capsuleId, destinationRuntimeId, destinationInstanceId) }

    // -- Runtime node / worker ---------------------------------------------
    //
    // Needs the engine release after 0.7.1. Handlers registered with
    // [registerHandler] serve remote tasks; their input carries a reserved
    // `__orch8` member, see [Orch8TaskContext.fromInput] (`effectId` is the
    // idempotency key for downstream side effects).

    /** Stable runtime UUID of this installation (the lease `worker_id`). */
    suspend fun nodeRuntimeId(): String = offload { backend.nodeRuntimeId() }

    /**
     * Authenticate every control-plane call (node registration, worker
     * leases, delegation, sync reporting) with short-lived **device
     * sessions** instead of the static, legacy `EngineConfig.syncApiKey`.
     *
     * [fetchToken] should ask the app's backend for a fresh `dst_…` token;
     * the backend holds the operator key and mints it with
     * `POST /runtimes/device-sessions` for this device id and [nodeRuntimeId].
     * It is awaited once here for the initial token and again whenever the
     * control plane answers `401` (the request is then retried once); each
     * refresh is bounded by [refreshTimeout]. Call it before [registerNode].
     * Never ship an operator key in an app.
     *
     * Needs the engine release after 0.7.1.
     */
    suspend fun setTokenProvider(
        refreshTimeout: Duration = 30.seconds,
        fetchToken: suspend () -> String,
    ) {
        require(refreshTimeout.isPositive()) { "refreshTimeout must be greater than zero" }
        val initial = fetchToken()
        require(initial.isNotBlank()) { "fetchToken returned an empty token" }
        val tokens = DeviceSessionTokens(initial, fetchToken, refreshTimeout)
        offload { backend.setTokenProvider(tokens) }
    }

    /**
     * Join the runtime mesh: registers the device and its capabilities with
     * `EngineConfig.syncUrl` and the node credential ([setTokenProvider], or
     * the legacy `syncApiKey`), then re-advertises before the five-minute
     * capability TTL. Safe to call on every launch.
     */
    suspend fun registerNode(capabilities: NodeCapabilities = NodeCapabilities()): NodeRegistration =
        offload { backend.registerNode(capabilities) }

    /** Push fresh liveness facts (battery 0..100, connectivity). */
    suspend fun updateNodeStatus(connectivity: NodeConnectivity? = null, batteryPercent: Int? = null) {
        require(batteryPercent == null || batteryPercent in 0..100) { "batteryPercent must be in 0..100" }
        offload { backend.updateNodeStatus(connectivity, batteryPercent) }
    }

    /** Stop the worker, advertise `draining`, stop re-advertising. */
    suspend fun unregisterNode() = offload { backend.unregisterNode() }

    /**
     * Start the Rust worker loop: polls as kind `mobile`, runs registered
     * handlers, heartbeats per lease, completes / fails / releases tasks.
     * Register handlers first.
     */
    suspend fun startWorker(options: WorkerOptions = WorkerOptions()) = offload { backend.startWorker(options) }

    suspend fun stopWorker() = offload { backend.stopWorker() }

    /**
     * Claim and run remote tasks inside an OS-granted background window
     * (BGTask, WorkManager, push-wake handler). Claims even while paused.
     */
    suspend fun runWorkerWindow(timeBudget: Duration = 25.seconds): WorkerWindowResult {
        require(timeBudget.isPositive()) { "timeBudget must be greater than zero" }
        return offload { backend.runWorkerWindow(timeBudget.inWholeMilliseconds.coerceAtLeast(1)) }
    }

    suspend fun workerStats(): WorkerStats = offload { backend.workerStats() }

    /**
     * Forward an id-only wake push (`{task_id?, runtime_id?, reason?}`); the
     * worker polls immediately. Returns false when the push is addressed to
     * another runtime.
     */
    fun onPushWake(envelopeJson: String): Boolean = backend.onPushWake(envelopeJson)

    fun onPushWake(taskId: String? = null, runtimeId: String? = null, reason: String? = null): Boolean =
        onPushWake(
            buildJsonObject {
                taskId?.let { put("task_id", it) }
                runtimeId?.let { put("runtime_id", it) }
                reason?.let { put("reason", it) }
            }.toString(),
        )

    /** Enable an opt-in builtin handler (`http_request`) before [resume]. */
    fun enableBuiltin(name: String) = backend.enableBuiltin(name)

    // -- Delegation from phone-local workflows ------------------------------
    //
    // Needs the engine release after 0.7.1. A step of a workflow running on
    // this engine whose `$runtime` places it on another runtime is handed to
    // that runtime through the server mailbox; the local instance parks and
    // resumes exactly once with the result.

    /**
     * Start the delegation pump. Requires [registerNode] and a node credential
     * allowed to call the continuity API. Delegations are journaled locally
     * and survive disconnects and app kills: call again after every launch.
     */
    suspend fun startDelegation(options: DelegationOptions) = offload { backend.startDelegation(options) }

    /** Pause the pump. Journaled delegations resume with the next [startDelegation]. */
    suspend fun stopDelegation() = offload { backend.stopDelegation() }

    /**
     * Delegate a server-side sub-sequence on behalf of a local instance
     * without parking a step. Returns the delegation id; read the outcome
     * with [delegationStatus]. Requires [startDelegation].
     */
    suspend fun delegate(request: DelegateRequest): String = offload { backend.delegate(request) }

    /** The locally journaled state of a delegation (NOT_FOUND when unknown). */
    suspend fun delegationStatus(delegationId: String): DelegationStatus {
        require(delegationId.isNotBlank()) { "delegationId must not be blank" }
        return offload { backend.delegationStatus(delegationId) }
    }

    /** Every journaled delegation, oldest first. */
    suspend fun listDelegations(): List<DelegationStatus> = offload { backend.listDelegations() }

    /** Pump counters (zeros while it is not running). */
    suspend fun delegationStats(): DelegationStats = offload { backend.delegationStats() }

    /**
     * Cold flow of distinct statuses of one delegation, polled every
     * [pollInterval]. Completes after emitting a terminal state.
     */
    fun observeDelegation(delegationId: String, pollInterval: Duration = 1.seconds): Flow<DelegationStatus> =
        flow {
            while (true) {
                val status = backend.delegationStatus(delegationId)
                emit(status)
                if (status.state.isTerminal) return@flow
                delay(pollInterval)
            }
        }.distinctUntilChanged().flowOn(ioDispatcher)

    /**
     * Cold flow of distinct snapshots of one instance, polled every
     * [pollInterval]. Completes after emitting a terminal state.
     */
    fun observeInstance(instanceId: String, pollInterval: Duration = 500.milliseconds): Flow<InstanceSnapshot> =
        flow {
            while (true) {
                val snapshot = backend.getInstance(instanceId)
                emit(snapshot)
                if (snapshot.state.isTerminal) return@flow
                delay(pollInterval)
            }
        }.distinctUntilChanged().flowOn(ioDispatcher)

    /** Cold flow of the active-instance list, polled every [pollInterval], distinct. */
    fun observeActiveInstances(pollInterval: Duration = 1.seconds): Flow<List<InstanceSummary>> =
        flow {
            while (true) {
                emit(backend.activeInstances())
                delay(pollInterval)
            }
        }.distinctUntilChanged().flowOn(ioDispatcher)

    /** Shut down the engine. The instance must not be used afterwards. */
    override fun close() = backend.shutdown()

    private suspend fun <T> offload(block: () -> T): T = withContext(ioDispatcher) { block() }

    private fun Orch8StepHandler.toBlocking(): BlockingStepHandler =
        BlockingStepHandler { stepName, input ->
            try {
                runHandlerBlocking { execute(stepName, input) }
            } catch (e: Orch8HandlerException) {
                throw e
            } catch (e: CancellationException) {
                throw Orch8HandlerException.Retryable("handler cancelled: ${e.message}", e)
            } catch (e: Exception) {
                throw Orch8HandlerException.Retryable(e.message ?: e::class.simpleName ?: "handler failed", e)
            }
        }

    companion object {
        private const val EVENT_BUFFER = 256

        /** Open (or create) the engine database at [dbPath]. */
        fun open(dbPath: String, config: EngineConfig = EngineConfig()): Orch8Engine {
            require(dbPath.isNotBlank()) { "dbPath must not be blank" }
            return Orch8Engine(openPlatformBackend(dbPath, config.validate()))
        }
    }
}
