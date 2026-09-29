package io.orch8.kmp

import io.orch8.kmp.internal.BlockingStepHandler
import io.orch8.kmp.internal.EngineBackend
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private class FakeBackend : EngineBackend {
    val handlers = mutableMapOf<String, BlockingStepHandler>()
    var sink: (EngineEvent) -> Unit = {}
    val started = mutableListOf<Triple<String, String, String?>>()
    val completed = mutableListOf<Triple<String, String, String>>()
    val states = ArrayDeque<InstanceState>()
    var cancelled = mutableListOf<String>()
    var shutdown = false

    override fun registerHandler(name: String, handler: BlockingStepHandler) {
        handlers[name] = handler
    }

    override fun setListener(sink: (EngineEvent) -> Unit) {
        this.sink = sink
    }

    override fun resume() = Unit

    override fun pause() = Unit

    override fun shutdown() {
        shutdown = true
    }

    override fun tickOnce() = TickResult(1, 1, false)

    override fun runUntilIdle(maxTicks: Int, timeBudgetMs: Long) = BackgroundRunResult(maxTicks, 0, 0, false, false)

    override fun reportPowerState(state: PowerState) = Unit

    override fun onPushReceived() = Unit

    override fun start(sequenceName: String, inputJson: String, dedupKey: String?): String {
        started += Triple(sequenceName, inputJson, dedupKey)
        return "i${started.size}"
    }

    override fun cancelInstance(instanceId: String) {
        cancelled += instanceId
    }

    override fun getInstance(instanceId: String): InstanceSnapshot {
        val state = if (states.size > 1) states.removeFirst() else states.first()
        return InstanceSnapshot(instanceId, "field-inspection", state, "{}", "c", "u")
    }

    override fun activeInstances() = emptyList<InstanceSummary>()

    override fun completeStep(instanceId: String, stepName: String, outputJson: String) {
        completed += Triple(instanceId, stepName, outputJson)
    }

    override fun loadSequenceFromJson(json: String) = Unit

    override fun loadSequencesFromUrl(url: String) = 0

    override fun loadedSequences() = emptyList<SequenceInfo>()

    override fun sync(manifestUrl: String, tokens: Orch8TokenSource?) = SyncResult(0, 0, 0, 0, 0)

    override fun flushTelemetry(endpointUrl: String) = FlushResult(0, 0)

    override fun setDeviceContext(ctx: DeviceContext) = Unit

    override fun importContinuityCapsule(
        capsuleJson: String,
        payloadBase64: String,
        payloadKeyBase64: String,
        destinationRuntimeId: String,
        destinationInstanceId: String,
    ) = ContinuityImportResult("c", "k", destinationInstanceId, 1, "paused")

    override fun activateContinuityCapsule(capsuleId: String, destinationRuntimeId: String, destinationInstanceId: String) = Unit

    var registered: NodeCapabilities? = null
    var workerOptions: WorkerOptions? = null
    var windowBudgetMs: Long? = null
    val wakes = mutableListOf<String>()
    val builtins = mutableListOf<String>()

    override fun nodeRuntimeId() = "rt-1"

    var tokens: Orch8TokenSource? = null

    override fun setTokenProvider(tokens: Orch8TokenSource) {
        this.tokens = tokens
    }

    override fun registerNode(capabilities: NodeCapabilities): NodeRegistration {
        registered = capabilities
        return NodeRegistration("rt-1", "dev-1", capabilities.handlers, "2026-01-01T00:05:00Z")
    }

    override fun updateNodeStatus(connectivity: NodeConnectivity?, batteryPercent: Int?) = Unit

    override fun unregisterNode() = Unit

    override fun startWorker(options: WorkerOptions) {
        workerOptions = options
    }

    override fun stopWorker() = Unit

    override fun runWorkerWindow(timeBudgetMs: Long): WorkerWindowResult {
        windowBudgetMs = timeBudgetMs
        return WorkerWindowResult(1, 1, 0, 0, false)
    }

    override fun workerStats() = WorkerStats(true, 0, 1, 1, 0, 0, 0)

    override fun onPushWake(envelopeJson: String): Boolean {
        wakes += envelopeJson
        return true
    }

    override fun enableBuiltin(name: String) {
        builtins += name
    }

    var delegationOptions: DelegationOptions? = null
    var delegated: DelegateRequest? = null
    var delegationStopped = false
    val delegationStates = ArrayDeque<DelegationState>()

    override fun startDelegation(options: DelegationOptions) {
        delegationOptions = options
    }

    override fun stopDelegation() {
        delegationStopped = true
    }

    override fun delegate(request: DelegateRequest): String {
        delegated = request
        return "d-1"
    }

    override fun delegationStatus(delegationId: String): DelegationStatus {
        val state = if (delegationStates.size > 1) delegationStates.removeFirst() else delegationStates.first()
        val output = if (state == DelegationState.COMPLETED) "{}" else null
        return DelegationStatus(delegationId, state, "i1", null, "rt-2", output, null)
    }

    override fun listDelegations() = listOf(delegationStatus("d-1"))

    override fun delegationStats() = DelegationStats(true, 1, 1, 0, 0, 1)
}

class Orch8EngineTest {
    @Test
    fun startEncodesJsonInputAndDedupKey() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val id = engine.start("field-inspection", buildJsonObject { put("site", "A-12") }, dedupKey = "insp:A-12")
        assertEquals("i1", id)
        assertEquals(Triple("field-inspection", """{"site":"A-12"}""", "insp:A-12"), backend.started.single())
    }

    @Test
    fun eventsFlowAndPendingStepsTrackTheListener() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val firstEvent = async(UnconfinedTestDispatcher(testScheduler)) { engine.events.first() }

        backend.sink(EngineEvent.StepPending("i1", "capture_checklist", "capture_checklist"))
        backend.sink(EngineEvent.StepPending("i1", "supervisor_approval", "human_review"))

        assertEquals(EngineEvent.StepPending("i1", "capture_checklist", "capture_checklist"), firstEvent.await())
        assertEquals(2, engine.pendingSteps.value.size)

        engine.completeStep("i1", "capture_checklist", buildJsonObject { put("passed", true) })
        assertEquals(listOf("supervisor_approval"), engine.pendingSteps.value.map { it.stepName })
        assertEquals("""{"passed":true}""", backend.completed.single().third)

        engine.cancelInstance("i1")
        assertTrue(engine.pendingSteps.value.isEmpty())
    }

    @Test
    fun answerBuildsTheGatePayloadTheEngineAccepts() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        engine.answer("i1", "capture_checklist", "complete", buildJsonObject { put("failed", 1) })
        assertEquals("""{"failed":1,"value":"complete"}""", backend.completed.single().third)
        assertFailsWith<IllegalArgumentException> {
            engine.answer("i1", "s", "yes", buildJsonObject { put("value", "no") })
        }
        assertFailsWith<IllegalArgumentException> { engine.answer("i1", "s", " ") }
    }

    @Test
    fun handlersMapFailuresToRetryableOrPermanent() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        engine.registerHandler("echo") { step, input -> """{"step":"$step","input":$input}""" }
        engine.registerHandler("fatal") { _, _ -> throw Orch8HandlerException.Permanent("corrupt photo") }
        engine.registerHandler("flaky") { _, _ -> throw IllegalStateException("camera busy") }

        assertEquals("""{"step":"s","input":{}}""", backend.handlers.getValue("echo").execute("s", "{}"))
        assertFailsWith<Orch8HandlerException.Permanent> { backend.handlers.getValue("fatal").execute("s", "{}") }
        val retry = assertFailsWith<Orch8HandlerException.Retryable> { backend.handlers.getValue("flaky").execute("s", "{}") }
        assertEquals("camera busy", retry.message)
    }

    @Test
    fun runUntilIdleValidatesItsBudget() = runTest {
        val engine = Orch8Engine(FakeBackend(), UnconfinedTestDispatcher(testScheduler))
        assertFailsWith<IllegalArgumentException> { engine.runUntilIdle(maxTicks = 0) }
        assertFailsWith<IllegalArgumentException> { engine.runUntilIdle(timeBudget = 0.milliseconds) }
        assertEquals(7, engine.runUntilIdle(maxTicks = 7).ticksExecuted)
    }

    @Test
    fun observeInstanceEmitsDistinctStatesAndStopsAtTerminal() = runTest {
        val backend = FakeBackend()
        backend.states.addAll(
            listOf(
                InstanceState.RUNNING,
                InstanceState.WAITING,
                InstanceState.WAITING,
                InstanceState.RUNNING,
                InstanceState.COMPLETED,
            ),
        )
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val seen = engine.observeInstance("i1", pollInterval = 100.milliseconds).toList().map { it.state }
        assertEquals(
            listOf(InstanceState.RUNNING, InstanceState.WAITING, InstanceState.RUNNING, InstanceState.COMPLETED),
            seen,
        )
    }

    @Test
    fun runtimeNodeCallsReachTheBackend() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val reg = engine.registerNode(NodeCapabilities(handlers = listOf("scan"), hardware = listOf("camera")))
        assertEquals("rt-1", reg.runtimeId)
        assertEquals(listOf("camera"), backend.registered!!.hardware)

        engine.startWorker(WorkerOptions(maxConcurrentTasks = 2))
        assertEquals(2, backend.workerOptions!!.maxConcurrentTasks)
        assertEquals(1L, engine.runWorkerWindow(20.seconds).completed)
        assertEquals(20_000L, backend.windowBudgetMs)
        assertTrue(engine.workerStats().running)

        assertTrue(engine.onPushWake(taskId = "t1", reason = "work"))
        assertEquals("""{"task_id":"t1","reason":"work"}""", backend.wakes.single())
        engine.enableBuiltin("http_request")
        assertEquals(listOf("http_request"), backend.builtins)
    }

    @Test
    fun runtimeNodeArgumentsAreValidated() = runTest {
        val engine = Orch8Engine(FakeBackend(), UnconfinedTestDispatcher(testScheduler))
        assertFailsWith<IllegalArgumentException> { NodeCapabilities(batteryPercent = 101) }
        assertFailsWith<IllegalArgumentException> { WorkerOptions(maxConcurrentTasks = 0) }
        assertFailsWith<IllegalArgumentException> { engine.updateNodeStatus(batteryPercent = -1) }
        assertFailsWith<IllegalArgumentException> { engine.runWorkerWindow(0.milliseconds) }
    }

    @Test
    fun delegationCallsReachTheBackend() = runTest {
        val backend = FakeBackend()
        backend.delegationStates.add(DelegationState.COMPLETED)
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        engine.startDelegation(DelegationOptions(tenantId = "acme", ttl = 30.seconds))
        assertEquals(30.seconds, backend.delegationOptions!!.ttl)
        assertEquals(2.seconds, backend.delegationOptions!!.pollInterval)

        val id = engine.delegate(DelegateRequest("i1", "rt-2", "seq-1", """{"photo":"p"}"""))
        assertEquals("d-1", id)
        assertEquals("seq-1", backend.delegated!!.subSequenceId)
        assertEquals(DelegationState.COMPLETED, engine.delegationStatus(id).state)
        assertEquals(1, engine.listDelegations().size)
        assertEquals(1L, engine.delegationStats().resumed)
        engine.stopDelegation()
        assertTrue(backend.delegationStopped)
    }

    @Test
    fun observeDelegationStopsAtTerminalState() = runTest {
        val backend = FakeBackend()
        backend.delegationStates.addAll(
            listOf(DelegationState.PREPARING, DelegationState.DELEGATED, DelegationState.DELEGATED, DelegationState.FAILED),
        )
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val seen = engine.observeDelegation("d-1", pollInterval = 100.milliseconds).toList().map { it.state }
        assertEquals(listOf(DelegationState.PREPARING, DelegationState.DELEGATED, DelegationState.FAILED), seen)
    }

    @Test
    fun delegationArgumentsAreValidated() = runTest {
        val engine = Orch8Engine(FakeBackend(), UnconfinedTestDispatcher(testScheduler))
        assertFailsWith<IllegalArgumentException> { DelegationOptions(tenantId = " ") }
        assertFailsWith<IllegalArgumentException> { DelegationOptions(tenantId = "t", ttl = 86_401.seconds) }
        assertFailsWith<IllegalArgumentException> { DelegationOptions(tenantId = "t", ttl = 500.milliseconds) }
        assertFailsWith<IllegalArgumentException> { DelegationOptions(tenantId = "t", pollInterval = 0.seconds) }
        assertFailsWith<IllegalArgumentException> { DelegateRequest("i", "r", "s", "[1]") }
        assertFailsWith<IllegalArgumentException> { DelegateRequest("i", "r", "s", "nope") }
        assertFailsWith<IllegalArgumentException> { DelegateRequest("", "r", "s") }
        assertFailsWith<IllegalArgumentException> { engine.delegationStatus("") }
        assertEquals(DelegationState.ABANDONED, DelegationState.fromWire("abandoned"))
        assertEquals(Orch8ErrorKind.ENGINE, assertFailsWith<Orch8Exception> { DelegationState.fromWire("x") }.kind)
    }

    @Test
    fun taskContextIsParsedFromHandlerInput() {
        val ctx = Orch8TaskContext.fromInput(
            """{"doc":"passport","__orch8":{"effect_id":"eff-1","task_id":"t","instance_id":"i",
               "block_id":"b","attempt":2,"runtime_id":"r","continuity_epoch":3,"resume_checkpoint":null}}""",
        )!!
        assertEquals("eff-1", ctx.effectId)
        assertEquals(2, ctx.attempt)
        assertEquals(3L, ctx.continuityEpoch)
        assertEquals(null, ctx.resumeCheckpoint)
        assertEquals(null, Orch8TaskContext.fromInput("{}"))
        assertEquals(null, Orch8TaskContext.fromInput("[]"))
        assertEquals(null, Orch8TaskContext.fromInput("nope"))
        assertEquals(null, Orch8TaskContext.fromInput("""{"__orch8":{"effect_id":null}}""")!!.effectId)
    }

    @Test
    fun tokenProviderAwaitsTheFirstTokenAndRefreshesThroughTheSuspendFetch() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        var fetched = 0
        engine.setTokenProvider { "dst_${++fetched}" }
        val tokens = backend.tokens!!
        assertEquals("dst_1", tokens.currentToken())
        // A 401 on the engine's blocking thread runs the suspend fetch again.
        assertEquals("dst_2", tokens.refreshToken())
        assertEquals("dst_2", tokens.currentToken())
        assertEquals(2, fetched)
    }

    @Test
    fun tokenProviderRejectsEmptyTokensAndBoundsRefreshes() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        assertFailsWith<IllegalArgumentException> { engine.setTokenProvider { " " } }
        assertEquals(null, backend.tokens)
        assertFailsWith<IllegalArgumentException> { engine.setTokenProvider(0.seconds) { "dst_1" } }

        var calls = 0
        engine.setTokenProvider(refreshTimeout = 50.milliseconds) {
            if (++calls > 1) kotlinx.coroutines.delay(10.seconds)
            "dst_1"
        }
        val timedOut = assertFailsWith<Orch8Exception> { backend.tokens!!.refreshToken() }
        assertEquals(Orch8ErrorKind.NETWORK, timedOut.kind)
        // The cached token survives a failed refresh.
        assertEquals("dst_1", backend.tokens!!.currentToken())

        var empty = 0
        engine.setTokenProvider { if (++empty == 1) "dst_1" else "" }
        assertEquals(Orch8ErrorKind.INVALID_INPUT, assertFailsWith<Orch8Exception> { backend.tokens!!.refreshToken() }.kind)
    }

    @Test
    fun closeShutsTheEngineDown() = runTest {
        val backend = FakeBackend()
        Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler)).use { }
        assertTrue(backend.shutdown)
    }
}
