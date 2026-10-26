package io.orch8.kmp

import io.orch8.kmp.internal.BlockingStepHandler
import io.orch8.kmp.internal.EngineBackend
import kotlinx.coroutines.async
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

internal class FakeBackend : EngineBackend {
    override fun openAccount(identity: AccountIdentity): io.orch8.kmp.internal.AccountBackend = error("unexpected scoped account call")

    val handlers = mutableMapOf<String, BlockingStepHandler>()
    var sink: (EngineEvent) -> Unit = {}
    val started = mutableListOf<Triple<String, String, String?>>()
    val completed = mutableListOf<Triple<String, String, String>>()
    val states = ArrayDeque<InstanceState>()
    var cancelled = mutableListOf<String>()
    var shutdown = false
    var instanceFailure: Orch8Exception? = null
    var instanceReads = 0
    var activeReads = 0
    var delegationReads = 0

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
        (tokens as? DeviceSessionTokens)?.close()
        tokens = null
        installation = null
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
        instanceReads++
        instanceFailure?.let { throw it }
        val state = if (states.size > 1) states.removeFirst() else states.first()
        return InstanceSnapshot(instanceId, "field-inspection", state, "{}", "c", "u")
    }

    override fun activeInstances(): List<InstanceSummary> { activeReads++; return emptyList() }

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
    private var serial = 0
    private var installation: String? = null
    var afterBegin: () -> Unit = {}
    var afterComplete: () -> Unit = {}

    override fun setTokenProvider(tokens: Orch8TokenSource) {
        installation = "sync-${++serial}"
        this.tokens = tokens
    }

    override fun beginTokenProviderInstallation(tokens: Orch8TokenSource): String {
        if (shutdown) throw Orch8Exception(Orch8ErrorKind.SHUTDOWN, "engine closed")
        this.tokens = tokens
        val ticket = "ticket-${++serial}"
        installation = ticket
        afterBegin()
        return ticket
    }
    override fun completeTokenProviderInstallation(installationId: String, token: String): Boolean {
        val accepted = !shutdown && installation == installationId && token.isNotBlank()
        if (accepted) afterComplete()
        return accepted
    }
    override fun cancelTokenProviderInstallation(installationId: String): Boolean {
        if (installation != installationId) return false
        installation = null
        (tokens as? DeviceSessionTokens)?.close()
        tokens = null
        return true
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
        delegationReads++
        val state = if (delegationStates.size > 1) delegationStates.removeFirst() else delegationStates.first()
        val output = if (state == DelegationState.COMPLETED) "{}" else null
        return DelegationStatus(delegationId, state, "i1", null, "rt-2", output, null)
    }

    override fun listDelegations() = listOf(delegationStatus("d-1"))

    override fun delegationStats() = DelegationStats(true, 1, 1, 0, 0, 1)
}

private open class TestObservationLifecycle : Orch8ObservationObserver {
    val errors = mutableListOf<Orch8Exception>()
    var completions = 0
    override fun onError(error: Orch8Exception) { errors += error }
    override fun onComplete() { completions++ }
}

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
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
    fun observersRefuseNonpositiveOrInfinitePollingBeforeAnyRead() = runTest {
        val backend = FakeBackend()
        backend.states.add(InstanceState.COMPLETED)
        backend.delegationStates.add(DelegationState.COMPLETED)
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        for (interval in listOf(Duration.ZERO, (-1).milliseconds, Duration.INFINITE)) {
            assertFailsWith<IllegalArgumentException> { engine.observeInstance("i1", interval).first() }
            assertFailsWith<IllegalArgumentException> { engine.observeActiveInstances(interval).first() }
            assertFailsWith<IllegalArgumentException> { engine.observeDelegation("d-1", interval).first() }
        }
        assertEquals(0, backend.instanceReads)
        assertEquals(0, backend.activeReads)
        assertEquals(0, backend.delegationReads)
    }

    @Test
    fun instanceSubscriptionStopsAfterTerminalAndErrorCallbacksCannotEscape() = runTest {
        val backend = FakeBackend()
        backend.states.addAll(listOf(InstanceState.RUNNING, InstanceState.COMPLETED))
        val engine = Orch8Engine(backend, StandardTestDispatcher(testScheduler))
        val seen = mutableListOf<InstanceState>()
        val observer = object : TestObservationLifecycle(), InstanceSnapshotObserver {
            override fun onInstance(snapshot: InstanceSnapshot) { seen += snapshot.state }
        }
        val subscription = engine.subscribeInstance("i1", observer, 100)
        runCurrent(); advanceTimeBy(100); runCurrent()
        assertEquals(listOf(InstanceState.RUNNING, InstanceState.COMPLETED), seen)
        assertEquals(1, observer.completions)
        assertTrue(observer.errors.isEmpty())
        subscription.close(); advanceTimeBy(1000); runCurrent()
        assertEquals(2, backend.instanceReads)
        backend.instanceFailure = Orch8Exception(Orch8ErrorKind.NOT_FOUND, "missing")
        val failed = object : TestObservationLifecycle(), InstanceSnapshotObserver {
            override fun onInstance(snapshot: InstanceSnapshot) = error("unexpected snapshot")
            override fun onError(error: Orch8Exception) { super.onError(error); error("broken error callback") }
        }
        val broken = engine.subscribeInstance("missing", failed)
        runCurrent()
        assertEquals(Orch8ErrorKind.NOT_FOUND, failed.errors.single().kind)
        assertEquals(0, failed.completions)
        broken.close(); engine.close()
    }

    @Test
    fun engineCloseStopsEveryOwnedStreamAndLateEventsCannotReachObservers() = runTest {
        val backend = FakeBackend()
        backend.delegationStates.add(DelegationState.DELEGATED)
        val engine = Orch8Engine(backend, StandardTestDispatcher(testScheduler))
        val events = mutableListOf<EngineEvent>()
        val pending = mutableListOf<List<PendingStep>>()
        val eventObserver = object : TestObservationLifecycle(), EngineEventObserver {
            override fun onEvent(event: EngineEvent) { events += event }
        }
        val stepObserver = object : TestObservationLifecycle(), PendingStepsObserver {
            override fun onSteps(steps: List<PendingStep>) { pending += steps }
        }
        val activeObserver = object : TestObservationLifecycle(), ActiveInstancesObserver {
            override fun onInstances(instances: List<InstanceSummary>) = Unit
        }
        val delegationObserver = object : TestObservationLifecycle(), DelegationStatusObserver {
            override fun onStatus(status: DelegationStatus) = Unit
        }
        val subscriptions = listOf(engine.subscribeEvents(eventObserver), engine.subscribePendingSteps(stepObserver),
            engine.subscribeActiveInstances(activeObserver, 100), engine.subscribeDelegation("d-1", delegationObserver, 100))
        runCurrent()
        backend.sink(EngineEvent.StepPending("i1", "approve", "echo")); runCurrent()
        assertEquals(1, events.size)
        assertEquals(listOf(emptyList(), listOf(PendingStep("i1", "approve", "echo"))), pending)
        assertEquals(1, backend.activeReads); assertEquals(1, backend.delegationReads)
        engine.close(); runCurrent()
        backend.sink(EngineEvent.InstanceCompleted("i1", "{}"))
        advanceTimeBy(1000); runCurrent()
        assertEquals(1, events.size); assertEquals(2, pending.size)
        assertEquals(1, backend.activeReads); assertEquals(1, backend.delegationReads)
        assertTrue(listOf(eventObserver, stepObserver, activeObserver, delegationObserver).all { it.errors.isEmpty() && it.completions == 0 })
        assertEquals(Orch8ErrorKind.SHUTDOWN,
            assertFailsWith<Orch8Exception> { engine.subscribeEvents(eventObserver) }.kind)
        subscriptions.forEach { it.close() }
    }

    @Test
    fun returnedEventSubscriptionIsReadyBeforeTheNextProducerCall() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, StandardTestDispatcher(testScheduler))
        val seen = mutableListOf<EngineEvent>()
        val subscription = engine.subscribeEvents(object : TestObservationLifecycle(), EngineEventObserver {
            override fun onEvent(event: EngineEvent) { seen += event }
        })
        val event = EngineEvent.InstanceCompleted("i1", "{}")
        backend.sink(event) // no scheduler turn between subscription and production
        runCurrent()
        assertEquals(listOf<EngineEvent>(event), seen)
        subscription.close(); engine.close()
    }

    @Test
    fun closingOneEventSubscriptionPreservesTheOtherAndTheEngine() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, StandardTestDispatcher(testScheduler))
        var first = 0
        var second = 0
        val a = engine.subscribeEvents(object : TestObservationLifecycle(), EngineEventObserver {
            override fun onEvent(event: EngineEvent) { first++ }
        })
        val b = engine.subscribeEvents(object : TestObservationLifecycle(), EngineEventObserver {
            override fun onEvent(event: EngineEvent) { second++ }
        })
        runCurrent(); a.close(); runCurrent()
        backend.sink(EngineEvent.InstanceCompleted("i1", "{}")); runCurrent()
        assertEquals(0, first); assertEquals(1, second); assertEquals(false, backend.shutdown)
        b.close(); engine.close()
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

    @Test
    fun olderInitialCompletionCannotReplaceNewerCredentialsAcrossAliases() = runTest {
        val backend = FakeBackend()
        val first = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val second = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<String>()
        val older = async {
            runCatching { first.setTokenProvider { entered.complete(Unit); release.await() } }
        }
        entered.await()
        second.setTokenProvider { "dst_newer" }
        release.complete("dst_older")
        assertTrue(older.await().isFailure, "obsolete installation must fail")
        assertEquals("dst_newer", backend.tokens!!.currentToken())
    }

    @Test
    fun cancellationDuringNativeReservationDoesNotLoseTheTicket() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        var fetched = false
        val installing = async(start = kotlinx.coroutines.CoroutineStart.LAZY) {
            engine.setTokenProvider { fetched = true; "dst_unexpected" }
        }
        backend.afterBegin = { installing.cancel() }
        installing.start()
        assertFailsWith<kotlinx.coroutines.CancellationException> { installing.await() }
        assertEquals(null, backend.tokens)
        assertEquals(false, fetched)
    }

    @Test
    fun cancellationDuringNativeCompletionRetiresTheCommittedOwner() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val installing = async(start = kotlinx.coroutines.CoroutineStart.LAZY) { engine.setTokenProvider { "dst_first" } }
        backend.afterComplete = { installing.cancel() }
        installing.start()
        assertFailsWith<kotlinx.coroutines.CancellationException> { installing.await() }
        assertEquals(null, backend.tokens)
    }

    @Test
    fun cancellingAnOlderCallerCannotRetireTheNewerInstallation() = runTest {
        val backend = FakeBackend()
        val first = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val second = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<String>()
        val older = async { first.setTokenProvider { entered.complete(Unit); release.await() } }
        entered.await()
        second.setTokenProvider { "dst_newer" }
        older.cancel()
        assertFailsWith<kotlinx.coroutines.CancellationException> { older.await() }
        release.complete("dst_old")
        assertEquals("dst_newer", backend.tokens!!.currentToken())
    }

    @Test
    fun closedEngineAndInfiniteDeadlineRejectBeforeHostFetch() = runTest {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend, UnconfinedTestDispatcher(testScheduler))
        var fetched = false
        assertFailsWith<IllegalArgumentException> { engine.setTokenProvider(kotlin.time.Duration.INFINITE) { fetched = true; "dst_bad" } }
        engine.close()
        assertEquals(Orch8ErrorKind.SHUTDOWN, assertFailsWith<Orch8Exception> { engine.setTokenProvider { fetched = true; "dst_bad" } }.kind)
        assertEquals(false, fetched)
    }
}
