package io.orch8.kmp

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class EngineConfigTest {
    @Test
    fun defaultsMatchDocumentedMobileEngineConfig() {
        val c = EngineConfig().validate()
        assertEquals(500, c.tickIntervalMs)
        assertEquals(4, c.maxConcurrentSteps)
        assertEquals(1000, c.maxStepsPerInstance)
        assertEquals(10, c.maxConcurrentInstances)
        assertEquals(1_048_576, c.maxSequenceSizeBytes)
        assertEquals(30_000, c.handlerTimeoutMs)
        assertEquals("production", c.environment)
        assertFalse(c.serverSyncEnabled)
    }

    @Test
    fun rejectsNonPositiveLimits() {
        val e = assertFailsWith<Orch8Exception> { EngineConfig(tickIntervalMs = 0).validate() }
        assertEquals(Orch8ErrorKind.INVALID_INPUT, e.kind)
        assertFailsWith<Orch8Exception> { EngineConfig(maxConcurrentInstances = -1).validate() }
        assertFailsWith<Orch8Exception> { EngineConfig(memoryBudgetBytes = -1).validate() }
    }

    @Test
    fun requiresHttpsUrls() {
        assertFailsWith<Orch8Exception> { EngineConfig(syncUrl = "http://example.com/sync", deviceId = "d").validate() }
        assertFailsWith<Orch8Exception> { EngineConfig(telemetryUrl = "ftp://x").validate() }
        EngineConfig(syncUrl = "https://api.example.com/mobile/sync", deviceId = "d").validate()
    }

    @Test
    fun serverSyncNeedsDeviceId() {
        assertFailsWith<Orch8Exception> { EngineConfig(syncUrl = "https://api.example.com/mobile/sync").validate() }
    }

    @Test
    fun rejectsUnknownEnvironment() {
        assertFailsWith<Orch8Exception> { EngineConfig(environment = "prod").validate() }
        EngineConfig(environment = "staging").validate()
    }

    @Test
    fun toStringNeverLeaksApiKey() {
        val text = EngineConfig(syncApiKey = "sk_live_secret").toString()
        assertFalse("sk_live_secret" in text)
        assertTrue("<redacted>" in text)
    }
}

class PowerStateTest {
    @Test
    fun chargingWinsOverLevel() {
        assertEquals(PowerState.CHARGING, PowerState.fromBattery(3, charging = true))
    }

    @Test
    fun thresholdsMatchEngineDocs() {
        assertEquals(PowerState.UNPLUGGED, PowerState.fromBattery(21, charging = false))
        assertEquals(PowerState.LOW_BATTERY, PowerState.fromBattery(20, charging = false))
        assertEquals(PowerState.LOW_BATTERY, PowerState.fromBattery(6, charging = false))
        assertEquals(PowerState.CRITICAL_BATTERY, PowerState.fromBattery(5, charging = false))
        assertEquals(PowerState.CRITICAL_BATTERY, PowerState.fromBattery(-10, charging = false))
        assertEquals(PowerState.UNPLUGGED, PowerState.fromBattery(250, charging = false))
    }
}

class ErrorKindTest {
    @Test
    fun parsesAllWireSpellings() {
        assertEquals(Orch8ErrorKind.NOT_FOUND, Orch8ErrorKind.fromWire("NotFound"))
        assertEquals(Orch8ErrorKind.NOT_FOUND, Orch8ErrorKind.fromWire("not_found"))
        assertEquals(Orch8ErrorKind.NOT_FOUND, Orch8ErrorKind.fromWire("NOT_FOUND"))
        assertEquals(Orch8ErrorKind.INVALID_INPUT, Orch8ErrorKind.fromWire("InvalidInput"))
        assertEquals(Orch8ErrorKind.SIGNATURE_INVALID, Orch8ErrorKind.fromWire("SignatureInvalid"))
        assertEquals(Orch8ErrorKind.UNKNOWN, Orch8ErrorKind.fromWire("Bogus"))
    }

    @Test
    fun retryabilityFollowsKind() {
        assertTrue(Orch8Exception(Orch8ErrorKind.NETWORK, "x").isRetryable)
        assertFalse(Orch8Exception(Orch8ErrorKind.INVALID_INPUT, "x").isRetryable)
        assertFalse(Orch8Exception(Orch8ErrorKind.SIGNATURE_INVALID, "x").isRetryable)
    }

    @Test
    fun instanceStateParsingAndTerminality() {
        assertEquals(InstanceState.WAITING, InstanceState.fromWire("waiting"))
        assertEquals(InstanceState.WAITING, InstanceState.fromWire("WAITING"))
        assertTrue(InstanceState.CANCELLED.isTerminal)
        assertFalse(InstanceState.WAITING.isTerminal)
        assertFailsWith<Orch8Exception> { InstanceState.fromWire("sleeping") }
    }
}

class PendingStepsTest {
    private val pending = EngineEvent.StepPending("i1", "capture_checklist", "capture_checklist")

    @Test
    fun addsAndDeduplicatesPendingSteps() {
        var list = PendingSteps.reduce(emptyList(), pending)
        list = PendingSteps.reduce(list, pending.copy(handler = "renamed"))
        assertEquals(listOf(PendingStep("i1", "capture_checklist", "renamed")), list)
    }

    @Test
    fun terminalEventsClearTheInstance() {
        var list = PendingSteps.reduce(emptyList(), pending)
        list = PendingSteps.reduce(list, EngineEvent.StepPending("i2", "a", "a"))
        list = PendingSteps.reduce(list, EngineEvent.InstanceFailed("i1", "boom"))
        assertEquals(listOf("i2"), list.map { it.instanceId })
        list = PendingSteps.reduce(list, EngineEvent.InstanceCompleted("i2", "{}"))
        assertTrue(list.isEmpty())
    }

    @Test
    fun localCompletionRemovesOnlyThatStep() {
        var list = PendingSteps.reduce(emptyList(), pending)
        list = PendingSteps.reduce(list, EngineEvent.StepPending("i1", "capture_photos", "capture_photos"))
        list = PendingSteps.withoutStep(list, "i1", "capture_checklist")
        assertEquals(listOf("capture_photos"), list.map { it.stepName })
    }

    @Test
    fun snapshotContextParsesOrFallsBack() {
        val good = InstanceSnapshot("i", "s", InstanceState.RUNNING, """{"data":{"site":"A-12"}}""", "t", "t")
        assertTrue("data" in good.context)
        val bad = InstanceSnapshot("i", "s", InstanceState.RUNNING, "not json", "t", "t")
        assertTrue(bad.context.isEmpty())
    }
}
