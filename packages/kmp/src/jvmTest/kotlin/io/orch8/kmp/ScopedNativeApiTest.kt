package io.orch8.kmp

import io.orch8.mobile.MobileAccountIdentity
import io.orch8.mobile.MobileCatalogConfiguration
import io.orch8.mobile.MobileCatalogCallbackContext
import io.orch8.mobile.MobileCatalogStepHandler
import io.orch8.mobile.MobileEngine
import io.orch8.mobile.MobileEngineConfig
import io.orch8.mobile.TokenProvider
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Assume.assumeTrue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ScopedNativeApiTest {
    private fun identity(name: String) = MobileAccountIdentity(name, "issuer", name)
    private fun configuration(name: String) = MobileCatalogConfiguration(
        name, "source", "ed25519:11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo=",
        "https://catalog.example/$name/manifest", "https://catalog.example/identity"
    )
    private fun withEngine(block: (MobileEngine) -> Unit) {
        assumeTrue("set orch8.native.lib.path to run native integration", System.getProperty("orch8.native.integration") == "true")
        val directory = Files.createTempDirectory("orch8-scoped-native-")
        val engine = MobileEngine(directory.resolve("node.db").toString(), MobileEngineConfig(
            tickIntervalMs = 500uL, maxConcurrentSteps = 4u, maxStepsPerInstance = 1000u,
            maxConcurrentInstances = 10u, maxTickDurationMs = 5000uL,
            maxInstanceLifetimeSecs = 86400uL, maxStoredSequences = 50u,
            maxSequenceSizeBytes = 1048576uL, handlerTimeoutMs = 1000uL,
            operationTimeoutMs = 3000uL, telemetryEnabled = false, telemetryUrl = "",
            environment = "test", rootPublicKey = "", sdkVersion = "0.7.1",
            memoryBudgetBytes = 0uL, sequencesUrl = "", syncUrl = "", deviceId = "scope-test", syncApiKey = ""
        ))
        try { block(engine) } finally {
            engine.shutdown()
            engine.close()
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun explicitOwnerAndRetirementAreSeparateFromLocalViewClose() = withEngine { engine ->
        val alice = engine.openAccount(identity("alice"))
        val sibling = engine.openAccount(identity("alice"))
        val bob = engine.openAccount(identity("bob"))
        assertEquals(alice.accountId(), sibling.accountId())
        assertNotEquals(alice.accountId(), bob.accountId())
        val one = alice.openCatalog(configuration("one"))
        val two = alice.openCatalog(configuration("two"))
        val foreign = bob.openCatalog(configuration("one"))
        assertNotEquals(one.catalogId(), two.catalogId())
        assertNotEquals(one.catalogId(), foreign.catalogId())
        one.registerHandler("echo", object : MobileCatalogStepHandler {
            override fun execute(context: MobileCatalogCallbackContext, stepName: String, input: String) = "{}"
        }, 1000uL)
        assertTrue(one.history(null, 1u).items.isEmpty())
        alice.closeView()
        assertFails { one.history(null, 1u) }
        val calls = AtomicInteger()
        assertFails { one.sync(object : TokenProvider {
            override fun currentToken(): String { calls.incrementAndGet(); return "must-not-fetch" }
            override fun refreshToken(): String { calls.incrementAndGet(); return "must-not-refresh" }
        }) }
        assertEquals(0, calls.get())
        assertTrue(foreign.history(null, 1u).items.isEmpty())
        val epoch = sibling.sessionEpoch()
        sibling.retire()
        val fresh = engine.openAccount(identity("alice"))
        assertTrue(fresh.sessionEpoch() > epoch)
        assertTrue(fresh.openCatalog(configuration("one")).history(null, 1u).items.isEmpty())
        assertFails { engine.openAccount(MobileAccountIdentity("alice", "issuer", "mallory")) }
    }

    @Test
    fun issuedRunSurvivesGeneratedParentUseBlocksAndExplicitCloseFencesIt() = withEngine { engine ->
        val run = engine.openAccount(identity("alice")).use { account ->
            account.openCatalog(configuration("one")).use { catalog ->
                catalog.run(UUID.randomUUID().toString())
            }
        }
        assertNull(run.snapshot())
        assertNull(run.cancel())
        run.closeView()
        assertFails { run.snapshot() }
        run.close()
    }
}
