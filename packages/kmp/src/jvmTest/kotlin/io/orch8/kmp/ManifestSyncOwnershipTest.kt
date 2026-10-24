package io.orch8.kmp

import io.orch8.kmp.bridge.ORCH8_BRIDGE_PROTOCOL
import io.orch8.kmp.bridge.Orch8BridgeCallbacks
import io.orch8.kmp.bridge.Orch8JsonBridge
import io.orch8.kmp.internal.JsonBridgeBackend
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ManifestSyncOwnershipTest {
    @Test
    fun parallelSyncsKeepCredentialsUntilTheirOwnCallReturns() {
        val outerEntered = CountDownLatch(1)
        val innerEntered = CountDownLatch(1)
        val releaseInner = CountDownLatch(1)
        val innerFinished = CountDownLatch(1)
        val outerId = AtomicReference<String>()
        val innerId = AtomicReference<String>()
        val installedCallbacks = AtomicReference<Orch8BridgeCallbacks>()
        val bridge = object : Orch8JsonBridge {
            override fun setCallbacks(callbacks: Orch8BridgeCallbacks) { installedCallbacks.set(callbacks) }

            override fun call(method: String, argsJson: String): String {
                if (method == "open") return """{"ok":{"protocol":$ORCH8_BRIDGE_PROTOCOL}}"""
                if (method != "sync") return """{"ok":null}"""
                val args = Json.parseToJsonElement(argsJson).jsonObject
                val id = args["providerId"]!!.jsonPrimitive.content
                if (args["manifestUrl"]!!.jsonPrimitive.content.contains("account-a")) {
                    outerId.set(id)
                    outerEntered.countDown()
                    assertTrue(innerEntered.await(30, TimeUnit.SECONDS))
                    assertEquals("account_a", installedCallbacks.get().currentManifestToken(id))
                    assertEquals("account_b", installedCallbacks.get().currentManifestToken(innerId.get()))
                    releaseInner.countDown()
                    assertTrue(innerFinished.await(30, TimeUnit.SECONDS))
                    assertEquals("account_a", installedCallbacks.get().currentManifestToken(id))
                    assertEquals("", installedCallbacks.get().currentManifestToken(innerId.get()))
                } else {
                    innerId.set(id)
                    innerEntered.countDown()
                    assertTrue(releaseInner.await(30, TimeUnit.SECONDS))
                }
                return """{"ok":{"added":0,"updated":0,"removed":0,"skipped":0,"signatureFailures":0}}"""
            }
        }
        fun tokens(account: String) = object : Orch8TokenSource {
            override fun currentToken() = account
            override fun refreshToken() = "${account}_refreshed"
        }
        val backend = JsonBridgeBackend.open(bridge, "/tmp/orch8-manifest-ownership.db", EngineConfig())
        val executor = Executors.newFixedThreadPool(2)
        try {
            val outer = executor.submit<SyncResult> { backend.sync("https://account-a.example/manifest.json", tokens("account_a")) }
            assertTrue(outerEntered.await(30, TimeUnit.SECONDS))
            val inner = executor.submit<SyncResult> {
                try { backend.sync("https://account-b.example/manifest.json", tokens("account_b")) }
                finally { innerFinished.countDown() }
            }
            inner.get(30, TimeUnit.SECONDS)
            outer.get(30, TimeUnit.SECONDS)
            assertEquals("", installedCallbacks.get().currentManifestToken(outerId.get()))
            assertEquals("", installedCallbacks.get().currentManifestToken(innerId.get()))
        } finally {
            releaseInner.countDown()
            innerFinished.countDown()
            backend.shutdown()
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS))
        }
    }
}
