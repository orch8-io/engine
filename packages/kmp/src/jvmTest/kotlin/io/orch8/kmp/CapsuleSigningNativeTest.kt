package io.orch8.kmp

import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPairGenerator
import java.security.Signature
import java.util.Base64
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assume.assumeTrue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CapsuleSigningNativeTest {
    @Test
    fun hostSignatureIsVerifiedByTheReceivingRuntime() = runBlocking {
        assumeTrue(
            "set orch8.native.lib.path for native integration",
            System.getProperty("orch8.native.integration") == "true",
        )
        val fixture = Path.of(checkNotNull(System.getProperty("orch8.native.fixture.path")))
        val metadata = Json.parseToJsonElement(Files.readString(fixture.resolve("metadata.json")))
            .jsonObject.getValue("capsule").jsonObject
        val directory = Files.createTempDirectory("orch8-kmp-capsule-")
        val sourcePath = directory.resolve("source.db")
        val signer = JcaSigner()
        val transferKey = Base64.getEncoder().encodeToString(ByteArray(32) { 19 })
        val runtimeId = UUID.randomUUID().toString()
        val instanceId = UUID.randomUUID().toString()
        try {
            Files.copy(fixture.resolve("capsule-source.db"), sourcePath)
            val source = Orch8Engine.open(sourcePath.toString(), EngineConfig(telemetryEnabled = false))
            val exported = try {
                source.exportContinuityCapsule(
                    metadata.getValue("sourceInstance").jsonPrimitive.content, runtimeId, transferKey, 600, signer,
                )
            } finally {
                source.close()
            }
            assertEquals(1, signer.calls.get())
            assertEquals(metadata.getValue("continuityId").jsonPrimitive.content, exported.continuityId)
            assertEquals(0L, exported.sourceEpoch)

            val receiver = Orch8Engine.open(
                directory.resolve("receiver.db").toString(),
                EngineConfig(rootPublicKey = signer.publicKeyBase64, telemetryEnabled = false),
            )
            try {
                receiver.loadSequence(metadata.getValue("sequence").toString())
                val manifest = Json.parseToJsonElement(exported.capsuleJson).jsonObject
                val signature = Base64.getDecoder().decode(manifest.getValue("signature").jsonPrimitive.content)
                signature[0] = (signature[0].toInt() xor 1).toByte()
                val damaged = JsonObject(
                    manifest + ("signature" to JsonPrimitive(Base64.getEncoder().encodeToString(signature))),
                )
                assertFailsWith<Orch8Exception> {
                    receiver.importContinuityCapsule(damaged.toString(), exported.payloadBase64, transferKey, runtimeId, instanceId)
                }
                val imported = receiver.importContinuityCapsule(
                    exported.capsuleJson, exported.payloadBase64, transferKey, runtimeId, instanceId,
                )
                assertEquals(exported.capsuleId, imported.capsuleId)
                assertEquals(exported.continuityId, imported.continuityId)
                assertEquals(instanceId, imported.instanceId)
                receiver.activateContinuityCapsule(imported.capsuleId, runtimeId, instanceId)
                val restored = receiver.getInstance(instanceId)
                assertEquals("from-source", restored.context.getValue("payload").jsonPrimitive.content)
            } finally {
                receiver.close()
            }
        } finally {
            directory.toFile().deleteRecursively()
        }
        Unit
    }

    private class JcaSigner : Orch8CapsuleSigner {
        private val keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair()
        val calls = AtomicInteger()
        override val keyId = "kmp-test-signer"
        override val publicKeyBase64: String =
            Base64.getEncoder().encodeToString(keyPair.public.encoded.takeLast(32).toByteArray())

        override fun signManifestSha256(digest: String): String {
            assertTrue(digest.matches(Regex("[0-9a-f]{64}")))
            calls.incrementAndGet()
            val signature = Signature.getInstance("Ed25519")
            signature.initSign(keyPair.private)
            signature.update(digest.toByteArray(Charsets.US_ASCII))
            return Base64.getEncoder().encodeToString(signature.sign())
        }
    }
}
