package io.orch8.inspection

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.io.IOException
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class InspectionReconnectTest {
    @Test
    fun resumesACompletedInspectionAfterServerAcceptsAndResponseIsLost() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val directory = File(context.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        try {
            val database = File(directory, "inspection.db")
            val localId = Inspection.open(context, database).use { inspection ->
                inspection.start().also { inspection.record(it.instanceId, Inspection.Result.PASSED) }.instanceId
            }
            val execution = UUID.randomUUID()
            val sequence = UUID.randomUUID()
            var posts = 0
            var lookups = 0
            var accepted: JSONObject? = null
            val config = InspectionReconnectConfiguration(
                InspectionDestination("https://inspection.example", "demo", sequence), null,
            )
            val client = InspectionSubmissionClient(config) { url, headers, body ->
                assertEquals("demo", headers["X-Tenant-Id"])
                if (body == null) {
                    lookups++
                    assertEquals("/api/v1/instances/$execution", url.path)
                    JSONObject().put("sequence_id", sequence.toString())
                        .put("context", requireNotNull(accepted).getJSONObject("context"))
                } else {
                    posts++
                    assertEquals("/api/v1/instances", url.path)
                    assertEquals("field-inspection:${UUID.fromString(localId)}", body.getString("idempotency_key"))
                    val previous = accepted
                    if (previous == null) {
                        accepted = body
                        throw IOException("Lost the accepted response")
                    }
                    assertEquals(previous.toString(), body.toString())
                    JSONObject().put("id", execution.toString()).put("deduplicated", true)
                }
            }
            val queue = InspectionUploadQueue(database)
            assertThrows(IOException::class.java) { uploadInspections(queue, client) { true } }
            assertTrue(queue.entries().single().pending)
            assertNull(queue.entries().single().executionId)

            Inspection.open(context, database).use { inspection ->
                assertEquals(localId, inspection.current()?.instanceId)
            }
            val reopened = InspectionUploadQueue(database)
            uploadInspections(reopened, client) { true }
            assertEquals(execution, reopened.entries().single().executionId)
            uploadInspections(InspectionUploadQueue(database), client) { true }
            assertEquals(2, posts)
            assertEquals(1, lookups)
        } finally {
            directory.deleteRecursively()
        }
    }

    @Test
    fun reopenedQueueKeepsDestinationAndRejectsMisnamedEvidence() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val directory = File(context.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        try {
            val database = File(directory, "inspection.db")
            val evidence = InspectionEvidence(UUID.randomUUID(), "A-12", "passed")
            val destination = InspectionDestination("https://inspection.example", "demo", UUID.randomUUID())
            val queue = InspectionUploadQueue(database)
            queue.enqueue(evidence)
            queue.prepare(evidence.instanceId, destination)
            val reopened = InspectionUploadQueue(database)
            val wrong = InspectionSubmissionClient(
                InspectionReconnectConfiguration(destination.copy(tenant = "other"), null),
            ) { _, _, _ -> error("No request should go to the wrong tenant") }
            assertThrows(InspectionDestinationChanged::class.java) { uploadInspections(reopened, wrong) { true } }
            assertEquals(destination, reopened.entries().single().destination)
            assertTrue(reopened.entries().single().pending)

            val folder = File(database.path + ".uploads")
            File(folder, "notes.json").writeText("unrelated")
            assertEquals(evidence, reopened.entries().single().evidence)
            val original = File(folder, "${evidence.instanceId}.json")
            assertTrue(original.renameTo(File(folder, "${UUID.randomUUID()}.json")))
            assertThrows(InspectionEvidenceConflict::class.java) { reopened.entries() }
        } finally {
            directory.deleteRecursively()
        }
    }

    @Test
    fun rejectedSubmissionSurvivesRestartAndDoesNotBlockTheNextUpload() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val directory = File(context.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        try {
            val database = File(directory, "inspection.db")
            val first = UUID.fromString("00000000-0000-4000-8000-000000000001")
            val second = UUID.fromString("00000000-0000-4000-8000-000000000002")
            val accepted = UUID.randomUUID()
            val queue = InspectionUploadQueue(database)
            for (id in listOf(first, second)) queue.enqueue(InspectionEvidence(id, "A-12", "passed"))
            val attempts = mutableListOf<UUID>()
            val destination = InspectionDestination("https://inspection.example", "demo", UUID.randomUUID())
            val client = InspectionSubmissionClient(InspectionReconnectConfiguration(destination, null)) { _, _, body ->
                val id = UUID.fromString(requireNotNull(body).getString("idempotency_key").removePrefix("field-inspection:"))
                attempts.add(id)
                if (id == first) throw InspectionServerStatus(422)
                JSONObject().put("id", accepted.toString()).put("deduplicated", false)
            }
            uploadInspections(queue, client) { true }
            uploadInspections(InspectionUploadQueue(database), client) { true }
            val reopened = InspectionUploadQueue(database).entries()
            assertEquals(listOf(first, second), attempts)
            assertTrue(reopened.none { it.pending })
            assertEquals(422, reopened.first().rejectionStatus)
            assertEquals(accepted, reopened.last().executionId)
            queue.retryRejected(first)
            queue.retryRejected(second)
            val pending = InspectionUploadQueue(database).entries().filter { it.pending }
            assertEquals(listOf(first), pending.map { it.evidence.instanceId })
            assertEquals(destination, pending.single().destination)
        } finally {
            directory.deleteRecursively()
        }
    }
}
