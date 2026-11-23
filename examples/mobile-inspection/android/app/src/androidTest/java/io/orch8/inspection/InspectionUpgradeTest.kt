package io.orch8.inspection

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import io.orch8.mobile.InstanceStateKind
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class InspectionUpgradeTest {
    @Test
    fun pendingSubmissionSurvivesPublishedSdkUpgrade() {
        val args = InstrumentationRegistry.getArguments()
        val phase = args.getString("upgradePhase")
        assumeTrue(phase == "queue" || phase == "submit" || phase == "deliver")
        assertEquals(args.getString("sdkVersion"), BuildConfig.ORCH8_VERSION)

        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val directory = File(context.filesDir, "published-upgrade").apply { mkdirs() }
        val database = File(directory, "inspection.db")
        val identityFile = File(directory, "submission-id")
        val queue = InspectionUploadQueue(database)
        if (phase == "queue") {
            assertFalse(identityFile.exists())
            Inspection.open(context, database).use { inspection ->
                val waiting = inspection.start()
                assertEquals(InstanceStateKind.WAITING, waiting.state)
                val completed = inspection.record(waiting.instanceId, Inspection.Result.PASSED)
                assertEquals(InstanceStateKind.COMPLETED, completed.state)
                val queued = queue.entries().single()
                assertTrue(queued.pending)
                assertEquals(completed.instanceId, queued.evidence.instanceId.toString())
                identityFile.writeText(completed.instanceId)
            }
            return
        }

        val originalId = identityFile.readText()
        Inspection.open(context, database).use { inspection ->
            val run = requireNotNull(inspection.current())
            assertEquals(originalId, run.instanceId)
            assertEquals(InstanceStateKind.COMPLETED, run.state)
            assertEquals("passed", inspection.recordedResult(run))
        }
        val pending = queue.entries().single()
        assertTrue(pending.pending)
        assertEquals(originalId, pending.evidence.instanceId.toString())
        val destination = InspectionDestination(
            requireNotNull(args.getString("receiver")),
            "upgrade",
            UUID.fromString(requireNotNull(args.getString("sequence"))),
        )
        val client = InspectionSubmissionClient(
            InspectionReconnectConfiguration(destination, args.getString("apiKey")),
        )
        if (phase == "submit") {
            val missingAcknowledgement = assertThrows(InspectionServerStatus::class.java) {
                uploadInspections(queue, client) { true }
            }
            assertEquals(503, missingAcknowledgement.status)
            assertEquals(pending.copy(destination = destination), InspectionUploadQueue(database).entries().single())
            return
        }

        assertEquals(destination, pending.destination)
        uploadInspections(queue, client) { true }
        val receipt = queue.entries().single()
        assertFalse(receipt.pending)
        assertEquals(originalId, receipt.evidence.instanceId.toString())
        File(directory, "receipt-id").writeText(requireNotNull(receipt.executionId).toString())
        uploadInspections(InspectionUploadQueue(database), client) { true }
        assertEquals(receipt, InspectionUploadQueue(database).entries().single())
    }
}
