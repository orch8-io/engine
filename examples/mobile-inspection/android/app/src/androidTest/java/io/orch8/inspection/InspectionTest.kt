package io.orch8.inspection

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import io.orch8.mobile.InstanceStateKind
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class InspectionTest {
    @Test
    fun recordsAnOfflineInspectionAfterReopeningTheDatabase() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val directory = File(context.cacheDir, UUID.randomUUID().toString()).apply { mkdirs() }
        val database = File(directory, "inspection.db")
        try {
            val waiting = Inspection.open(context, database).use { inspection ->
                assertNull(inspection.current())
                inspection.start().also {
                    assertEquals(InstanceStateKind.WAITING, it.state)
                    assertThrows(IllegalArgumentException::class.java) { inspection.export(it) }
                }
            }
            Inspection.open(context, database).use { inspection ->
                assertEquals(waiting.instanceId, inspection.current()?.instanceId)
                val completed = inspection.record(waiting.instanceId, Inspection.Result.PASSED)
                assertEquals(InstanceStateKind.COMPLETED, completed.state)
                assertEquals("passed", inspection.recordedResult(completed))
            }
            Inspection.open(context, database).use { inspection ->
                val completed = requireNotNull(inspection.current())
                assertEquals(waiting.instanceId, completed.instanceId)
                assertEquals(InstanceStateKind.COMPLETED, completed.state)
                val exported = JSONObject(inspection.export(completed))
                assertEquals(setOf("instance_id", "site_id", "inspection_result"), exported.keys().asSequence().toSet())
                assertEquals(waiting.instanceId, exported.getString("instance_id"))
                assertEquals("A-12", exported.getString("site_id"))
                assertEquals("passed", exported.getString("inspection_result"))
            }
        } finally {
            directory.deleteRecursively()
        }
    }
}
