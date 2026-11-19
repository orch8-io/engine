package io.orch8.inspection

import android.content.Context
import android.util.AtomicFile
import io.orch8.mobile.InstanceState
import io.orch8.mobile.InstanceStateKind
import io.orch8.mobile.MobileEngine
import io.orch8.mobile.MobileEngineConfig
import io.orch8.mobile.StepHandler
import org.json.JSONObject
import java.io.File
import java.io.FileNotFoundException

internal class Inspection private constructor(
    private val engine: MobileEngine,
    private val selectedRun: AtomicFile,
    private val uploads: InspectionUploadQueue,
) : AutoCloseable {
    enum class Result(val value: String) { PASSED("passed"), REPAIR_NEEDED("repair_needed") }

    fun current(window: InspectionExecutionWindow? = null): InstanceState? {
        window?.remainingMilliseconds()
        val active = engine.activeInstances().firstOrNull { it.sequenceName == SEQUENCE }
        if (active != null) {
            remember(active.instanceId)
            return advance(active.instanceId, window)
        }
        val id = selectedId() ?: return null
        return advance(id, window)
    }

    private fun selectedId(): String? = try {
        selectedRun.openRead().bufferedReader().use { it.readText() }
    } catch (_: FileNotFoundException) {
        null
    }

    fun start(window: InspectionExecutionWindow? = null): InstanceState {
        window?.remainingMilliseconds()
        // Recover an earlier completion before changing the selected run.
        selectedId()?.let { queueCompletion(engine.getInstance(it)) }
        val id = engine.start(SEQUENCE, """{"site_id":"A-12"}""", "inspection:A-12")
        remember(id)
        return advance(id, window)
    }

    fun record(id: String, result: Result, window: InspectionExecutionWindow? = null): InstanceState {
        window?.remainingMilliseconds()
        engine.completeStep(id, "record_inspection", JSONObject().put("value", result.value).toString())
        return advance(id, window)
    }

    fun recordedResult(state: InstanceState): String =
        JSONObject(state.context).getString("inspection_result")

    fun export(state: InstanceState): String {
        require(state.state == InstanceStateKind.COMPLETED && state.sequenceName == SEQUENCE) {
            "Only completed inspections can be exported"
        }
        return JSONObject()
            .put("instance_id", state.instanceId)
            .put("site_id", "A-12")
            .put("inspection_result", recordedResult(state))
            .toString()
    }

    private fun advance(id: String, window: InspectionExecutionWindow?): InstanceState {
        engine.runUntilIdle(20u, window?.remainingMilliseconds() ?: 5_000uL)
        window?.remainingMilliseconds()
        return engine.getInstance(id).also(::queueCompletion)
    }

    private fun queueCompletion(state: InstanceState) {
        if (state.state != InstanceStateKind.COMPLETED || state.sequenceName != SEQUENCE) return
        uploads.enqueue(InspectionEvidence.read(JSONObject(export(state))))
    }

    private fun remember(id: String) {
        val stream = selectedRun.startWrite()
        try {
            stream.write(id.toByteArray(Charsets.UTF_8))
            selectedRun.finishWrite(stream)
        } catch (error: Exception) {
            selectedRun.failWrite(stream)
            throw error
        }
    }

    override fun close() {
        try {
            engine.shutdown()
        } finally {
            engine.close()
        }
    }

    companion object {
        private const val SEQUENCE = "field-inspection-offline"

        fun open(context: Context, database: File, window: InspectionExecutionWindow? = null): Inspection {
            val config = MobileEngineConfig(
                tickIntervalMs = 500uL,
                maxConcurrentSteps = 1u,
                maxStepsPerInstance = 20u,
                maxConcurrentInstances = 1u,
                maxTickDurationMs = 5_000uL,
                maxInstanceLifetimeSecs = 86_400uL,
                maxStoredSequences = 5u,
                maxSequenceSizeBytes = 65_536uL,
                handlerTimeoutMs = 5_000uL,
                operationTimeoutMs = 10_000uL,
                telemetryEnabled = false,
                telemetryUrl = "",
                environment = "production",
                rootPublicKey = "",
                sdkVersion = BuildConfig.ORCH8_VERSION,
                memoryBudgetBytes = 0uL,
                sequencesUrl = "",
                syncUrl = "",
                deviceId = "",
                syncApiKey = "",
            )
            window?.remainingMilliseconds()
            val engine = MobileEngine(database.absolutePath, config)
            val inspection = Inspection(
                engine, AtomicFile(File(database.path + ".selected-run")), InspectionUploadQueue(database),
            )
            try {
                window?.remainingMilliseconds()
                engine.registerHandler("save_inspection", object : StepHandler {
                    override fun execute(stepName: String, input: String): String = "{}"
                })
                window?.remainingMilliseconds()
                if (engine.loadedSequences().none { it.name == SEQUENCE && it.version == 1 }) {
                    val workflow = context.assets.open("field-inspection-offline.json")
                        .bufferedReader().use { it.readText() }
                    window?.remainingMilliseconds()
                    engine.loadSequenceFromJson(workflow)
                }
                return inspection
            } catch (error: Exception) {
                inspection.close()
                throw error
            }
        }
    }
}
