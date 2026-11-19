package io.orch8.inspection

import android.app.Activity
import android.content.ClipData
import android.content.ClipboardManager
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.View
import android.widget.Button
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import io.orch8.mobile.InstanceState
import io.orch8.mobile.InstanceStateKind
import java.util.concurrent.Executors

class InspectionActivity : Activity() {
    private companion object {
        val worker = Executors.newSingleThreadExecutor()
        val uploadWorker = Executors.newSingleThreadExecutor()
    }
    private val windows = InspectionActivityWindows()
    private var inspection: Inspection? = null // Accessed on worker.
    private var currentId: String? = null
    private var exportPayload: String? = null
    private val handler = Handler(Looper.getMainLooper())
    private val reconnect by lazy {
        runCatching { InspectionReconnectConfiguration.load(applicationContext)?.let { InspectionSubmissionClient(it) } }
    }
    @Volatile private var reconnectWindow = 0
    private var uploads = emptyList<InspectionUpload>()
    private var uploadError: String? = null
    private lateinit var summary: TextView
    private lateinit var syncStatus: TextView
    private lateinit var retryUpload: Button
    private lateinit var start: Button
    private lateinit var passed: Button
    private lateinit var repair: Button
    private lateinit var refresh: Button
    private lateinit var export: Button

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val padding = (24 * resources.displayMetrics.density).toInt()
        val content = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(padding, padding, padding, padding)
            setOnApplyWindowInsetsListener { view, insets ->
                @Suppress("DEPRECATION")
                view.setPadding(padding, padding + insets.systemWindowInsetTop,
                    padding, padding + insets.systemWindowInsetBottom)
                insets
            }
        }
        setContentView(content)
        content.addView(TextView(this).apply { text = "Field inspection · Site A-12"; textSize = 24f })
        content.addView(TextView(this).apply { text = "Saved on this device" })
        summary = TextView(this).also { content.addView(it) }
        syncStatus = TextView(this).also { content.addView(it) }
        fun button(label: String, action: () -> Unit) = Button(this).also {
            it.text = label
            it.setOnClickListener { action() }
            content.addView(it)
        }
        retryUpload = button("Retry inspection upload", ::retrySelectedUpload).apply {
            visibility = View.GONE
        }
        start = button("Start inspection") { perform { store, window -> store.start(window) } }
        passed = button("Mark passed") { record(Inspection.Result.PASSED) }
        repair = button("Mark repair needed") { record(Inspection.Result.REPAIR_NEEDED) }
        export = button("Copy inspection export") {
            exportPayload?.let { json ->
                getSystemService(ClipboardManager::class.java)
                    ?.setPrimaryClip(ClipData.newPlainText("Inspection JSON", json))
                Toast.makeText(this, "Inspection JSON copied", Toast.LENGTH_SHORT).show()
            }
        }
        refresh = button("Refresh status", ::refresh)
    }

    override fun onStart() {
        super.onStart()
        windows.enter()
        refresh()
        reconnectWindow++
        uploadWhileStarted(reconnectWindow, 5_000L)
    }

    override fun onStop() {
        windows.leave()
        reconnectWindow++
        handler.removeCallbacksAndMessages(null)
        super.onStop()
    }

    private fun uploadWhileStarted(window: Int, delay: Long) {
        uploadWorker.execute {
            if (reconnectWindow != window) return@execute
            val client = reconnect.getOrElse {
                handler.post {
                    if (window == reconnectWindow) {
                        uploadError = "Could not read the inspection reconnect configuration."
                        updateUploadStatus()
                    }
                }
                return@execute
            } ?: return@execute
            val queue = InspectionUploadQueue(filesDir.resolve("inspection.db"))
            val result = runCatching { uploadInspections(queue, client) { reconnectWindow == window } }
            val entries = runCatching { queue.entries() }
            handler.post {
                if (window != reconnectWindow) return@post
                entries.onSuccess { uploads = it }
                uploadError = when (result.exceptionOrNull()) {
                    null -> null
                    is InspectionDestinationChanged -> "A queued inspection belongs to the previous server configuration."
                    else -> "Upload pending · Retrying automatically"
                }
                updateUploadStatus()
                val nextDelay = if (result.isSuccess) 5_000L else (delay * 2).coerceAtMost(30_000L)
                handler.postDelayed({ uploadWhileStarted(window, nextDelay) }, nextDelay)
            }
        }
    }

    private fun updateUploadStatus() {
        retryUpload.visibility = if (selectedRejectedUpload() == null) View.GONE else View.VISIBLE
        val execution = uploads.firstOrNull { it.evidence.instanceId.toString().equals(currentId, ignoreCase = true) }
            ?.executionId
        val pending = uploads.count { it.pending }
        syncStatus.text = when {
            uploadError != null -> uploadError
            execution != null -> "Received by server · $execution"
            uploads.any { it.rejectionStatus != null } -> "A queued inspection needs attention. The server rejected its evidence."
            uploads.any { it.rejected } -> "Server has different evidence for a queued inspection ID."
            pending != 0 -> "Pending upload · $pending"
            else -> ""
        }
    }

    private fun selectedRejectedUpload() = uploads.firstOrNull {
        it.rejectionStatus != null && it.evidence.instanceId.toString().equals(currentId, ignoreCase = true)
    }?.evidence?.instanceId

    private fun retrySelectedUpload() {
        val id = selectedRejectedUpload() ?: return
        val window = reconnectWindow
        uploadWorker.execute {
            if (window != reconnectWindow) return@execute
            val queue = InspectionUploadQueue(filesDir.resolve("inspection.db"))
            val result = runCatching {
                queue.retryRejected(id)
                queue.entries()
            }
            handler.post {
                if (window != reconnectWindow) return@post
                result.onSuccess { uploads = it }
                uploadError = if (result.isSuccess) null else "Could not queue the inspection for retry."
                updateUploadStatus()
            }
        }
    }

    private fun refresh() = perform { store, window -> store.current(window) }

    private fun record(result: Inspection.Result) {
        val id = currentId ?: return
        perform { store, window -> store.record(id, result, window) }
    }

    private fun perform(action: (Inspection, InspectionExecutionWindow) -> InstanceState?) {
        val window = windows.begin() ?: return
        listOf(start, passed, repair, export, refresh).forEach { it.isEnabled = false }
        summary.text = "Reading inspection…"
        worker.execute {
            val result = runCatching {
                window.remainingMilliseconds()
                val store = inspection ?: Inspection.open(applicationContext, filesDir.resolve("inspection.db"), window)
                    .also { inspection = it }
                val state = action(store, window)
                val answer = if (state?.state == InstanceStateKind.COMPLETED) store.recordedResult(state) else null
                val exported = if (state?.state == InstanceStateKind.COMPLETED) store.export(state) else null
                window.remainingMilliseconds()
                Triple(state, answer, exported)
            }
            runOnUiThread {
                if (!windows.finish(window) || isDestroyed) return@runOnUiThread
                result.onSuccess { (state, answer, exported) ->
                    currentId = state?.instanceId
                    updateUploadStatus()
                    exportPayload = exported
                    export.isEnabled = exported != null
                    val waiting = state?.state == InstanceStateKind.WAITING
                    start.isEnabled = state == null || state.state in setOf(
                        InstanceStateKind.COMPLETED, InstanceStateKind.FAILED, InstanceStateKind.CANCELLED,
                    )
                    passed.isEnabled = waiting
                    repair.isEnabled = waiting
                    summary.text = when (state?.state) {
                        null -> "Ready to start"
                        InstanceStateKind.WAITING -> "Waiting for the inspection result"
                        InstanceStateKind.COMPLETED -> if (answer == "passed") "Saved · Passed" else "Saved · Repair needed"
                        InstanceStateKind.FAILED -> "Inspection failed"
                        InstanceStateKind.CANCELLED -> "Inspection cancelled"
                        else -> "Inspection in progress"
                    }
                }.onFailure {
                    summary.text = "Could not read or save the inspection. Refresh to retry."
                }
                refresh.isEnabled = true
            }
        }
    }

    override fun onDestroy() {
        worker.execute { inspection?.close() }
        super.onDestroy()
    }
}
