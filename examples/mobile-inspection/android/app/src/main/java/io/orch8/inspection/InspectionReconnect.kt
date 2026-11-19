package io.orch8.inspection

import android.content.Context
import org.json.JSONObject
import java.io.File
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URI
import java.util.UUID

private fun parseId(value: String): UUID = UUID.fromString(value).also {
    require(it.toString().equals(value, ignoreCase = true)) { "Invalid inspection identifier" }
}

internal data class InspectionEvidence(
    val instanceId: UUID,
    val siteId: String,
    val inspectionResult: String,
    val siteTagVerified: Boolean? = null,
) {
    fun json(): JSONObject = JSONObject()
        .put("instance_id", instanceId.toString())
        .put("site_id", siteId)
        .put("inspection_result", inspectionResult)
        .apply { if (siteTagVerified != null) put("site_tag_verified", siteTagVerified) }

    companion object {
        fun read(json: JSONObject) = InspectionEvidence(
            parseId(json.get("instance_id") as String),
            json.get("site_id") as String,
            json.get("inspection_result") as String,
            if (json.has("site_tag_verified")) json.get("site_tag_verified") as Boolean else null,
        )
    }
}

internal data class InspectionDestination(val url: String, val tenant: String, val sequenceId: UUID) {
    fun json(): JSONObject = JSONObject().put("url", url).put("tenant", tenant)
        .put("sequence_id", sequenceId.toString())

    companion object {
        fun read(json: JSONObject) = InspectionDestination(
            json.getString("url"), json.getString("tenant"), parseId(json.getString("sequence_id")),
        )
    }
}

internal data class InspectionReconnectConfiguration(val destination: InspectionDestination, val apiKey: String?) {
    companion object {
        fun load(context: Context): InspectionReconnectConfiguration? {
            val json = try {
                val source = context.assets.open("reconnect-config.json").bufferedReader().use { it.readText() }
                JSONObject(source)
            } catch (_: FileNotFoundException) {
                return null
            }
            return InspectionReconnectConfiguration(
                InspectionDestination.read(json), if (json.has("api_key")) json.getString("api_key") else null,
            )
        }
    }
}

internal data class InspectionUpload(
    val evidence: InspectionEvidence,
    val destination: InspectionDestination? = null,
    val executionId: UUID? = null,
    val rejected: Boolean = false,
    val rejectionStatus: Int? = null,
) {
    val pending: Boolean get() = executionId == null && !rejected
}

internal class InspectionUploadQueue(database: File) {
    private val directory = File(database.path + ".uploads")

    fun enqueue(evidence: InspectionEvidence) = synchronized(lock) {
        val upload = read(evidence.instanceId)
        if (upload == null) {
            save(InspectionUpload(evidence))
        } else if (upload.evidence != evidence) {
            throw InspectionEvidenceConflict()
        }
    }

    fun entries(): List<InspectionUpload> = synchronized(lock) {
        if (!directory.exists()) return@synchronized emptyList()
        val files = directory.listFiles() ?: throw IOException("Could not read inspection upload queue")
        files.sortedBy { it.name }.filter { it.extension == "json" }
            .mapNotNull { file ->
                val id = try {
                    parseId(file.nameWithoutExtension)
                } catch (_: IllegalArgumentException) {
                    return@mapNotNull null
                }
                read(id)
            }
    }

    fun prepare(id: UUID, destination: InspectionDestination): InspectionUpload? = synchronized(lock) {
        val entry = read(id)?.takeIf { it.pending } ?: return@synchronized null
        if (entry.destination == null) {
            return@synchronized entry.copy(destination = destination).also(::save)
        }
        if (entry.destination != destination) throw InspectionDestinationChanged()
        entry
    }

    fun record(upload: InspectionUpload, executionId: UUID?, rejectionStatus: Int? = null) = synchronized(lock) {
        val entry = read(upload.evidence.instanceId)?.takeIf { it.pending } ?: return@synchronized
        save(entry.copy(executionId = executionId, rejected = executionId == null, rejectionStatus = rejectionStatus))
    }

    fun retryRejected(id: UUID) = synchronized(lock) {
        val entry = read(id)?.takeIf { it.executionId == null && it.rejectionStatus != null }
            ?: return@synchronized
        save(entry.copy(rejected = false, rejectionStatus = null))
    }

    private fun path(id: UUID): File = File(directory, "$id.json")

    private fun read(id: UUID): InspectionUpload? {
        val file = path(id)
        if (!file.exists()) return null
        val json = JSONObject(file.readText())
        return InspectionUpload(
            evidence = InspectionEvidence.read(json.getJSONObject("evidence")).also {
                if (it.instanceId != id) throw InspectionEvidenceConflict()
            },
            destination = json.optJSONObject("destination")?.let(InspectionDestination::read),
            executionId = json.optString("execution_id").takeIf { it.isNotEmpty() }?.let(::parseId),
            rejected = json.optBoolean("rejected"),
            rejectionStatus = json.optInt("rejection_status").takeIf { it != 0 },
        )
    }

    private fun save(upload: InspectionUpload) {
        if (!directory.mkdirs() && !directory.isDirectory) throw IOException("Could not open inspection upload queue")
        val data = JSONObject().put("evidence", upload.evidence.json())
            .put("destination", upload.destination?.json())
            .put("execution_id", upload.executionId?.toString())
            .put("rejected", upload.rejected)
            .put("rejection_status", upload.rejectionStatus)
        val temporary = File.createTempFile("upload-", ".tmp", directory)
        try {
            FileOutputStream(temporary).use { stream ->
                stream.write(data.toString().toByteArray(Charsets.UTF_8))
                stream.fd.sync()
            }
            if (!temporary.renameTo(path(upload.evidence.instanceId))) {
                throw IOException("Could not commit inspection upload queue")
            }
        } finally {
            temporary.delete()
        }
    }

    private companion object {
        val lock = Any()
    }
}

internal class InspectionEvidenceConflict : IOException("An inspection ID already has different evidence")
internal class InspectionDestinationChanged : IOException("A pending upload belongs to a different server")
internal class InspectionServerStatus(val status: Int) : IOException("Engine returned HTTP $status")

internal class InspectionSubmissionClient(
    configuration: InspectionReconnectConfiguration,
    private val transport: (URI, Map<String, String>, JSONObject?) -> JSONObject = ::sendInspectionRequest,
) {
    val destination = configuration.destination
    private val base: URI
    private val headers: Map<String, String>

    init {
        val url = URI(destination.url)
        require(url.scheme == "https" && !url.host.isNullOrBlank() && url.userInfo == null &&
            url.query == null && url.fragment == null && destination.tenant.matches(Regex("[A-Za-z0-9_-]{1,64}"))
        ) { "Invalid reconnect destination" }
        require(configuration.apiKey?.any { it == '\r' || it == '\n' } != true)
        val trimmed = url.toString().trimEnd('/')
        base = URI(if (trimmed.endsWith("/api/v1")) trimmed else "$trimmed/api/v1")
        headers = buildMap {
            put("X-Tenant-Id", destination.tenant)
            put("Content-Type", "application/json")
            configuration.apiKey?.let { put("X-API-Key", it) }
        }
    }

    fun submit(evidence: InspectionEvidence): UUID {
        val payload = JSONObject()
            .put("sequence_id", destination.sequenceId.toString())
            .put("tenant_id", destination.tenant)
            .put("namespace", "default")
            .put("idempotency_key", "field-inspection:${evidence.instanceId}")
            .put("context", JSONObject().put("data", evidence.json()))
        val response = transport(URI("$base/instances"), headers, payload)
        val execution = parseId(response.getString("id"))
        if (response.opt("deduplicated") != false) {
            val stored = transport(URI("$base/instances/$execution"), headers, null)
            val original = InspectionEvidence.read(stored.getJSONObject("context").getJSONObject("data"))
            if (parseId(stored.getString("sequence_id")) != destination.sequenceId || original != evidence) {
                throw InspectionEvidenceConflict()
            }
        }
        return execution
    }
}

internal fun uploadInspections(queue: InspectionUploadQueue, client: InspectionSubmissionClient, active: () -> Boolean) {
    var deferredError: IOException? = null
    for (entry in queue.entries().filter { it.pending }) {
        if (!active()) return
        val upload = try {
            queue.prepare(entry.evidence.instanceId, client.destination) ?: continue
        } catch (error: InspectionDestinationChanged) {
            deferredError = error
            continue
        }
        val execution = try {
            client.submit(upload.evidence)
        } catch (_: InspectionEvidenceConflict) {
            null
        } catch (error: InspectionServerStatus) {
            if (error.status != 400 && error.status != 413 && error.status != 422) throw error
            queue.record(upload, null, error.status)
            continue
        }
        queue.record(upload, execution)
    }
    deferredError?.let { throw it }
}

private fun sendInspectionRequest(url: URI, headers: Map<String, String>, payload: JSONObject?): JSONObject {
    val connection = url.toURL().openConnection() as HttpURLConnection
    try {
        connection.instanceFollowRedirects = false
        connection.useCaches = false
        connection.connectTimeout = 10_000
        connection.readTimeout = 10_000
        connection.requestMethod = if (payload == null) "GET" else "POST"
        headers.forEach(connection::setRequestProperty)
        if (payload != null) {
            val data = payload.toString().toByteArray(Charsets.UTF_8)
            connection.doOutput = true
            connection.setFixedLengthStreamingMode(data.size)
            connection.outputStream.use { it.write(data) }
        }
        val status = connection.responseCode
        if (status !in 200..299) throw InspectionServerStatus(status)
        return connection.inputStream.use { stream ->
            val buffer = ByteArray(8_192)
            val body = java.io.ByteArrayOutputStream()
            var count = stream.read(buffer)
            while (count != -1) {
                if (body.size() + count > 1_048_576) throw IOException("Engine response exceeded the inspection limit")
                body.write(buffer, 0, count)
                count = stream.read(buffer)
            }
            JSONObject(body.toString("UTF-8"))
        }
    } finally {
        connection.disconnect()
    }
}
