package io.orch8.reactnative

import android.system.ErrnoException
import android.system.Os
import android.system.OsConstants
import android.system.StructStat
import java.io.EOFException
import java.io.FileDescriptor
import java.security.MessageDigest

internal object MediaFileRange {
    private const val MAX_BYTES = 4 * 1024 * 1024
    private const val MAX_SAFE_INTEGER = 9_007_199_254_740_991L
    private const val MAX_CHUNKS = 65_536

    class SourceChanged : Exception("Media file no longer matches the queued upload")

    data class Manifest(
        val length: Long, val identity: String, val chunkBytes: Int,
        val sha256: List<String>, val fileSha256: String,
    )

    fun describe(path: String, chunkBytes: Double): Manifest {
        val count = checkedLength(chunkBytes)
        return withFile(path) { fd, info ->
            val length = info.st_size
            require(length in 1..MAX_SAFE_INTEGER && (length - 1) / count < MAX_CHUNKS) {
                "Invalid media manifest size"
            }
            val identity = identityOf(info)
            val hashes = mutableListOf<String>()
            val whole = MessageDigest.getInstance("SHA-256")
            var offset = 0L
            while (offset < length) {
                val size = minOf(count.toLong(), length - offset).toInt()
                val bytes = read(fd, offset, size)
                hashes.add(sha256(bytes))
                whole.update(bytes)
                offset += size
            }
            check(identityOf(Os.fstat(fd)) == identity) { "Media file changed during inspection" }
            Manifest(length, identity, count, hashes, hex(whole.digest()))
        }
    }

    fun read(path: String, offset: Double, length: Double, identity: String?, expectedSha256: String?): ByteArray {
        val start = offset.toLong()
        val count = checkedLength(length)
        require(start.toDouble() == offset && start in 0..MAX_SAFE_INTEGER - count) {
            "Invalid media file byte range"
        }
        return withFile(path) { fd, info ->
            if (identity != null && identityOf(info) != identity) throw SourceChanged()
            if (info.st_size < start + count) throw EOFException("Media source ended before the requested range")
            val bytes = read(fd, start, count)
            if (identity != null && identityOf(Os.fstat(fd)) != identity) throw SourceChanged()
            if (expectedSha256 != null && sha256(bytes) != expectedSha256) throw SourceChanged()
            bytes
        }
    }

    private fun checkedLength(length: Double): Int {
        val count = length.toInt()
        require(count.toDouble() == length && count in 1..MAX_BYTES) { "Invalid media byte range" }
        return count
    }

    private inline fun <T> withFile(path: String, body: (FileDescriptor, StructStat) -> T): T {
        require(path.startsWith("/") && '\u0000' !in path) { "Invalid media file path" }
        val fd = Os.open(path, OsConstants.O_RDONLY or OsConstants.O_NONBLOCK or OsConstants.O_CLOEXEC, 0)
        try {
            val info = Os.fstat(fd)
            require(OsConstants.S_ISREG(info.st_mode)) { "Media source is not a regular file" }
            return body(fd, info)
        } finally {
            Os.close(fd)
        }
    }

    private fun identityOf(info: StructStat): String =
        "${info.st_dev}:${info.st_ino}:${info.st_size}:${info.st_mtime}:${info.st_ctime}"

    private fun sha256(bytes: ByteArray): String =
        hex(MessageDigest.getInstance("SHA-256").digest(bytes))

    private fun hex(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it) }

    private fun read(fd: FileDescriptor, start: Long, count: Int): ByteArray {
        val bytes = ByteArray(count)
        var filled = 0
        while (filled < count) {
            val read = try {
                Os.pread(fd, bytes, filled, count - filled, start + filled)
            } catch (error: ErrnoException) {
                if (error.errno == OsConstants.EINTR) continue
                throw error
            }
            if (read == 0) throw EOFException("Media source ended before the requested range")
            filled += read
        }
        return bytes
    }
}
