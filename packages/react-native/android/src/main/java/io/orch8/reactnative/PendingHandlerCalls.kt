package io.orch8.reactnative

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** Outstanding native-to-host replies, keyed by request id. */
internal class PendingHandlerCalls {
    class Outcome(val output: String?, val error: String?, val permanent: Boolean)

    private class Slot {
        val latch = CountDownLatch(1)

        @Volatile var outcome: Outcome? = null
    }

    private val slots = ConcurrentHashMap<String, Slot>()

    fun open(id: String) {
        slots[id] = Slot()
    }

    fun await(id: String, timeoutMs: Long): Outcome? {
        val slot = slots[id] ?: return null
        val done = slot.latch.await(timeoutMs, TimeUnit.MILLISECONDS)
        slots.remove(id)
        return if (done) slot.outcome else null
    }

    // Rust times out the logical operation, while its foreign call remains
    // physically charged until the host future settles.
    fun awaitSettlement(id: String): Outcome? {
        val slot = slots[id] ?: return null
        var interrupted = false
        while (true) {
            try { slot.latch.await(); break }
            catch (_: InterruptedException) { interrupted = true }
        }
        slots.remove(id)
        if (interrupted) Thread.currentThread().interrupt()
        return slot.outcome
    }

    fun cancel(id: String) {
        slots.remove(id)
    }

    fun resolve(id: String, output: String?, error: String?, permanent: Boolean) {
        val slot = slots[id] ?: return // timed out or unknown
        synchronized(slot) {
            if (slot.outcome != null) return
            slot.outcome = Outcome(output, error, permanent)
        }
        slot.latch.countDown()
    }

    fun failAll(message: String) {
        for (id in slots.keys.toList()) resolve(id, null, message, false)
    }
}
