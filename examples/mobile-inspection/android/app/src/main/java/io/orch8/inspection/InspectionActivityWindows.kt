package io.orch8.inspection

import android.os.SystemClock
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean

internal class InspectionExecutionWindow(
    private val deadline: Long,
    private val now: () -> Long,
) {
    private val cancelled = AtomicBoolean()

    fun remainingMilliseconds(): ULong {
        val remaining = deadline - now()
        if (cancelled.get() || remaining <= 0) throw CancellationException("Inspection window expired")
        return remaining.toULong()
    }

    fun expire() { cancelled.set(true) }
}

/** Main-thread-owned admission for the Activity, with cancellation visible to the worker thread. */
internal class InspectionActivityWindows(private val now: () -> Long = SystemClock::elapsedRealtime) {
    private var foreground = false
    private var active: InspectionExecutionWindow? = null

    fun enter() { foreground = true }

    fun leave() {
        foreground = false
        active?.expire()
        active = null
    }

    fun begin(): InspectionExecutionWindow? {
        if (!foreground || active != null) return null
        return InspectionExecutionWindow(now() + 5_000, now).also { active = it }
    }

    fun finish(window: InspectionExecutionWindow): Boolean {
        if (active !== window) return false
        active = null
        return true
    }
}
