package io.orch8.inspection

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CancellationException

class ActivityWindowTest {
    @Test
    fun budgetCannotBeUsedAfterDeadline() {
        var now = 100L
        val lifecycle = InspectionActivityWindows { now }
        lifecycle.enter()
        val window = requireNotNull(lifecycle.begin())
        now += 5_000
        assertThrows(CancellationException::class.java) { window.remainingMilliseconds() }
        assertTrue(lifecycle.finish(window))
    }

    @Test
    fun stoppedActivityCannotPublishAndNewWindowGetsItsOwnBudget() {
        var now = 100L
        val lifecycle = InspectionActivityWindows { now }
        assertNull(lifecycle.begin())
        lifecycle.enter()
        val old = requireNotNull(lifecycle.begin())
        assertNull(lifecycle.begin())
        now += 4_500
        assertEquals(500uL, old.remainingMilliseconds())
        lifecycle.leave()
        assertThrows(CancellationException::class.java) { old.remainingMilliseconds() }
        lifecycle.enter()
        val current = requireNotNull(lifecycle.begin())
        assertFalse(lifecycle.finish(old))
        assertEquals(5_000uL, current.remainingMilliseconds())
        assertTrue(lifecycle.finish(current))
    }
}
