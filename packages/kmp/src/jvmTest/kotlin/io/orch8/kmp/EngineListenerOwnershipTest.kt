package io.orch8.kmp

import java.lang.ref.WeakReference
import kotlinx.coroutines.flow.SharedFlow
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class EngineListenerOwnershipTest {
    private class Probe(val backend: FakeBackend, val engine: WeakReference<Orch8Engine>, val events: SharedFlow<EngineEvent>)
    private fun probe(): Probe {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend)
        return Probe(backend, WeakReference(engine), engine.events)
    }

    @Test
    fun retainedNativeListenerAndEventFlowDoNotPinTheEngineWrapper() {
        val probe = probe()
        repeat(100) {
            if (probe.engine.get() == null) return@repeat
            System.gc()
            Thread.sleep(20)
        }
        assertNull(probe.engine.get(), "a native listener's closure retains its engine wrapper")
        // A physically accepted late callback can still settle into its own
        // flow state after the wrapper is gone, without resurrecting the engine.
        probe.backend.sink(EngineEvent.InstanceCompleted("late", "{}"))
        assertEquals(0, probe.events.replayCache.size)
        assertNull(probe.engine.get())
    }
}
