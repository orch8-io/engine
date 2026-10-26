package io.orch8.kmp

import io.orch8.kmp.internal.UniffiBackend
import io.orch8.mobile.MobileEngine
import io.orch8.mobile.NoHandle
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class NativeOwnerFailureTest {
    @Test
    fun failingShutdownStillDisposesNativeOwnerAndPreservesTheFailure() {
        val failure=IllegalStateException("injected native shutdown failure")
        val native=object:MobileEngine(NoHandle) {
            var disposals=0
            override fun shutdown():Unit=throw failure
            override fun close() {disposals++;super.close()}
        }
        val backend=UniffiBackend(native)
        assertSame(failure,assertFailsWith<IllegalStateException> {backend.shutdown()})
        assertTrue(native.uniffiIsDestroyed)
        assertEquals(1,native.disposals)
        backend.shutdown()
        assertEquals(1,native.disposals)
        assertEquals(Orch8ErrorKind.SHUTDOWN,assertFailsWith<Orch8Exception> {backend.nodeRuntimeId()}.kind)
    }
}
