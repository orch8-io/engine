package io.orch8.kmp

import io.orch8.kmp.internal.toUniffi
import io.orch8.mobile.MobileException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class HostTokenFailureTest {
    @Test
    fun directUniffiRefreshConvertsUnknownErrorsWithoutInvokingTheirMessageGetter() {
        val failure = ThrowingMessageException()
        val provider = object : Orch8TokenSource {
            override fun currentToken() = "token"
            override fun refreshToken(): String = throw failure
        }.toUniffi()
        assertFailsWith<MobileException.Engine> { provider.refreshToken() }
        assertEquals(0, failure.reads)
    }
}
