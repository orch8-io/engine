package io.orch8.kmp

import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

internal class ThrowingMessageException : RuntimeException() {
    var reads = 0
    override val message: String get() { reads++; error("HOST_EXCEPTION_MESSAGE_GETTER") }
}

class HostCallbackFailureTest {
    @Test
    fun unknownHandlerFailureDoesNotInvokeItsMessageGetter() {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend)
        val failure = ThrowingMessageException()
        engine.registerHandler("broken", Orch8StepHandler { _, _ -> throw failure })
        try {
            val reported = assertFailsWith<Orch8HandlerException.Retryable> { backend.handlers.getValue("broken").execute("work", "{}") }
            assertEquals(0, failure.reads)
            assertSame(failure, reported.cause)
        } finally { engine.close() }
    }

    @Test
    fun handlerCancellationDoesNotInvokeAnOverriddenMessageGetter() {
        val backend = FakeBackend()
        val engine = Orch8Engine(backend)
        var reads = 0
        val failure = object : CancellationException() {
            override val message: String get() { reads++; error("CANCELLATION_MESSAGE_GETTER") }
        }
        engine.registerHandler("cancelled", Orch8StepHandler { _, _ -> throw failure })
        try {
            val reported = assertFailsWith<Orch8HandlerException.Retryable> { backend.handlers.getValue("cancelled").execute("work", "{}") }
            assertEquals(0, reads)
            assertSame(failure, reported.cause)
        } finally { engine.close() }
    }
    @Test
    fun declaredHandlerErrorsKeepTheirOriginalIdentityAndMessage() {
        val backend=FakeBackend();val engine=Orch8Engine(backend)
        try {
            for (failure in listOf(Orch8HandlerException.Permanent("permanent-detail"),Orch8HandlerException.Retryable("retry-detail"))) {
                engine.registerHandler("typed",Orch8StepHandler {_,_->throw failure})
                val observed=assertFailsWith<Orch8HandlerException> { backend.handlers.getValue("typed").execute("work","{}") }
                assertSame(failure,observed)
            }
        } finally { engine.close() }
    }

    @Test
    fun aSubclassOfAStandardExceptionStillCannotInjectAMessageGetter() {
        val backend=FakeBackend();val engine=Orch8Engine(backend);var reads=0
        val failure=object:IllegalStateException("hidden") {
            override val message:String get() {reads++;error("SUBCLASS_MESSAGE_GETTER")}
        }
        engine.registerHandler("subclass",Orch8StepHandler {_,_->throw failure})
        try {
            assertFailsWith<Orch8HandlerException.Retryable> {backend.handlers.getValue("subclass").execute("work","{}")}
            assertEquals(0,reads)
        } finally {engine.close()}
    }

}
