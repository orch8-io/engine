package io.orch8.flutter

import io.orch8.mobile.MobileEngine
import io.orch8.mobile.MobileEngineConfig
import io.orch8.mobile.MobileException
import java.lang.ref.WeakReference
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/** Serializes native construction/retirement without holding a state lock in native code. */
internal class NativeEngineLifecycle(
    private val lane: ExecutorService = Executors.newSingleThreadExecutor { task ->
        Thread(task, "orch8-native-lifecycle").apply { isDaemon = true }
    },
    private val create: (String, MobileEngineConfig) -> MobileEngine = ::MobileEngine,
) {
    internal class Scope(owner: NativeEngineLifecycle) {
        private val owner = WeakReference(owner)
        fun isCurrent(): Boolean = owner.get()?.isCurrent(this) == true
    }
    private class Entry(val engine: MobileEngine, val scope: Scope)
    private val lock = Any()
    private var desired: Scope? = null
    private var entry: Entry? = null
    private var closed = false
    val engine: MobileEngine? get() = synchronized(lock) { entry?.engine }
    val isClosed: Boolean get() = synchronized(lock) { closed }
    private fun isCurrent(scope: Scope) = synchronized(lock) { !closed && entry?.scope === scope }
    private fun wanted(scope: Scope) = synchronized(lock) { !closed && desired === scope }
    fun scopeOf(engine: MobileEngine): Scope = synchronized(lock) {
        entry?.takeIf { it.engine === engine }?.scope
            ?: throw MobileException.Shutdown("engine retired")
    }

    fun initialize(path: String, config: MobileEngineConfig, retired: () -> Unit,
                   configure: (MobileEngine, Scope) -> Unit, complete: (Exception?) -> Unit) {
        var rejected = false
        synchronized(lock) {
            if (closed) rejected = true
            else {
                val scope = Scope(this)
                desired = scope
                val previous = entry?.engine
                entry = null
                // This callback only retires pending host replies; it performs no native work.
                retired()
                lane.execute {
                    var candidate: MobileEngine? = null
                    var failure: Exception? = null
                    try {
                        dispose(previous)
                        if (!wanted(scope)) throw MobileException.Shutdown("initialization superseded")
                        val created = create(path, config)
                        candidate = created
                        if (!wanted(scope)) throw MobileException.Shutdown("initialization superseded")
                        configure(created, scope)
                        val published = synchronized(lock) {
                            if (closed || desired !== scope) false
                            else { entry = Entry(created, scope); true }
                        }
                        if (!published) throw MobileException.Shutdown("initialization superseded")
                        candidate = null // ownership transferred to entry
                    } catch (error: Exception) { failure = error }
                    finally {
                        try { dispose(candidate) } catch (error: Exception) {
                            if (failure == null) failure = error else if (failure !== error) failure.addSuppressed(error)
                        }
                    }
                    complete(failure)
                }
            }
        }
        if (rejected) complete(MobileException.Shutdown("bridge detached"))
    }

    fun shutdown(permanent: Boolean, retired: () -> Unit, complete: (Exception?) -> Unit) {
        var alreadyClosed = false
        synchronized(lock) {
            if (closed) alreadyClosed = true
            else {
                closed = permanent
                desired = null
                val previous = entry?.engine
                entry = null
                retired()
                lane.execute {
                    val failure = try { dispose(previous); null } catch (error: Exception) { error }
                    complete(failure)
                }
                if (permanent) lane.shutdown() // queued initializers still drain and retire their candidates
            }
        }
        if (alreadyClosed) complete(null)
    }

    private fun dispose(engine: MobileEngine?) {
        if (engine != null) try { engine.shutdown() } finally { engine.close() }
    }
}
