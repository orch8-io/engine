package io.orch8.kmp

import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow

/** Accepted callback subscriptions, including callbacks surviving cancellation. */
internal class ObservationBudget(
    private val processLimit: Int = 32,
    private val engineLimit: Int = 8,
    private val accountLimit: Int = 4,
) {
    // Identity equality keeps StateFlow CAS independent of callback equality.
    private class State(
        val total: Int = 0,
        val engines: Map<Job, Int> = emptyMap(),
        val accounts: Map<String, Int> = emptyMap(),
    )
    private val state = MutableStateFlow(State())
    val availablePermits: Int get() = processLimit - state.value.total

    init { require(processLimit > 0 && engineLimit > 0 && accountLimit > 0) }

    fun reserve(engine: Job?, account: String?): Reservation {
        while (true) {
            val before = state.value
            val engineUsed = engine?.let { before.engines[it] } ?: 0
            val accountUsed = account?.let { before.accounts[it] } ?: 0
            if (before.total >= processLimit || (engine != null && engineUsed >= engineLimit) ||
                (account != null && accountUsed >= accountLimit)) {
                throw Orch8Exception(Orch8ErrorKind.RESOURCE_LIMIT, "observation subscription capacity exhausted")
            }
            val after = State(before.total + 1,
                if (engine == null) before.engines else before.engines + (engine to engineUsed + 1),
                if (account == null) before.accounts else before.accounts + (account to accountUsed + 1))
            if (state.compareAndSet(before, after)) return Reservation(this, engine, account)
        }
    }

    private fun <K> decrement(values: Map<K, Int>, key: K?): Map<K, Int> {
        if (key == null) return values
        val used = checkNotNull(values[key])
        return if (used == 1) values - key else values + (key to used - 1)
    }

    private fun release(engine: Job?, account: String?) {
        while (true) {
            val before = state.value
            check(before.total > 0)
            val after = State(before.total - 1, decrement(before.engines, engine), decrement(before.accounts, account))
            if (state.compareAndSet(before, after)) return
        }
    }

    class Reservation internal constructor(
        private val budget: ObservationBudget,
        private val engine: Job?,
        private val account: String?,
    ) {
        private val released = MutableStateFlow(false)
        fun close() {
            if (released.compareAndSet(false, true)) budget.release(engine, account)
        }
    }

    companion object { val shared = ObservationBudget() }
}
