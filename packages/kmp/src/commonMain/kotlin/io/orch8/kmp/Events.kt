package io.orch8.kmp

/** Lifecycle events delivered by the engine listener, exposed as a Flow. */
sealed class EngineEvent {
    abstract val instanceId: String

    data class InstanceCompleted(override val instanceId: String, val outputJson: String) : EngineEvent()

    data class InstanceFailed(override val instanceId: String, val error: String) : EngineEvent()

    /** A step is parked on `wait_for_input` (or a handler) and needs the host. */
    data class StepPending(
        override val instanceId: String,
        val stepName: String,
        val handler: String,
    ) : EngineEvent()
}

/** A step currently waiting for the host (UI input, approval, etc.). */
data class PendingStep(val instanceId: String, val stepName: String, val handler: String)

/**
 * Pure, immutable reducer that turns the event stream into the current set of
 * pending steps. The engine applies it with an atomic `StateFlow.update`, so
 * listener callbacks from engine threads never race.
 *
 * - `StepPending` adds a step (idempotent: the engine may re-announce a step
 *   after a restart; the latest handler wins, order is preserved).
 * - Completing a step locally removes it.
 * - A terminal instance event or a cancel removes every step of that instance.
 */
object PendingSteps {
    fun reduce(current: List<PendingStep>, event: EngineEvent): List<PendingStep> =
        when (event) {
            is EngineEvent.StepPending -> {
                val step = PendingStep(event.instanceId, event.stepName, event.handler)
                val index = current.indexOfFirst {
                    it.instanceId == event.instanceId && it.stepName == event.stepName
                }
                if (index < 0) {
                    current + step
                } else {
                    current.toMutableList().also { it[index] = step }
                }
            }
            is EngineEvent.InstanceCompleted -> withoutInstance(current, event.instanceId)
            is EngineEvent.InstanceFailed -> withoutInstance(current, event.instanceId)
        }

    fun withoutStep(current: List<PendingStep>, instanceId: String, stepName: String): List<PendingStep> =
        current.filterNot { it.instanceId == instanceId && it.stepName == stepName }

    fun withoutInstance(current: List<PendingStep>, instanceId: String): List<PendingStep> =
        current.filterNot { it.instanceId == instanceId }
}
