package io.orch8.kmp

/**
 * Device power state. The engine stretches its tick interval 2x at
 * [LOW_BATTERY] and 4x at [CRITICAL_BATTERY].
 */
enum class PowerState {
    CHARGING,
    UNPLUGGED,
    LOW_BATTERY,
    CRITICAL_BATTERY,
    ;

    companion object {
        /** Battery percentage at or below which the engine is told [LOW_BATTERY]. */
        const val LOW_BATTERY_PERCENT: Int = 20

        /** Battery percentage at or below which the engine is told [CRITICAL_BATTERY]. */
        const val CRITICAL_BATTERY_PERCENT: Int = 5

        /**
         * Map a battery reading to a power state using the thresholds the
         * engine documents. [levelPercent] is clamped to 0..100.
         */
        fun fromBattery(levelPercent: Int, charging: Boolean): PowerState {
            if (charging) return CHARGING
            val level = levelPercent.coerceIn(0, 100)
            return when {
                level <= CRITICAL_BATTERY_PERCENT -> CRITICAL_BATTERY
                level <= LOW_BATTERY_PERCENT -> LOW_BATTERY
                else -> UNPLUGGED
            }
        }
    }
}
