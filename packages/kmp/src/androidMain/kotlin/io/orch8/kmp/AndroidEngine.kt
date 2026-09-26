package io.orch8.kmp

import android.content.Context
import android.content.Intent
import android.os.BatteryManager

/**
 * Open the engine in the app's database directory
 * (`context.getDatabasePath(dbName)`), which is excluded from Auto Backup
 * only if your backup rules say so — see docs/MOBILE_SDK.md.
 */
fun Orch8Engine.Companion.open(
    context: Context,
    config: EngineConfig = EngineConfig(),
    dbName: String = "orch8.db",
): Orch8Engine = open(context.applicationContext.getDatabasePath(dbName).absolutePath, config)

/**
 * Derive a [PowerState] from an `ACTION_BATTERY_CHANGED` sticky intent:
 *
 * ```kotlin
 * val intent = context.registerReceiver(null, IntentFilter(Intent.ACTION_BATTERY_CHANGED))
 * intent?.let { engine.reportPowerState(PowerState.fromBatteryIntent(it)) }
 * ```
 */
fun PowerState.Companion.fromBatteryIntent(intent: Intent): PowerState {
    val level = intent.getIntExtra(BatteryManager.EXTRA_LEVEL, -1)
    val scale = intent.getIntExtra(BatteryManager.EXTRA_SCALE, -1)
    val status = intent.getIntExtra(BatteryManager.EXTRA_STATUS, -1)
    val charging = status == BatteryManager.BATTERY_STATUS_CHARGING || status == BatteryManager.BATTERY_STATUS_FULL
    val percent = if (level >= 0 && scale > 0) level * 100 / scale else 100
    return fromBattery(percent, charging)
}
