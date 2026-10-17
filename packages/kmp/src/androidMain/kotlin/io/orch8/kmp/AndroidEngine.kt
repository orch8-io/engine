package io.orch8.kmp

import android.content.Context
import android.content.Intent
import android.os.BatteryManager

/**
 * Open the engine in `context.noBackupFilesDir`, which Android excludes from
 * Auto Backup and device-to-device transfer. A database that already exists in
 * the previous default location (`context.getDatabasePath(dbName)`, backed up
 * unless your backup rules say otherwise) keeps being used so upgrades do not
 * lose data.
 */
fun Orch8Engine.Companion.open(
    context: Context,
    config: EngineConfig = EngineConfig(),
    dbName: String = "orch8.db",
): Orch8Engine {
    val app = context.applicationContext
    val existing = app.getDatabasePath(dbName)
    val path = if (existing.exists()) existing else java.io.File(app.noBackupFilesDir, dbName)
    return open(path.absolutePath, config)
}

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
