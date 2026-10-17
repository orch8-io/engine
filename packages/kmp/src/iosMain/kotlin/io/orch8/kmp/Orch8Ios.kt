package io.orch8.kmp

import io.orch8.kmp.bridge.Orch8JsonBridgeFactory
import kotlin.concurrent.Volatile
import kotlinx.cinterop.ExperimentalForeignApi
import platform.Foundation.NSApplicationSupportDirectory
import platform.Foundation.NSDocumentDirectory
import platform.Foundation.NSFileManager
import platform.Foundation.NSFileProtectionCompleteUntilFirstUserAuthentication
import platform.Foundation.NSFileProtectionKey
import platform.Foundation.NSSearchPathForDirectoriesInDomains
import platform.Foundation.NSURL
import platform.Foundation.NSURLIsExcludedFromBackupKey
import platform.Foundation.NSUserDomainMask

/**
 * iOS entry point. The engine itself ships as the `Orch8Mobile` Swift package
 * (UniFFI); this library reaches it through a small Swift adapter that the app
 * compiles (`packages/kmp/ios-bridge/Orch8KmpBridge.swift`).
 *
 * ```swift
 * import Orch8Kmp
 * Orch8Ios.shared.install(factory: Orch8KmpBridgeFactory())
 * ```
 */
object Orch8Ios {
    @Volatile
    private var factory: Orch8JsonBridgeFactory? = null

    /** Install the Swift bridge. Call once at launch, before `Orch8Engine.open`. */
    fun install(factory: Orch8JsonBridgeFactory) {
        this.factory = factory
    }

    internal fun installedFactory(): Orch8JsonBridgeFactory? = factory

    /**
     * Default database path. New installs use
     * `<Application Support>/Orch8/orch8.db`: hidden from the Files app,
     * excluded from iCloud/iTunes backup, and protected until first unlock so
     * background tasks can open it while the device is locked. A database that
     * already exists at the previous default (`<Documents>/orch8.db`) keeps
     * being used so upgrades do not lose data.
     */
    @OptIn(ExperimentalForeignApi::class)
    fun defaultDatabasePath(fileName: String = "orch8.db"): String {
        val fm = NSFileManager.defaultManager
        val documents = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, true)
            .firstOrNull() as? String
        if (documents != null && fm.fileExistsAtPath("$documents/$fileName")) {
            return "$documents/$fileName"
        }
        val support = NSSearchPathForDirectoriesInDomains(NSApplicationSupportDirectory, NSUserDomainMask, true)
            .firstOrNull() as? String
            ?: throw Orch8Exception(Orch8ErrorKind.STORAGE, "no Application Support directory available")
        val dir = "$support/Orch8"
        fm.createDirectoryAtPath(
            dir,
            withIntermediateDirectories = true,
            attributes = mapOf<Any?, Any?>(NSFileProtectionKey to NSFileProtectionCompleteUntilFirstUserAuthentication),
            error = null,
        )
        NSURL.fileURLWithPath(dir).setResourceValue(true, forKey = NSURLIsExcludedFromBackupKey, error = null)
        return "$dir/$fileName"
    }
}
