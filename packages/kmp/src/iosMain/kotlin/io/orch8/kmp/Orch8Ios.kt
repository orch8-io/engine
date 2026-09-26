package io.orch8.kmp

import io.orch8.kmp.bridge.Orch8JsonBridgeFactory
import kotlin.concurrent.Volatile
import platform.Foundation.NSDocumentDirectory
import platform.Foundation.NSSearchPathForDirectoriesInDomains
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

    /** `<Documents>/orch8.db`, the location used by the Swift quick start. */
    fun defaultDatabasePath(fileName: String = "orch8.db"): String {
        val dirs = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, true)
        val documents = dirs.firstOrNull() as? String
            ?: throw Orch8Exception(Orch8ErrorKind.STORAGE, "no Documents directory available")
        return "$documents/$fileName"
    }
}
