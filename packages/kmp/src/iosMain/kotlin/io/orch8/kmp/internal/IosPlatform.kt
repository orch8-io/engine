package io.orch8.kmp.internal

import io.orch8.kmp.EngineConfig
import io.orch8.kmp.Orch8ErrorKind
import io.orch8.kmp.Orch8Exception
import io.orch8.kmp.Orch8Ios
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.runBlocking

internal actual fun openPlatformBackend(dbPath: String, config: EngineConfig): EngineBackend {
    val factory = Orch8Ios.installedFactory()
        ?: throw Orch8Exception(
            Orch8ErrorKind.INVALID_INPUT,
            "Orch8 iOS bridge not installed: call Orch8Ios.shared.install(factory: Orch8KmpBridgeFactory()) " +
                "from Swift before Orch8Engine.open (see packages/kmp/README.md)",
        )
    return JsonBridgeBackend.open(factory.create(), dbPath, config)
}

internal actual val platformIoDispatcher: CoroutineDispatcher = Dispatchers.IO

internal actual fun <T> runHandlerBlocking(block: suspend () -> T): T = runBlocking { block() }
