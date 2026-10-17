package io.orch8.reactnative

import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.Promise
import io.orch8.mobile.MobileException
import io.orch8.mobile.SyncException

/**
 * Maps UniFFI errors to a stable `kind` (mirrors `Orch8ErrorKind` in
 * src/types.ts) and rejects promises with `code = "ORCH8_<KIND>"`. The
 * pre-0.8 code (`ORCH8_ERROR`, `INIT_ERROR`) is kept in `userInfo.legacyCode`.
 */
internal object Orch8Errors {
    fun kind(e: Throwable): String = when (e) {
        is MobileException.Engine -> "engine"
        is MobileException.Storage -> "storage"
        is MobileException.InvalidInput -> "invalid_input"
        is MobileException.NotFound -> "not_found"
        is MobileException.ResourceLimit -> "resource_limit"
        is MobileException.AlreadyExists -> "already_exists"
        is MobileException.Shutdown -> "shutdown"
        is SyncException.Network -> "network"
        is SyncException.SignatureInvalid -> "signature_invalid"
        is SyncException.InvalidManifest -> "invalid_manifest"
        else -> "unknown"
    }

    fun code(kind: String): String = "ORCH8_" + kind.uppercase()

    fun reject(promise: Promise, e: Throwable, legacyCode: String = "ORCH8_ERROR") {
        val kind = kind(e)
        val info = Arguments.createMap().apply {
            putString("kind", kind)
            putString("legacyCode", legacyCode)
        }
        promise.reject(code(kind), e.message ?: e.toString(), e, info)
    }
}
