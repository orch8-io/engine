import Foundation
import React
import Orch8Mobile

/// Maps UniFFI errors to a stable `kind` (mirrors `Orch8ErrorKind` in
/// src/types.ts). Promises reject with `code = "ORCH8_<KIND>"`; the pre-0.8
/// code (`ORCH8_ERROR`, `INIT_ERROR`) is kept in `userInfo.legacyCode`.
enum Orch8Errors {
    static func kind(_ error: Error) -> String {
        if error is NativeEngineLifecycleError { return "shutdown" }
        if let e = error as? MobileError {
            switch e {
            case .Engine: return "engine"
            case .Storage: return "storage"
            case .InvalidInput: return "invalid_input"
            case .NotFound: return "not_found"
            case .ResourceLimit: return "resource_limit"
            case .AlreadyExists: return "already_exists"
            case .Shutdown: return "shutdown"
            }
        }
        if let e = error as? SyncError {
            switch e {
            case .Network: return "network"
            case .SignatureInvalid: return "signature_invalid"
            case .InvalidManifest: return "invalid_manifest"
            }
        }
        return "unknown"
    }

    static func code(for kind: String) -> String { "ORCH8_" + kind.uppercased() }

    static func reject(
        _ reject: RCTPromiseRejectBlock,
        _ error: Error,
        legacyCode: String = "ORCH8_ERROR"
    ) {
        let kind = kind(error)
        let message = "\(error)"
        let wrapped = NSError(domain: "io.orch8", code: 1, userInfo: [
            NSLocalizedDescriptionKey: message,
            "kind": kind,
            "legacyCode": legacyCode,
        ])
        reject(code(for: kind), message, wrapped)
    }

    /// Default database location. New installs use
    /// `Application Support/Orch8/orch8.db`: not user-visible in Files, excluded
    /// from iCloud/iTunes backup, and protected until first unlock (so background
    /// tasks can open it while the device is locked). A database already in the
    /// legacy `Documents/orch8.db` location keeps being used.
    static func defaultDatabasePath() -> String {
        let fm = FileManager.default
        let legacy = fm.urls(for: .documentDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("orch8.db")
        if fm.fileExists(atPath: legacy.path) { return legacy.path }

        var dir = fm.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Orch8", isDirectory: true)
        try? fm.createDirectory(
            at: dir,
            withIntermediateDirectories: true,
            attributes: [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication]
        )
        var values = URLResourceValues()
        values.isExcludedFromBackup = true
        try? dir.setResourceValues(values)
        return dir.appendingPathComponent("orch8.db").path
    }
}
