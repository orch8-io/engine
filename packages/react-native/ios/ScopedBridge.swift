import CoreFoundation
import Foundation
import Orch8Mobile

protocol RNScopedCallbacks: AnyObject {
    var scopedPending: PendingHandlerCalls { get }
    @discardableResult func emit(scope: NativeEngineScope, _ name: String, _ body: [String: Any]) -> Bool
}

/// Bounded, generation-qualified native views. Entries never retain the root engine.
final class RNScopedBridge: @unchecked Sendable {
    private enum Node {
        case account(MobileAccount)
        case catalog(MobileCatalog)
        case run(MobileCatalogRun)
        case cursor(MobileCatalogHistoryCursor, String, String, Int64)
    }
    private struct Entry {
        let node: Node
        let scope: NativeEngineScope
        let parent: String?
        let account: String
        let catalog: String?
        let epoch: String
    }
    private let lock = NSLock()
    private var entries: [String: Entry] = [:]
    private var activeScope: NativeEngineScope?
    private static let maxViews = 4096
    func prune() {
        lock.lock()
        if activeScope?.isCurrent == true {
            lock.unlock()
            return
        }
        let retired = entries.filter { !$0.value.scope.isCurrent }
        for key in retired.keys { entries.removeValue(forKey: key) }
        lock.unlock()
        withExtendedLifetime(retired) {}  // Native ARC release stays outside the table lock.
    }
    private func insert(_ node: Node, _ scope: NativeEngineScope, parent: String? = nil) throws -> [String:
        Any]
    {
        guard scope.isCurrent else { throw MobileError.Shutdown(detail: "engine retired") }
        let id = UUID().uuidString
        var result: [String: Any] = ["handle": id]
        switch node {
        case .account(let v):
            result.merge([
                "kind": "account", "accountId": v.accountId(), "sessionEpoch": String(v.sessionEpoch()),
            ]) { _, new in new }
        case .catalog(let v):
            result.merge([
                "kind": "catalog", "accountId": v.accountId(), "catalogId": v.catalogId(),
                "sessionEpoch": String(v.sessionEpoch()),
            ]) { _, new in new }
        case .run(let v):
            result.merge([
                "kind": "run", "accountId": v.accountId(), "catalogId": v.catalogId(),
                "sessionEpoch": String(v.sessionEpoch()), "instanceId": v.instanceId(),
            ]) { _, new in new }
        case .cursor(_, let account, let catalog, let epoch):
            result.merge([
                "kind": "cursor", "accountId": account, "catalogId": catalog, "sessionEpoch": String(epoch),
            ]) { _, new in new }
        }
        lock.lock()
        let account = result["accountId"] as! String
        let catalog = result["catalogId"] as? String
        let current = scope.isCurrent
        let accepted =
            current && entries.count < Self.maxViews
            && entries.values.lazy.filter { $0.account == account }.count < 1024
            && (catalog == nil
                || entries.values.lazy.filter { $0.account == account && $0.catalog == catalog }.count < 512)
        if accepted {
            entries[id] = Entry(
                node: node, scope: scope, parent: parent, account: account, catalog: catalog,
                epoch: result["sessionEpoch"] as! String)
            activeScope = scope
        }
        lock.unlock()
        guard current else { throw MobileError.Shutdown(detail: "engine retired") }
        guard accepted else {
            throw MobileError.ResourceLimit(
                detail: "native scoped view capacity exhausted or engine retired")
        }
        return result
    }
    private func releaseTree(_ root: String) {
        lock.lock()
        var keys: Set<String> = [root]
        while true {
            let previous = keys.count
            for (id, entry) in entries {
                if let parent = entry.parent, keys.contains(parent) { keys.insert(id) }
            }
            if previous == keys.count { break }
        }
        let retired = entries.filter { keys.contains($0.key) }
        for id in retired.keys { entries.removeValue(forKey: id) }
        lock.unlock()
        withExtendedLifetime(retired) {}
    }
    private func releaseAccount(_ account: String, epoch: String) {
        lock.lock()
        let retired = entries.filter { $0.value.account == account && $0.value.epoch == epoch }
        for id in retired.keys { entries.removeValue(forKey: id) }
        lock.unlock()
        withExtendedLifetime(retired) {}
    }
    private func lookup(_ id: String) throws -> Entry {
        lock.lock()
        let value = entries[id]
        lock.unlock()
        guard let value, value.scope.isCurrent else {
            throw MobileError.Shutdown(detail: "scope view disposed or retired")
        }
        return value
    }
    func call(
        engine: MobileEngine, scope: NativeEngineScope, module: RNScopedCallbacks, handle: String,
        method: String, argsJson: String
    ) throws -> String {
        prune()
        guard argsJson.utf8.count <= 2 * 1024 * 1024,
            let data = argsJson.data(using: .utf8),
            let args = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else { throw MobileError.InvalidInput(detail: "scoped arguments must be a bounded JSON object") }
        let result: Any?
        if method == "dispose" {
            lock.lock()
            let retired = entries.removeValue(forKey: handle)
            lock.unlock()
            withExtendedLifetime(retired) {}
            result = nil
        } else if handle.isEmpty && method == "openAccount" {
            result = try insert(
                .account(
                    engine.openAccount(
                        identity: MobileAccountIdentity(
                            accountKey: args.string("accountKey"), issuer: args.string("issuer"),
                            subject: args.string("subject")))), scope)
        } else {
            let entry = try lookup(handle)
            switch (entry.node, method) {
            case (.account(let v), "openCatalog"):
                result = try insert(
                    .catalog(
                        v.openCatalog(
                            configuration: MobileCatalogConfiguration(
                                catalogKey: args.string("catalogKey"), source: args.string("source"),
                                signingAuthority: args.string("signingAuthority"),
                                manifestUrl: args.string("manifestUrl"),
                                identityUrl: args.string("identityUrl")))), scope, parent: handle)
            case (.account(let v), "retire"):
                try v.retire()
                releaseAccount(entry.account, epoch: entry.epoch)
                result = nil
            case (.account(let v), "closeView"):
                v.closeView()
                releaseTree(handle)
                result = nil
            case (.catalog(let v), "closeView"):
                v.closeView()
                releaseTree(handle)
                result = nil
            case (.run(let v), "closeView"):
                v.closeView()
                releaseTree(handle)
                result = nil
            case (.cursor, "closeView"):
                releaseTree(handle)
                result = nil
            case (.catalog(let v), "prepareRun"):
                let version: Int32? = try args.optionalVersion()
                result = try insert(
                    .run(
                        v.prepareRun(
                            request: MobileCatalogRunRequest(
                                workflow: args.string("workflow"), version: version,
                                inputJson: args.string("inputJson"), operationKey: args.string("operationKey")
                            ))), scope, parent: handle)
            case (.catalog(let v), "run"):
                result = try insert(.run(v.run(instanceId: args.string("instanceId"))), scope, parent: handle)
            case (.catalog(let v), "history"):
                let limit = try args.positiveNumber("limit", maximum: 128)
                var cursor: MobileCatalogHistoryCursor?
                if let id = args["cursor"] as? String {
                    guard case .cursor(let value, let account, let catalog, let epoch) = try lookup(id).node,
                        account == v.accountId(), catalog == v.catalogId(), epoch == v.sessionEpoch()
                    else {
                        throw MobileError.InvalidInput(detail: "cursor belongs to another catalog session")
                    }
                    cursor = value
                } else if let value = args["cursor"], !(value is NSNull) {
                    throw MobileError.InvalidInput(detail: "invalid cursor")
                }
                let page = try v.history(cursor: cursor, limit: UInt32(limit))
                let next = try page.next.map {
                    try insert(
                        .cursor($0, v.accountId(), v.catalogId(), v.sessionEpoch()), scope, parent: handle)
                }
                result = ["items": page.items.map(Self.summary), "next": next as Any? ?? NSNull()]
            case (.catalog(let v), "registerHandler"):
                let timeout = try args.positiveNumber("timeoutMs", maximum: 9_007_199_254_740_991)
                try v.registerHandler(
                    name: args.string("name"),
                    handler: RNScopedHandler(
                        module: module, scope: scope, catalog: v, key: args.string("callbackKey")),
                    timeoutMs: UInt64(timeout))
                result = nil
            case (.catalog(let v), "sync"):
                let synced = try v.sync(
                    tokenProvider: RNScopedTokens(
                        module: module, scope: scope, key: args.string("providerKey"),
                        token: args.string("initialToken")))
                result = [
                    "added": synced.added, "updated": synced.updated, "removed": synced.removed,
                    "skipped": synced.skipped, "signatureFailures": synced.signatureFailures,
                ]
            case (.run(let v), "start"):
                try v.start()
                result = nil
            case (.run(let v), "snapshot"):
                result = try v.snapshot().map {
                    [
                        "instanceId": $0.instanceId, "accountId": $0.accountId, "catalogId": $0.catalogId,
                        "sessionEpoch": String($0.sessionEpoch), "state": Self.state($0.state),
                        "dataJson": $0.dataJson, "pendingSteps": $0.pendingSteps, "createdAt": $0.createdAt,
                        "updatedAt": $0.updatedAt,
                    ] as [String: Any]
                }
            case (.run(let v), "cancel"): result = try v.cancel().map(Self.state)
            case (.run(let v), "completeStep"):
                result = try v.completeStep(
                    expectedUpdatedAt: args.string("expectedUpdatedAt"), stepName: args.string("stepName"),
                    outputJson: args.string("outputJson"))
            case (.run(let v), "forgetCancelledRequest"): result = try v.forgetCancelledRequest()
            case (.run(let v), "archiveAfterAcknowledgement"):
                result = try v.archiveAfterAcknowledgement(acknowledgement: args.string("acknowledgement"))
                    .map {
                        [
                            "instanceId": $0.instanceId, "accountId": $0.accountId, "catalogId": $0.catalogId,
                            "acknowledgementDigest": $0.acknowledgementDigest, "archivedAt": $0.archivedAt,
                            "outcome": $0.outcome.map(Self.state) as Any? ?? NSNull(),
                        ] as [String: Any]
                    }
            default: throw MobileError.InvalidInput(detail: "unknown scoped operation")
            }
        }
        // Scalars and null are valid top-level bridge replies.
        let output = try JSONSerialization.data(
            withJSONObject: result ?? NSNull(), options: [.fragmentsAllowed, .sortedKeys])
        return String(decoding: output, as: UTF8.self)
    }
    private static func state(_ v: InstanceStateKind) -> String {
        switch v {
        case .scheduled: return "scheduled"
        case .running: return "running"
        case .waiting: return "waiting"
        case .paused: return "paused"
        case .completed: return "completed"
        case .failed: return "failed"
        case .cancelled: return "cancelled"
        }
    }
    private static func summary(_ v: MobileCatalogRunSummary) -> [String: Any] {
        let native: [String: Any]
        switch v.nativeState {
        case .notMaterialized: native = ["kind": "not_materialized"]
        case .unavailable: native = ["kind": "unavailable"]
        case .present(state: let value): native = ["kind": "present", "state": state(value)]
        case .recorded(state: let value): native = ["kind": "recorded", "state": state(value)]
        }
        let request: String
        switch v.requestState {
        case .prepared: request = "prepared"
        case .cancelled: request = "cancelled"
        case .archived: request = "archived"
        }
        return [
            "instanceId": v.instanceId, "accountId": v.accountId, "catalogId": v.catalogId,
            "ownerEpoch": String(v.ownerEpoch), "workflow": v.workflow, "version": v.version,
            "sequenceId": v.sequenceId, "createdAt": v.createdAt, "requestState": request,
            "nativeState": native,
        ]
    }
}
extension Dictionary where Key == String, Value == Any {
    fileprivate func string(_ name: String) throws -> String {
        guard let value = self[name] as? String else {
            throw MobileError.InvalidInput(detail: "missing string: \(name)")
        }
        return value
    }
    fileprivate func positiveNumber(_ name: String, maximum: Int64) throws -> Int64 {
        guard let n = self[name] as? NSNumber, CFGetTypeID(n) != CFBooleanGetTypeID(),
            n.doubleValue == Double(n.int64Value), (1...maximum).contains(n.int64Value)
        else { throw MobileError.InvalidInput(detail: "invalid positive integer: \(name)") }
        return n.int64Value
    }
    fileprivate func optionalVersion() throws -> Int32? {
        guard let value = self["version"], !(value is NSNull) else { return nil }
        return Int32(try positiveNumber("version", maximum: Int64(Int32.max)))
    }
}
private final class RNScopedHandler: MobileCatalogStepHandler, @unchecked Sendable {
    private weak var module: RNScopedCallbacks?
    private let scope: NativeEngineScope
    private let account: String, catalog: String, epoch: Int64, key: String
    init(
        module: RNScopedCallbacks, scope: NativeEngineScope, catalog: MobileCatalog, key: String
    ) {
        self.module = module
        self.scope = scope
        self.account = catalog.accountId()
        self.catalog = catalog.catalogId()
        self.epoch = catalog.sessionEpoch()
        self.key = key
    }
    func execute(context: MobileCatalogCallbackContext, stepName: String, input: String) throws -> String {
        guard scope.isCurrent, let module else { throw HandlerError.Retryable(detail: "engine retired") }
        guard context.accountId == account, context.catalogId == catalog, context.ownerEpoch == epoch else {
            throw HandlerError.Permanent(detail: "callback owner mismatch")
        }
        let id = UUID().uuidString
        module.scopedPending.open(id)
        guard
            module.emit(
                scope: scope, "orch8:scopedExecuteStep",
                [
                    "callbackKey": key, "requestId": id, "stepName": stepName, "inputJson": input,
                    "context": [
                        "accountId": context.accountId, "catalogId": context.catalogId,
                        "ownerEpoch": String(context.ownerEpoch), "runId": context.runId,
                        "blockId": context.blockId, "attempt": context.attempt, "effectId": context.effectId,
                    ],
                ])
        else {
            module.scopedPending.cancel(id)
            throw HandlerError.Retryable(detail: "JS runtime unavailable")
        }
        guard let reply = module.scopedPending.waitForSettlement(id), scope.isCurrent else {
            throw HandlerError.Retryable(detail: "catalog handler unavailable or retired")
        }
        if let error = reply.error {
            if reply.permanent { throw HandlerError.Permanent(detail: error) }
            throw HandlerError.Retryable(detail: error)
        }
        return reply.output ?? "{}"
    }
}
private final class RNScopedTokens: TokenProvider, @unchecked Sendable {
    private weak var module: RNScopedCallbacks?
    private let scope: NativeEngineScope
    private let key: String
    private let lock = NSLock()
    private var token: String
    init(module: RNScopedCallbacks, scope: NativeEngineScope, key: String, token: String) {
        self.module = module
        self.scope = scope
        self.key = key
        self.token = token
    }
    func currentToken() -> String {
        guard scope.isCurrent else { return "" }
        lock.lock()
        defer { lock.unlock() }
        return token
    }
    func refreshToken() throws -> String {
        guard scope.isCurrent, let module else { throw MobileError.Shutdown(detail: "engine retired") }
        let id = UUID().uuidString
        module.scopedPending.open(id)
        guard module.emit(scope: scope, "orch8:scopedRefreshToken", ["providerKey": key, "requestId": id])
        else {
            module.scopedPending.cancel(id)
            throw MobileError.Engine(detail: "JS runtime unavailable")
        }
        guard let reply = module.scopedPending.waitForSettlement(id), scope.isCurrent else {
            throw MobileError.Shutdown(detail: "token callback unavailable or retired")
        }
        if let error = reply.error { throw MobileError.Engine(detail: error) }
        guard let value = reply.output, !value.isEmpty else {
            throw MobileError.Engine(detail: "empty token")
        }
        lock.lock()
        token = value
        lock.unlock()
        return value
    }
}
