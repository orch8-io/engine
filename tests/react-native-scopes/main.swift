import Foundation
import Orch8Mobile

private final class Host: MobileScopedCallbacks {
    let scopedPending = PendingHandlerCalls()
    private let lock = NSLock()
    private var received: [[String: Any]] = []
    private var holding = false
    private var withheld: [String] = []
    let entered = DispatchSemaphore(value: 0)
    func holdCalls() {
        lock.lock()
        holding = true
        lock.unlock()
    }
    func releaseCalls() {
        lock.lock()
        let ids = withheld
        withheld = []
        holding = false
        lock.unlock()
        for id in ids { scopedPending.resolve(id, output: "{}", error: nil, permanent: false) }
    }
    var contexts: [[String: Any]] {
        lock.lock()
        defer { lock.unlock() }
        return received
    }
    func emit(scope: NativeEngineScope, _ name: String, _ body: [String: Any]) -> Bool {
        precondition(scope.isCurrent)
        guard name == "orch8:scopedExecuteStep", let context = body["context"] as? [String: Any],
            let id = body["requestId"] as? String
        else { return false }
        lock.lock()
        received.append(context)
        let hold = holding
        if hold { withheld.append(id) }
        lock.unlock()
        if hold {
            entered.signal()
            return true
        }
        scopedPending.resolve(id, output: "{\"ok\":true}", error: nil, permanent: false)
        return true
    }
}
private func obj(_ value: Any?) -> [String: Any] { value as! [String: Any] }
private func str(_ value: Any?) -> String { value as! String }
private struct ExpectedFailure: Error {}
private func refuses(_ body: () throws -> Void) throws {
    do { try body() } catch { return }
    throw ExpectedFailure()
}
private final class AccountSwitchEcho: MobileCatalogStepHandler {
    func execute(context: MobileCatalogCallbackContext, stepName: String, input: String) throws -> String {
        "{}"
    }
}

private func verifyAccountSwitch(
    database: URL, config: MobileEngineConfig, first: [String: Any], second: [String: Any]
) throws {
    func identity(_ record: [String: Any]) -> MobileAccountIdentity {
        MobileAccountIdentity(
            accountKey: str(record["accountKey"]), issuer: str(record["issuer"]), subject: str(record["subject"]))
    }
    func configured(_ account: MobileAccount, _ record: [String: Any]) throws -> MobileCatalog {
        try account.openCatalog(configuration: MobileCatalogConfiguration(
            catalogKey: str(record["catalogKey"]), source: str(record["source"]),
            signingAuthority: str(record["signingAuthority"]), manifestUrl: str(record["manifestUrl"]),
            identityUrl: str(record["identityUrl"])))
    }
    func prepare(_ catalog: MobileCatalog, _ key: String) throws -> MobileCatalogRun {
        try catalog.prepareRun(request: MobileCatalogRunRequest(
            workflow: "flow", version: nil, inputJson: "{}", operationKey: key))
    }
    func onlyRun(_ catalog: MobileCatalog) throws -> MobileCatalogRunSummary {
        let page = try catalog.history(cursor: nil, limit: 5)
        precondition(page.items.count == 1)
        return page.items[0]
    }

    let operation = "same-pending-operation-after-switch"
    let firstEngine = try MobileEngine(dbPath: database.path, config: config)
    defer { firstEngine.shutdown() }
    let firstSession = try firstEngine.openAccount(identity: identity(first))
    let firstCatalog = try configured(firstSession, first)
    let firstId = try prepare(firstCatalog, operation).instanceId()
    let firstCached = try onlyRun(firstCatalog)
    let firstEpoch = firstSession.sessionEpoch()
    firstCatalog.closeView()
    firstSession.closeView()

    let secondSession = try firstEngine.openAccount(identity: identity(second))
    let secondCatalog = try configured(secondSession, second)
    let secondId = try prepare(secondCatalog, operation).instanceId()
    precondition(firstId != secondId)
    precondition(secondSession.accountId() != str(first["accountId"]))
    let secondCached = try onlyRun(secondCatalog)
    precondition(secondCached.instanceId == secondId && secondCached.sequenceId != firstCached.sequenceId)
    let foreignFirst = try secondCatalog.run(instanceId: firstId).snapshot()
    precondition(foreignFirst == nil)
    secondCatalog.closeView()
    secondSession.closeView()
    firstEngine.shutdown()

    let restarted = try MobileEngine(dbPath: database.path, config: config)
    defer { restarted.shutdown() }
    let recoveredFirst = try restarted.openAccount(identity: identity(first))
    let recoveredSecond = try restarted.openAccount(identity: identity(second))
    let catalogA = try configured(recoveredFirst, first)
    let catalogB = try configured(recoveredSecond, second)
    let historyA = try onlyRun(catalogA)
    let historyB = try onlyRun(catalogB)
    precondition(historyA.instanceId == firstId && historyB.instanceId == secondId)
    precondition(historyA.sequenceId == firstCached.sequenceId && historyB.sequenceId == secondCached.sequenceId)
    let recoveredA = try prepare(catalogA, operation)
    let recoveredB = try prepare(catalogB, operation)
    precondition(recoveredA.instanceId() == firstId && recoveredB.instanceId() == secondId)
    let foreignSecond = try catalogA.run(instanceId: secondId).snapshot()
    precondition(foreignSecond == nil)

    try catalogA.registerHandler(name: "echo", handler: AccountSwitchEcho(), timeoutMs: 1000)
    try recoveredA.start()
    let retireDeadline = Date().addingTimeInterval(5)
    var staleApproval: MobileCatalogRunSnapshot?
    while Date() < retireDeadline {
        _ = try restarted.tickOnce()
        if let snapshot = try recoveredA.snapshot(), snapshot.state == .waiting && snapshot.pendingSteps.count == 2 {
            staleApproval = snapshot
            break
        }
        Thread.sleep(forTimeInterval: 0.005)
    }
    guard let staleApproval else { throw ExpectedFailure() }
    try recoveredFirst.retire()
    try refuses {
        _ = try recoveredA.completeStep(
            expectedUpdatedAt: staleApproval.updatedAt, stepName: "left", outputJson: #"{"value":"yes"}"#)
    }
    try refuses { try recoveredA.start() }
    try refuses { _ = try catalogA.history(cursor: nil, limit: 5) }
    let loggedBackIn = try restarted.openAccount(identity: identity(first))
    precondition(loggedBackIn.accountId() == str(first["accountId"]))
    precondition(loggedBackIn.sessionEpoch() > firstEpoch)
    let newCatalog = try configured(loggedBackIn, first)
    try refuses { _ = try prepare(newCatalog, operation) }
    try refuses { try recoveredFirst.retire() }
    let newOperation = "new-session-cached-definition"
    let newRun = try prepare(newCatalog, newOperation)
    let retry = try prepare(newCatalog, newOperation)
    precondition(retry.instanceId() == newRun.instanceId() && newRun.accountId() == loggedBackIn.accountId())
    try refuses {
        _ = try newCatalog.prepareRun(request: MobileCatalogRunRequest(
            workflow: "flow", version: nil, inputJson: #"{"changed":true}"#, operationKey: newOperation))
    }
    let stillPending = try onlyRun(catalogB)
    precondition(stillPending.instanceId == secondId && stillPending.sequenceId == secondCached.sequenceId)
    try catalogB.registerHandler(name: "echo", handler: AccountSwitchEcho(), timeoutMs: 1000)
    try recoveredB.start()
    let deadline = Date().addingTimeInterval(5)
    var waiting = false
    while Date() < deadline {
        _ = try restarted.tickOnce()
        if let snapshot = try recoveredB.snapshot(), snapshot.state == .waiting && snapshot.pendingSteps.count == 2 {
            waiting = true
            break
        }
        Thread.sleep(forTimeInterval: 0.005)
    }
    precondition(waiting)
    let foreignRun = try newCatalog.run(instanceId: secondId)
    let foreignActive = try foreignRun.snapshot()
    precondition(foreignActive == nil)
    let foreignCancellation = try foreignRun.cancel()
    precondition(foreignCancellation == nil)
    let unaffected = try recoveredB.snapshot()
    precondition(unaffected?.state == .waiting && unaffected?.pendingSteps.count == 2)
    let currentEpoch = loggedBackIn.sessionEpoch()
    restarted.shutdown()

    let cold = try MobileEngine(dbPath: database.path, config: config)
    defer { cold.shutdown() }
    let coldA = try cold.openAccount(identity: identity(first))
    let coldB = try cold.openAccount(identity: identity(second))
    precondition(coldA.sessionEpoch() == currentEpoch)
    let coldCatalogA = try configured(coldA, first)
    try refuses { _ = try prepare(coldCatalogA, operation) }
    let freshRetry = try prepare(coldCatalogA, newOperation)
    precondition(freshRetry.instanceId() == newRun.instanceId())
    let coldCatalogB = try configured(coldB, second)
    let coldRun = try prepare(coldCatalogB, operation)
    precondition(coldRun.instanceId() == secondId)
    let recoveredWaiting = try coldRun.snapshot()
    guard let recoveredWaiting, recoveredWaiting.state == .waiting,
          Set(recoveredWaiting.pendingSteps) == Set(["left", "right"]) else { throw ExpectedFailure() }
    let answer = #"{"value":"yes"}"#
    let firstAnswer = try coldRun.completeStep(
        expectedUpdatedAt: recoveredWaiting.updatedAt, stepName: "left", outputJson: answer)
    precondition(firstAnswer)
    let duplicate = try coldRun.completeStep(
        expectedUpdatedAt: recoveredWaiting.updatedAt, stepName: "left", outputJson: answer)
    precondition(!duplicate)
    let foreignAfterRestart = try coldCatalogA.run(instanceId: secondId).snapshot()
    precondition(foreignAfterRestart == nil)
    print("Public native account API passed: switch, restart, per-account pending work and cached definitions, logout and re-login")

}

private func engineConfig(rootPublicKey: String = "") -> MobileEngineConfig {
    MobileEngineConfig(
        tickIntervalMs: 500, maxConcurrentSteps: 4, maxStepsPerInstance: 1000, maxConcurrentInstances: 10,
        maxTickDurationMs: 5000, maxInstanceLifetimeSecs: 86400, maxStoredSequences: 50,
        maxSequenceSizeBytes: 1_048_576, handlerTimeoutMs: 1000, operationTimeoutMs: 10000,
        telemetryEnabled: false, telemetryUrl: "", environment: "test", rootPublicKey: rootPublicKey,
        sdkVersion: "0.7.1", memoryBudgetBytes: 0, sequencesUrl: "", syncUrl: "",
        deviceId: "rn-scoped-test", syncApiKey: "")
}

private func verifyBundledCatalog(fixture: URL, directory: URL, record: [String: Any]) throws {
    let config = engineConfig(rootPublicKey: str(record["rootPublicKey"]))
    let package = str(record["packageJson"])
    let sequenceName = str(record["sequenceName"])

    func withEngine(_ database: URL, exercise: (MobileEngine) throws -> Void) throws {
        let engine = try MobileEngine(dbPath: database.path, config: config)
        defer { engine.shutdown() }
        try exercise(engine)
    }
    func assertSeededDefinition(_ engine: MobileEngine) throws {
        let sequences = try engine.loadedSequences()
        precondition(sequences.count == 1)
        precondition(sequences[0].name == sequenceName && sequences[0].version == 1)
    }

    let seededDatabase = directory.appendingPathComponent("catalog-seed.db")
    try withEngine(seededDatabase) { engine in
        let result = try engine.installBundledCatalogWithResult(packageJson: package)
        precondition(result.disposition == .seeded && result.installed == 1 && result.skipped == 0)
        try assertSeededDefinition(engine)
    }
    try withEngine(seededDatabase) { engine in
        let result = try engine.installBundledCatalogWithResult(packageJson: package)
        precondition(result.disposition == .alreadyPresent && result.installed == 0 && result.skipped == 1)
        try assertSeededDefinition(engine)
    }

    let ownedDatabase = directory.appendingPathComponent("manifest-owned.db")
    try FileManager.default.copyItem(
        at: fixture.appendingPathComponent("manifest-owned.db"), to: ownedDatabase)
    for _ in 0..<2 {
        try withEngine(ownedDatabase) { engine in
            let result = try engine.installBundledCatalogWithResult(packageJson: package)
            precondition(result.disposition == .manifestOwned && result.installed == 0 && result.skipped == 1)
            let sequences = try engine.loadedSequences()
            precondition(sequences.isEmpty, "The withdrawn definition reappeared")
        }
    }
    print("Public bundled catalog API passed: seeded, already present, and manifest owned across restart")
}

@main
private enum Tests {
    static func main() throws {
        precondition(mobileCallbackContractV2() == 2)
        let fixture = URL(fileURLWithPath: CommandLine.arguments[1])
        let directory = URL(fileURLWithPath: CommandLine.arguments[2]).appendingPathComponent("database")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("engine.db")
        try FileManager.default.copyItem(at: fixture.appendingPathComponent("scoped.db"), to: path)
        let metadata = obj(
            try JSONSerialization.jsonObject(
                with: Data(contentsOf: fixture.appendingPathComponent("metadata.json"))))
        let cases = metadata["cases"] as! [[String: Any]]
        precondition(cases.count == 4)
        try verifyBundledCatalog(fixture: fixture, directory: directory, record: obj(metadata["bundledCatalog"]))
        let config = engineConfig()
        let switchPath = directory.appendingPathComponent("account-switch.db")
        try FileManager.default.copyItem(at: fixture.appendingPathComponent("scoped.db"), to: switchPath)
        try verifyAccountSwitch(database: switchPath, config: config, first: cases[0], second: cases[2])
        let lane = DispatchQueue(label: "test.rn.scopes")
        let owner = NativeEngineLifecycle<MobileEngine>(lane: lane, dispose: { $0.shutdown() })
        owner.initialize(
            retired: {}, create: { try MobileEngine(dbPath: path.path, config: config) },
            configure: { _, _ in }, complete: { precondition($0 == nil) })
        lane.sync {}
        let engine = owner.engine!
        let scope = try owner.scope(for: engine)
        let bridge = MobileScopedBridge()
        let host = Host()
        defer {
            owner.shutdown(permanent: true, retired: {}, complete: {})
            lane.sync {}
            bridge.prune()
        }
        func call(_ handle: String, _ method: String, _ args: [String: Any] = [:]) throws -> Any {
            let json = String(decoding: try JSONSerialization.data(withJSONObject: args), as: UTF8.self)
            let result = try bridge.call(
                engine: engine, scope: scope, module: host, handle: handle, method: method, argsJson: json)
            return try JSONSerialization.jsonObject(with: Data(result.utf8), options: .fragmentsAllowed)
        }
        func snapshot(_ run: String, state: String, pending: Int? = nil) throws -> [String: Any] {
            for _ in 0..<100 {
                _ = try engine.tickOnce()
                let value = try call(run, "snapshot")
                if let snapshot = value as? [String: Any], snapshot["state"] as? String == state,
                    pending == nil || (snapshot["pendingSteps"] as? [String])?.count == pending
                {
                    return snapshot
                }
                Thread.sleep(forTimeInterval: 0.005)
            }
            throw ExpectedFailure()
        }
        var catalogs: [String] = []
        var runs: [String] = []
        var accounts: [String] = []
        var expectedOwners: [String: [String: Any]] = [:]
        for (index, fixture) in cases.enumerated() {
            let account = obj(try call("", "openAccount", fixture))
            precondition(
                account["sessionEpoch"] is String && str(account["accountId"]) == str(fixture["accountId"]))
            let accountHandle = str(account["handle"])
            let catalog = obj(try call(accountHandle, "openCatalog", fixture))
            precondition(str(catalog["catalogId"]) == str(fixture["catalogId"]))
            let catalogHandle = str(catalog["handle"])
            _ = try call(
                catalogHandle, "registerHandler",
                ["name": "echo", "callbackKey": "handler-\(index)", "timeoutMs": 1000])
            let run = obj(
                try call(
                    catalogHandle, "prepareRun",
                    [
                        "workflow": "flow", "inputJson": "{\"account_id\":\"forged\"}",
                        "operationKey": "same-key",
                    ]))
            let runHandle = str(run["handle"])
            expectedOwners[str(run["instanceId"])] = fixture
            _ = try call(runHandle, "start")
            let waiting = try snapshot(runHandle, state: "waiting", pending: 2)
            precondition(str(waiting["accountId"]) == str(fixture["accountId"]))
            precondition(Set(waiting["pendingSteps"] as! [String]) == Set(["left", "right"]))
            catalogs.append(catalogHandle)
            runs.append(runHandle)
            accounts.append(accountHandle)
        }
        for (index, run) in runs.enumerated() {
            // The foreign catalog cannot read the owned run, even with its instance ID.
            let original = obj(try call(run, "snapshot"))
            let foreign = obj(
                try call(catalogs[(index + 1) % 4], "run", ["instanceId": original["instanceId"]!]))
            let foreignSnapshot = try call(str(foreign["handle"]), "snapshot")
            precondition(foreignSnapshot is NSNull)
            for step in ["left", "right"] {
                let current = obj(try call(run, "snapshot"))
                let accepted =
                    try call(
                        run, "completeStep",
                        [
                            "expectedUpdatedAt": current["updatedAt"]!, "stepName": step,
                            "outputJson": "{\"value\":\"yes\"}",
                        ]) as! Bool
                precondition(accepted)
                if step == "left" {
                    _ = try snapshot(run, state: "waiting", pending: 1)
                    let stale =
                        try call(
                            run, "completeStep",
                            [
                                "expectedUpdatedAt": current["updatedAt"]!, "stepName": step,
                                "outputJson": "{\"value\":\"no\"}",
                            ]) as! Bool
                    precondition(!stale)
                }
            }
            _ = try snapshot(run, state: "completed")
            let receipt = obj(
                try call(run, "archiveAfterAcknowledgement", ["acknowledgement": "ack-\(index)"]))
            precondition(str(receipt["outcome"]) == "completed" && receipt["dataJson"] == nil)
            _ = try call(run, "archiveAfterAcknowledgement", ["acknowledgement": "ack-\(index)"])
            try refuses { _ = try call(run, "archiveAfterAcknowledgement", ["acknowledgement": "different"]) }
        }
        precondition(host.contexts.count == 8, "unexpected callback count: \(host.contexts.count)")
        for context in host.contexts {
            let fixture = expectedOwners[str(context["runId"])]!
            precondition(
                str(context["accountId"]) == str(fixture["accountId"])
                    && str(context["catalogId"]) == str(fixture["catalogId"]))
            precondition(context["ownerEpoch"] is String && !(str(context["effectId"])).isEmpty)
        }
        let legacyInstances = try engine.activeInstances()
        precondition(legacyInstances.isEmpty)
        var wrongPrincipal = cases[0]
        wrongPrincipal["subject"] = "mallory"
        try refuses { _ = try call("", "openAccount", wrongPrincipal) }
        _ = try call(
            catalogs[1], "prepareRun", ["workflow": "flow", "inputJson": "{}", "operationKey": "second-page"])
        // A legal native input can exceed 2 MiB once JSON-escaped in the
        // platform envelope. Exercise the actual bridge parser, not a DTO mock.
        let maximumInput = "{\"text\":\"" + String(repeating: "\\\\", count: 524282) + "\"}"
        precondition(maximumInput.utf8.count == 1_048_575)
        let maximumRun = str(obj(try call(catalogs[1], "prepareRun", [
            "workflow": "flow", "inputJson": maximumInput, "operationKey": "maximum-input"
        ]))["handle"])
        _ = try call(maximumRun, "cancel")
        _ = try call(maximumRun, "forgetCancelledRequest")
        let page = obj(try call(catalogs[1], "history", ["limit": 1]))
        let cursor = str(obj(page["next"])["handle"])
        _ = try call(catalogs[1], "history", ["limit": 1, "cursor": cursor])
        try refuses { _ = try call(catalogs[2], "history", ["limit": 1, "cursor": cursor]) }
        _ = try call(cursor, "dispose")
        _ = try call(accounts[0], "dispose")
        let history = obj(try call(catalogs[0], "history", ["limit": 1]))
        precondition(history["items"] is [Any])
        _ = try call(catalogs[0], "closeView")
        try refuses { _ = try call(runs[0], "snapshot") }
        _ = try call(accounts[2], "retire")
        try refuses { _ = try call(catalogs[3], "history", ["limit": 1]) }
        let fresh = obj(try call("", "openAccount", cases[2]))
        precondition(str(fresh["sessionEpoch"]) == "2")
        // Numeric, fractional and boolean versions must fail before admission.
        for version in [true, 1.5, 2_147_483_648] as [Any] {
            try refuses {
                _ = try call(
                    catalogs[1], "prepareRun",
                    ["workflow": "flow", "inputJson": "{}", "operationKey": "bad", "version": version])
            }
        }
        var issued: [String] = []
        var saturated = false
        for _ in 0..<4096 {
            do {
                issued.append(
                    str(obj(try call(catalogs[1], "run", ["instanceId": UUID().uuidString]))["handle"]))
            } catch MobileError.ResourceLimit(detail: _) {
                saturated = true
                break
            }
        }
        precondition(
            saturated && issued.count > 400 && issued.count < 512, "catalog view bound was not exercised")
        _ = try call(issued.last!, "dispose")
        _ = try call(catalogs[1], "run", ["instanceId": UUID().uuidString])
        _ = try call(accounts[1], "retire")  // Reclaim every native view of the retired account epoch.
        let reopened = obj(try call("", "openAccount", cases[1]))
        precondition(str(reopened["sessionEpoch"]) == "2")
        let recovered = obj(try call(str(reopened["handle"]), "openCatalog", cases[1]))
        _ = try call(str(recovered["handle"]), "history", ["limit": 1])
        var globalViews: [String] = []
        for index in 0..<4 {
            var count = 0
            for _ in 0..<1025 {
                do {
                    let view = obj(
                        try call(
                            "", "openAccount",
                            [
                                "accountKey": "view-capacity-\(index)", "issuer": "issuer",
                                "subject": "view-capacity-\(index)",
                            ]))
                    globalViews.append(str(view["handle"]))
                    count += 1
                } catch MobileError.ResourceLimit(detail: _) { break }
            }
            precondition(index == 3 ? count < 1024 : count == 1024, "account/global view ceiling mismatch")
        }
        precondition(globalViews.count > 4000 && globalViews.count < 4096)
        _ = try call(globalViews.last!, "dispose")
        _ = try call(
            "", "openAccount",
            ["accountKey": "capacity-recovered", "issuer": "issuer", "subject": "capacity-recovered"])
        for handle in globalViews.suffix(32) { _ = try call(handle, "dispose") }
        let recoveredCatalog = str(recovered["handle"])
        _ = try call(
            recoveredCatalog, "registerHandler",
            ["name": "echo", "callbackKey": "straggler", "timeoutMs": 100])
        let slow = obj(
            try call(
                recoveredCatalog, "prepareRun",
                ["workflow": "flow", "inputJson": "{}", "operationKey": "straggler"]))
        let slowRun = str(slow["handle"])
        _ = try call(slowRun, "start")
        let slowWaiting = try snapshot(slowRun, state: "waiting", pending: 2)
        host.holdCalls()
        let safety = DispatchWorkItem { host.releaseCalls() }
        DispatchQueue.global().asyncAfter(deadline: .now() + 30, execute: safety)
        defer {
            safety.cancel()
            host.releaseCalls()
        }
        _ = try call(
            slowRun, "completeStep",
            [
                "expectedUpdatedAt": slowWaiting["updatedAt"]!, "stepName": "left",
                "outputJson": "{\"value\":\"yes\"}",
            ])
        var entered = false
        for _ in 0..<100 {
            _ = try engine.tickOnce()
            if host.entered.wait(timeout: .now()) == .success {
                entered = true
                break
            }
            Thread.sleep(forTimeInterval: 0.005)
        }
        precondition(entered)
        Thread.sleep(forTimeInterval: 0.15)
        precondition(host.scopedPending.count == 1, "logical timeout released the physical host call")
        owner.shutdown(permanent: true, retired: {}, complete: {})
        lane.sync {}
        bridge.prune()
        precondition(host.scopedPending.count == 1, "root retirement synthesized JS completion")
        host.releaseCalls()
        let settledBy = Date().addingTimeInterval(2)
        while host.scopedPending.count > 0 && Date() < settledBy { Thread.sleep(forTimeInterval: 0.005) }
        precondition(host.scopedPending.count == 0)
        try refuses { _ = try call(catalogs[1], "history", ["limit": 1]) }
        print(
            "RN native scopes passed: four workflows, eight callbacks, approvals/archives, 4096-view bound/reclamation and generation fences"
        )
    }
}
