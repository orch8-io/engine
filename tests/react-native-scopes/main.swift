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
        let cases =
            obj(
                try JSONSerialization.jsonObject(
                    with: Data(contentsOf: fixture.appendingPathComponent("metadata.json"))))["cases"]
            as! [[String: Any]]
        precondition(cases.count == 4)
        let config = MobileEngineConfig(
            tickIntervalMs: 500, maxConcurrentSteps: 4, maxStepsPerInstance: 1000, maxConcurrentInstances: 10,
            maxTickDurationMs: 5000, maxInstanceLifetimeSecs: 86400, maxStoredSequences: 50,
            maxSequenceSizeBytes: 1_048_576, handlerTimeoutMs: 1000, operationTimeoutMs: 10000,
            telemetryEnabled: false, telemetryUrl: "", environment: "test", rootPublicKey: "",
            sdkVersion: "0.7.1", memoryBudgetBytes: 0, sequencesUrl: "", syncUrl: "",
            deviceId: "rn-scoped-test", syncApiKey: "")
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
