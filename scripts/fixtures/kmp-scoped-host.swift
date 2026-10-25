import Foundation
import Orch8Kmp
import Orch8Mobile

private struct Failure: Error { let message: String }
private final class ResultCell<T>: @unchecked Sendable {
    private let lock = NSLock()
    private var result: Result<T, Error>?
    func set(_ result: Result<T, Error>) { lock.lock(); self.result = result; lock.unlock() }
    func get() -> Result<T, Error>? { lock.lock(); defer { lock.unlock() }; return result }
}
private func wait<T>(_ begin: (@escaping @Sendable (Result<T, Error>) -> Void) -> Void) throws -> T {
    let cell = ResultCell<T>()
    begin { cell.set($0) }
    let deadline = Date().addingTimeInterval(20)
    while cell.get() == nil {
        if Date() > deadline { throw Failure(message: "Kotlin completion timed out") }
        _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.005))
    }
    return try cell.get()!.get()
}
private func optional<T>(_ begin: (@escaping @Sendable (T?, Error?) -> Void) -> Void) throws -> T? {
    try wait { done in begin { value, error in if let error { done(.failure(error)) } else { done(.success(value)) } } }
}
private func object<T>(_ begin: (@escaping @Sendable (T?, Error?) -> Void) -> Void) throws -> T {
    guard let result = try optional(begin) else { throw Failure(message: "missing Kotlin result") }
    return result
}
private func unit(_ begin: (@escaping @Sendable (Error?) -> Void) -> Void) throws {
    let _: Bool = try wait { done in begin { error in if let error { done(.failure(error)) } else { done(.success(true)) } } }
}
private func check(_ value: Bool, _ message: String) throws {
    if !value { throw Failure(message: message) }
}
private func refused(_ operation: () throws -> Void) throws {
    do { try operation() } catch { return }
    throw Failure(message: "operation unexpectedly accepted")
}
private final class SnapshotProbe: NSObject, CatalogSnapshotObserver, @unchecked Sendable {
    private let runId: String
    private let accountId: String
    private let catalogId: String
    private let expectShutdown: Bool
    private let finished: @Sendable (Result<Bool, Error>) -> Void
    init(run: Orch8CatalogRun, expectShutdown: Bool, finished: @escaping @Sendable (Result<Bool, Error>) -> Void) {
        runId = run.instanceId; accountId = run.accountId; catalogId = run.catalogId
        self.expectShutdown = expectShutdown; self.finished = finished
    }
    func onSnapshot(snapshot: CatalogRunSnapshot?) {
        if let snapshot, snapshot.instanceId != runId || snapshot.accountId != accountId || snapshot.catalogId != catalogId {
            finished(.failure(Failure(message: "observer crossed run ownership")))
        }
    }
    func onError(error: Orch8Exception) { finished(.success(expectShutdown && error.kind == .shutdown)) }
    func onComplete() { finished(.success(!expectShutdown)) }
}
private func checkObservation(_ run: Orch8CatalogRun, expectShutdown: Bool = false) throws {
    var subscription: CatalogSnapshotSubscription?
    defer { subscription?.close() }
    let accepted: Bool = try wait { done in
        do {
            subscription = try run.subscribeSnapshots(observer: SnapshotProbe(run: run, expectShutdown: expectShutdown, finished: done),
                pollIntervalMs: 100)
        } catch { done(.failure(error)) }
    }
    try check(accepted, "Swift subscription did not report the expected completion/error")
}

private final class RootSnapshotProbe: NSObject, InstanceSnapshotObserver, @unchecked Sendable {
    private let expectedId: String
    private let expectedError: Orch8ErrorKind?
    private let finished: @Sendable (Result<Bool, Error>) -> Void
    private let lock = NSLock()
    private var completed = false
    init(id: String, error: Orch8ErrorKind?, finished: @escaping @Sendable (Result<Bool, Error>) -> Void) {
        expectedId = id; expectedError = error; self.finished = finished
    }
    func onInstance(snapshot: Orch8Kmp.InstanceSnapshot) {
        if snapshot.instanceId != expectedId { finished(.failure(Failure(message: "legacy observer crossed instance identity"))) }
        lock.lock(); completed = snapshot.state == .completed; lock.unlock()
    }
    func onError(error: Orch8Exception) { finished(.success(error.kind == expectedError)) }
    func onComplete() {
        lock.lock(); let terminal = completed; lock.unlock()
        finished(.success(expectedError == nil && terminal))
    }
}
private func checkRootObservation(_ engine: Orch8Engine, id: String, error: Orch8ErrorKind? = nil) throws {
    var subscription: Orch8ObservationSubscription?
    defer { subscription?.close() }
    let accepted: Bool = try wait { done in
        do { subscription = try engine.subscribeInstance(instanceId: id,
            observer: RootSnapshotProbe(id: id, error: error, finished: done), pollIntervalMs: 10) }
        catch { done(.failure(error)) }
    }
    try check(accepted, "legacy Swift observation missed completion/error")
}
private final class LegacyEcho: NSObject, Orch8StepHandler, @unchecked Sendable {
    func execute(stepName: String, inputJson: String, completionHandler: @escaping (String?, Error?) -> Void) {
        completionHandler("{}", nil)
    }
}
private final class LegacyEventProbe: NSObject, EngineEventObserver, PendingStepsObserver, @unchecked Sendable {
    private let lock = NSLock()
    private var ids: [String] = []
    private var nonemptySteps = false
    private var failed = false
    func onEvent(event: EngineEvent) { lock.lock(); ids.append(event.instanceId); lock.unlock() }
    func onSteps(steps: [PendingStep]) { lock.lock(); nonemptySteps = nonemptySteps || !steps.isEmpty; lock.unlock() }
    func onError(error: Orch8Exception) { lock.lock(); failed = true; lock.unlock() }
    func onComplete() {}
    func state() -> ([String], Bool, Bool) { lock.lock(); defer { lock.unlock() }; return (ids, nonemptySteps, failed) }
}

private final class Echo: NSObject, Orch8CatalogStepHandler, @unchecked Sendable {
    private let lock = NSLock()
    private var contexts: [CatalogCallbackContext] = []
    func execute(context: CatalogCallbackContext, stepName: String, inputJson: String,
                 completionHandler: @escaping (String?, Error?) -> Void) {
        lock.lock(); contexts.append(context); lock.unlock()
        completionHandler("{\"ok\":true}", nil)
    }
    func delivered() -> [CatalogCallbackContext] { lock.lock(); defer { lock.unlock() }; return contexts }
}
private func configuration(environment: String = "staging") -> EngineConfig {
    EngineConfig(tickIntervalMs: 500, maxConcurrentSteps: 4, maxStepsPerInstance: 1000,
        maxConcurrentInstances: 10, maxTickDurationMs: 5000, maxInstanceLifetimeSecs: 86400,
        maxStoredSequences: 50, maxSequenceSizeBytes: 1048576, handlerTimeoutMs: 1000,
        operationTimeoutMs: 10000, telemetryEnabled: false, telemetryUrl: "", environment: environment,
        rootPublicKey: "", sdkVersion: "0.7.1", memoryBudgetBytes: 0, sequencesUrl: "",
        syncUrl: "", deviceId: "kmp-host-scope", syncApiKey: "")
}
private func advance(_ engine: Orch8Engine, _ run: Orch8CatalogRun, _ state: Orch8Kmp.InstanceState,
                     pending: Int? = nil) throws -> CatalogRunSnapshot {
    let deadline = Date().addingTimeInterval(10)
    while Date() < deadline {
        let _: Orch8Kmp.TickResult = try object { engine.tick(completionHandler: $0) }
        let snapshot: CatalogRunSnapshot = try object { run.snapshot(completionHandler: $0) }
        if snapshot.state == state && (pending == nil || snapshot.pendingSteps.count == pending!) { return snapshot }
        _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.005))
    }
    throw Failure(message: "workflow did not reach expected state")
}

@main private enum ScopedHostSmoke {
    static func main() throws {
        guard CommandLine.arguments.count == 2 else { throw Failure(message: "fixture directory required") }
        let fixture = URL(fileURLWithPath: CommandLine.arguments[1])
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("orch8-kmp-scoped-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = directory.appendingPathComponent("node.db")
        try FileManager.default.copyItem(at: fixture.appendingPathComponent("scoped.db"), to: database)
        let metadata = try JSONSerialization.jsonObject(with: Data(contentsOf: fixture.appendingPathComponent("metadata.json"))) as! [String: Any]
        let cases = metadata["cases"] as! [[String: Any]]
        Orch8Ios.shared.install(factory: Orch8KmpBridgeFactory())
        let invalid = directory.appendingPathComponent("must-not-open.db")
        try refused { _ = try Orch8Engine.companion.open(dbPath: invalid.path, config: configuration(environment: "test")) }
        try check(!FileManager.default.fileExists(atPath: invalid.path), "invalid config opened a database")
        let engine = try Orch8Engine.companion.open(dbPath: database.path, config: configuration())
        defer { engine.close() }
        let legacyEvents = LegacyEventProbe()
        let eventSubscription = try engine.subscribeEvents(observer: legacyEvents)
        let stepSubscription = try engine.subscribePendingSteps(observer: legacyEvents)
        defer { eventSubscription.close(); stepSubscription.close() }
        let echo = Echo()
        var retained: [(Orch8Catalog, Orch8CatalogRun, [String: Any])] = []
        for record in cases {
            let identity = AccountIdentity(accountKey: record["accountKey"] as! String,
                issuer: record["issuer"] as! String, subject: record["subject"] as! String)
            let account: Orch8Account = try object { engine.openAccount(identity: identity, completionHandler: $0) }
            let config = CatalogConfiguration(catalogKey: record["catalogKey"] as! String,
                source: record["source"] as! String, signingAuthority: record["signingAuthority"] as! String,
                manifestUrl: record["manifestUrl"] as! String, identityUrl: record["identityUrl"] as! String)
            let catalog: Orch8Catalog = try object { account.openCatalog(configuration: config, completionHandler: $0) }
            try unit { catalog.registerHandler(name: "echo", handler: echo, timeoutMs: 1000, completionHandler: $0) }
            let request = CatalogRunRequest(workflow: "flow", inputJson: "{\"account_id\":\"forged\"}", operationKey: "same-key", version: nil)
            let run: Orch8CatalogRun = try object { catalog.prepareRun(request: request, completionHandler: $0) }
            account.close() // Issued children retain their own native view.
            try unit { run.start(completionHandler: $0) }
            retained.append((catalog, run, record))
        }
        for (index, item) in retained.enumerated() {
            let (catalog, run, record) = item
            let waiting = try advance(engine, run, .waiting, pending: 2)
            try check(waiting.accountId == record["accountId"] as! String, "snapshot account mismatch")
            try check(waiting.catalogId == record["catalogId"] as! String, "snapshot catalog mismatch")
            let first = index % 2 == 0 ? "left" : "right"
            let result: KotlinBoolean = try object { run.completeStep(expectedUpdatedAt: waiting.updatedAt,
                stepName: first, outputJson: "{\"value\":\"yes\"}", completionHandler: $0) }
            try check(result.boolValue, "answer did not commit")
            let replay: KotlinBoolean = try object { run.completeStep(expectedUpdatedAt: waiting.updatedAt,
                stepName: first, outputJson: "{\"value\":\"no\"}", completionHandler: $0) }
            try check(!replay.boolValue, "stale answer committed")
            let next = try advance(engine, run, .waiting, pending: 1)
            let second = first == "left" ? "right" : "left"
            let final: KotlinBoolean = try object { run.completeStep(expectedUpdatedAt: next.updatedAt,
                stepName: second, outputJson: "{\"value\":\"yes\"}", completionHandler: $0) }
            try check(final.boolValue, "second answer did not commit")
            let completed = try advance(engine, run, .completed)
            let data = try JSONSerialization.jsonObject(with: Data(completed.dataJson.utf8)) as! [String: Any]
            try check(data["left"] as? String == "yes" && data["right"] as? String == "yes", "approval data mismatch")
            try checkObservation(run)
            let page: CatalogRunPage = try object { catalog.history(cursor: nil, limit: 1, completionHandler: $0) }
            try check(page.items.count == 1 && page.next == nil, "history mismatch")
        }
        let contexts = echo.delivered()
        try check(contexts.count == 8 && Set(contexts.map(\.effectId)).count == 8, "callback/effect identity mismatch")
        for context in contexts {
            let owner = retained.first { $0.1.instanceId == context.runId }!
            try check(context.accountId == owner.1.accountId && context.catalogId == owner.1.catalogId,
                "callback crossed catalog ownership")
            try check(context.ownerEpoch == owner.1.sessionEpoch, "callback epoch mismatch")
        }
        let catalog = retained[0].0
        try catalog.closeView()
        try refused { let _: CatalogRunPage = try object { catalog.history(cursor: nil, limit: 1, completionHandler: $0) } }
        try checkObservation(retained[0].1, expectShutdown: true)
        let privateEvents = legacyEvents.state()
        try check(privateEvents.0.isEmpty && !privateEvents.1 && !privateEvents.2, "legacy observation exposed scoped events")
        try checkRootObservation(engine, id: retained[0].1.instanceId, error: .notFound)
        try engine.registerHandler(name: "legacy_echo", handler: LegacyEcho())
        let definition: [String: Any] = [
            "id": UUID().uuidString, "tenant_id": "mobile", "namespace": "default",
            "name": "legacy-observation", "version": 1,
            "blocks": [["type": "step", "id": "work", "handler": "legacy_echo", "params": [:]]],
            "created_at": ISO8601DateFormatter().string(from: Date()),
        ]
        let sequence = String(decoding: try JSONSerialization.data(withJSONObject: definition), as: UTF8.self)
        try unit { engine.loadSequence(json: sequence, completionHandler: $0) }
        let legacyId: String = try object { engine.startJson(sequenceName: "legacy-observation", inputJson: "{}", dedupKey: nil, completionHandler: $0) }
        let deadline = Date().addingTimeInterval(10)
        while true {
            let _: Orch8Kmp.TickResult = try object { engine.tick(completionHandler: $0) }
            let value: Orch8Kmp.InstanceSnapshot = try object { engine.getInstance(instanceId: legacyId, completionHandler: $0) }
            if value.state == .completed { break }
            if Date() > deadline { throw Failure(message: "legacy workflow did not complete") }
        }
        try checkRootObservation(engine, id: legacyId)
        while legacyEvents.state().0.isEmpty && Date() < deadline {
            _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.005))
        }
        let publicEvents = legacyEvents.state()
        try check(!publicEvents.0.isEmpty && publicEvents.0.allSatisfy { $0 == legacyId } && !publicEvents.1 && !publicEvents.2,
            "legacy observation failed owner isolation or event delivery")
        engine.close()
        try refused { _ = try engine.subscribeEvents(observer: legacyEvents) }
        print("KMP native host: 4 scoped workflows, 8 contextual effects, parallel approvals in both orders, history and close fences passed")
    }
}
