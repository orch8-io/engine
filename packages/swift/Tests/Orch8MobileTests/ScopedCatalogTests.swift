import Foundation
import XCTest
@testable import Orch8Mobile

final class ScopedCatalogTests: XCTestCase {
    private final class WeakEngine {
        weak var value: MobileEngine?
        init(_ value: MobileEngine?) { self.value = value }
    }
    private final class Echo: MobileCatalogStepHandler {
        func execute(context: MobileCatalogCallbackContext, stepName: String, input: String) throws -> String { "{}" }
    }
    private final class MustNotFetch: TokenProvider {
        func currentToken() -> String { XCTFail("closed catalog entered token provider"); return "" }
        func refreshToken() throws -> String { XCTFail("closed catalog entered refresh"); return "" }
    }
    private func makeEngine() throws -> (MobileEngine, URL) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let config = MobileEngineConfig(
            tickIntervalMs: 500, maxConcurrentSteps: 4, maxStepsPerInstance: 1000,
            maxConcurrentInstances: 10, maxTickDurationMs: 5000,
            maxInstanceLifetimeSecs: 86400, maxStoredSequences: 50,
            maxSequenceSizeBytes: 1_048_576, handlerTimeoutMs: 5000,
            operationTimeoutMs: 10000, telemetryEnabled: false, telemetryUrl: "",
            environment: "test", rootPublicKey: "", sdkVersion: "0.7.1",
            memoryBudgetBytes: 0, sequencesUrl: "", syncUrl: "", deviceId: "scoped-test", syncApiKey: ""
        )
        return (try MobileEngine(dbPath: directory.appendingPathComponent("node.db").path, config: config), directory)
    }
    private func identity(_ name: String) -> MobileAccountIdentity {
        MobileAccountIdentity(accountKey: name, issuer: "issuer", subject: name)
    }
    private func configuration(_ name: String) -> MobileCatalogConfiguration {
        MobileCatalogConfiguration(catalogKey: name, source: "source",
            signingAuthority: "ed25519:11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo=",
            manifestUrl: "https://catalog.example/\(name)/manifest", identityUrl: "https://catalog.example/identity")
    }

    func testScopedObjectsKeepIdentityCloseChildrenAndRetireAcrossCommittedBindings() throws {
        let (engine, directory) = try makeEngine()
        defer { engine.shutdown(); try? FileManager.default.removeItem(at: directory) }
        let alice = try engine.openAccount(identity: identity("alice"))
        let sibling = try engine.openAccount(identity: identity("alice"))
        let bob = try engine.openAccount(identity: identity("bob"))
        XCTAssertEqual(alice.accountId(), sibling.accountId())
        XCTAssertNotEqual(alice.accountId(), bob.accountId())
        let one = try alice.openCatalog(configuration: configuration("one"))
        let two = try alice.openCatalog(configuration: configuration("two"))
        let foreign = try bob.openCatalog(configuration: configuration("one"))
        XCTAssertNotEqual(one.catalogId(), two.catalogId())
        XCTAssertNotEqual(one.catalogId(), foreign.catalogId())
        try one.registerHandler(name: "echo", handler: Echo(), timeoutMs: 1000)
        XCTAssertTrue(try one.history(cursor: nil, limit: 1).items.isEmpty)
        let unknown = try one.run(instanceId: UUID().uuidString)
        XCTAssertNil(try unknown.snapshot())
        XCTAssertNil(try unknown.cancel())
        alice.closeView()
        XCTAssertThrowsError(try one.history(cursor: nil, limit: 1))
        XCTAssertThrowsError(try unknown.snapshot())
        XCTAssertThrowsError(try one.sync(tokenProvider: MustNotFetch()))
        XCTAssertTrue(try foreign.history(cursor: nil, limit: 1).items.isEmpty)
        let epoch = sibling.sessionEpoch()
        try sibling.retire()
        let fresh = try engine.openAccount(identity: identity("alice"))
        XCTAssertGreaterThan(fresh.sessionEpoch(), epoch)
        let current = try fresh.openCatalog(configuration: configuration("one"))
        XCTAssertTrue(try current.history(cursor: nil, limit: 1).items.isEmpty)
        XCTAssertThrowsError(try engine.openAccount(identity: MobileAccountIdentity(accountKey: "alice", issuer: "issuer", subject: "mallory")))
    }

    func testIssuedRunOutlivesTemporaryParentViews() throws {
        let (engine, directory) = try makeEngine()
        defer { engine.shutdown(); try? FileManager.default.removeItem(at: directory) }
        let run = try engine.openAccount(identity: identity("alice"))
            .openCatalog(configuration: configuration("one"))
            .run(instanceId: UUID().uuidString)
        XCTAssertNil(try run.snapshot())
    }

    func testScopedObjectsDoNotRetainTheNativeEngine() throws {
        var engine: MobileEngine?
        let directory: URL
        do {
            let pair = try makeEngine()
            engine = pair.0
            directory = pair.1
        }
        let weakEngine = WeakEngine(engine)
        let account = try XCTUnwrap(engine).openAccount(identity: identity("alice"))
        let catalog = try account.openCatalog(configuration: configuration("one"))
        engine = nil
        XCTAssertNil(weakEngine.value)
        XCTAssertThrowsError(try catalog.history(cursor: nil, limit: 1))
        try? FileManager.default.removeItem(at: directory)
    }
}
