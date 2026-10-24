import Foundation
import XCTest
@testable import Orch8Mobile

final class TokenInstallationTests: XCTestCase {
    private final class Gate: @unchecked Sendable {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<String, Never>?
        private var released = false
        let entered: XCTestExpectation
        init(_ entered: XCTestExpectation) { self.entered = entered }
        func fetch() async -> String {
            await withCheckedContinuation { continuation in
                lock.lock()
                let released = self.released
                if !released { self.continuation = continuation }
                lock.unlock()
                entered.fulfill()
                if released { continuation.resume(returning: "dst_older") }
            }
        }
        func release() {
            lock.lock(); released = true; let waiting = continuation; continuation = nil; lock.unlock()
            waiting?.resume(returning: "dst_older")
        }
    }

    private final class FixedProvider: TokenProvider, @unchecked Sendable {
        func currentToken() -> String { "dst_newer" }
        func refreshToken() throws -> String { "dst_rotated" }
    }

    private func makeEngine() throws -> (MobileEngine, URL) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let config = MobileEngineConfig(
            tickIntervalMs: 500, maxConcurrentSteps: 4, maxStepsPerInstance: 1000,
            maxConcurrentInstances: 10, maxTickDurationMs: 5000,
            maxInstanceLifetimeSecs: 86400, maxStoredSequences: 50,
            maxSequenceSizeBytes: 1_048_576, handlerTimeoutMs: 30000,
            operationTimeoutMs: 10000, telemetryEnabled: false, telemetryUrl: "",
            environment: "production", rootPublicKey: "", sdkVersion: "0.7.1",
            memoryBudgetBytes: 0, sequencesUrl: "", syncUrl: "", deviceId: "ticket-test", syncApiKey: ""
        )
        return (try MobileEngine(dbPath: directory.appendingPathComponent("node.db").path, config: config), directory)
    }

    func testOlderAsyncCompletionCannotReplaceNewerSyncProviderAcrossAliases() async throws {
        let (engine, directory) = try makeEngine()
        defer { engine.shutdown(); try? FileManager.default.removeItem(at: directory) }
        let oldAlias = Orch8RuntimeNode(engine: engine)
        let newAlias = Orch8RuntimeNode(engine: engine)
        let entered = expectation(description: "older initial fetch started")
        let gate = Gate(entered)
        defer { gate.release() }
        let older = Task {
            do { try await oldAlias.setTokenProvider { await gate.fetch() }; return false }
            catch { return true }
        }
        await fulfillment(of: [entered], timeout: 5)
        var newer: FixedProvider? = FixedProvider()
        weak var retained = newer
        newAlias.setTokenProvider(newer!)
        newer = nil
        XCTAssertNotNil(retained)
        gate.release()
        let rejected = await older.value
        XCTAssertTrue(rejected, "an obsolete installation must be rejected")
        XCTAssertNotNil(retained, "older completion must not replace the newer native owner")
        retained = nil
    }

    func testOlderAsyncCompletionCannotReplaceNewerAsyncInstallation() async throws {
        let (engine, directory) = try makeEngine()
        defer { engine.shutdown(); try? FileManager.default.removeItem(at: directory) }
        let oldAlias = Orch8RuntimeNode(engine: engine)
        let newAlias = Orch8RuntimeNode(engine: engine)
        let entered = expectation(description: "older async fetch started")
        let gate = Gate(entered)
        defer { gate.release() }
        let older = Task {
            do { try await oldAlias.setTokenProvider { await gate.fetch() }; return false }
            catch { return true }
        }
        await fulfillment(of: [entered], timeout: 5)
        try await newAlias.setTokenProvider { "dst_newer" }
        gate.release()
        let rejected = await older.value
        XCTAssertTrue(rejected)
    }

    func testShutdownRejectsAnInitialFetchThatFinishesAfterward() async throws {
        let (engine, directory) = try makeEngine()
        defer { engine.shutdown(); try? FileManager.default.removeItem(at: directory) }
        let node = Orch8RuntimeNode(engine: engine)
        let entered = expectation(description: "initial fetch started before shutdown")
        let gate = Gate(entered)
        defer { gate.release() }
        let initial = Task {
            do { try await node.setTokenProvider { await gate.fetch() }; return false }
            catch { return true }
        }
        await fulfillment(of: [entered], timeout: 5)
        engine.shutdown()
        gate.release()
        let rejected = await initial.value
        XCTAssertTrue(rejected, "shutdown must reject the obsolete installation")
    }

    func testCancellingAnOlderInitialWaitCannotRetireNewerNativeCredentials() async throws {
        let (engine, directory) = try makeEngine()
        defer { engine.shutdown(); try? FileManager.default.removeItem(at: directory) }
        let node = Orch8RuntimeNode(engine: engine)
        let entered = expectation(description: "old initial fetch entered")
        let completed = expectation(description: "old caller cancelled before host completion")
        let gate = Gate(entered)
        defer { gate.release() }
        let older = Task {
            do { try await node.setTokenProvider { await gate.fetch() }; return false }
            catch { return true }
        }
        await fulfillment(of: [entered], timeout: 5)
        var newer: FixedProvider? = FixedProvider()
        weak var retained = newer
        Orch8RuntimeNode(engine: engine).setTokenProvider(newer!)
        newer = nil
        older.cancel()
        let observer = Task { let result = await older.value; completed.fulfill(); return result }
        await fulfillment(of: [completed], timeout: 5)
        XCTAssertNotNil(retained, "old cancellation must not clear the successor owner")
        gate.release()
        let rejected = await observer.value
        XCTAssertTrue(rejected)
        XCTAssertNotNil(retained)
        retained = nil
    }

    func testClosedEngineRejectsInstallationBeforeHostFetch() async throws {
        let (engine, directory) = try makeEngine()
        defer { engine.shutdown(); try? FileManager.default.removeItem(at: directory) }
        engine.shutdown()
        do {
            try await Orch8RuntimeNode(engine: engine).setTokenProvider {
                XCTFail("closed native authority must reject before fetching")
                return "dst_unexpected"
            }
            XCTFail("closed installation succeeded")
        } catch { /* expected terminal shutdown */ }
    }
}
