import XCTest
@testable import Orch8Mobile

final class AsyncTokenProviderTests: XCTestCase {
    private actor Tokens {
        var next = ["dst_second"]
        func pop() throws -> String {
            guard !next.isEmpty else { throw URLError(.userAuthenticationRequired) }
            return next.removeFirst()
        }
    }

    func testServesInitialTokenAndRefreshesThroughTheClosure() throws {
        let tokens = Tokens()
        let provider = AsyncTokenProvider(initialToken: "dst_first", timeout: 5) { try await tokens.pop() }
        XCTAssertEqual(provider.currentToken(), "dst_first")
        XCTAssertEqual(try provider.refreshToken(), "dst_second")
        XCTAssertEqual(provider.currentToken(), "dst_second")
        // The closure throws: the refresh fails and the cached token stays.
        XCTAssertThrowsError(try provider.refreshToken())
        XCTAssertEqual(provider.currentToken(), "dst_second")
    }

    func testEmptyTokenAndTimeoutAreErrors() {
        let empty = AsyncTokenProvider(initialToken: "dst_first", timeout: 5) { "" }
        XCTAssertThrowsError(try empty.refreshToken())
        XCTAssertEqual(empty.currentToken(), "dst_first")

        let slow = AsyncTokenProvider(initialToken: "dst_first", timeout: 0.05) {
            try await Task.sleep(nanoseconds: 2_000_000_000)
            return "dst_late"
        }
        XCTAssertThrowsError(try slow.refreshToken())
        XCTAssertEqual(slow.currentToken(), "dst_first")
    }

    private final class Gate: @unchecked Sendable {
        private let lock = NSLock()
        private var continuations: [CheckedContinuation<String, Never>] = []
        private var count = 0
        private var released = false
        private let started: XCTestExpectation?
        init(started: XCTestExpectation? = nil) { self.started = started }
        func fetch() async -> String {
            let invocation = begin()
            if invocation > 1 { return "dst_recovered" }
            return await withCheckedContinuation { continuation in
                lock.lock()
                let alreadyReleased = released
                if !alreadyReleased { continuations.append(continuation) }
                lock.unlock()
                started?.fulfill()
                if alreadyReleased { continuation.resume(returning: "dst_late") }
            }
        }
        private func begin() -> Int {
            lock.lock(); defer { lock.unlock() }
            count += 1
            return count
        }
        func calls() -> Int { lock.lock(); defer { lock.unlock() }; return count }
        func release() {
            lock.lock(); released = true; let pending = continuations; continuations = []; lock.unlock()
            for continuation in pending { continuation.resume(returning: "dst_late") }
        }
    }

    func testTimeoutCancelsACooperativeFetch() {
        let cancelled = expectation(description: "fetch observes cancellation")
        let provider = AsyncTokenProvider(initialToken: "dst_first", timeout: 5) {
            do {
                try await Task.sleep(nanoseconds: 10_000_000_000)
                return "dst_late"
            } catch {
                cancelled.fulfill()
                throw error
            }
        }
        XCTAssertThrowsError(try provider.refreshToken())
        wait(for: [cancelled], timeout: 10)
        XCTAssertEqual(provider.currentToken(), "dst_first")
    }

    func testNonCooperativeFetchRemainsSingleFlightUntilPhysicalCompletion() {
        let started = expectation(description: "first fetch entered")
        let timedOut = expectation(description: "first synchronous refresh timed out")
        let gate = Gate(started: started)
        let provider = AsyncTokenProvider(initialToken: "dst_first", timeout: 5) { await gate.fetch() }
        DispatchQueue.global().async {
            do {
                _ = try provider.refreshToken()
                XCTFail("the blocked first fetch must time out")
            } catch { /* expected timeout */ }
            timedOut.fulfill()
        }
        wait(for: [started, timedOut], timeout: 15)
        XCTAssertEqual(gate.calls(), 1)
        XCTAssertThrowsError(try provider.refreshToken(), "a timed-out fetch still owns admission")
        let concurrentCalls = gate.calls()
        gate.release()
        XCTAssertEqual(concurrentCalls, 1, "timeout must not start a second physical fetch")
        let deadline = Date().addingTimeInterval(10)
        var recovered: String?
        while Date() < deadline && recovered == nil {
            recovered = try? provider.refreshToken()
            if recovered == nil { Thread.sleep(forTimeInterval: 0.005) }
        }
        XCTAssertEqual(recovered, "dst_recovered")
        XCTAssertEqual(provider.currentToken(), "dst_recovered")
        XCTAssertEqual(gate.calls(), 2)
    }

    func testRetiredProviderCanDeallocateWhileItsNonCooperativeFetchFinishes() {
        let gate = Gate()
        var provider: AsyncTokenProvider? = AsyncTokenProvider(initialToken: "dst_first", timeout: 5) { await gate.fetch() }
        weak var retired = provider
        XCTAssertThrowsError(try provider?.refreshToken())
        provider = nil
        XCTAssertNil(retired, "a detached fetch must not keep the provider alive")
        retired = nil
        gate.release()
    }

    func testInvalidTimeoutDoesNotInvokeFetch() {
        for timeout in [Double.nan, .infinity, -.infinity, 0, -1, 10_000_000_000, .greatestFiniteMagnitude] {
            let provider = AsyncTokenProvider(initialToken: "dst_first", timeout: timeout) {
                XCTFail("invalid timeout must fail before calling the host")
                return "dst_unexpected"
            }
            XCTAssertThrowsError(try provider.refreshToken())
            XCTAssertEqual(provider.currentToken(), "dst_first")
        }
    }

}
