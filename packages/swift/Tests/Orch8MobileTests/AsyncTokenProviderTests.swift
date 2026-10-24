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

    func testRetiredProvidersSharePhysicalFetchAdmissionAndRecoverAfterCompletion() async throws {
        try await waitForBudget(.shared, available: 8)
        let entered = expectation(description: "eight physical fetches entered")
        entered.expectedFulfillmentCount = 8
        let gates = (0..<8).map { _ in Gate(started: entered) }
        let refused = Gate()
        // Physical entry is the synchronization point. A short timeout could
        // expire before a loaded simulator ever starts a host callback, which
        // correctly releases unused admission and does not test retirement.
        let callers = gates.map { gate in
            Task {
                let provider = AsyncTokenProvider(initialToken: "", timeout: 120) { await gate.fetch() }
                do { _ = try await provider.fetchInitialToken(); return false }
                catch is CancellationError { return true }
                catch { return false }
            }
        }
        defer {
            callers.forEach { $0.cancel() }
            gates.forEach { $0.release() }
            refused.release()
        }
        await fulfillment(of: [entered], timeout: 30)
        callers.forEach { $0.cancel() }
        for caller in callers {
            let cancelled = await caller.value
            XCTAssertTrue(cancelled, "retire logical callers while host fetches remain blocked")
        }
        XCTAssertEqual(TokenFetchBudget.shared.availablePermits, 0)
        let replacement = AsyncTokenProvider(initialToken: "dst_cached", timeout: 5) { await refused.fetch() }
        XCTAssertThrowsError(try replacement.refreshToken())
        XCTAssertEqual(refused.calls(), 0, "retiring adapters must not free physical fetch capacity")
        XCTAssertEqual(replacement.currentToken(), "dst_cached")
        for _ in 0..<128 {
            let provider = AsyncTokenProvider(initialToken: "dst_cached", timeout: 5) {
                XCTFail("exhausted admission must not invoke or queue host work")
                return "dst_unexpected"
            }
            XCTAssertThrowsError(try provider.refreshToken())
        }
        gates.forEach { $0.release() }
        refused.release()
        try await waitForBudget(.shared, available: 8)
        let recovery = AsyncTokenProvider(initialToken: "", timeout: 30) { "dst_recovered" }
        let token = try await recovery.fetchInitialToken()
        XCTAssertEqual(token, "dst_recovered")
        try await waitForBudget(.shared, available: 8)
    }

    private func waitForBudget(_ budget: TokenFetchBudget, available: Int) async throws {
        let deadline = Date().addingTimeInterval(5)
        while budget.availablePermits != available && Date() < deadline {
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertEqual(budget.availablePermits, available)
    }

    func testInitialFetchValidatesTimeoutAndRejectsBlankResultsWithoutLosingAdmission() async throws {
        let budget = TokenFetchBudget(capacity: 1)
        for timeout in [Double.nan, .infinity, -.infinity, 0, -1, 10_000_000_000, .greatestFiniteMagnitude] {
            let provider = AsyncTokenProvider(initialToken: "", timeout: timeout, budget: budget) {
                XCTFail("invalid first-fetch timeout must not invoke host code")
                return "dst_unexpected"
            }
            do { _ = try await provider.fetchInitialToken(); XCTFail("invalid timeout succeeded") }
            catch { /* expected validation failure */ }
            XCTAssertEqual(budget.availablePermits, 1)
        }
        for value in ["", " ", "\t\r\n"] {
            let provider = AsyncTokenProvider(initialToken: "", timeout: 5, budget: budget) { value }
            do { _ = try await provider.fetchInitialToken(); XCTFail("blank token succeeded") }
            catch { /* expected invalid token */ }
            XCTAssertEqual(provider.currentToken(), "")
            try await waitForBudget(budget, available: 1)
        }
        let failed = AsyncTokenProvider(initialToken: "", timeout: 5, budget: budget) {
            throw URLError(.notConnectedToInternet)
        }
        do { _ = try await failed.fetchInitialToken(); XCTFail("host failure succeeded") }
        catch { /* expected host failure */ }
        try await waitForBudget(budget, available: 1)
        let provider = AsyncTokenProvider(initialToken: "", timeout: 5, budget: budget) { "dst_first" }
        let token = try await provider.fetchInitialToken()
        XCTAssertEqual(token, "dst_first")
        XCTAssertEqual(provider.currentToken(), "dst_first")
        try await waitForBudget(budget, available: 1)
    }

    func testCancelledInitialWaitReturnsBeforeNonCooperativeFetchAndRetainsCapacity() async throws {
        let budget = TokenFetchBudget(capacity: 1)
        let entered = expectation(description: "initial host fetch entered")
        let completed = expectation(description: "cancelled initial waiter returned")
        let gate = Gate(started: entered)
        defer { gate.release() }
        let initial = Task {
            let provider = AsyncTokenProvider(initialToken: "", timeout: 30, budget: budget) { await gate.fetch() }
            do { _ = try await provider.fetchInitialToken(); return false }
            catch is CancellationError { return true }
            catch { return false }
        }
        await fulfillment(of: [entered], timeout: 5)
        initial.cancel()
        let observer = Task { let result = await initial.value; completed.fulfill(); return result }
        await fulfillment(of: [completed], timeout: 5)
        XCTAssertEqual(budget.availablePermits, 0, "cancellation must not retire live host work")
        let replacement = AsyncTokenProvider(initialToken: "dst_cached", timeout: 5, budget: budget) {
            XCTFail("initial and refresh work must share admission")
            return "dst_unexpected"
        }
        XCTAssertThrowsError(try replacement.refreshToken())
        gate.release()
        let cancelled = await observer.value
        XCTAssertTrue(cancelled)
        try await waitForBudget(budget, available: 1)
        XCTAssertEqual(gate.calls(), 1)
    }

    func testInitialTimeoutRetainsPhysicalAdmissionUntilLateCompletion() async throws {
        let budget = TokenFetchBudget(capacity: 1)
        let entered = expectation(description: "initial fetch entered before timeout")
        let gate = Gate(started: entered)
        defer { gate.release() }
        let provider = AsyncTokenProvider(initialToken: "", timeout: 5, budget: budget) { await gate.fetch() }
        let initial = Task { try await provider.fetchInitialToken() }
        await fulfillment(of: [entered], timeout: 5)
        do { _ = try await initial.value; XCTFail("held initial fetch must time out") }
        catch { /* expected timeout */ }
        XCTAssertEqual(budget.availablePermits, 0)
        XCTAssertEqual(provider.currentToken(), "")
        gate.release()
        try await waitForBudget(budget, available: 1)
        XCTAssertEqual(provider.currentToken(), "", "late first fetch cannot populate the cache")
        let token = try await provider.fetchInitialToken()
        XCTAssertEqual(token, "dst_recovered")
    }

    func testAlreadyCancelledInitialFetchDoesNotInvokeOrReserveHostWork() async {
        let budget = TokenFetchBudget(capacity: 1)
        let provider = AsyncTokenProvider(initialToken: "", timeout: 5, budget: budget) {
            XCTFail("cancelled caller must not enter the host")
            return "dst_unexpected"
        }
        let initial = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            do { _ = try await provider.fetchInitialToken(); return false }
            catch is CancellationError { return true }
            catch { return false }
        }
        let cancelled = await initial.value
        XCTAssertTrue(cancelled)
        XCTAssertEqual(budget.availablePermits, 1)
    }

    func testInitialCompletionCancellationRacesReturnAdmissionExactlyOnce() async throws {
        let budget = TokenFetchBudget(capacity: 1)
        for iteration in 0..<64 {
            let provider = AsyncTokenProvider(initialToken: "", timeout: 5, budget: budget) {
                await Task.yield()
                return "dst_first"
            }
            let initial = Task { try await provider.fetchInitialToken() }
            if iteration % 2 == 0 { await Task.yield() }
            initial.cancel()
            do { _ = try await initial.value }
            catch { /* cancellation may beat completion */ }
            try await waitForBudget(budget, available: 1)
        }
    }

}
