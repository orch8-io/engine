import Foundation

/// Process-wide admission also covers work whose provider has already retired.
final class TokenFetchBudget: @unchecked Sendable {
    static let shared = TokenFetchBudget(capacity: 8)
    private let lock = NSLock()
    private var remaining: Int

    init(capacity: Int) { precondition(capacity > 0); remaining = capacity }

    var availablePermits: Int {
        lock.lock(); defer { lock.unlock() }
        return remaining
    }

    final class Permit: @unchecked Sendable {
        private let budget: TokenFetchBudget
        fileprivate init(_ budget: TokenFetchBudget) { self.budget = budget }
        deinit {
            budget.lock.lock(); budget.remaining += 1; budget.lock.unlock()
        }
    }

    func tryAcquire() -> Permit? {
        lock.lock(); defer { lock.unlock() }
        guard remaining > 0 else { return nil }
        remaining -= 1
        return Permit(self)
    }
}

/// Bridges the async host fetch to UniFFI's synchronous blocking callback.
/// A timeout cancels the fetch but retains admission until it actually returns.
final class AsyncTokenProvider: TokenProvider, @unchecked Sendable {
    private final class Flight: @unchecked Sendable {
        let deadline: DispatchTime
        let completion = DispatchGroup()
        // Accessed only while the provider's lock is held.
        var abandoned = false
        var result: Result<String, Error>?

        init(deadline: DispatchTime) {
            self.deadline = deadline
            completion.enter()
        }
    }

    private struct Active {
        let flight: Flight
        let task: Task<Void, Never>
    }

    private let lock = NSLock()
    private var token: String
    private let timeout: TimeInterval
    private let fetch: @Sendable () async throws -> String
    private let budget: TokenFetchBudget
    private var active: Active?

    init(initialToken: String, timeout: TimeInterval, budget: TokenFetchBudget = .shared, fetch: @escaping @Sendable () async throws -> String) {
        self.token = initialToken
        self.timeout = timeout
        self.fetch = fetch
        self.budget = budget
    }

    deinit { active?.task.cancel() }

    func currentToken() -> String {
        lock.lock(); defer { lock.unlock() }
        return token
    }

    private func timeoutError() -> MobileError {
        MobileError.Engine(message: "token provider timed out")
    }

    func refreshToken() throws -> String {
        let flight = try beginFlight()
        _ = flight.completion.wait(timeout: flight.deadline)
        return try result(for: flight)
    }

    private func beginFlight() throws -> Flight {
        // DispatchTime uses UInt64 nanoseconds. Reject invalid or overflowing
        // intervals before converting them or adding them to the clock.
        let now = DispatchTime.now()
        guard timeout.isFinite, timeout > 0,
              max(timeout, 0.001) < Double(Int64.max) / 1_000_000_000,
              max(timeout, 0.001) < Double(UInt64.max - now.uptimeNanoseconds) / 1_000_000_000 else {
            throw MobileError.Engine(message: "token provider timeout must be finite, positive and representable")
        }
        lock.lock()
        let flight: Flight
        if let active {
            flight = active.flight
            if flight.abandoned || DispatchTime.now() >= flight.deadline {
                flight.abandoned = true
                lock.unlock()
                active.task.cancel()
                throw timeoutError()
            }
        } else {
            guard let permit = budget.tryAcquire() else {
                lock.unlock()
                throw MobileError.Engine(message: "token fetch capacity exhausted")
            }
            flight = Flight(deadline: now + max(timeout, 0.001))
            let fetch = self.fetch
            // The task owns the flight, never the provider. In particular a
            // non-cooperative host closure cannot keep a retired provider alive.
            let task = Task.detached(priority: .utility) { [weak self, flight, fetch, permit] in
                defer { withExtendedLifetime(permit) {} }
                let result: Result<String, Error>
                do {
                    try Task.checkCancellation()
                    guard DispatchTime.now() < flight.deadline else {
                        throw MobileError.Engine(message: "token provider timed out")
                    }
                    let fresh = try await fetch()
                    try Task.checkCancellation()
                    result = .success(fresh)
                } catch {
                    result = .failure(error)
                }
                if let self {
                    self.finish(flight, result: result)
                } else {
                    flight.completion.leave()
                }
            }
            active = Active(flight: flight, task: task)
        }
        lock.unlock()
        return flight
    }

    private func result(for flight: Flight) throws -> String {
        lock.lock()
        // Completion can publish before the deadline even if this waiter is
        // scheduled later. Only completion publishes the cache, never waiters.
        if let result = flight.result {
            let cached = token
            lock.unlock()
            switch result {
            case .success: return cached
            case .failure(let error): throw error
            }
        }
        flight.abandoned = true
        let task = active?.flight === flight ? active?.task : nil
        lock.unlock()
        task?.cancel()
        throw timeoutError()
    }

    private func abandon(_ flight: Flight) {
        lock.lock()
        flight.abandoned = true
        let task = active?.flight === flight ? active?.task : nil
        lock.unlock()
        task?.cancel()
    }

    /// Initial fetches use the same physical budget without blocking a Swift
    /// executor thread. Timeout/cancellation ends the wait, not host ownership.
    func fetchInitialToken() async throws -> String {
        try Task.checkCancellation()
        let flight = try beginFlight()
        let waiter = InitialWaiter()
        flight.completion.notify(queue: .global(qos: .utility)) { [weak self, flight, waiter] in
            guard let self else {
                waiter.complete(.failure(CancellationError()))
                return
            }
            waiter.complete(Result { try self.result(for: flight) })
        }
        waiter.arm(deadline: flight.deadline) { [weak self, weak flight, weak waiter] in
            guard let self, let flight, let waiter else { return }
            // Preserve completion published before the deadline even if the
            // dispatch notification was scheduled late.
            waiter.complete(Result { try self.result(for: flight) })
        }
        let token = try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { continuation in
                waiter.install(continuation)
            }
        }, onCancel: {
            if waiter.complete(.failure(CancellationError())) {
                self.abandon(flight)
            }
        })
        try Task.checkCancellation()
        return token
    }

    private final class InitialWaiter: @unchecked Sendable {
        private let lock = NSLock()
        private var outcome: Result<String, Error>?
        private var continuation: CheckedContinuation<String, Error>?
        private var timer: DispatchSourceTimer?

        func install(_ continuation: CheckedContinuation<String, Error>) {
            lock.lock()
            let completed = outcome
            if completed == nil { self.continuation = continuation }
            lock.unlock()
            if let completed { continuation.resume(with: completed) }
        }

        func arm(deadline: DispatchTime, action: @escaping @Sendable () -> Void) {
            let timer = DispatchSource.makeTimerSource(queue: .global(qos: .utility))
            timer.schedule(deadline: deadline)
            timer.setEventHandler(handler: action)
            timer.resume()
            lock.lock()
            let completed = outcome != nil
            if !completed { self.timer = timer }
            lock.unlock()
            if completed { timer.cancel() }
        }

        @discardableResult
        func complete(_ result: Result<String, Error>) -> Bool {
            lock.lock()
            guard outcome == nil else { lock.unlock(); return false }
            outcome = result
            let continuation = self.continuation
            self.continuation = nil
            let timer = self.timer
            self.timer = nil
            lock.unlock()
            timer?.cancel()
            continuation?.resume(with: result)
            return true
        }
    }

    private func finish(_ flight: Flight, result: Result<String, Error>) {
        // A host-defined Error may re-enter from deinit. Keep it alive until
        // after the provider lock is released, even under ARC optimization.
        defer { withExtendedLifetime(result) {} }
        lock.lock()
        if flight.abandoned || DispatchTime.now() >= flight.deadline {
            flight.result = .failure(timeoutError())
        } else {
            switch result {
            case .success(let fresh) where !fresh.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty:
                token = fresh
                flight.result = .success(fresh)
            case .success:
                flight.result = .failure(MobileError.Engine(message: "token provider returned an empty token"))
            case .failure:
                flight.result = .failure(MobileError.Engine(message: "token provider failed"))
            }
        }
        if active?.flight === flight { active = nil }
        lock.unlock()
        flight.completion.leave()
    }
}
