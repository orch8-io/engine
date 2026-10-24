import Foundation

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
    private var active: Active?

    init(initialToken: String, timeout: TimeInterval, fetch: @escaping @Sendable () async throws -> String) {
        self.token = initialToken
        self.timeout = timeout
        self.fetch = fetch
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
            flight = Flight(deadline: now + max(timeout, 0.001))
            let fetch = self.fetch
            // The task owns the flight, never the provider. In particular a
            // non-cooperative host closure cannot keep a retired provider alive.
            let task = Task.detached(priority: .utility) { [weak self, flight, fetch] in
                let result: Result<String, Error>
                do {
                    try Task.checkCancellation()
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

        _ = flight.completion.wait(timeout: flight.deadline)
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

    private func finish(_ flight: Flight, result: Result<String, Error>) {
        lock.lock()
        if flight.abandoned || DispatchTime.now() >= flight.deadline {
            flight.result = .failure(timeoutError())
        } else {
            switch result {
            case .success(let fresh) where !fresh.isEmpty:
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
