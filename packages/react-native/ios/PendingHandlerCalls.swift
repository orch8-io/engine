import Foundation

/// Outstanding native-to-JS handler calls, keyed by request id.
final class PendingHandlerCalls: @unchecked Sendable {
    struct Outcome {
        let output: String?
        let error: String?
        let permanent: Bool
    }

    private final class Slot {
        let semaphore = DispatchSemaphore(value: 0)
        var outcome: Outcome?
    }

    private let lock = NSLock()
    private var slots: [String: Slot] = [:]
    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return slots.count
    }

    func open(_ id: String) {
        lock.lock()
        slots[id] = Slot()
        lock.unlock()
    }

    func cancel(_ id: String) {
        lock.lock()
        slots.removeValue(forKey: id)
        lock.unlock()
    }

    func wait(_ id: String, timeoutMs: UInt64) -> Outcome? {
        lock.lock()
        let slot = slots[id]
        lock.unlock()
        guard let slot else { return nil }
        let result = slot.semaphore.wait(
            timeout: .now() + .milliseconds(Int(min(timeoutMs, UInt64(Int32.max)))))
        lock.lock()
        slots.removeValue(forKey: id)
        let outcome = slot.outcome
        lock.unlock()
        return result == .success ? outcome : nil
    }

    /// Rust bounds the operation. The physical foreign call stays charged until
    /// JS settles; a native timeout cannot cancel an arbitrary JS promise.
    func waitForSettlement(_ id: String) -> Outcome? {
        lock.lock()
        let slot = slots[id]
        lock.unlock()
        guard let slot else { return nil }
        slot.semaphore.wait()
        lock.lock()
        slots.removeValue(forKey: id)
        let outcome = slot.outcome
        lock.unlock()
        return outcome
    }

    func resolve(_ id: String, output: String?, error: String?, permanent: Bool) {
        lock.lock()
        guard let slot = slots[id], slot.outcome == nil else {
            lock.unlock()
            return
        }  // timed out, unknown, or answered
        slot.outcome = Outcome(output: output, error: error, permanent: permanent)
        lock.unlock()
        slot.semaphore.signal()
    }

    func failAll(_ message: String) {
        lock.lock()
        let open = slots.values.filter { $0.outcome == nil }
        for slot in open { slot.outcome = Outcome(output: nil, error: message, permanent: false) }
        lock.unlock()
        for slot in open { slot.semaphore.signal() }
    }
}
