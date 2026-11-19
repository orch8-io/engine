import Foundation
import SwiftUI
import UIKit

final class InspectionExecutionWindow: @unchecked Sendable {
    enum Failure: Error { case expired }

    private let deadline = DispatchTime.now() + .seconds(5)
    private let lock = NSLock()
    private var cancelled = false

    func remainingMilliseconds() throws -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        let now = DispatchTime.now().uptimeNanoseconds
        guard !cancelled, now < deadline.uptimeNanoseconds else { throw Failure.expired }
        return max(1, (deadline.uptimeNanoseconds - now) / 1_000_000)
    }

    func expire() {
        lock.lock()
        cancelled = true
        lock.unlock()
    }
}

/// Gives an already-started foreground operation a bounded opportunity to finish on app switch.
@MainActor
final class InspectionLifecycle: ObservableObject {
    typealias Expire = @MainActor @Sendable () -> Void
    typealias Begin = (@escaping Expire) -> UIBackgroundTaskIdentifier
    private static let inspectionWorker = DispatchQueue(label: "io.orch8.inspection")
    private let worker: DispatchQueue
    private let begin: Begin
    private let end: (UIBackgroundTaskIdentifier) -> Void
    private var running: InspectionExecutionWindow?
    private var lease: UIBackgroundTaskIdentifier = .invalid

    convenience init() {
        self.init(
            worker: Self.inspectionWorker,
            begin: { expire in
                UIApplication.shared.beginBackgroundTask(withName: "Save inspection") {
                    // UIKit invokes the expiration handler on the main thread.
                    MainActor.assumeIsolated { expire() }
                }
            },
            end: { UIApplication.shared.endBackgroundTask($0) }
        )
    }

    init(worker: DispatchQueue, begin: @escaping Begin, end: @escaping (UIBackgroundTaskIdentifier) -> Void) {
        self.worker = worker
        self.begin = begin
        self.end = end
    }

    @discardableResult
    func run<T>(
        _ operation: @escaping (InspectionExecutionWindow) throws -> T,
        completion: @escaping (Result<T, Error>) -> Void
    ) -> Bool {
        guard running == nil else { return false }
        let window = InspectionExecutionWindow()
        running = window
        let acquired = begin { [weak self] in
            window.expire()
            self?.finish(window, result: .failure(InspectionExecutionWindow.Failure.expired), completion: completion)
        }
        guard running === window else {
            if acquired != .invalid { end(acquired) }
            return false
        }
        lease = acquired
        worker.async {
            let result = Result {
                _ = try window.remainingMilliseconds()
                let value = try operation(window)
                _ = try window.remainingMilliseconds()
                return value
            }
            DispatchQueue.main.async { self.finish(window, result: result, completion: completion) }
        }
        return true
    }

    private func finish<T>(
        _ window: InspectionExecutionWindow, result: Result<T, Error>, completion: (Result<T, Error>) -> Void
    ) {
        guard running === window else { return }
        running = nil
        if lease != .invalid {
            end(lease)
            lease = .invalid
        }
        completion(result)
    }
}
