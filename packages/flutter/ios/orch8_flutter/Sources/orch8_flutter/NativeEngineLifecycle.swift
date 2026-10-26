import Foundation

enum NativeEngineLifecycleError: Error, LocalizedError {
    case retired
    var errorDescription: String? { "engine retired" }
}

final class NativeEngineScope: @unchecked Sendable {
    fileprivate let id: UUID
    private let current: () -> Bool
    fileprivate init(id: UUID, current: @escaping () -> Bool) {
        self.id = id
        self.current = current
    }
    var isCurrent: Bool { current() }
}

/// Serial native construction/retirement; no native code or completion holds the state lock.
final class NativeEngineLifecycle<Engine: AnyObject>: @unchecked Sendable {
    // Once unpublished, only the serial lane accesses engine. Capturing the
    // box, rather than the engine, lets retirement release ARC ownership before
    // the successor constructor runs, even while the caller still owns the box.
    private final class Entry: @unchecked Sendable {
        var engine: Engine?
        let scope: NativeEngineScope
        init(_ engine: Engine, _ scope: NativeEngineScope) {
            self.engine = engine
            self.scope = scope
        }
    }
    private let lock = NSLock()
    private let lane: DispatchQueue
    private let dispose: (Engine) -> Void
    private var entry: Entry?
    private var desired: UUID?
    private var closed = false

    init(lane: DispatchQueue = DispatchQueue(label: "orch8.native-lifecycle", qos: .utility),
         dispose: @escaping (Engine) -> Void) {
        self.lane = lane
        self.dispose = dispose
    }
    var engine: Engine? { lock.lock(); defer { lock.unlock() }; return entry?.engine }
    var isClosed: Bool { lock.lock(); defer { lock.unlock() }; return closed }
    private func isCurrent(_ id: UUID) -> Bool {
        lock.lock(); defer { lock.unlock() }
        return !closed && entry?.scope.id == id
    }
    private func wanted(_ id: UUID) -> Bool {
        lock.lock(); defer { lock.unlock() }
        return !closed && desired == id
    }
    func scope(for engine: Engine) throws -> NativeEngineScope {
        lock.lock(); defer { lock.unlock() }
        guard let entry, entry.engine === engine else { throw NativeEngineLifecycleError.retired }
        return entry.scope
    }

    func initialize(retired: () -> Void, create: @escaping () throws -> Engine,
                    configure: @escaping (Engine, NativeEngineScope) throws -> Void,
                    complete: @escaping (Error?) -> Void) {
        lock.lock()
        if closed { lock.unlock(); complete(NativeEngineLifecycleError.retired); return }
        let id = UUID()
        let scope = NativeEngineScope(id: id) { [weak self] in self?.isCurrent(id) == true }
        desired = id
        let previous = entry
        entry = nil
        // Internal pending-reply retirement only; no native or user callback.
        retired()
        lane.async {
            autoreleasepool {
                self.retire(previous)
                var candidate: Entry?
                var failure: Error?
                do {
                    guard self.wanted(id) else { throw NativeEngineLifecycleError.retired }
                    let prepared = Entry(try create(), scope)
                    candidate = prepared
                    guard self.wanted(id) else { throw NativeEngineLifecycleError.retired }
                    try self.configure(prepared, using: configure)
                    self.lock.lock()
                    let published = !self.closed && self.desired == id
                    if published { self.entry = candidate }
                    self.lock.unlock()
                    guard published else { throw NativeEngineLifecycleError.retired }
                    candidate = nil
                } catch { failure = error }
                self.retire(candidate)
                complete(failure)
            }
        }
        lock.unlock()
    }

    private func configure(_ candidate: Entry, using configure: (Engine, NativeEngineScope) throws -> Void) throws {
        guard let engine = candidate.engine else { throw NativeEngineLifecycleError.retired }
        try configure(engine, candidate.scope)
    }
    private func retire(_ retired: Entry?) {
        let engine = retired?.engine
        retired?.engine = nil
        if let engine { dispose(engine) }
    }

    func shutdown(permanent: Bool, retired: () -> Void, complete: @escaping () -> Void) {
        lock.lock()
        if closed { lock.unlock(); complete(); return }
        closed = permanent
        desired = nil
        let previous = entry
        entry = nil
        retired()
        lane.async {
            autoreleasepool { self.retire(previous); complete() }
        }
        lock.unlock()
    }

    deinit {
        guard let previous = entry else { return }
        let dispose = self.dispose
        lane.async {
            autoreleasepool {
                let engine = previous.engine
                previous.engine = nil
                if let engine { dispose(engine) }
            }
        }
    }
}
