import Foundation

private final class Probe {
    private let lock = NSLock()
    private var values: [String] = []
    func record(_ value: String) { lock.lock(); values.append(value); lock.unlock() }
    var events: [String] { lock.lock(); defer { lock.unlock() }; return values }
}

private final class Resource {
    let name: String
    let probe: Probe
    let onDrop: () -> Void
    init(_ name: String, _ probe: Probe, onDrop: @escaping () -> Void = {}) {
        self.name = name; self.probe = probe; self.onDrop = onDrop
        probe.record("create:\(name)")
    }
    deinit { onDrop(); probe.record("drop:\(name)") }
}

private typealias Owner = NativeEngineLifecycle<Resource>
private final class WeakOwner {
    weak var value: Owner?
    init(_ value: Owner?) { self.value = value }
}
private struct ConfigurationFailure: Error {}

private func drain(_ queue: DispatchQueue) { queue.sync {}; queue.sync {} }

private func supersessionAndRetirement() {
    let lane = DispatchQueue(label: "test.supersession")
    let probe = Probe()
    let owner = Owner(lane: lane) { probe.record("dispose:\($0.name)") }
    let entered = DispatchSemaphore(value: 0), release = DispatchSemaphore(value: 0)
    lane.async { entered.signal(); release.wait() }
    precondition(entered.wait(timeout: .now() + 2) == .success)
    var scope: NativeEngineScope?
    let replies = Probe()
    owner.initialize(retired: {}, create: { Resource("old", probe) }, configure: { _, _ in }, complete: {
        precondition($0 is NativeEngineLifecycleError); replies.record("old")
    })
    owner.initialize(retired: {}, create: { Resource("new", probe) }, configure: { _, token in scope = token }, complete: {
        precondition($0 == nil); replies.record("new")
    })
    release.signal(); drain(lane)
    precondition(probe.events == ["create:new"])
    precondition(replies.events == ["old", "new"])
    precondition(scope?.isCurrent == true)
    owner.shutdown(permanent: false, retired: {}, complete: {})
    precondition(owner.engine == nil && scope?.isCurrent == false)
    drain(lane)
    precondition(probe.events == ["create:new", "dispose:new", "drop:new"])
    owner.shutdown(permanent: true, retired: {}, complete: {}); drain(lane)
}

private func detachDuringConstruction() {
    let lane = DispatchQueue(label: "test.detach")
    let probe = Probe()
    let owner = Owner(lane: lane) { probe.record("dispose:\($0.name)") }
    var configured = false
    let replies = Probe()
    owner.initialize(retired: {}, create: {
        owner.shutdown(permanent: true, retired: {}, complete: {})
        return Resource("candidate", probe)
    }, configure: { _, _ in configured = true }, complete: {
        precondition($0 is NativeEngineLifecycleError); replies.record("refused")
    })
    drain(lane)
    precondition(!configured && owner.engine == nil && owner.isClosed)
    precondition(replies.events == ["refused"])
    precondition(probe.events == ["create:candidate", "dispose:candidate", "drop:candidate"])
}

private func failureThenRetry() {
    let lane = DispatchQueue(label: "test.failure")
    let probe = Probe()
    let owner = Owner(lane: lane) { probe.record("dispose:\($0.name)") }
    owner.initialize(retired: {}, create: { Resource("failed", probe) }, configure: { _, _ in throw ConfigurationFailure() }, complete: {
        precondition($0 is ConfigurationFailure)
    })
    drain(lane)
    owner.initialize(retired: {}, create: {
        precondition(probe.events == ["create:failed", "dispose:failed", "drop:failed"])
        return Resource("retry", probe)
    }, configure: { _, _ in }, complete: { precondition($0 == nil) })
    drain(lane)
    owner.shutdown(permanent: true, retired: {}, complete: {}); drain(lane)
    precondition(probe.events.suffix(3) == ["create:retry", "dispose:retry", "drop:retry"])
}

private func replacementReleasesBeforeConstruction() {
    let lane = DispatchQueue(label: "test.replacement")
    let probe = Probe()
    let owner = Owner(lane: lane) { probe.record("dispose:\($0.name)") }
    var oldScope: NativeEngineScope?
    owner.initialize(retired: {}, create: { Resource("old", probe) }, configure: { _, scope in oldScope = scope }, complete: {
        precondition($0 == nil)
    })
    drain(lane)
    owner.initialize(retired: {}, create: {
        precondition(oldScope?.isCurrent == false)
        precondition(probe.events == ["create:old", "dispose:old", "drop:old"])
        return Resource("replacement", probe)
    }, configure: { _, _ in }, complete: { precondition($0 == nil) })
    precondition(oldScope?.isCurrent == false)
    drain(lane)
    owner.shutdown(permanent: true, retired: {}, complete: {}); drain(lane)
    precondition(probe.events.suffix(3) == ["create:replacement", "dispose:replacement", "drop:replacement"])
}

private func detachDuringConfiguration() {
    let lane = DispatchQueue(label: "test.configure-detach")
    let probe = Probe()
    let owner = Owner(lane: lane) { probe.record("dispose:\($0.name)") }
    var scope: NativeEngineScope?
    owner.initialize(retired: {}, create: { Resource("candidate", probe) }, configure: { engine, token in
        precondition(engine.name == "candidate")
        scope = token
        owner.shutdown(permanent: true, retired: {}, complete: { probe.record("closed") })
    }, complete: {
        precondition($0 is NativeEngineLifecycleError)
        precondition(probe.events == ["create:candidate", "dispose:candidate", "drop:candidate"])
    })
    drain(lane)
    precondition(owner.isClosed && owner.engine == nil && scope?.isCurrent == false)
    precondition(probe.events == ["create:candidate", "dispose:candidate", "drop:candidate", "closed"])
}

private func nativeWorkAndCompletionsRunOutsideLock() {
    let lane = DispatchQueue(label: "test.reentry")
    let probe = Probe()
    weak var weakOwner: Owner?
    let owner = Owner(lane: lane) { resource in
        _ = weakOwner?.engine
        probe.record("dispose:\(resource.name)")
    }
    weakOwner = owner
    owner.initialize(retired: {}, create: {
        let acquired = DispatchSemaphore(value: 0)
        DispatchQueue.global().async { _ = owner.engine; acquired.signal() }
        precondition(acquired.wait(timeout: .now() + 2) == .success, "constructor holds lifecycle lock")
        return Resource("reentrant", probe) { _ = weakOwner?.engine }
    }, configure: { _, _ in }, complete: { precondition($0 == nil) })
    drain(lane)
    owner.shutdown(permanent: true, retired: {}, complete: {}); drain(lane)
    let completed = Probe()
    owner.initialize(retired: {}, create: { preconditionFailure("closed constructor") }, configure: { _, _ in }, complete: { error in
        let acquired = DispatchSemaphore(value: 0)
        DispatchQueue.global().async { _ = owner.engine; acquired.signal() }
        precondition(acquired.wait(timeout: .now() + 2) == .success, "closed completion holds lifecycle lock")
        precondition(error is NativeEngineLifecycleError); completed.record("closed")
    })
    precondition(completed.events == ["closed"])
    precondition(probe.events == ["create:reentrant", "dispose:reentrant", "drop:reentrant"])
}

private func scopeDoesNotRetainOwner() {
    let lane = DispatchQueue(label: "test.weak-owner")
    let probe = Probe()
    var owner: Owner? = Owner(lane: lane) { probe.record("dispose:\($0.name)") }
    let weakOwner = WeakOwner(owner)
    var scope: NativeEngineScope?
    owner!.initialize(retired: {}, create: { Resource("abandoned", probe) }, configure: { _, value in scope = value }, complete: {
        precondition($0 == nil)
    })
    drain(lane)
    precondition(scope?.isCurrent == true)
    owner = nil
    precondition(weakOwner.value == nil && scope?.isCurrent == false)
    drain(lane)
    precondition(probe.events == ["create:abandoned", "dispose:abandoned", "drop:abandoned"])
}

@main
private enum Tests {
    static func main() {
        supersessionAndRetirement()
        detachDuringConstruction()
        failureThenRetry()
        replacementReleasesBeforeConstruction()
        detachDuringConfiguration()
        nativeWorkAndCompletionsRunOutsideLock()
        scopeDoesNotRetainOwner()
        print("7 Swift lifecycle tests passed")
    }
}
