import Foundation
import Orch8Mobile

private final class WeakEngine {
    weak var value: MobileEngine?
    init(_ value: MobileEngine?) { self.value = value }
}

@main
private enum NativeTests {
    static func main() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("orch8-swift-retirement-\(UUID())")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("engine.db").path
        let config = MobileEngineConfig(
            tickIntervalMs: 500, maxConcurrentSteps: 1, maxStepsPerInstance: 1000,
            maxConcurrentInstances: 10, maxTickDurationMs: 5000, maxInstanceLifetimeSecs: 86400,
            maxStoredSequences: 50, maxSequenceSizeBytes: 1_048_576, handlerTimeoutMs: 1000,
            operationTimeoutMs: 3000, telemetryEnabled: false, telemetryUrl: "", environment: "production",
            rootPublicKey: "", sdkVersion: "0.7.1", memoryBudgetBytes: 0, sequencesUrl: "", syncUrl: "",
            deviceId: "swift-retirement", syncApiKey: ""
        )
        var owners: [NativeEngineLifecycle<MobileEngine>] = []
        var scopes: [NativeEngineScope] = []
        var expectedRuntime: String?
        var baseline = 0
        for index in 0..<17 {
            let lane = DispatchQueue(label: "test.native-retirement.\(index)")
            let owner = NativeEngineLifecycle<MobileEngine>(lane: lane, dispose: { $0.shutdown() })
            owner.initialize(retired: {}, create: { try MobileEngine(dbPath: path, config: config) }, configure: { engine, scope in
                let runtime = try engine.nodeRuntimeId()
                if let expectedRuntime { precondition(runtime == expectedRuntime, "database identity changed after reopen") }
                else { expectedRuntime = runtime }
                scopes.append(scope)
            }, complete: { precondition($0 == nil, "real native initialization failed: \(String(describing: $0))") })
            lane.sync {}
            let peer = WeakEngine(owner.engine)
            precondition(peer.value != nil && scopes.last?.isCurrent == true)
            owner.shutdown(permanent: true, retired: {}, complete: {})
            lane.sync {}
            precondition(peer.value == nil, "closed owner retained the generated Swift native peer")
            precondition(scopes.last?.isCurrent == false)
            owners.append(owner) // Deliberately keep every closed controller reachable.
            if index == 0 { baseline = try descriptorCount() }
        }
        let deadline = Date().addingTimeInterval(5)
        var after = try descriptorCount()
        while after > baseline + 8 && Date() < deadline {
            Thread.sleep(forTimeInterval: 0.01)
            after = try descriptorCount()
        }
        precondition(baseline > 0 && after <= baseline + 8, "retained closed controllers grew descriptors: \(baseline) -> \(after)")
        precondition(owners.count == 17 && scopes.count == 17)
        print("Swift/native retirement passed: 17 retained closed owners, descriptors \(baseline) -> \(after)")
        withExtendedLifetime(owners) {}
        withExtendedLifetime(scopes) {}
    }

    private static func descriptorCount() throws -> Int {
        try FileManager.default.contentsOfDirectory(atPath: "/dev/fd").count
    }
}
