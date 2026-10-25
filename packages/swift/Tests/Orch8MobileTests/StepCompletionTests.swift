import Foundation
import XCTest
@testable import Orch8Mobile

final class StepCompletionTests: XCTestCase {
    private final class Echo: StepHandler {
        func execute(stepName: String, input: String) throws -> String { "{\"ok\":true}" }
    }

    private func makeEngine() throws -> (MobileEngine, URL) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let config = MobileEngineConfig(
            tickIntervalMs: 500, maxConcurrentSteps: 4, maxStepsPerInstance: 1000,
            maxConcurrentInstances: 10, maxTickDurationMs: 5000,
            maxInstanceLifetimeSecs: 86400, maxStoredSequences: 50,
            maxSequenceSizeBytes: 1_048_576, handlerTimeoutMs: 5000,
            operationTimeoutMs: 10000, telemetryEnabled: false, telemetryUrl: "",
            environment: "test", rootPublicKey: "", sdkVersion: "0.7.1",
            memoryBudgetBytes: 0, sequencesUrl: "", syncUrl: "", deviceId: "answer-test", syncApiKey: ""
        )
        return (try MobileEngine(dbPath: directory.appendingPathComponent("node.db").path, config: config), directory)
    }

    private func advance(_ engine: MobileEngine, _ id: String, to target: InstanceStateKind) throws -> InstanceState {
        for _ in 0..<30 {
            _ = try engine.tickOnce()
            let state = try engine.getInstance(instanceId: id)
            if state.state == target { return state }
        }
        let state = try engine.getInstance(instanceId: id)
        XCTAssertEqual(state.state, target, "workflow did not reach its expected state")
        return state
    }

    func testParallelWaitingStepsCompleteInEitherOrderThroughCommittedBindings() throws {
        for first in ["left", "right"] {
            let (engine, directory) = try makeEngine()
            defer { engine.shutdown(); try? FileManager.default.removeItem(at: directory) }
            try engine.registerHandler(name: "approval_echo", handler: Echo())
            let step: (String) -> [String: Any] = { name in
                ["type": "step", "id": name, "handler": "approval_echo", "params": [:],
                 "wait_for_input": ["prompt": "Approve?", "timeout": 60000,
                                    "choices": [["label": "Yes", "value": "yes"]], "store_as": name]]
            }
            let definition: [String: Any] = [
                "id": UUID().uuidString, "tenant_id": "mobile", "namespace": "default",
                "name": "parallel-answers", "version": 1,
                "blocks": [["type": "parallel", "id": "both", "branches": [[step("left")], [step("right")]]]],
                "created_at": ISO8601DateFormatter().string(from: Date())
            ]
            let data = try JSONSerialization.data(withJSONObject: definition)
            try engine.loadSequenceFromJson(json: String(decoding: data, as: UTF8.self))
            let id = try engine.start(sequenceName: "parallel-answers", input: "{}", dedupKey: nil)
            let waiting = try advance(engine, id, to: .waiting)
            XCTAssertThrowsError(try engine.completeStep(instanceId: id, stepName: "not-pending", output: "{\"injected\":true}"))
            let unchanged = try engine.getInstance(instanceId: id)
            XCTAssertEqual(unchanged.context, waiting.context)
            XCTAssertEqual(unchanged.updatedAt, waiting.updatedAt)
            XCTAssertEqual(unchanged.state, .waiting)
            try engine.completeStep(instanceId: id, stepName: first, output: "{\"value\":\"yes\"}")
            _ = try advance(engine, id, to: .waiting)
            let second = first == "left" ? "right" : "left"
            try engine.completeStep(instanceId: id, stepName: second, output: "{\"value\":\"yes\"}")
            let completed = try advance(engine, id, to: .completed)
            let context = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(completed.context.utf8)) as? [String: Any])
            XCTAssertEqual(context["left"] as? String, "yes")
            XCTAssertEqual(context["right"] as? String, "yes")
        }
    }
}
