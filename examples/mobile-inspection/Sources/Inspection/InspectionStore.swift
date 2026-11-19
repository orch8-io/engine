import Foundation
import Orch8Mobile

final class InspectionStore {
    enum Result: String {
        case passed
        case repairNeeded = "repair_needed"
    }

    private enum StoreError: Error {
        case missingWorkflow
        case missingRecordedResult
        case inspectionIncomplete
    }

    private struct InspectionOutput: Decodable {
        let inspectionResult: String?
        let siteTagVerified: Bool?

        enum CodingKeys: String, CodingKey {
            case inspectionResult = "inspection_result"
            case siteTagVerified = "site_tag_verified"
        }
    }

    private final class SaveInspection: StepHandler {
        func execute(stepName: String, input: String) throws -> String { "{}" }
    }

    private static let sequence = "field-inspection-offline"
    private let engine: MobileEngine
    private let selectedRun: URL
    private let window: InspectionExecutionWindow?
    private let uploads: InspectionUploadQueue

    init(database: URL, window: InspectionExecutionWindow? = nil) throws {
        _ = try window?.remainingMilliseconds()
        let engine = try MobileEngine(dbPath: database.path, config: MobileEngineConfig(
            tickIntervalMs: 500,
            maxConcurrentSteps: 1,
            maxStepsPerInstance: 20,
            maxConcurrentInstances: 1,
            maxTickDurationMs: 5_000,
            maxInstanceLifetimeSecs: 86_400,
            maxStoredSequences: 5,
            maxSequenceSizeBytes: 65_536,
            handlerTimeoutMs: 5_000,
            operationTimeoutMs: 10_000,
            telemetryEnabled: false,
            telemetryUrl: "",
            environment: "production",
            rootPublicKey: "",
            sdkVersion: orch8MobileVersion,
            memoryBudgetBytes: 0,
            sequencesUrl: "",
            syncUrl: "",
            deviceId: "",
            syncApiKey: ""
        ))
        do {
            _ = try window?.remainingMilliseconds()
            try engine.registerHandler(name: "save_inspection", handler: SaveInspection())
            _ = try window?.remainingMilliseconds()
            if try !engine.loadedSequences().contains(where: { $0.name == Self.sequence && $0.version == 1 }) {
                guard let workflow = Bundle.main.url(forResource: Self.sequence, withExtension: "json") else {
                    throw StoreError.missingWorkflow
                }
                let source = try String(contentsOf: workflow, encoding: .utf8)
                _ = try window?.remainingMilliseconds()
                try engine.loadSequenceFromJson(json: source)
            }
        } catch {
            engine.shutdown()
            throw error
        }
        self.engine = engine
        self.selectedRun = database.appendingPathExtension("selected-run")
        self.window = window
        self.uploads = InspectionUploadQueue(database: database)
    }

    static func appDatabase() throws -> URL {
        try FileManager.default.url(
            for: .applicationSupportDirectory, in: .userDomainMask,
            appropriateFor: nil, create: true
        ).appendingPathComponent("inspection.db")
    }

    func current() throws -> InstanceState? {
        _ = try window?.remainingMilliseconds()
        if let active = try engine.activeInstances().first(where: { $0.sequenceName == Self.sequence }) {
            try remember(active.instanceId)
            return try advance(active.instanceId)
        }
        guard FileManager.default.fileExists(atPath: selectedRun.path) else { return nil }
        let id = try String(contentsOf: selectedRun, encoding: .utf8)
        return try advance(id)
    }

    func start() throws -> InstanceState {
        _ = try window?.remainingMilliseconds()
        // Recover an earlier completion before changing the selected run.
        if FileManager.default.fileExists(atPath: selectedRun.path) {
            let previous = try String(contentsOf: selectedRun, encoding: .utf8)
            try queueCompletion(engine.getInstance(instanceId: previous))
        }
        let id = try engine.start(
            sequenceName: Self.sequence,
            input: #"{"site_id":"A-12"}"#,
            dedupKey: "inspection:A-12"
        )
        try remember(id)
        return try advance(id)
    }

    func record(_ id: String, result: Result, tagVerified: Bool = false) throws -> InstanceState {
        _ = try window?.remainingMilliseconds()
        let answer: [String: Any] = ["value": result.rawValue, "site_tag_verified": tagVerified]
        let encoded = try JSONSerialization.data(withJSONObject: answer)
        try engine.completeStep(instanceId: id, stepName: "record_inspection", output: String(decoding: encoded, as: UTF8.self))
        return try advance(id)
    }

    func recordedResult(_ state: InstanceState) throws -> String {
        let output = try inspectionOutput(state)
        guard let result = output.inspectionResult else {
            throw StoreError.missingRecordedResult
        }
        return result
    }

    func export(_ state: InstanceState) throws -> String {
        guard state.state == .completed, state.sequenceName == Self.sequence else {
            throw StoreError.inspectionIncomplete
        }
        let output = try inspectionOutput(state)
        guard let result = output.inspectionResult else { throw StoreError.missingRecordedResult }
        var payload: [String: Any] = [
            "instance_id": state.instanceId,
            "site_id": "A-12",
            "inspection_result": result
        ]
        if let verified = output.siteTagVerified {
            payload["site_tag_verified"] = verified
        }
        return String(decoding: try JSONSerialization.data(withJSONObject: payload, options: .sortedKeys), as: UTF8.self)
    }

    private func inspectionOutput(_ state: InstanceState) throws -> InspectionOutput {
        try JSONDecoder().decode(InspectionOutput.self, from: Data(state.context.utf8))
    }

    private func queueCompletion(_ state: InstanceState) throws {
        guard state.state == .completed, state.sequenceName == Self.sequence else { return }
        let output = try inspectionOutput(state)
        guard let id = UUID(uuidString: state.instanceId), let result = output.inspectionResult else {
            throw StoreError.missingRecordedResult
        }
        try uploads.enqueue(InspectionEvidence(
            instanceId: id, siteId: "A-12", inspectionResult: result, siteTagVerified: output.siteTagVerified
        ))
    }

    func close() { engine.shutdown() }

    private func remember(_ id: String) throws {
        try id.write(to: selectedRun, atomically: true, encoding: .utf8)
    }

    private func advance(_ id: String) throws -> InstanceState {
        _ = try engine.runUntilIdle(maxTicks: 20, timeBudgetMs: window?.remainingMilliseconds() ?? 5_000)
        _ = try window?.remainingMilliseconds()
        let state = try engine.getInstance(instanceId: id)
        try queueCompletion(state)
        return state
    }
}
