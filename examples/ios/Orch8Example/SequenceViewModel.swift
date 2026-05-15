import Foundation
import Orch8Mobile

@MainActor
class SequenceViewModel: ObservableObject {
    @Published var isInitialized = false
    @Published var isRunning = false
    @Published var currentSequence: String?
    @Published var currentStep: String?
    @Published var logEntries: [String] = []

    private var client: Orch8Client?

    func initializeSDK() {
        log("Initializing SDK…")

        let config = Orch8Config(
            apiUrl: "https://api.example.com",
            apiKey: "example-api-key",
            syncIntervalSecs: 300,
            maxStoredSequences: 50
        )

        do {
            client = try Orch8Client(config: config)
            isInitialized = true
            log("SDK initialized")
        } catch {
            log("Init failed: \(error.localizedDescription)")
        }
    }

    func sync() {
        guard let client else { return }
        log("Syncing…")

        Task {
            do {
                try await client.sync()
                log("Sync complete")
            } catch {
                log("Sync failed: \(error.localizedDescription)")
            }
        }
    }

    func start() {
        guard let client else { return }
        log("Starting sequence execution…")

        Task {
            do {
                try await client.start()
                isRunning = true
                log("Execution started")
            } catch {
                log("Start failed: \(error.localizedDescription)")
            }
        }
    }

    func stop() {
        guard let client else { return }
        log("Stopping…")

        Task {
            do {
                try await client.stop()
                isRunning = false
                currentSequence = nil
                currentStep = nil
                log("Stopped")
            } catch {
                log("Stop failed: \(error.localizedDescription)")
            }
        }
    }

    private func log(_ message: String) {
        let timestamp = ISO8601DateFormatter().string(from: Date())
        logEntries.append("[\(timestamp)] \(message)")
    }
}
