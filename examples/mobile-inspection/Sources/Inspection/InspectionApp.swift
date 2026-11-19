import AVFoundation
import Foundation
import Orch8Mobile
import SwiftUI

@main
struct InspectionApp: App {
    var body: some Scene {
        WindowGroup { InspectionView() }
    }
}

private struct InspectionView: View {
    @Environment(\.scenePhase) private var scenePhase
    @StateObject private var lifecycle = InspectionLifecycle()
    @State private var status = "Reading inspection…"
    @State private var run: InstanceState?
    @State private var busy = true
    @State private var ready = false
    @State private var scannerVisible = false
    @State private var tagVerified = false
    @State private var scannerStatus: String?
    @State private var exportPayload: String?
    @State private var reconnect = InspectionReconnect()
    @State private var uploads: [InspectionUpload] = []
    @State private var reconnectError: String?
    @State private var reconnectConfigured = false

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Field inspection").font(.largeTitle.bold())
            Text("Site A-12 · Saved on this device").foregroundStyle(.secondary)
            Text(status).font(.headline)
            if reconnectConfigured, let text = uploadStatus {
                Text(text).foregroundStyle(.secondary)
            }
            if let id = selectedRejectedUpload {
                Button("Retry inspection upload") { retryUpload(id) }
            }
            if ready, run == nil || [.completed, .failed, .cancelled].contains(run?.state) {
                Button("Start inspection") { perform { try $0.start() } }
                    .buttonStyle(.borderedProminent)
            }
            if let run, run.state == .waiting {
                Button("Scan site QR tag", action: openScanner).buttonStyle(.bordered)
                if let scannerStatus { Text(scannerStatus).foregroundStyle(.secondary) }
                Button("Mark passed") { record(run.instanceId, result: .passed) }
                    .buttonStyle(.borderedProminent)
                Button("Mark repair needed") { record(run.instanceId, result: .repairNeeded) }
                    .buttonStyle(.bordered)
            }
            if let exportPayload {
                Button("Copy inspection export") {
                    UIPasteboard.general.string = exportPayload
                    status = "Inspection JSON copied"
                }
            }
            Button("Refresh status", action: refresh)
            Spacer()
        }
        .disabled(busy)
        .padding(24)
        .frame(maxWidth: .infinity, alignment: .leading)
        .task { refresh() }
        .task(id: scenePhase) {
            if scenePhase == .active { await uploadWhileActive() }
        }
        .onChange(of: scenePhase) { phase in
            if phase == .active { refresh() }
        }
        .sheet(isPresented: $scannerVisible) {
            InspectionTagScanner { outcome in
                scannerVisible = false
                tagVerified = outcome == .matched
                switch outcome {
                case .matched: scannerStatus = "Site A-12 verified"
                case .mismatch: scannerStatus = "The image did not contain the Site A-12 tag."
                case .cancelled: scannerStatus = nil
                case .failed: scannerStatus = "Could not read the image."
                }
            }
        }
    }

    private func record(_ id: String, result: InspectionStore.Result) {
        let verified = tagVerified
        perform { try $0.record(id, result: result, tagVerified: verified) }
    }

    private func openScanner() {
        Task { @MainActor in
            if InspectionCamera.currentAccess == .permissionRequired {
                _ = await AVCaptureDevice.requestAccess(for: .video)
            }
            switch InspectionCamera.currentAccess {
            case .ready: scannerVisible = true
            case .foregroundRequired: scannerStatus = "Bring the app to the foreground and try again."
            case .unavailable: scannerStatus = "This device has no available camera."
            case .permissionDenied, .permissionRequired: scannerStatus = "Camera access is off. You can continue without scanning."
            }
        }
    }

    private func refresh() {
        perform { try $0.current() }
    }

    private var selectedRejectedUpload: UUID? {
        guard let selected = run.flatMap({ UUID(uuidString: $0.instanceId) }),
              uploads.contains(where: {
                  $0.evidence.instanceId == selected && $0.rejectionStatus != nil
              }) else { return nil }
        return selected
    }

    private func retryUpload(_ id: UUID) {
        do {
            let queue = try InspectionUploadQueue(database: InspectionStore.appDatabase())
            try queue.retryRejected(id)
            uploads = try queue.entries()
            reconnectError = nil
        } catch {
            reconnectError = "Could not queue the inspection for retry."
        }
    }

    private var uploadStatus: String? {
        let selected = run.flatMap { UUID(uuidString: $0.instanceId) }
        if let error = reconnectError { return error }
        if let execution = uploads.first(where: { $0.evidence.instanceId == selected })?.executionId {
            return "Received by server · \(execution.uuidString.lowercased())"
        }
        if uploads.contains(where: { $0.rejectionStatus != nil }) {
            return "A queued inspection needs attention. The server rejected its evidence."
        }
        if uploads.contains(where: { $0.rejected == true }) {
            return "Server has different evidence for a queued inspection ID."
        }
        let pending = uploads.filter(\.pending).count
        return pending == 0 ? nil : "Pending upload · \(pending)"
    }

    private func uploadWhileActive() async {
        let queue: InspectionUploadQueue
        let client: InspectionSubmissionClient
        do {
            guard let config = try InspectionReconnectConfiguration.load() else { return }
            reconnectConfigured = true
            queue = try InspectionUploadQueue(database: InspectionStore.appDatabase())
            client = try InspectionSubmissionClient(configuration: config)
        } catch {
            reconnectConfigured = true
            reconnectError = "Could not read the inspection reconnect configuration."
            return
        }
        var retrySeconds: UInt64 = 5
        while !Task.isCancelled {
            do {
                uploads = try queue.entries()
                try await reconnect.upload(queue, using: client)
                try Task.checkCancellation()
                uploads = try queue.entries()
                reconnectError = nil
                retrySeconds = 5
            } catch InspectionSubmissionError.destinationChanged {
                reconnectError = "A queued inspection belongs to the previous server configuration."
            } catch {
                guard !Task.isCancelled else { return }
                reconnectError = "Upload pending · Retrying automatically"
                retrySeconds = min(retrySeconds * 2, 30)
            }
            do {
                try await Task.sleep(nanoseconds: retrySeconds * 1_000_000_000)
            } catch {
                return
            }
        }
    }

    private func perform(_ operation: @escaping (InspectionStore) throws -> InstanceState?) {
        guard scenePhase == .active else { return }
        let started = lifecycle.run({ window in
            let store = try InspectionStore(database: InspectionStore.appDatabase(), window: window)
            defer { store.close() }
            let snapshot = try operation(store)
            let answer = try snapshot.flatMap { $0.state == .completed ? try store.recordedResult($0) : nil }
            let exported = try snapshot.flatMap { $0.state == .completed ? try store.export($0) : nil }
            return (snapshot, answer, exported)
        }) { result in
            busy = false
            switch result {
            case let .success((snapshot, answer, exported)):
                if snapshot?.instanceId != run?.instanceId {
                    tagVerified = false
                    scannerStatus = nil
                }
                run = snapshot
                exportPayload = exported
                ready = true
                switch snapshot?.state {
                case nil: status = "Ready to start"
                case .waiting: status = "Waiting for the inspection result"
                case .completed: status = answer == "passed" ? "Saved · Passed" : "Saved · Repair needed"
                case .failed: status = "Inspection failed"
                case .cancelled: status = "Inspection cancelled"
                default: status = "Inspection in progress"
                }
            case .failure:
                ready = false
                exportPayload = nil
                status = "Could not read or save the inspection. Refresh to retry."
            }
        }
        if started { busy = true }
    }
}
