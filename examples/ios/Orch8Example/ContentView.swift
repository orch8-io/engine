import SwiftUI

struct ContentView: View {
    @ObservedObject var viewModel: SequenceViewModel

    var body: some View {
        NavigationView {
            VStack(spacing: 24) {
                statusSection
                controlSection
                logSection
            }
            .padding()
            .navigationTitle("Orch8 Example")
        }
    }

    private var statusSection: some View {
        GroupBox("Status") {
            VStack(alignment: .leading, spacing: 8) {
                HStack {
                    Circle()
                        .fill(viewModel.isRunning ? .green : .gray)
                        .frame(width: 10, height: 10)
                    Text(viewModel.isRunning ? "Running" : "Stopped")
                }
                if let seq = viewModel.currentSequence {
                    Text("Sequence: \(seq)")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                if let step = viewModel.currentStep {
                    Text("Step: \(step)")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private var controlSection: some View {
        VStack(spacing: 12) {
            Button("Initialize SDK") {
                viewModel.initializeSDK()
            }
            .buttonStyle(.borderedProminent)
            .disabled(viewModel.isInitialized)

            HStack(spacing: 12) {
                Button("Sync") {
                    viewModel.sync()
                }
                .buttonStyle(.bordered)
                .disabled(!viewModel.isInitialized)

                Button(viewModel.isRunning ? "Stop" : "Start") {
                    if viewModel.isRunning {
                        viewModel.stop()
                    } else {
                        viewModel.start()
                    }
                }
                .buttonStyle(.bordered)
                .disabled(!viewModel.isInitialized)
            }
        }
    }

    private var logSection: some View {
        GroupBox("Log") {
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 4) {
                    ForEach(viewModel.logEntries, id: \.self) { entry in
                        Text(entry)
                            .font(.system(.caption, design: .monospaced))
                    }
                }
            }
            .frame(maxWidth: .infinity, maxHeight: 200, alignment: .topLeading)
        }
    }
}
