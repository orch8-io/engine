import SwiftUI

@main
struct Orch8ExampleApp: App {
    @StateObject private var viewModel = SequenceViewModel()

    var body: some Scene {
        WindowGroup {
            ContentView(viewModel: viewModel)
        }
    }
}
