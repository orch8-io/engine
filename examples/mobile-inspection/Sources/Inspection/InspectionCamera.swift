import AVFoundation
import CoreImage
import SwiftUI

enum InspectionCamera {
    enum Access: Equatable {
        case foregroundRequired, unavailable, permissionRequired, permissionDenied, ready
    }

    private enum ScanError: Error { case decoderUnavailable }

    static func access(foreground: Bool, available: Bool, authorization: AVAuthorizationStatus) -> Access {
        guard foreground else { return .foregroundRequired }
        guard available else { return .unavailable }
        switch authorization {
        case .authorized: return .ready
        case .notDetermined: return .permissionRequired
        default: return .permissionDenied
        }
    }

    @MainActor static var currentAccess: Access {
        access(
            foreground: UIApplication.shared.applicationState == .active,
            available: UIImagePickerController.isSourceTypeAvailable(.camera),
            authorization: AVCaptureDevice.authorizationStatus(for: .video)
        )
    }

    static func matchesSiteTag(_ image: CGImage) throws -> Bool {
        let context = CIContext(options: [.useSoftwareRenderer: true])
        guard let detector = CIDetector(
            ofType: CIDetectorTypeQRCode, context: context,
            options: [CIDetectorAccuracy: CIDetectorAccuracyHigh]
        ) else { throw ScanError.decoderUnavailable }
        return detector.features(in: CIImage(cgImage: image)).contains {
            ($0 as? CIQRCodeFeature)?.messageString == "A-12"
        }
    }
}

struct InspectionTagScanner: UIViewControllerRepresentable {
    enum Outcome: Equatable { case matched, mismatch, cancelled, failed }

    let finish: (Outcome) -> Void

    func makeUIViewController(context: Context) -> UIImagePickerController {
        let picker = UIImagePickerController()
        picker.sourceType = .camera
        picker.delegate = context.coordinator
        return picker
    }

    func updateUIViewController(_ controller: UIImagePickerController, context: Context) {}

    func makeCoordinator() -> Coordinator { Coordinator(finish: finish) }

    final class Coordinator: NSObject, UIImagePickerControllerDelegate, UINavigationControllerDelegate {
        private let finish: (Outcome) -> Void

        init(finish: @escaping (Outcome) -> Void) { self.finish = finish }

        func imagePickerControllerDidCancel(_ picker: UIImagePickerController) { finish(.cancelled) }

        func imagePickerController(
            _ picker: UIImagePickerController,
            didFinishPickingMediaWithInfo info: [UIImagePickerController.InfoKey: Any]
        ) {
            guard let image = (info[.originalImage] as? UIImage)?.cgImage else {
                finish(.failed)
                return
            }
            DispatchQueue.global(qos: .userInitiated).async {
                let outcome: Outcome
                do {
                    outcome = try InspectionCamera.matchesSiteTag(image) ? .matched : .mismatch
                } catch {
                    outcome = .failed
                }
                DispatchQueue.main.async { self.finish(outcome) }
            }
        }
    }
}
