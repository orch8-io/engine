import AVFoundation
import CoreImage.CIFilterBuiltins
import XCTest
@testable import Inspection

final class InspectionCameraTests: XCTestCase {
    func testCameraIsBlockedOutsideForeground() {
        XCTAssertEqual(InspectionCamera.access(foreground: false, available: true, authorization: .authorized), .foregroundRequired)
    }

    func testDeniedPermissionNeverOpensTheCamera() {
        XCTAssertEqual(InspectionCamera.access(foreground: true, available: true, authorization: .denied), .permissionDenied)
        XCTAssertEqual(InspectionCamera.access(foreground: true, available: true, authorization: .restricted), .permissionDenied)
    }

    func testScannerReturnsOnlyWhetherTheExpectedSiteTagWasPresent() throws {
        XCTAssertTrue(try InspectionCamera.matchesSiteTag(qrCode("A-12")))
        XCTAssertFalse(try InspectionCamera.matchesSiteTag(qrCode("private-unrelated-identifier")))
    }

    private func qrCode(_ payload: String) throws -> CGImage {
        let filter = CIFilter.qrCodeGenerator()
        filter.message = Data(payload.utf8)
        let code = try XCTUnwrap(filter.outputImage).transformed(by: .init(scaleX: 12, y: 12))
        let bordered = code.composited(over: CIImage(color: .white).cropped(to: code.extent.insetBy(dx: -48, dy: -48)))
        return try XCTUnwrap(CIContext().createCGImage(bordered, from: bordered.extent))
    }
}
