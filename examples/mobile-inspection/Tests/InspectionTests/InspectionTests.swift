import Foundation
import XCTest
import Orch8Mobile
@testable import Inspection

final class InspectionTests: XCTestCase {
    func testRecordsAnOfflineInspectionAfterReopeningTheDatabase() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = directory.appendingPathComponent("inspection.db")
        let id = try parkInspection(at: database)

        try completeInspection(id, at: database)

        let reopened = try InspectionStore(database: database, window: InspectionExecutionWindow())
        defer { reopened.close() }
        let completed = try XCTUnwrap(reopened.current())
        XCTAssertEqual(completed.instanceId, id)
        XCTAssertEqual(completed.state, .completed)
        let export = try reopened.export(completed)
        let exported = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(export.utf8)) as? [String: Any])
        XCTAssertEqual(Set(exported.keys), ["instance_id", "site_id", "inspection_result", "site_tag_verified"])
        XCTAssertEqual(exported["instance_id"] as? String, id)
        XCTAssertEqual(exported["site_id"] as? String, "A-12")
        XCTAssertEqual(exported["inspection_result"] as? String, "passed")
        XCTAssertEqual(exported["site_tag_verified"] as? Bool, true)
        let uploads = try InspectionUploadQueue(database: database).entries()
        XCTAssertEqual(uploads.count, 1)
        let upload = try XCTUnwrap(uploads.first)
        XCTAssertTrue(upload.pending)
        XCTAssertEqual(upload.evidence.instanceId, UUID(uuidString: id))
        XCTAssertEqual(upload.evidence.inspectionResult, "passed")
        XCTAssertEqual(upload.evidence.siteTagVerified, true)
    }

    private func parkInspection(at database: URL) throws -> String {
        let store = try InspectionStore(database: database, window: InspectionExecutionWindow())
        defer { store.close() }
        XCTAssertNil(try store.current())
        let waiting = try store.start()
        XCTAssertEqual(waiting.state, .waiting)
        XCTAssertThrowsError(try store.export(waiting))
        return waiting.instanceId
    }

    private func completeInspection(_ id: String, at database: URL) throws {
        let recovered = try InspectionStore(database: database, window: InspectionExecutionWindow())
        defer { recovered.close() }
        XCTAssertEqual(try recovered.current()?.instanceId, id)
        let completed = try recovered.record(id, result: .passed, tagVerified: true)
        XCTAssertEqual(completed.state, .completed)
        XCTAssertEqual(try recovered.recordedResult(completed), "passed")
    }
}
