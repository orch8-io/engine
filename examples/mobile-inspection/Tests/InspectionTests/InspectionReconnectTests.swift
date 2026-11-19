import Foundation
import XCTest
@testable import Inspection

final class InspectionReconnectTests: XCTestCase {
    func testResumesAfterServerAcceptsAndResponseIsLost() async throws {
        let database = try temporaryDatabase()
        let config = try configuration()
        let localId = UUID()
        let execution = UUID()
        let evidence = InspectionEvidence(
            instanceId: localId, siteId: "A-12", inspectionResult: "passed", siteTagVerified: true
        )
        let queue = InspectionUploadQueue(database: database)
        try queue.enqueue(evidence)
        var posts = 0
        var accepted: [String: Any]?
        var lookups = 0
        let client = try InspectionSubmissionClient(configuration: config) { request in
            let url = try XCTUnwrap(request.url)
            XCTAssertEqual(request.value(forHTTPHeaderField: "X-Tenant-Id"), "demo")
            if request.httpMethod == "GET" {
                lookups += 1
                XCTAssertEqual(url.path, "/api/v1/instances/\(execution.uuidString.lowercased())")
                let original = try XCTUnwrap(accepted)
                return try self.response(url, ["sequence_id": original["sequence_id"]!, "context": original["context"]!])
            }
            posts += 1
            XCTAssertEqual(url.path, "/api/v1/instances")
            let body = try XCTUnwrap(request.httpBody)
            let json = try XCTUnwrap(JSONSerialization.jsonObject(with: body) as? [String: Any])
            XCTAssertEqual(json["idempotency_key"] as? String, "field-inspection:\(localId.uuidString.lowercased())")
            if let original = accepted {
                XCTAssertEqual(original as NSDictionary, json as NSDictionary)
            } else {
                accepted = json
                throw URLError(.networkConnectionLost)
            }
            return try self.response(url, ["id": execution.uuidString, "deduplicated": true])
        }
        do {
            try await InspectionReconnect().upload(queue, using: client)
            XCTFail("Expected the connection to drop after server acceptance")
        } catch let error as URLError {
            XCTAssertEqual(error.code, .networkConnectionLost)
        }
        XCTAssertEqual(try queue.entries().filter(\.pending).count, 1)
        XCTAssertNil(try queue.entries().first?.executionId)

        let reopened = InspectionUploadQueue(database: database)
        try reopened.enqueue(evidence)
        try await InspectionReconnect().upload(reopened, using: client)
        XCTAssertEqual(try reopened.entries().first?.executionId, execution)
        try await InspectionReconnect().upload(InspectionUploadQueue(database: database), using: client)
        XCTAssertEqual(posts, 2)
        XCTAssertEqual(lookups, 1)
    }

    func testRejectsDeduplicatedExecutionWithDifferentEvidence() async throws {
        let database = try temporaryDatabase()
        let queue = InspectionUploadQueue(database: database)
        let config = try configuration()
        let evidence = InspectionEvidence(
            instanceId: UUID(), siteId: "A-12", inspectionResult: "passed", siteTagVerified: nil
        )
        try queue.enqueue(evidence)
        let client = try InspectionSubmissionClient(configuration: config) { request in
            let url = try XCTUnwrap(request.url)
            if request.httpMethod == "POST" {
                return try self.response(url, ["id": UUID().uuidString, "deduplicated": true])
            }
            return try self.response(url, [
                "sequence_id": config.destination.sequenceId.uuidString,
                "context": ["data": [
                    "instance_id": evidence.instanceId.uuidString,
                    "site_id": evidence.siteId,
                    "inspection_result": "repair_needed"
                ]]
            ])
        }
        try await InspectionReconnect().upload(queue, using: client)
        let entry = try XCTUnwrap(queue.entries().first)
        XCTAssertEqual(entry.rejected, true)
        XCTAssertFalse(entry.pending)
        XCTAssertNil(entry.executionId)
    }

    func testPendingUploadStaysBoundToOriginalDestinationAfterRestart() async throws {
        let database = try temporaryDatabase()
        let config = try configuration()
        let evidence = InspectionEvidence(
            instanceId: UUID(), siteId: "A-12", inspectionResult: "passed", siteTagVerified: nil
        )
        let queue = InspectionUploadQueue(database: database)
        try queue.enqueue(evidence)
        _ = try XCTUnwrap(queue.prepare(evidence.instanceId, destination: config.destination))

        let changed = try JSONDecoder().decode(InspectionReconnectConfiguration.self, from: Data(
            #"{"url":"https://other.example","tenant":"demo","sequence_id":"0c2b4115-0f35-4ab7-a9c7-81fe496b560d"}"#.utf8
        ))
        let wrongClient = try InspectionSubmissionClient(configuration: changed) { _ in
            XCTFail("An upload bound to the original destination must never be sent to the new host")
            throw URLError(.cancelled)
        }
        let reopened = InspectionUploadQueue(database: database)
        do {
            try await InspectionReconnect().upload(reopened, using: wrongClient)
            XCTFail("Expected a destination conflict")
        } catch InspectionSubmissionError.destinationChanged {}
        XCTAssertEqual(try reopened.entries().first?.destination, config.destination)
        XCTAssertEqual(try reopened.entries().first?.pending, true)

        let execution = UUID()
        let client = try InspectionSubmissionClient(configuration: config) { request in
            try self.response(XCTUnwrap(request.url), ["id": execution.uuidString, "deduplicated": false])
        }
        try await InspectionReconnect().upload(reopened, using: client)
        XCTAssertEqual(try reopened.entries().first?.executionId, execution)
    }

    func testQueueIgnoresUnrelatedFilesAndRejectsMisnamedEvidence() throws {
        let database = try temporaryDatabase()
        let evidence = InspectionEvidence(
            instanceId: UUID(), siteId: "A-12", inspectionResult: "passed", siteTagVerified: nil
        )
        let queue = InspectionUploadQueue(database: database)
        try queue.enqueue(evidence)
        let folder = database.appendingPathExtension("uploads")
        try Data("unrelated".utf8).write(to: folder.appendingPathComponent("notes.json"))
        XCTAssertEqual(try queue.entries().map(\.evidence), [evidence])
        let original = folder.appendingPathComponent(evidence.instanceId.uuidString.lowercased() + ".json")
        let wrong = folder.appendingPathComponent(UUID().uuidString.lowercased() + ".json")
        try FileManager.default.moveItem(at: original, to: wrong)
        XCTAssertThrowsError(try queue.entries()) {
            guard case InspectionSubmissionError.conflict = $0 else {
                return XCTFail("Unexpected error: \($0)")
            }
        }
    }

    func testRejectedSubmissionSurvivesRestartAndDoesNotBlockTheNextUpload() async throws {
        let database = try temporaryDatabase()
        let config = try configuration()
        let first = try XCTUnwrap(UUID(uuidString: "00000000-0000-4000-8000-000000000001"))
        let second = try XCTUnwrap(UUID(uuidString: "00000000-0000-4000-8000-000000000002"))
        let accepted = UUID()
        let queue = InspectionUploadQueue(database: database)
        for id in [first, second] {
            try queue.enqueue(InspectionEvidence(
                instanceId: id, siteId: "A-12", inspectionResult: "passed", siteTagVerified: nil
            ))
        }
        var attempts: [UUID] = []
        let client = try InspectionSubmissionClient(configuration: config) { request in
            let payload = try JSONSerialization.jsonObject(with: XCTUnwrap(request.httpBody)) as? [String: Any]
            let key = try XCTUnwrap(payload?["idempotency_key"] as? String)
            let id = try XCTUnwrap(UUID(uuidString: String(key.dropFirst("field-inspection:".count))))
            attempts.append(id)
            let response = try XCTUnwrap(HTTPURLResponse(
                url: XCTUnwrap(request.url), statusCode: id == first ? 422 : 201,
                httpVersion: nil, headerFields: nil
            ))
            return (try JSONSerialization.data(withJSONObject: [
                "id": accepted.uuidString, "deduplicated": false
            ]), response)
        }
        try await InspectionReconnect().upload(queue, using: client)
        try await InspectionReconnect().upload(
            InspectionUploadQueue(database: database), using: client
        )
        XCTAssertEqual(attempts, [first, second])
        let reopened = try InspectionUploadQueue(database: database).entries()
        XCTAssertTrue(reopened.filter(\.pending).isEmpty)
        XCTAssertEqual(reopened.first?.rejectionStatus, 422)
        XCTAssertEqual(reopened.last?.executionId, accepted)
        try queue.retryRejected(first)
        try queue.retryRejected(second)
        let pending = try InspectionUploadQueue(database: database).entries().filter(\.pending)
        XCTAssertEqual(pending.map { $0.evidence.instanceId }, [first])
        XCTAssertEqual(pending.first?.destination, config.destination)
    }

    private func temporaryDatabase() throws -> URL {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        addTeardownBlock { try FileManager.default.removeItem(at: directory) }
        return directory.appendingPathComponent("inspection.db")
    }

    private func configuration() throws -> InspectionReconnectConfiguration {
        let json = #"{"url":"https://inspection.example","tenant":"demo","sequence_id":"0c2b4115-0f35-4ab7-a9c7-81fe496b560d"}"#
        return try JSONDecoder().decode(InspectionReconnectConfiguration.self, from: Data(json.utf8))
    }

    private func response(_ url: URL, _ body: [String: Any]) throws -> (Data, URLResponse) {
        let http = try XCTUnwrap(HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil))
        return (try JSONSerialization.data(withJSONObject: body), http)
    }
}
