import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import XCTest
@testable import Orch8Mobile

final class DistributedWorkerClientTests: XCTestCase {
    override func tearDown() {
        WorkerURLProtocol.reset()
        super.tearDown()
    }

    func testRejectsPlainHTTPForRemoteHosts() {
        XCTAssertThrowsError(try DistributedWorkerClient(baseURL: URL(string: "http://example.com")!)) {
            XCTAssertEqual($0 as? DistributedWorkerError, .invalidBaseURL)
        }
        XCTAssertThrowsError(try DistributedWorkerClient(baseURL: URL(string: "ftp://localhost")!)) {
            XCTAssertEqual($0 as? DistributedWorkerError, .invalidBaseURL)
        }
        XCTAssertNoThrow(try DistributedWorkerClient(baseURL: URL(string: "http://localhost:8080")!))
    }

    func testPollEncodesCapabilityAdvertisementAndDecodesTask() async throws {
        let runtimeId = UUID(uuidString: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")!
        let taskId = UUID(uuidString: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")!
        let instanceId = UUID(uuidString: "cccccccc-cccc-4ccc-8ccc-cccccccccccc")!
        WorkerURLProtocol.respond(status: 200, json: """
        {"tasks":[{"id":"\(taskId.uuidString)","instance_id":"\(instanceId.uuidString)","block_id":"render","handler_name":"browser","queue_name":"gpu","params":{"url":"https://example.com"},"context":{},"claim_epoch":7}],"lease_secs":60,"heartbeat_interval_secs":15,"poll_after_ms":1000}
        """)
        let client = try makeClient()
        let capabilities = DistributedRuntimeCapabilities(
            runtimeId: runtimeId,
            kind: "desktop",
            handlers: ["browser"],
            plugins: ["chrome"],
            regions: ["norway"],
            hardware: ["cuda"],
            connectivity: "ethernet",
            observedAt: Date(timeIntervalSince1970: 1_788_220_800),
            expiresAt: Date(timeIntervalSince1970: 1_788_221_040)
        )

        let tasks = try await client.poll(
            handler: "browser",
            capabilities: capabilities,
            queue: "gpu",
            limit: 5,
            version: "1.2.3"
        )

        XCTAssertEqual(tasks.count, 1)
        XCTAssertEqual(tasks[0].id, taskId)
        XCTAssertEqual(tasks[0].claimEpoch, 7)
        let request = try XCTUnwrap(WorkerURLProtocol.lastRequest())
        XCTAssertEqual(request.url?.path, "/api/v1/workers/tasks/poll/queue")
        XCTAssertEqual(request.value(forHTTPHeaderField: "x-orch8-tenant-id"), "tenant-a")
        XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer secret")
        let body = try jsonBody(request)
        XCTAssertEqual(body["handler_name"] as? String, "browser")
        XCTAssertEqual(body["worker_id"] as? String, runtimeId.uuidString.lowercased())
        XCTAssertEqual(body["queue_name"] as? String, "gpu")
        XCTAssertEqual(body["limit"] as? Int, 5)
        let encodedCapabilities = try XCTUnwrap(body["capabilities"] as? [String: Any])
        XCTAssertEqual(encodedCapabilities["hardware"] as? [String], ["cuda"])
        XCTAssertEqual(encodedCapabilities["regions"] as? [String], ["norway"])
    }

    func testUploadUsesStableIdentityDigestAndLeaseCoordinates() async throws {
        let task = fixtureTask(claimEpoch: 9)
        let runtimeId = UUID(uuidString: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")!
        let uploadId = UUID(uuidString: "dddddddd-dddd-4ddd-8ddd-dddddddddddd")!
        let bytes = Data("artifact body".utf8)
        let file = FileManager.default.temporaryDirectory.appendingPathComponent("report.txt")
        try bytes.write(to: file)
        defer { try? FileManager.default.removeItem(at: file) }
        WorkerURLProtocol.respond(status: 200, json: """
        {"artifact":{"id":"\(uploadId.uuidString.lowercased())","instance_id":"\(task.instanceId.uuidString.lowercased())","key":"worker/report.txt","content_type":"text/plain","size":13,"uri":"memory://artifact"},"upload_id":"\(uploadId.uuidString)","file_name":"report.txt","sha256":"9938be87d35f2a7a2b80237e8dc71806b209aaea8252f12c1b12949f61d40476","size":13}
        """)
        let client = try makeClient()

        let receipt = try await client.upload(
            fileURL: file,
            task: task,
            runtimeId: runtimeId,
            uploadId: uploadId,
            contentType: "text/plain"
        )

        XCTAssertEqual(receipt.uploadId, uploadId)
        let request = try XCTUnwrap(WorkerURLProtocol.lastRequest())
        XCTAssertEqual(
            request.url?.path,
            "/api/v1/workers/tasks/\(task.id.uuidString)/artifacts/\(uploadId.uuidString)"
        )
        let query = URLComponents(url: try XCTUnwrap(request.url), resolvingAgainstBaseURL: false)?.queryItems
        XCTAssertEqual(query?.first(where: { $0.name == "worker_id" })?.value, runtimeId.uuidString.lowercased())
        XCTAssertEqual(query?.first(where: { $0.name == "claim_epoch" })?.value, "9")
        XCTAssertEqual(query?.first(where: { $0.name == "file_name" })?.value, "report.txt")
        XCTAssertEqual(query?.first(where: { $0.name == "sha256" })?.value, "9938be87d35f2a7a2b80237e8dc71806b209aaea8252f12c1b12949f61d40476")
        XCTAssertEqual(request.value(forHTTPHeaderField: "Content-Type"), "text/plain")
        XCTAssertEqual(request.httpBody, bytes)
    }

    func testCompletePreservesMaximumClaimEpochAsInteger() async throws {
        WorkerURLProtocol.respond(status: 204, json: "")
        let client = try makeClient()

        try await client.complete(
            task: fixtureTask(claimEpoch: UInt64.max),
            runtimeId: UUID(uuidString: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")!,
            output: .object(["ok": .bool(true)])
        )

        let request = try XCTUnwrap(WorkerURLProtocol.lastRequest())
        let body = try XCTUnwrap(String(data: try XCTUnwrap(request.httpBody), encoding: .utf8))
        XCTAssertTrue(body.contains("\"claim_epoch\":18446744073709551615"), body)
    }

    func testNonSuccessResponseRetainsStatusAndServerMessage() async throws {
        WorkerURLProtocol.respond(status: 409, json: "lease expired")
        let client = try makeClient()

        do {
            _ = try await client.poll(
                handler: "browser",
                capabilities: DistributedRuntimeCapabilities(
                    runtimeId: UUID(), kind: "mobile", handlers: ["browser"]
                )
            )
            XCTFail("Expected the server response to fail")
        } catch let error as DistributedWorkerError {
            XCTAssertEqual(error, .server(status: 409, message: "lease expired"))
        }
    }

    private func makeClient() throws -> DistributedWorkerClient {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [WorkerURLProtocol.self]
        return try DistributedWorkerClient(
            baseURL: URL(string: "https://worker.test")!,
            tenantId: "tenant-a",
            apiKey: "secret",
            session: URLSession(configuration: configuration)
        )
    }

    private func fixtureTask(claimEpoch: UInt64) -> DistributedWorkerTask {
        try! JSONDecoder().decode(DistributedWorkerTask.self, from: Data("""
        {"id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","instance_id":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","block_id":"render","handler_name":"browser","queue_name":"gpu","params":{},"context":{},"claim_epoch":\(claimEpoch)}
        """.utf8))
    }

    private func jsonBody(_ request: URLRequest) throws -> [String: Any] {
        try XCTUnwrap(
            JSONSerialization.jsonObject(with: try XCTUnwrap(request.httpBody)) as? [String: Any]
        )
    }
}

private final class WorkerURLProtocol: URLProtocol, @unchecked Sendable {
    private static let lock = NSLock()
    private static var responseStatus = 200
    private static var responseData = Data()
    private static var capturedRequest: URLRequest?

    static func respond(status: Int, json: String) {
        lock.lock()
        responseStatus = status
        responseData = Data(json.utf8)
        capturedRequest = nil
        lock.unlock()
    }

    static func lastRequest() -> URLRequest? {
        lock.lock()
        defer { lock.unlock() }
        return capturedRequest
    }

    static func reset() {
        respond(status: 200, json: "")
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        Self.lock.lock()
        Self.capturedRequest = request
        let status = Self.responseStatus
        let data = Self.responseData
        Self.lock.unlock()
        let response = HTTPURLResponse(
            url: request.url!,
            statusCode: status,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": "application/json"]
        )!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: data)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}
