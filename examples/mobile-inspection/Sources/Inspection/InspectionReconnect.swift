import Foundation

struct InspectionEvidence: Codable, Equatable {
    let instanceId: UUID
    let siteId: String
    let inspectionResult: String
    let siteTagVerified: Bool?

    enum CodingKeys: String, CodingKey {
        case instanceId = "instance_id"
        case siteId = "site_id"
        case inspectionResult = "inspection_result"
        case siteTagVerified = "site_tag_verified"
    }

    // The manual reconnect utility uses lowercase UUIDs in both the export and idempotency key.
    func encode(to encoder: Encoder) throws {
        var fields = encoder.container(keyedBy: CodingKeys.self)
        try fields.encode(instanceId.uuidString.lowercased(), forKey: .instanceId)
        try fields.encode(siteId, forKey: .siteId)
        try fields.encode(inspectionResult, forKey: .inspectionResult)
        try fields.encodeIfPresent(siteTagVerified, forKey: .siteTagVerified)
    }
}

struct InspectionDestination: Codable, Equatable {
    let url: URL
    let tenant: String
    let sequenceId: UUID

    enum CodingKeys: String, CodingKey {
        case url, tenant
        case sequenceId = "sequence_id"
    }
}

struct InspectionReconnectConfiguration: Decodable {
    let destination: InspectionDestination
    let apiKey: String?

    init(from decoder: Decoder) throws {
        destination = try InspectionDestination(from: decoder)
        let values = try decoder.container(keyedBy: CodingKeys.self)
        apiKey = try values.decodeIfPresent(String.self, forKey: .apiKey)
    }

    private enum CodingKeys: String, CodingKey {
        case apiKey = "api_key"
    }

    static func load() throws -> Self? {
        guard let url = Bundle.main.url(forResource: "reconnect-config", withExtension: "json") else { return nil }
        return try JSONDecoder().decode(Self.self, from: Data(contentsOf: url))
    }
}

struct InspectionUpload: Codable {
    let evidence: InspectionEvidence
    var destination: InspectionDestination?
    var executionId: UUID?
    var rejected: Bool?
    var rejectionStatus: Int?

    var pending: Bool { executionId == nil && rejected != true }
}

struct InspectionUploadQueue {
    private static let lock = NSLock()
    private let directory: URL

    init(database: URL) { directory = database.appendingPathExtension("uploads") }

    func enqueue(_ evidence: InspectionEvidence) throws {
        try Self.lock.withLock {
            if let upload = try read(evidence.instanceId) {
                guard upload.evidence == evidence else { throw InspectionSubmissionError.conflict }
                return
            }
            try save(InspectionUpload(evidence: evidence))
        }
    }

    func entries() throws -> [InspectionUpload] {
        try Self.lock.withLock {
            guard FileManager.default.fileExists(atPath: directory.path) else { return [] }
            return try FileManager.default.contentsOfDirectory(atPath: directory.path).sorted().compactMap {
                guard $0.hasSuffix(".json"), let id = UUID(uuidString: String($0.dropLast(5))) else { return nil }
                return try read(id)
            }
        }
    }

    func prepare(_ id: UUID, destination: InspectionDestination) throws -> InspectionUpload? {
        try Self.lock.withLock {
            guard var entry = try read(id), entry.pending else { return nil }
            if let original = entry.destination {
                guard original == destination else { throw InspectionSubmissionError.destinationChanged }
            } else {
                entry.destination = destination
                try save(entry)
            }
            return entry
        }
    }

    func record(_ upload: InspectionUpload, executionId: UUID?, rejectionStatus: Int? = nil) throws {
        try Self.lock.withLock {
            guard var entry = try read(upload.evidence.instanceId), entry.pending else { return }
            entry.executionId = executionId
            entry.rejected = executionId == nil
            entry.rejectionStatus = rejectionStatus
            try save(entry)
        }
    }

    func retryRejected(_ id: UUID) throws {
        try Self.lock.withLock {
            guard var entry = try read(id), entry.executionId == nil,
                  entry.rejectionStatus != nil else { return }
            entry.rejected = nil
            entry.rejectionStatus = nil
            try save(entry)
        }
    }

    private func path(_ id: UUID) -> URL {
        directory.appendingPathComponent(id.uuidString.lowercased()).appendingPathExtension("json")
    }

    private func read(_ id: UUID) throws -> InspectionUpload? {
        let url = path(id)
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        let upload = try JSONDecoder().decode(InspectionUpload.self, from: Data(contentsOf: url))
        guard upload.evidence.instanceId == id else { throw InspectionSubmissionError.conflict }
        return upload
    }

    private func save(_ upload: InspectionUpload) throws {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        try JSONEncoder().encode(upload).write(to: path(upload.evidence.instanceId), options: .atomic)
    }
}

enum InspectionSubmissionError: Error {
    case invalidDestination
    case invalidResponse
    case serverStatus(Int)
    case conflict
    case destinationChanged
}

private final class NoInspectionRedirects: NSObject, URLSessionTaskDelegate {
    func urlSession(
        _ session: URLSession, task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

struct InspectionSubmissionClient {
    typealias Send = (URLRequest) async throws -> (Data, URLResponse)

    private static let session: URLSession = {
        let config = URLSessionConfiguration.ephemeral
        config.timeoutIntervalForRequest = 10
        config.timeoutIntervalForResource = 15
        return URLSession(configuration: config, delegate: NoInspectionRedirects(), delegateQueue: nil)
    }()

    private struct Start: Encodable {
        struct Context: Encodable { let data: InspectionEvidence }
        let sequence_id: String
        let tenant_id: String
        let namespace = "default"
        let idempotency_key: String
        let context: Context
    }

    private struct Accepted: Decodable {
        let id: UUID
        let deduplicated: Bool?
    }

    private struct Stored: Decodable {
        struct Context: Decodable { let data: InspectionEvidence }
        let sequence_id: UUID
        let context: Context
    }

    let destination: InspectionDestination
    private let apiKey: String?
    private let base: URL
    private let send: Send

    init(
        configuration: InspectionReconnectConfiguration,
        send: @escaping Send = { try await Self.session.data(for: $0) }
    ) throws {
        let destination = configuration.destination
        guard destination.url.scheme == "https", destination.url.host?.isEmpty == false,
              destination.url.user == nil, destination.url.password == nil,
              destination.url.query == nil, destination.url.fragment == nil,
              destination.tenant.range(of: #"^[A-Za-z0-9_-]{1,64}$"#, options: .regularExpression) != nil,
              configuration.apiKey?.rangeOfCharacter(from: .newlines) == nil else {
            throw InspectionSubmissionError.invalidDestination
        }
        self.destination = destination
        apiKey = configuration.apiKey
        let trimmed = destination.url.absoluteString.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        base = URL(string: trimmed.hasSuffix("/api/v1") ? trimmed : trimmed + "/api/v1")!
        self.send = send
    }

    func submit(_ evidence: InspectionEvidence) async throws -> UUID {
        let payload = Start(
            sequence_id: destination.sequenceId.uuidString.lowercased(), tenant_id: destination.tenant,
            idempotency_key: "field-inspection:\(evidence.instanceId.uuidString.lowercased())",
            context: Start.Context(data: evidence)
        )
        let response = try await request("instances", body: JSONEncoder().encode(payload), as: Accepted.self)
        if response.deduplicated != false {
            let stored = try await request(
                "instances/\(response.id.uuidString.lowercased())", as: Stored.self
            )
            guard stored.sequence_id == destination.sequenceId, stored.context.data == evidence else {
                throw InspectionSubmissionError.conflict
            }
        }
        return response.id
    }

    private func request<T: Decodable>(_ path: String, body: Data? = nil, as: T.Type) async throws -> T {
        var request = URLRequest(url: base.appendingPathComponent(path))
        request.httpMethod = body == nil ? "GET" : "POST"
        request.setValue(destination.tenant, forHTTPHeaderField: "X-Tenant-Id")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let apiKey { request.setValue(apiKey, forHTTPHeaderField: "X-API-Key") }
        request.httpBody = body
        let (data, response) = try await send(request)
        guard let http = response as? HTTPURLResponse, data.count <= 1_048_576 else {
            throw InspectionSubmissionError.invalidResponse
        }
        guard (200..<300).contains(http.statusCode) else {
            throw InspectionSubmissionError.serverStatus(http.statusCode)
        }
        return try JSONDecoder().decode(T.self, from: data)
    }
}

actor InspectionReconnect {
    private var running = false

    func upload(_ queue: InspectionUploadQueue, using client: InspectionSubmissionClient) async throws {
        guard !running else { return }
        running = true
        defer { running = false }
        var lastError: Error?
        for entry in try queue.entries() where entry.pending {
            try Task.checkCancellation()
            do {
                guard let prepared = try queue.prepare(entry.evidence.instanceId, destination: client.destination) else {
                    continue
                }
                do {
                    let execution = try await client.submit(prepared.evidence)
                    try queue.record(prepared, executionId: execution)
                } catch InspectionSubmissionError.conflict {
                    try queue.record(prepared, executionId: nil)
                } catch InspectionSubmissionError.serverStatus(let status) where [400, 413, 422].contains(status) {
                    try queue.record(prepared, executionId: nil, rejectionStatus: status)
                }
            } catch InspectionSubmissionError.destinationChanged {
                lastError = InspectionSubmissionError.destinationChanged
            }
        }
        if let lastError { throw lastError }
    }
}
