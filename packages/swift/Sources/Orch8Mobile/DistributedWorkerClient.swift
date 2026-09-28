import CryptoKit
import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// JSON values used by distributed task params, context, checkpoints, and output.
public enum Orch8JSON: Codable, Equatable, Sendable {
    case null
    case bool(Bool)
    case number(Double)
    case string(String)
    case array([Orch8JSON])
    case object([String: Orch8JSON])

    public init(from decoder: Decoder) throws {
        let value = try decoder.singleValueContainer()
        if value.decodeNil() { self = .null }
        else if let decoded = try? value.decode(Bool.self) { self = .bool(decoded) }
        else if let decoded = try? value.decode(Double.self) { self = .number(decoded) }
        else if let decoded = try? value.decode(String.self) { self = .string(decoded) }
        else if let decoded = try? value.decode([Orch8JSON].self) { self = .array(decoded) }
        else { self = .object(try value.decode([String: Orch8JSON].self)) }
    }

    public func encode(to encoder: Encoder) throws {
        var value = encoder.singleValueContainer()
        switch self {
        case .null: try value.encodeNil()
        case .bool(let item): try value.encode(item)
        case .number(let item): try value.encode(item)
        case .string(let item): try value.encode(item)
        case .array(let item): try value.encode(item)
        case .object(let item): try value.encode(item)
        }
    }
}

public struct DistributedRuntimeCapabilities: Codable, Equatable, Sendable {
    public let runtimeId: UUID
    public let kind: String
    public let trust: String
    public let handlers: [String]
    public let plugins: [String]
    public let credentials: [String]
    public let regions: [String]
    public let hardware: [String]
    public let offlineCapable: Bool
    public let connectivity: String?
    public let batteryPercent: UInt8?
    public let estimatedCostMicrounits: UInt64?
    public let estimatedLatencyMs: UInt64?
    public let draining: Bool
    public let observedAt: Date
    public let expiresAt: Date

    enum CodingKeys: String, CodingKey {
        case runtimeId = "runtime_id", kind, trust, handlers, plugins, credentials, regions
        case hardware, connectivity, draining
        case offlineCapable = "offline_capable"
        case batteryPercent = "battery_percent"
        case estimatedCostMicrounits = "estimated_cost_microunits"
        case estimatedLatencyMs = "estimated_latency_ms"
        case observedAt = "observed_at"
        case expiresAt = "expires_at"
    }

    public init(
        runtimeId: UUID,
        kind: String,
        trust: String = "registered",
        handlers: [String],
        plugins: [String] = [],
        credentials: [String] = [],
        regions: [String] = [],
        hardware: [String] = [],
        offlineCapable: Bool = false,
        connectivity: String? = nil,
        batteryPercent: UInt8? = nil,
        estimatedCostMicrounits: UInt64? = nil,
        estimatedLatencyMs: UInt64? = nil,
        draining: Bool = false,
        observedAt: Date = Date(),
        expiresAt: Date = Date().addingTimeInterval(240)
    ) {
        self.runtimeId = runtimeId; self.kind = kind; self.trust = trust
        self.handlers = handlers; self.plugins = plugins; self.credentials = credentials
        self.regions = regions; self.hardware = hardware; self.offlineCapable = offlineCapable
        self.connectivity = connectivity; self.batteryPercent = batteryPercent
        self.estimatedCostMicrounits = estimatedCostMicrounits
        self.estimatedLatencyMs = estimatedLatencyMs; self.draining = draining
        self.observedAt = observedAt; self.expiresAt = expiresAt
    }
}

public struct DistributedWorkerTask: Codable, Equatable, Sendable {
    public let id: UUID
    public let instanceId: UUID
    public let blockId: String
    public let handlerName: String
    public let queueName: String?
    public let params: Orch8JSON
    public let context: Orch8JSON
    public let claimEpoch: UInt64
    /// Deterministic idempotency key of the step's effect receipt. Pass it to
    /// downstream APIs (e.g. an `Idempotency-Key` header). `nil` on servers
    /// that predate the distributed-execution contract.
    public let effectId: String?
    /// Owner epoch at dispatch (fencing), when the server provides it.
    public let continuityEpoch: UInt64?
    /// Lease the server expects between heartbeats, when provided.
    public let leaseSecs: UInt32?

    enum CodingKeys: String, CodingKey {
        case id, params, context
        case instanceId = "instance_id"
        case blockId = "block_id"
        case handlerName = "handler_name"
        case queueName = "queue_name"
        case claimEpoch = "claim_epoch"
        case effectId = "effect_id"
        case continuityEpoch = "continuity_epoch"
        case leaseSecs = "lease_secs"
    }
}

public struct DistributedArtifactReceipt: Codable, Equatable, Sendable {
    public let artifact: Artifact
    public let uploadId: UUID
    public let fileName: String?
    public let sha256: String
    public let size: UInt64

    public struct Artifact: Codable, Equatable, Sendable {
        public let id: String
        public let instanceId: String
        public let key: String
        public let contentType: String
        public let size: UInt64
        public let uri: String

        enum CodingKeys: String, CodingKey {
            case id, key, size, uri
            case instanceId = "instance_id"
            case contentType = "content_type"
        }
    }

    enum CodingKeys: String, CodingKey {
        case artifact, sha256, size
        case uploadId = "upload_id"
        case fileName = "file_name"
    }
}

public enum DistributedWorkerError: Error, Equatable, Sendable {
    case invalidBaseURL
    case invalidResponse
    case server(status: Int, message: String)
}

private struct DistributedPollRequest: Encodable {
    let handlerName: String
    let workerId: String
    let queueName: String?
    let limit: UInt32
    let version: String?
    let capabilities: DistributedRuntimeCapabilities

    enum CodingKeys: String, CodingKey {
        case limit, version, capabilities
        case handlerName = "handler_name"
        case workerId = "worker_id"
        case queueName = "queue_name"
    }
}

private struct DistributedPollResponse: Decodable {
    let tasks: [DistributedWorkerTask]
    let leaseSecs: UInt64
    let heartbeatIntervalSecs: UInt64
    let pollAfterMs: UInt64

    enum CodingKeys: String, CodingKey {
        case tasks
        case leaseSecs = "lease_secs"
        case heartbeatIntervalSecs = "heartbeat_interval_secs"
        case pollAfterMs = "poll_after_ms"
    }
}

private struct DistributedLeaseRequest: Encodable {
    let workerId: String
    let claimEpoch: UInt64

    enum CodingKeys: String, CodingKey {
        case workerId = "worker_id"
        case claimEpoch = "claim_epoch"
    }
}

private struct DistributedFailRequest: Encodable {
    let workerId: String
    let claimEpoch: UInt64
    let message: String
    let retryable: Bool

    enum CodingKeys: String, CodingKey {
        case message, retryable
        case workerId = "worker_id"
        case claimEpoch = "claim_epoch"
    }
}

private struct DistributedReleaseRequest: Encodable {
    let workerId: String
    let claimEpoch: UInt64
    let started: Bool

    enum CodingKeys: String, CodingKey {
        case started
        case workerId = "worker_id"
        case claimEpoch = "claim_epoch"
    }
}

private struct DistributedCompleteRequest: Encodable {
    let workerId: String
    let claimEpoch: UInt64
    let output: Orch8JSON

    enum CodingKeys: String, CodingKey {
        case output
        case workerId = "worker_id"
        case claimEpoch = "claim_epoch"
    }
}

/// Low-level HTTP client for the worker lease protocol: capability-aware
/// poll, heartbeat, resumable file upload, complete, fail, and release. Keep a
/// stable runtime UUID and upload UUID across app launches.
///
/// Most apps should not drive this by hand: `Orch8RuntimeNode` (backed by the
/// Rust worker loop in `MobileEngine.registerNode` / `startWorker`) polls,
/// heartbeats per lease, journals claims so an OS kill releases them on the
/// next launch, and wakes on push. Use this client when a task needs an
/// artifact upload or other custom settlement.
public actor DistributedWorkerClient {
    private let baseURL: URL
    private let tenantId: String?
    private let apiKey: String?
    private let session: URLSession
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    public init(
        baseURL: URL,
        tenantId: String? = nil,
        apiKey: String? = nil,
        session: URLSession = .shared
    ) throws {
        let loopback = ["localhost", "127.0.0.1", "::1"].contains(baseURL.host ?? "")
        guard baseURL.scheme == "https" || (baseURL.scheme == "http" && loopback) else {
            throw DistributedWorkerError.invalidBaseURL
        }
        self.baseURL = baseURL; self.tenantId = tenantId; self.apiKey = apiKey; self.session = session
        let encoder = JSONEncoder(); encoder.dateEncodingStrategy = .iso8601; self.encoder = encoder
        let decoder = JSONDecoder(); decoder.dateDecodingStrategy = .iso8601; self.decoder = decoder
    }

    public func poll(
        handler: String,
        capabilities: DistributedRuntimeCapabilities,
        queue: String? = nil,
        limit: UInt32 = 1,
        version: String? = nil
    ) async throws -> [DistributedWorkerTask] {
        let body = DistributedPollRequest(
            handlerName: handler,
            workerId: capabilities.runtimeId.uuidString.lowercased(),
            queueName: queue,
            limit: min(limit, 1_000),
            version: version,
            capabilities: capabilities
        )
        let path = queue == nil ? "workers/tasks/poll" : "workers/tasks/poll/queue"
        let data = try await send(path: path, method: "POST", body: encoder.encode(body))
        return try decoder.decode(DistributedPollResponse.self, from: data).tasks
    }

    public func upload(
        fileURL: URL,
        task: DistributedWorkerTask,
        runtimeId: UUID,
        uploadId: UUID,
        contentType: String = "application/octet-stream"
    ) async throws -> DistributedArtifactReceipt {
        let bytes = try await Task.detached {
            try Data(contentsOf: fileURL, options: .mappedIfSafe)
        }.value
        let digest = SHA256.hash(data: bytes).map { String(format: "%02x", $0) }.joined()
        var components = URLComponents(
            url: url("workers/tasks/\(task.id.uuidString)/artifacts/\(uploadId.uuidString)"),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [
            URLQueryItem(name: "worker_id", value: runtimeId.uuidString.lowercased()),
            URLQueryItem(name: "claim_epoch", value: String(task.claimEpoch)),
            URLQueryItem(name: "file_name", value: fileURL.lastPathComponent),
            URLQueryItem(name: "sha256", value: digest)
        ]
        var request = request(url: components.url!, method: "POST")
        request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        let data = try await execute(request, body: bytes)
        return try decoder.decode(DistributedArtifactReceipt.self, from: data)
    }

    public func complete(
        task: DistributedWorkerTask,
        runtimeId: UUID,
        output: Orch8JSON
    ) async throws {
        let body = DistributedCompleteRequest(
            workerId: runtimeId.uuidString.lowercased(),
            claimEpoch: task.claimEpoch,
            output: output
        )
        _ = try await send(
            path: "workers/tasks/\(task.id.uuidString)/complete",
            method: "POST",
            body: encoder.encode(body)
        )
    }

    /// Extend the lease. Call at least every `leaseSecs / 3` seconds while the
    /// task runs; `DistributedWorkerError.server(status: 404 or 409, …)` means
    /// the lease was lost and the task must be abandoned.
    public func heartbeat(task: DistributedWorkerTask, runtimeId: UUID) async throws {
        let body = DistributedLeaseRequest(
            workerId: runtimeId.uuidString.lowercased(),
            claimEpoch: task.claimEpoch
        )
        _ = try await send(
            path: "workers/tasks/\(task.id.uuidString)/heartbeat",
            method: "POST",
            body: encoder.encode(body)
        )
    }

    /// Report a failed attempt. `retryable: true` lets the server retry the
    /// step under its retry policy.
    public func fail(
        task: DistributedWorkerTask,
        runtimeId: UUID,
        message: String,
        retryable: Bool
    ) async throws {
        let body = DistributedFailRequest(
            workerId: runtimeId.uuidString.lowercased(),
            claimEpoch: task.claimEpoch,
            message: message,
            retryable: retryable
        )
        _ = try await send(
            path: "workers/tasks/\(task.id.uuidString)/fail",
            method: "POST",
            body: encoder.encode(body)
        )
    }

    /// Give the task back (app backgrounding, user cancelled). `started:
    /// false` returns it to the queue untouched; `started: true` tells the
    /// server the effect may have happened. Servers without the release
    /// endpoint (404/405) get a retryable `fail` instead.
    public func release(task: DistributedWorkerTask, runtimeId: UUID, started: Bool) async throws {
        let body = DistributedReleaseRequest(
            workerId: runtimeId.uuidString.lowercased(),
            claimEpoch: task.claimEpoch,
            started: started
        )
        do {
            _ = try await send(
                path: "workers/tasks/\(task.id.uuidString)/release",
                method: "POST",
                body: encoder.encode(body)
            )
        } catch DistributedWorkerError.server(let status, _) where status == 404 || status == 405 {
            try await fail(
                task: task,
                runtimeId: runtimeId,
                message: started ? "mobile node stopped before finishing" : "released by mobile node",
                retryable: true
            )
        }
    }

    private func url(_ path: String) -> URL {
        (["api", "v1"] + path.split(separator: "/").map(String.init))
            .reduce(baseURL) { url, component in url.appendingPathComponent(component) }
    }

    private func request(url: URL, method: String) -> URLRequest {
        var request = URLRequest(url: url); request.httpMethod = method
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if let tenantId { request.setValue(tenantId, forHTTPHeaderField: "x-orch8-tenant-id") }
        if let apiKey { request.setValue("Bearer \(apiKey)", forHTTPHeaderField: "Authorization") }
        return request
    }

    private func send(path: String, method: String, body: Data) async throws -> Data {
        var request = request(url: url(path), method: method)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        return try await execute(request, body: body)
    }

    private func execute(_ request: URLRequest, body: Data) async throws -> Data {
        var request = request; request.httpBody = body
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else { throw DistributedWorkerError.invalidResponse }
        guard (200..<300).contains(http.statusCode) else {
            throw DistributedWorkerError.server(
                status: http.statusCode,
                message: String(data: data, encoding: .utf8) ?? ""
            )
        }
        return data
    }
}
