import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// The narrow runtime surface used by the trusted-device coordinator.
/// Keeping this separate from the generated protocol makes the coordinator
/// testable without mocking the complete mobile engine.
public protocol TrustedDeviceContinuityRuntime: Sendable {
    func importContinuityCapsule(
        capsuleJson: String,
        payloadBase64: String,
        payloadKeyBase64: String,
        destinationRuntimeId: String,
        destinationInstanceId: String
    ) throws -> ContinuityImportResult

    func activateContinuityCapsule(
        capsuleId: String,
        destinationRuntimeId: String,
        destinationInstanceId: String
    ) throws

    func getInstance(instanceId: String) throws -> InstanceState
    func runUntilIdle(maxTicks: UInt32, timeBudgetMs: UInt64) throws -> BackgroundRunResult

    func exportContinuityCapsule(
        instanceId: String,
        destinationRuntimeId: String,
        payloadKeyBase64: String,
        expiresInSeconds: UInt32,
        signer: CapsuleSigner
    ) throws -> ContinuityExportResult
}

extension MobileEngine: TrustedDeviceContinuityRuntime {}

/// A server-issued, destination-bound envelope. Transfer keys stay in memory
/// and must never be placed in notification payloads, logs, or UserDefaults.
public struct TrustedDeviceHandoffEnvelope: Sendable {
    public let handoffId: String
    public let tenantId: String
    public let capsuleJson: String
    public let payloadBase64: String
    public let payloadKeyBase64: String
    public let destinationRuntimeId: String
    public let destinationInstanceId: String
    public let acceptanceToken: String
    public let signedGrantJson: String

    public init(
        handoffId: String,
        tenantId: String,
        capsuleJson: String,
        payloadBase64: String,
        payloadKeyBase64: String,
        destinationRuntimeId: String,
        destinationInstanceId: String,
        acceptanceToken: String,
        signedGrantJson: String
    ) {
        self.handoffId = handoffId
        self.tenantId = tenantId
        self.capsuleJson = capsuleJson
        self.payloadBase64 = payloadBase64
        self.payloadKeyBase64 = payloadKeyBase64
        self.destinationRuntimeId = destinationRuntimeId
        self.destinationInstanceId = destinationInstanceId
        self.acceptanceToken = acceptanceToken
        self.signedGrantJson = signedGrantJson
    }
}

public struct ActivatedTrustedDeviceHandoff: Equatable, Sendable {
    public let handoffId: String
    public let tenantId: String
    public let capsuleId: String
    public let continuityId: String
    public let instanceId: String
    public let runtimeId: String
    public let sourceEpoch: UInt64
}

/// A server-created return handoff. The cloud destination creates the
/// transfer key and destination instance before this plan reaches the device.
public struct TrustedDeviceReturnPlan: Sendable {
    public let handoffId: String
    public let tenantId: String
    public let destinationRuntimeId: String
    public let destinationInstanceId: String
    public let payloadKeyBase64: String
    public let expiresInSeconds: UInt32

    public init(
        handoffId: String,
        tenantId: String,
        destinationRuntimeId: String,
        destinationInstanceId: String,
        payloadKeyBase64: String,
        expiresInSeconds: UInt32 = 300
    ) {
        self.handoffId = handoffId
        self.tenantId = tenantId
        self.destinationRuntimeId = destinationRuntimeId
        self.destinationInstanceId = destinationInstanceId
        self.payloadKeyBase64 = payloadKeyBase64
        self.expiresInSeconds = expiresInSeconds
    }
}

public struct TrustedDeviceReturnReceipt: Equatable, Sendable {
    public let handoffId: String
    public let capsuleId: String
    public let continuityId: String
    public let sourceEpoch: UInt64
}

public protocol TrustedDeviceHandoffTransport: Sendable {
    func acceptExternalHandoff(
        envelope: TrustedDeviceHandoffEnvelope,
        imported: ContinuityImportResult
    ) async throws

    func resumeExternalHandoff(
        envelope: TrustedDeviceHandoffEnvelope,
        imported: ContinuityImportResult
    ) async throws

    func completeReturnHandoff(
        plan: TrustedDeviceReturnPlan,
        exported: ContinuityExportResult
    ) async throws
}

public enum TrustedDeviceHandoffError: Error, Equatable, Sendable {
    case invalidIdentifier(String)
    case invalidJson(String)
    case importedInstanceMismatch(expected: String, actual: String)
    case continuityMismatch(expected: String, actual: String)
    case unsafeReturnBoundary(InstanceStateKind)
    case invalidBaseUrl
    case controlPlane(status: Int, message: String)
}

extension TrustedDeviceHandoffError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .invalidIdentifier(let field):
            return "Invalid UUID in \(field)"
        case .invalidJson(let field):
            return "Invalid JSON in \(field)"
        case .importedInstanceMismatch(let expected, let actual):
            return "Imported instance mismatch: expected \(expected), got \(actual)"
        case .continuityMismatch(let expected, let actual):
            return "Continuity mismatch: expected \(expected), got \(actual)"
        case .unsafeReturnBoundary(let state):
            return "Execution must be paused or waiting before return, got \(state)"
        case .invalidBaseUrl:
            return "Control-plane URL must use HTTPS, except for loopback development"
        case .controlPlane(let status, let message):
            return "Control plane returned HTTP \(status): \(message)"
        }
    }
}

/// Serializes ownership transitions so duplicate push delivery or foreground
/// restoration cannot race two claims for the same local engine.
public actor TrustedDeviceHandoffCoordinator {
    private let runtime: any TrustedDeviceContinuityRuntime
    private let transport: any TrustedDeviceHandoffTransport

    public init(
        runtime: any TrustedDeviceContinuityRuntime,
        transport: any TrustedDeviceHandoffTransport
    ) {
        self.runtime = runtime
        self.transport = transport
    }

    /// Import -> control-plane accept -> local activation -> resume evidence.
    /// Every phase is idempotent, so callers should retry the whole method
    /// after interruption rather than persist intermediate secrets locally.
    public func receive(
        _ envelope: TrustedDeviceHandoffEnvelope
    ) async throws -> ActivatedTrustedDeviceHandoff {
        try validate(envelope)
        let imported = try runtime.importContinuityCapsule(
            capsuleJson: envelope.capsuleJson,
            payloadBase64: envelope.payloadBase64,
            payloadKeyBase64: envelope.payloadKeyBase64,
            destinationRuntimeId: envelope.destinationRuntimeId,
            destinationInstanceId: envelope.destinationInstanceId
        )
        guard imported.instanceId == envelope.destinationInstanceId else {
            throw TrustedDeviceHandoffError.importedInstanceMismatch(
                expected: envelope.destinationInstanceId,
                actual: imported.instanceId
            )
        }
        try await transport.acceptExternalHandoff(envelope: envelope, imported: imported)
        try runtime.activateContinuityCapsule(
            capsuleId: imported.capsuleId,
            destinationRuntimeId: envelope.destinationRuntimeId,
            destinationInstanceId: imported.instanceId
        )
        try await transport.resumeExternalHandoff(envelope: envelope, imported: imported)
        return ActivatedTrustedDeviceHandoff(
            handoffId: envelope.handoffId,
            tenantId: envelope.tenantId,
            capsuleId: imported.capsuleId,
            continuityId: imported.continuityId,
            instanceId: imported.instanceId,
            runtimeId: envelope.destinationRuntimeId,
            sourceEpoch: imported.sourceEpoch
        )
    }

    /// Run only inside an OS-granted execution window. Remaining work is
    /// reported to the host so it can schedule another opportunity.
    public func runBackgroundWindow(
        maxTicks: UInt32 = 32,
        timeBudgetMs: UInt64
    ) throws -> BackgroundRunResult {
        try runtime.runUntilIdle(maxTicks: maxTicks, timeBudgetMs: timeBudgetMs)
    }

    /// Export only from an effect-safe boundary, then let the transport drive
    /// attach -> cloud accept -> cloud resume idempotently.
    public func returnToCloud(
        active: ActivatedTrustedDeviceHandoff,
        plan: TrustedDeviceReturnPlan,
        signer: CapsuleSigner
    ) async throws -> TrustedDeviceReturnReceipt {
        try validate(plan)
        let instance = try runtime.getInstance(instanceId: active.instanceId)
        guard instance.state == .waiting || instance.state == .paused else {
            throw TrustedDeviceHandoffError.unsafeReturnBoundary(instance.state)
        }
        let exported = try runtime.exportContinuityCapsule(
            instanceId: active.instanceId,
            destinationRuntimeId: plan.destinationRuntimeId,
            payloadKeyBase64: plan.payloadKeyBase64,
            expiresInSeconds: plan.expiresInSeconds,
            signer: signer
        )
        guard exported.continuityId == active.continuityId else {
            throw TrustedDeviceHandoffError.continuityMismatch(
                expected: active.continuityId,
                actual: exported.continuityId
            )
        }
        try await transport.completeReturnHandoff(plan: plan, exported: exported)
        return TrustedDeviceReturnReceipt(
            handoffId: plan.handoffId,
            capsuleId: exported.capsuleId,
            continuityId: exported.continuityId,
            sourceEpoch: exported.sourceEpoch
        )
    }

    private func validate(_ envelope: TrustedDeviceHandoffEnvelope) throws {
        try requireUuid(envelope.handoffId, field: "handoffId")
        try requireUuid(envelope.destinationRuntimeId, field: "destinationRuntimeId")
        try requireUuid(envelope.destinationInstanceId, field: "destinationInstanceId")
        try requireJsonObject(envelope.capsuleJson, field: "capsuleJson")
        try requireJsonObject(envelope.signedGrantJson, field: "signedGrantJson")
    }

    private func validate(_ plan: TrustedDeviceReturnPlan) throws {
        try requireUuid(plan.handoffId, field: "returnPlan.handoffId")
        try requireUuid(plan.destinationRuntimeId, field: "returnPlan.destinationRuntimeId")
        try requireUuid(plan.destinationInstanceId, field: "returnPlan.destinationInstanceId")
    }

    private func requireUuid(_ value: String, field: String) throws {
        guard UUID(uuidString: value) != nil else {
            throw TrustedDeviceHandoffError.invalidIdentifier(field)
        }
    }

    private func requireJsonObject(_ value: String, field: String) throws {
        guard
            let data = value.data(using: .utf8),
            (try? JSONSerialization.jsonObject(with: data)) is [String: Any]
        else {
            throw TrustedDeviceHandoffError.invalidJson(field)
        }
    }
}

/// REST adapter for the Orch8 control-plane external-runtime endpoints.
/// The caller supplies a device-scoped API key through `headers`.
public final class URLSessionTrustedDeviceHandoffTransport:
    TrustedDeviceHandoffTransport,
    @unchecked Sendable
{
    private let baseUrl: URL
    private let headers: [String: String]
    private let session: URLSession

    public init(
        baseUrl: URL,
        headers: [String: String],
        session: URLSession = .shared
    ) throws {
        let loopback = ["localhost", "127.0.0.1", "::1"].contains(baseUrl.host ?? "")
        guard baseUrl.scheme == "https" || (baseUrl.scheme == "http" && loopback) else {
            throw TrustedDeviceHandoffError.invalidBaseUrl
        }
        self.baseUrl = baseUrl
        self.headers = headers
        self.session = session
    }

    public func acceptExternalHandoff(
        envelope: TrustedDeviceHandoffEnvelope,
        imported: ContinuityImportResult
    ) async throws {
        let signedGrant = try jsonObject(envelope.signedGrantJson, field: "signedGrantJson")
        _ = try await post(
            "continuity/handoffs/\(envelope.handoffId)/accept-external",
            body: [
                "tenant_id": envelope.tenantId,
                "destination_instance_id": imported.instanceId,
                "capsule_id": imported.capsuleId,
                "token": envelope.acceptanceToken,
                "signed_grant": signedGrant,
            ]
        )
    }

    public func resumeExternalHandoff(
        envelope: TrustedDeviceHandoffEnvelope,
        imported: ContinuityImportResult
    ) async throws {
        _ = try await post(
            "continuity/handoffs/\(envelope.handoffId)/resume-external",
            body: [
                "tenant_id": envelope.tenantId,
                "destination_instance_id": imported.instanceId,
            ]
        )
    }

    public func completeReturnHandoff(
        plan: TrustedDeviceReturnPlan,
        exported: ContinuityExportResult
    ) async throws {
        let path = "continuity/handoffs/\(plan.handoffId)"
        let current = try await get(
            path,
            queryItems: [URLQueryItem(name: "tenant_id", value: plan.tenantId)]
        )
        var state = current["state"] as? String
        if state == "requested" || state == "quiescing" {
            let capsule = try jsonObject(exported.capsuleJson, field: "capsuleJson")
            let attached = try await post(
                "\(path)/attach-device-capsule",
                body: [
                    "tenant_id": plan.tenantId,
                    "destination_instance_id": plan.destinationInstanceId,
                    "capsule": capsule,
                    "payload_base64": exported.payloadBase64,
                    "payload_key_base64": plan.payloadKeyBase64,
                ]
            )
            state = (attached["handoff"] as? [String: Any])?["state"] as? String
        }
        if state == "exported" {
            let accepted = try await post(
                "\(path)/accept",
                body: [
                    "tenant_id": plan.tenantId,
                    "destination_instance_id": plan.destinationInstanceId,
                ]
            )
            state = (accepted["handoff"] as? [String: Any])?["state"] as? String
        }
        if state == "accepted" {
            let resumed = try await post(
                "\(path)/resume",
                body: ["tenant_id": plan.tenantId]
            )
            state = resumed["state"] as? String
        }
        guard state == "resumed" else {
            throw TrustedDeviceHandoffError.controlPlane(
                status: 409,
                message: "return handoff stopped in state \(state ?? "unknown")"
            )
        }
    }

    private func get(
        _ path: String,
        queryItems: [URLQueryItem] = []
    ) async throws -> [String: Any] {
        var components = URLComponents(url: endpoint(path), resolvingAgainstBaseURL: false)
        components?.queryItems = queryItems
        guard let url = components?.url else {
            throw TrustedDeviceHandoffError.invalidBaseUrl
        }
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        applyHeaders(to: &request)
        return try await send(request)
    }

    private func post(_ path: String, body: [String: Any]) async throws -> [String: Any] {
        var request = URLRequest(url: endpoint(path))
        request.httpMethod = "POST"
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        request.setValue("application/json", forHTTPHeaderField: "content-type")
        applyHeaders(to: &request)
        return try await send(request)
    }

    private func send(_ request: URLRequest) async throws -> [String: Any] {
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw TrustedDeviceHandoffError.controlPlane(
                status: 0,
                message: "non-HTTP response"
            )
        }
        guard (200..<300).contains(http.statusCode) else {
            let message = String(data: data, encoding: .utf8) ?? "request failed"
            throw TrustedDeviceHandoffError.controlPlane(
                status: http.statusCode,
                message: message
            )
        }
        guard !data.isEmpty else { return [:] }
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw TrustedDeviceHandoffError.invalidJson("controlPlaneResponse")
        }
        return object
    }

    private func endpoint(_ path: String) -> URL {
        baseUrl.appending(path: path)
    }

    private func applyHeaders(to request: inout URLRequest) {
        for (name, value) in headers {
            request.setValue(value, forHTTPHeaderField: name)
        }
    }

    private func jsonObject(_ value: String, field: String) throws -> [String: Any] {
        guard
            let data = value.data(using: .utf8),
            let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            throw TrustedDeviceHandoffError.invalidJson(field)
        }
        return object
    }

}
