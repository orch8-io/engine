import XCTest
@testable import Orch8Mobile

final class TrustedDeviceHandoffTests: XCTestCase {
    func testReceiveCompletesRetrySafeOwnershipPhasesInOrder() async throws {
        let ids = TestIds()
        let runtime = FakeRuntime(ids: ids)
        let transport = RecordingTransport()
        let coordinator = TrustedDeviceHandoffCoordinator(runtime: runtime, transport: transport)

        let active = try await coordinator.receive(envelope(ids: ids))

        XCTAssertEqual(active.instanceId, ids.deviceInstance)
        XCTAssertEqual(active.continuityId, ids.continuity)
        XCTAssertEqual(runtime.calls, ["import", "activate"])
        let calls = await transport.calls
        XCTAssertEqual(calls, ["accept", "resume"])
    }

    func testReceiveRejectsAnImportBoundToAnotherInstance() async throws {
        let ids = TestIds()
        let runtime = FakeRuntime(ids: ids)
        runtime.importedInstanceId = UUID().uuidString
        let coordinator = TrustedDeviceHandoffCoordinator(
            runtime: runtime,
            transport: RecordingTransport()
        )

        do {
            _ = try await coordinator.receive(envelope(ids: ids))
            XCTFail("Expected the imported-instance binding check to fail")
        } catch let error as TrustedDeviceHandoffError {
            XCTAssertEqual(
                error,
                .importedInstanceMismatch(
                    expected: ids.deviceInstance,
                    actual: runtime.importedInstanceId
                )
            )
        }
        XCTAssertEqual(runtime.calls, ["import"])
    }

    func testReceiveValidatesEnvelopeBeforeTouchingRuntimeOrTransport() async throws {
        let ids = TestIds()
        let runtime = FakeRuntime(ids: ids)
        let transport = RecordingTransport()
        let coordinator = TrustedDeviceHandoffCoordinator(runtime: runtime, transport: transport)
        var invalid = envelope(ids: ids)
        invalid = TrustedDeviceHandoffEnvelope(
            handoffId: "not-a-uuid",
            tenantId: invalid.tenantId,
            capsuleJson: invalid.capsuleJson,
            payloadBase64: invalid.payloadBase64,
            payloadKeyBase64: invalid.payloadKeyBase64,
            destinationRuntimeId: invalid.destinationRuntimeId,
            destinationInstanceId: invalid.destinationInstanceId,
            acceptanceToken: invalid.acceptanceToken,
            signedGrantJson: invalid.signedGrantJson
        )

        do {
            _ = try await coordinator.receive(invalid)
            XCTFail("Expected identifier validation to fail")
        } catch let error as TrustedDeviceHandoffError {
            XCTAssertEqual(error, .invalidIdentifier("handoffId"))
        }
        XCTAssertTrue(runtime.calls.isEmpty)
        let calls = await transport.calls
        XCTAssertTrue(calls.isEmpty)
    }

    func testReturnRequiresPausedOrWaitingBoundary() async throws {
        let ids = TestIds()
        let runtime = FakeRuntime(ids: ids)
        runtime.instanceState = .running
        let transport = RecordingTransport()
        let coordinator = TrustedDeviceHandoffCoordinator(runtime: runtime, transport: transport)

        do {
            _ = try await coordinator.returnToCloud(
                active: active(ids: ids),
                plan: returnPlan(ids: ids),
                signer: FakeSigner()
            )
            XCTFail("Expected the safe-boundary check to fail")
        } catch let error as TrustedDeviceHandoffError {
            XCTAssertEqual(error, .unsafeReturnBoundary(.running))
        }
        XCTAssertEqual(runtime.calls, ["get"])
        let calls = await transport.calls
        XCTAssertTrue(calls.isEmpty)
    }

    func testReturnExportsAndCompletesControlPlaneHandoff() async throws {
        let ids = TestIds()
        let runtime = FakeRuntime(ids: ids)
        runtime.instanceState = .waiting
        let transport = RecordingTransport()
        let coordinator = TrustedDeviceHandoffCoordinator(runtime: runtime, transport: transport)

        let receipt = try await coordinator.returnToCloud(
            active: active(ids: ids),
            plan: returnPlan(ids: ids),
            signer: FakeSigner()
        )

        XCTAssertEqual(receipt.continuityId, ids.continuity)
        XCTAssertEqual(runtime.calls, ["get", "export"])
        let calls = await transport.calls
        XCTAssertEqual(calls, ["return"])
    }

    func testReturnRejectsCapsuleFromAnotherContinuityBeforeControlPlaneCall() async throws {
        let ids = TestIds()
        let runtime = FakeRuntime(ids: ids)
        runtime.exportedContinuityId = UUID().uuidString
        let transport = RecordingTransport()
        let coordinator = TrustedDeviceHandoffCoordinator(runtime: runtime, transport: transport)

        do {
            _ = try await coordinator.returnToCloud(
                active: active(ids: ids),
                plan: returnPlan(ids: ids),
                signer: FakeSigner()
            )
            XCTFail("Expected continuity binding validation to fail")
        } catch let error as TrustedDeviceHandoffError {
            XCTAssertEqual(
                error,
                .continuityMismatch(expected: ids.continuity, actual: runtime.exportedContinuityId)
            )
        }
        XCTAssertEqual(runtime.calls, ["get", "export"])
        let calls = await transport.calls
        XCTAssertTrue(calls.isEmpty)
    }

    func testBackgroundWindowForwardsOperatingSystemBudget() async throws {
        let ids = TestIds()
        let runtime = FakeRuntime(ids: ids)
        let coordinator = TrustedDeviceHandoffCoordinator(
            runtime: runtime,
            transport: RecordingTransport()
        )

        let result = try await coordinator.runBackgroundWindow(maxTicks: 17, timeBudgetMs: 2_500)

        XCTAssertEqual(result.stepsExecuted, 1)
        XCTAssertEqual(runtime.backgroundArguments?.maxTicks, 17)
        XCTAssertEqual(runtime.backgroundArguments?.timeBudgetMs, 2_500)
    }

    private func envelope(ids: TestIds) -> TrustedDeviceHandoffEnvelope {
        TrustedDeviceHandoffEnvelope(
            handoffId: ids.inboundHandoff,
            tenantId: "mobile-test",
            capsuleJson: "{\"manifest\":{}}",
            payloadBase64: "payload",
            payloadKeyBase64: "key",
            destinationRuntimeId: ids.deviceRuntime,
            destinationInstanceId: ids.deviceInstance,
            acceptanceToken: "token",
            signedGrantJson: "{\"grant\":{}}"
        )
    }

    private func active(ids: TestIds) -> ActivatedTrustedDeviceHandoff {
        ActivatedTrustedDeviceHandoff(
            handoffId: ids.inboundHandoff,
            tenantId: "mobile-test",
            capsuleId: ids.inboundCapsule,
            continuityId: ids.continuity,
            instanceId: ids.deviceInstance,
            runtimeId: ids.deviceRuntime,
            sourceEpoch: 0
        )
    }

    private func returnPlan(ids: TestIds) -> TrustedDeviceReturnPlan {
        TrustedDeviceReturnPlan(
            handoffId: ids.returnHandoff,
            tenantId: "mobile-test",
            destinationRuntimeId: ids.cloudRuntime,
            destinationInstanceId: ids.cloudInstance,
            payloadKeyBase64: "return-key"
        )
    }
}

private struct TestIds {
    let inboundHandoff = UUID().uuidString
    let returnHandoff = UUID().uuidString
    let inboundCapsule = UUID().uuidString
    let returnCapsule = UUID().uuidString
    let continuity = UUID().uuidString
    let deviceRuntime = UUID().uuidString
    let cloudRuntime = UUID().uuidString
    let deviceInstance = UUID().uuidString
    let cloudInstance = UUID().uuidString
}

private final class FakeRuntime: TrustedDeviceContinuityRuntime, @unchecked Sendable {
    private let lock = NSLock()
    private let ids: TestIds
    var importedInstanceId: String
    var exportedContinuityId: String
    var instanceState: InstanceStateKind = .waiting
    private(set) var backgroundArguments: (maxTicks: UInt32, timeBudgetMs: UInt64)?
    private(set) var calls: [String] = []

    init(ids: TestIds) {
        self.ids = ids
        self.importedInstanceId = ids.deviceInstance
        self.exportedContinuityId = ids.continuity
    }

    func importContinuityCapsule(
        capsuleJson: String,
        payloadBase64: String,
        payloadKeyBase64: String,
        destinationRuntimeId: String,
        destinationInstanceId: String
    ) throws -> ContinuityImportResult {
        record("import")
        return ContinuityImportResult(
            capsuleId: ids.inboundCapsule,
            continuityId: ids.continuity,
            instanceId: importedInstanceId,
            sourceEpoch: 0,
            state: "paused"
        )
    }

    func activateContinuityCapsule(
        capsuleId: String,
        destinationRuntimeId: String,
        destinationInstanceId: String
    ) throws {
        record("activate")
    }

    func getInstance(instanceId: String) throws -> InstanceState {
        record("get")
        return InstanceState(
            instanceId: instanceId,
            sequenceName: "trusted-device",
            state: instanceState,
            context: "{}",
            createdAt: "2026-09-01T00:00:00Z",
            updatedAt: "2026-09-01T00:00:00Z"
        )
    }

    func runUntilIdle(maxTicks: UInt32, timeBudgetMs: UInt64) throws -> BackgroundRunResult {
        record("run")
        backgroundArguments = (maxTicks, timeBudgetMs)
        return BackgroundRunResult(
            ticksExecuted: 1,
            instancesAdvanced: 1,
            stepsExecuted: 1,
            hasPendingWork: false,
            budgetExhausted: false
        )
    }

    func exportContinuityCapsule(
        instanceId: String,
        destinationRuntimeId: String,
        payloadKeyBase64: String,
        expiresInSeconds: UInt32,
        signer: CapsuleSigner
    ) throws -> ContinuityExportResult {
        record("export")
        return ContinuityExportResult(
            capsuleId: ids.returnCapsule,
            continuityId: exportedContinuityId,
            sourceEpoch: 1,
            capsuleJson: "{\"manifest\":{}}",
            payloadBase64: "return-payload"
        )
    }

    private func record(_ call: String) {
        lock.lock()
        calls.append(call)
        lock.unlock()
    }
}

private actor RecordingTransport: TrustedDeviceHandoffTransport {
    private(set) var calls: [String] = []

    func acceptExternalHandoff(
        envelope: TrustedDeviceHandoffEnvelope,
        imported: ContinuityImportResult
    ) async throws {
        calls.append("accept")
    }

    func resumeExternalHandoff(
        envelope: TrustedDeviceHandoffEnvelope,
        imported: ContinuityImportResult
    ) async throws {
        calls.append("resume")
    }

    func completeReturnHandoff(
        plan: TrustedDeviceReturnPlan,
        exported: ContinuityExportResult
    ) async throws {
        calls.append("return")
    }
}

private final class FakeSigner: CapsuleSigner, @unchecked Sendable {
    func keyId() -> String { "test-key" }
    func publicKeyBase64() -> String { "test-public-key" }
    func signManifestSha256(digest: String) throws -> String { "signature" }
}
