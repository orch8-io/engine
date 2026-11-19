import XCTest
import UIKit
@testable import Inspection

final class InspectionLifecycleTests: XCTestCase {
    @MainActor
    func testCompletionReleasesTheLease() async {
        let lease = UIBackgroundTaskIdentifier(rawValue: 7)
        var ended = [UIBackgroundTaskIdentifier]()
        let lifecycle = InspectionLifecycle(worker: .global(), begin: { _ in lease }, end: { ended.append($0) })
        let completed = expectation(description: "reads durable state")

        XCTAssertTrue(lifecycle.run({ _ in "saved" }) { result in
            XCTAssertEqual(try? result.get(), "saved")
            XCTAssertEqual(ended, [lease])
            completed.fulfill()
        })
        await fulfillment(of: [completed], timeout: 2)
    }

    @MainActor
    func testExpirationClosesLeaseAndDropsLateNativeCompletion() async {
        let worker = DispatchQueue(label: "InspectionLifecycleTests.worker")
        worker.suspend()
        var expire: InspectionLifecycle.Expire?
        var ended = [UIBackgroundTaskIdentifier]()
        let lease = UIBackgroundTaskIdentifier(rawValue: 7)
        let lifecycle = InspectionLifecycle(worker: worker, begin: { expire = $0; return lease }, end: { ended.append($0) })
        let cancelled = expectation(description: "reports expiration")
        var completions = 0

        XCTAssertTrue(lifecycle.run({ try $0.remainingMilliseconds() }) { result in
            completions += 1
            if case .success = result { XCTFail("expired work published") }
            cancelled.fulfill()
        })
        XCTAssertFalse(lifecycle.run({ _ in 1 }) { _ in XCTFail("overlapping work ran") })
        expire?()
        worker.resume()
        let drained = expectation(description: "native worker returned")
        worker.async { DispatchQueue.main.async { drained.fulfill() } }
        await fulfillment(of: [cancelled, drained], timeout: 2)
        XCTAssertEqual(completions, 1)
        XCTAssertEqual(ended, [lease])
    }
}
