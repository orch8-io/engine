import XCTest
@testable import Orch8Mobile

final class AsyncTokenProviderTests: XCTestCase {
    private actor Tokens {
        var next = ["dst_second"]
        func pop() throws -> String {
            guard !next.isEmpty else { throw URLError(.userAuthenticationRequired) }
            return next.removeFirst()
        }
    }

    func testServesInitialTokenAndRefreshesThroughTheClosure() throws {
        let tokens = Tokens()
        let provider = AsyncTokenProvider(initialToken: "dst_first", timeout: 5) { try await tokens.pop() }
        XCTAssertEqual(provider.currentToken(), "dst_first")
        XCTAssertEqual(try provider.refreshToken(), "dst_second")
        XCTAssertEqual(provider.currentToken(), "dst_second")
        // The closure throws: the refresh fails and the cached token stays.
        XCTAssertThrowsError(try provider.refreshToken())
        XCTAssertEqual(provider.currentToken(), "dst_second")
    }

    func testEmptyTokenAndTimeoutAreErrors() {
        let empty = AsyncTokenProvider(initialToken: "dst_first", timeout: 5) { "" }
        XCTAssertThrowsError(try empty.refreshToken())
        XCTAssertEqual(empty.currentToken(), "dst_first")

        let slow = AsyncTokenProvider(initialToken: "dst_first", timeout: 0.05) {
            try await Task.sleep(nanoseconds: 2_000_000_000)
            return "dst_late"
        }
        XCTAssertThrowsError(try slow.refreshToken())
        XCTAssertEqual(slow.currentToken(), "dst_first")
    }
}
