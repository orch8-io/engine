// swift-tools-version:5.9
import Foundation
import PackageDescription

let ffiTarget: Target
if ProcessInfo.processInfo.environment["ORCH8_MOBILE_LOCAL_XCFRAMEWORK"] == "1" {
    ffiTarget = .binaryTarget(
        name: "Orch8MobileFFI",
        path: "Orch8Mobile.xcframework"
    )
} else {
    ffiTarget = .binaryTarget(
        name: "Orch8MobileFFI",
        url: "https://github.com/orch8-io/engine/releases/download/v0.7.1/Orch8Mobile-v0.7.1.xcframework.zip",
        checksum: "0a83ce860c5b41bb7d5dcd9401e4466eb48fc0513658194293a4ca691b3f61d5"
    )
}

let package = Package(
    name: "Orch8Mobile",
    platforms: [
        .iOS(.v16),
    ],
    products: [
        .library(
            name: "Orch8Mobile",
            targets: ["Orch8Mobile", "Orch8MobileFFI"]
        ),
    ],
    targets: [
        .target(
            name: "Orch8Mobile",
            dependencies: ["Orch8MobileFFI"],
            path: "Sources/Orch8Mobile",
            resources: [.copy("PrivacyInfo.xcprivacy")]
        ),
        ffiTarget,
        .testTarget(
            name: "Orch8MobileTests",
            dependencies: ["Orch8Mobile"],
            path: "Tests/Orch8MobileTests"
        ),
    ]
)
