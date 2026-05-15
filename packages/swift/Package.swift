// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "Orch8Mobile",
    platforms: [
        .iOS(.v15),
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
            path: "Sources/Orch8Mobile"
        ),
        .binaryTarget(
            name: "Orch8MobileFFI",
            path: "Orch8Mobile.xcframework"
        ),
    ]
)
