// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "orch8_flutter",
    platforms: [
        .iOS(.v16),
    ],
    products: [
        .library(name: "orch8-flutter", targets: ["orch8_flutter"]),
    ],
    dependencies: [
        .package(name: "FlutterFramework", path: "../FlutterFramework"),
        .package(
            url: "https://github.com/orch8-io/orch8-mobile-swift",
            exact: "0.7.1"
        ),
    ],
    targets: [
        .target(
            name: "orch8_flutter",
            dependencies: [
                .product(name: "FlutterFramework", package: "FlutterFramework"),
                .product(name: "Orch8Mobile", package: "orch8-mobile-swift"),
            ]
        ),
    ]
)
