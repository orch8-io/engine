// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "Orch8Example",
    platforms: [.iOS(.v15)],
    dependencies: [
        .package(path: "../../packages/swift"),
    ],
    targets: [
        .executableTarget(
            name: "Orch8Example",
            dependencies: [
                .product(name: "Orch8Mobile", package: "swift"),
            ],
            path: "Orch8Example"
        ),
    ]
)
