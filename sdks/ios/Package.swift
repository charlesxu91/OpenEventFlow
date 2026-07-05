// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "OpenEventFlow",
    platforms: [
        .iOS(.v13),
        .macOS(.v12)
    ],
    products: [
        .library(name: "OpenEventFlow", targets: ["OpenEventFlow"])
    ],
    targets: [
        .target(
            name: "OpenEventFlow",
            path: "Sources/OpenEventFlow"
        )
    ]
)
