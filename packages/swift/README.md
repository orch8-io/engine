# Orch8 Mobile for Swift

Run server-configurable, durable workflows on iOS with the Orch8 `0.7.1`
engine embedded in your application.

## Swift Package Manager

```swift
dependencies: [
    .package(
        url: "https://github.com/orch8-io/orch8-mobile-swift",
        exact: "0.7.1"
    ),
]
```

Add the `Orch8Mobile` product to your application target, then:

```swift
import Orch8Mobile
```

Requires iOS 16 or newer and Xcode 16 or newer.

## CocoaPods

```ruby
pod 'Orch8Mobile', '0.7.1'
```

The package pins the immutable XCFramework from the Orch8 engine `v0.7.1`
release by SHA-256 checksum.
