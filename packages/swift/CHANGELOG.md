# Changelog

## Unreleased

- Device sessions: `Orch8RuntimeNode.setTokenProvider { … }` takes an async
  closure that returns a short-lived `dst_` token from your backend (awaited
  once up front, and again after a `401`, bridged off the main thread with a
  `refreshTimeout`); `setTokenProvider(_:)` passes a synchronous
  `TokenProvider` through. Regenerated bindings expose
  `MobileEngine.setTokenProvider`. `syncApiKey` is documented as legacy, not
  for production apps.

## 0.7.1

- Fix CocoaPods installation by giving device and simulator XCFramework slices
  the same static-library name and declaring the native iOS 16 minimum.

## 0.7.0

- Initial public Swift SDK release aligned with Orch8 Engine 0.7.0.
- Includes generated UniFFI bindings and checksum-pinned iOS device and
  simulator binaries.
