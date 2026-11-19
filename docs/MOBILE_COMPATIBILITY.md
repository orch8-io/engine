# Mobile SDK compatibility

Use matching generated bindings and native binaries. The published 0.7.1 SDK and current development checkout have different native surfaces. An SDK compiled from one source revision must use the bindings generated from that revision.

| Consumer | Exact published install | Build and device prerequisites | Verification |
| --- | --- | --- | --- |
| SwiftPM | `orch8-io/orch8-mobile-swift`, `exact: "0.7.1"` | Xcode 16+, iOS 16+ | [Inspection starter](../examples/mobile-inspection) resolves the package and exercises local persistence on an iPhone simulator in the published install lane. |
| CocoaPods | `pod 'Orch8Mobile', '0.7.1'` | Xcode 16+, iOS 16+, CocoaPods | Check `Podfile.lock` and compile `import Orch8Mobile`. |
| Android | `io.orch8:orch8-mobile:0.7.1` from [Orch8 Maven](https://raw.githubusercontent.com/orch8-io/maven/main) | JDK 17, Android API 24+, Kotlin 2.1 for the starter. The AAR includes `arm64-v8a`, `armeabi-v7a`, and `x86_64`. | [Inspection starter](../examples/mobile-inspection) resolves the AAR and exercises local persistence on an Android emulator in the published install lane. |
| React Native | `@orch8.io/react-native-orch8` 0.7.1 | iOS 16+ or Android API 24+, linked native app build | Inspect the wrapper's native pins and follow the [React Native setup](../packages/react-native/README.md). |
| Expo | `@orch8.io/expo` 0.7.0 is the latest on npm, with the packaging issue documented in [Expo setup](MOBILE_SDK.md#expo). The `0.7.1` wrapper is absent from npm. | iOS 16+ or Android API 24+, Expo prebuild/development build | Inspect the wrapper package for the podspec and match its native version before prebuilding. [npm registry](https://registry.npmjs.org/@orch8.io%2fexpo) |
| Flutter | `orch8_flutter` 0.7.1 | Dart 3.12+, Flutter 3.44+, iOS 16+ or Android API 24+ | Check the wrapper's podspec and Gradle pins before building. [Published package](https://pub.dev/packages/orch8_flutter/versions/0.7.1) |
| Kotlin Multiplatform | 0.7.1 lacks the Kotlin Native bridge release asset. Use [KMP source development](../packages/kmp/README.md) for that baseline. | iOS 16+ or Android API 24+, JDK 17; Xcode for Apple targets | Future version builds resolve the AAR and Swift bridge for the same engine release. |

`0.7.1` Android bindings were generated with Kotlin 2.1. A Kotlin 1.9 wrapper that uses that AAR needs the metadata compatibility flag described in the [Android installation guide](MOBILE_SDK.md#android-gradle). The first later release switches the generated AAR to Kotlin metadata 1.9.0.

The `MobileAccount`, `MobileCatalog`, and `MobileCatalogRun` surfaces and the development callback protocol require a matched source build. The platform-specific development adapters and their rollout requirements are listed in the [scoped SDK guide](SDK_SCOPED_NATIVE.md).
