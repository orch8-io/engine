# Native Android bridge checks

This Gradle consumer compiles the production React Native and Flutter Android
modules and runs their lifecycle regression tests. It uses the React Native
version from the npm package's development dependencies and an actual pinned
Flutter embedding jar. It does not replace UniFFI classes with protocol stubs.

With JDK 17, Gradle 8.9 and Android SDK 35 installed, publish the Android producer
to an isolated local Maven repository, then run:

```bash
export ORCH8_MOBILE_VERSION=0.0.0-bridge-check
gradle -p packages/android -Dmaven.repo.local="$PWD/target/bridge-maven" \
  :orch8-mobile:publishReleasePublicationToMavenLocal
python3 scripts/fetch-flutter-embedding.py target/flutter/flutter.jar
gradle -p tests/android-bridges \
  -Porch8.dist.repo="$PWD/target/bridge-maven" \
  -Porch8.flutter.jar="$PWD/target/flutter/flutter.jar" \
  :reactNative:testDebugUnitTest :flutter:testDebugUnitTest
```

The producer requires generated Kotlin bindings and native Android libraries;
the Mobile workflow supplies these from the same commit before running this
check. The JVM lifecycle tests use the real generated `MobileEngine(NoHandle)`
test seam to count shutdown/disposal and control construction races. They do
not load Android native libraries into the host JVM or claim device coverage.
