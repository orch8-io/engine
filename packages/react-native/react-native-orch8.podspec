require 'json'
package = JSON.parse(File.read(File.join(__dir__, 'package.json')))

# Unified versioning: the npm package version is the Orch8 engine release whose
# Orch8Mobile pod (UniFFI Swift bindings + XCFramework) ios/Orch8Module.swift is
# compiled against. Pinned exactly, like the Android AAR in android/build.gradle.kts.
orch8_native_version = package['orch8NativeVersion'] || package['version']

Pod::Spec.new do |s|
  s.name         = "react-native-orch8"
  s.version      = package['version']
  s.summary      = package['description']
  s.homepage     = "https://github.com/orch8-io/react-native-orch8"
  s.license      = { :type => "Business Source License 1.1", :file => "LICENSE" }
  s.authors      = "Orch8"
  s.source       = { git: "https://github.com/orch8-io/react-native-orch8.git", tag: s.version }
  s.platforms    = { ios: "16.0" }
  s.swift_version = "5.9"
  s.source_files = "ios/**/*.{h,m,mm,swift}"
  s.dependency     "React-Core"
  s.dependency     "Orch8Mobile", orch8_native_version
end
