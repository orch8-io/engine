Pod::Spec.new do |s|
  s.name                = 'Orch8Mobile'
  s.version             = '0.7.1'
  s.summary             = 'Embedded durable workflow runtime for iOS'
  s.homepage            = 'https://github.com/orch8-io/orch8-mobile-swift'
  s.license             = { :type => 'Business Source License 1.1', :file => 'LICENSE' }
  s.author              = 'Orch8'
  s.source              = {
    :http => "https://github.com/orch8-io/orch8-mobile-swift/releases/download/#{s.version}/Orch8MobileCocoaPods-#{s.version}.zip"
  }
  s.source_files        = 'Sources/Orch8Mobile/**/*.swift'
  s.vendored_frameworks = 'Orch8Mobile.xcframework'
  s.platform            = :ios, '16.0'
  s.swift_version       = '5.9'
end
