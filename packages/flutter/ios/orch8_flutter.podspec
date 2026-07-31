Pod::Spec.new do |s|
  s.name             = 'orch8_flutter'
  s.version          = '0.7.1'
  s.summary          = 'Flutter plugin for the Orch8 Mobile SDK'
  s.homepage         = 'https://github.com/orch8-io/orch8-flutter'
  s.license          = { :type => 'Business Source License 1.1', :file => '../LICENSE' }
  s.author           = 'Orch8'
  s.source           = { :path => '.' }
  s.source_files     = 'orch8_flutter/Sources/orch8_flutter/**/*.swift'
  s.dependency         'Flutter'
  s.dependency         'Orch8Mobile', '0.7.1'
  s.platform         = :ios, '16.0'
  s.swift_version    = '5.9'
end
