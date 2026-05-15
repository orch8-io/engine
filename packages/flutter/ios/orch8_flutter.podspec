Pod::Spec.new do |s|
  s.name             = 'orch8_flutter'
  s.version          = '0.1.0'
  s.summary          = 'Flutter plugin for the Orch8 Mobile SDK'
  s.homepage         = 'https://github.com/orch8-io/orch8-flutter'
  s.license          = { :type => 'MIT' }
  s.author           = 'Orch8'
  s.source           = { :path => '.' }
  s.source_files     = 'Classes/**/*'
  s.dependency         'Flutter'
  s.dependency         'Orch8Mobile'
  s.platform         = :ios, '15.0'
  s.swift_version    = '5.9'
end
