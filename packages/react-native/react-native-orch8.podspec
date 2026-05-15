require 'json'
package = JSON.parse(File.read(File.join(__dir__, 'package.json')))

Pod::Spec.new do |s|
  s.name         = "react-native-orch8"
  s.version      = package['version']
  s.summary      = package['description']
  s.homepage     = package['repository']
  s.license      = package['license']
  s.authors      = "Orch8"
  s.source       = { git: "#{package['repository']}.git", tag: s.version }
  s.platforms    = { ios: "15.0" }
  s.source_files = "ios/**/*.{h,m,mm,swift}"
  s.dependency     "React-Core"
  s.dependency     "Orch8Mobile"
end
