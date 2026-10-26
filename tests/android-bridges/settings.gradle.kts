pluginManagement { repositories { google(); mavenCentral(); gradlePluginPortal() } }
rootProject.name = "orch8-native-bridge-check"
include(":reactNative", ":flutter")
project(":reactNative").projectDir = file("../../packages/react-native/android")
project(":flutter").projectDir = file("../../packages/flutter/android")
