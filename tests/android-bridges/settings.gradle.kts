pluginManagement { repositories { google(); mavenCentral(); gradlePluginPortal() } }

apply(from = file("../../gradle/android-build-dependencies.gradle"))

rootProject.name = "orch8-native-bridge-check"
include(":reactNative", ":flutter")
project(":reactNative").projectDir = file("../../packages/react-native/android")
project(":flutter").projectDir = file("../../packages/flutter/android")
