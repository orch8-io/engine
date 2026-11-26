pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
}

apply(from = file("../../gradle/android-build-dependencies.gradle"))

rootProject.name = "orch8-android"
include(":orch8-mobile")
