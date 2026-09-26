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
        // Orch8's public Maven repository hosts the Android AAR
        // (io.orch8:orch8-mobile) that androidMain wraps.
        maven("https://raw.githubusercontent.com/orch8-io/maven/main")
    }
}

rootProject.name = "orch8-kmp"
