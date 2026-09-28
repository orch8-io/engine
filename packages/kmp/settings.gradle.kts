pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        // During a release, io.orch8:orch8-mobile:<version> is published into
        // the local orch8-io/maven checkout moments before this build runs and
        // is not yet visible on raw.githubusercontent.com.
        providers.gradleProperty("orch8.dist.repo").orNull?.let { dir ->
            maven { url = uri(file(dir)) }
        }
        google()
        mavenCentral()
        // Orch8's public Maven repository hosts the Android AAR
        // (io.orch8:orch8-mobile) that androidMain wraps.
        maven("https://raw.githubusercontent.com/orch8-io/maven/main")
    }
}

rootProject.name = "orch8-kmp"
