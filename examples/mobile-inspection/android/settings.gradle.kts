pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
        exclusiveContent {
            forRepository { maven("https://raw.githubusercontent.com/orch8-io/maven/main") }
            filter { includeGroup("io.orch8") }
        }
    }
}

apply(from = file("../../../gradle/android-build-dependencies.gradle"))

rootProject.name = "Orch8Inspection"
include(":app")
