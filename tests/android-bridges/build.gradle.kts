import groovy.json.JsonSlurper

plugins {
    id("com.android.library") version "8.7.0" apply false
    id("org.jetbrains.kotlin.android") version "2.1.0" apply false
}

// Consume the producer's isolated Maven publication, including its real transitive dependencies.
val paired = file(providers.gradleProperty("orch8.dist.repo").get())
val nativeVersion = providers.environmentVariable("ORCH8_MOBILE_VERSION").get()
val flutterJar = file(providers.gradleProperty("orch8.flutter.jar").get())
require(flutterJar.isFile) { "Supply the actual Flutter embedding jar" }
require(paired.resolve("io/orch8/orch8-mobile/$nativeVersion/orch8-mobile-$nativeVersion.pom").isFile) {
    "Publish the matching Android AAR into orch8.dist.repo before running this check"
}
val packageJson = JsonSlurper().parse(file("../../packages/react-native/package.json")) as Map<*, *>
val reactVersion = (packageJson["devDependencies"] as Map<*, *>)["react-native"] as String

allprojects {
    repositories {
        exclusiveContent {
            forRepository { maven { url = paired.toURI() } }
            filter { includeModule("io.orch8", "orch8-mobile") }
        }
        google()
        mavenCentral()
    }
    configurations.configureEach {
        resolutionStrategy.eachDependency {
            if (requested.group == "io.orch8" && requested.name == "orch8-mobile") useVersion(nativeVersion)
            if (requested.group == "com.facebook.react" && requested.name == "react-android") useVersion(reactVersion)
        }
    }
}
project(":flutter") {
    plugins.withId("com.android.library") { dependencies.add("compileOnly", files(flutterJar)) }
}
