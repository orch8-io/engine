import com.android.build.api.dsl.LibraryExtension
import groovy.json.JsonSlurper
import org.gradle.api.artifacts.repositories.MavenArtifactRepository
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

// Unified versioning: the npm package version is the Orch8 engine release
// whose AAR (io.orch8:orch8-mobile) this bridge is compiled against, exactly
// like the podspec pins the Orch8Mobile pod. An explicit
// `orch8NativeVersion` in package.json overrides it.
@Suppress("UNCHECKED_CAST")
val packageJson = JsonSlurper().parse(file("../package.json")) as Map<String, Any?>
val orch8NativeVersion = (packageJson["orch8NativeVersion"] ?: packageJson["version"]) as String
val orch8MavenUrl = "https://raw.githubusercontent.com/orch8-io/maven/main"

extensions.configure<LibraryExtension> {
    namespace = "io.orch8.reactnative"
    compileSdk = 35

    defaultConfig {
        // liborch8_mobile.so and the AAR are built for API 24+.
        minSdk = 24
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        // The 0.7.1 AAR carries Kotlin 2.1 metadata; React Native 0.76 apps
        // compile this module with Kotlin 1.9. Later AARs are built at
        // language level 1.9 and do not need this flag (harmless on 2.x).
        freeCompilerArgs.add("-Xskip-metadata-version-check")
    }
}

// Gradle resolves the *app's* dependency graph with the app's repositories,
// so a repository declared only here lets this module compile but leaves
// `:app` unable to find io.orch8:orch8-mobile. Add Orch8's Maven repository to
// every project unless the app opts out with `orch8.addMavenRepository=false`
// in android/gradle.properties (for example when settings.gradle uses
// RepositoriesMode.FAIL_ON_PROJECT_REPOS; then declare the repository in
// dependencyResolutionManagement yourself, see the package README).
fun org.gradle.api.artifacts.dsl.RepositoryHandler.addOrch8Maven() {
    val present = any { it is MavenArtifactRepository && it.url.toString().trimEnd('/') == orch8MavenUrl }
    if (!present) maven { url = uri(orch8MavenUrl) }
}

repositories.addOrch8Maven()
if (findProperty("orch8.addMavenRepository")?.toString() != "false") {
    rootProject.allprojects { repositories.addOrch8Maven() }
}

dependencies {
    // Version supplied by the React Native Gradle plugin.
    implementation("com.facebook.react:react-android")
    implementation("io.orch8:orch8-mobile:$orch8NativeVersion")
}
