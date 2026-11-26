import org.jetbrains.kotlin.buildtools.api.ExperimentalBuildToolsApi
import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
    id("maven-publish")
}

group = "io.orch8"
// A release tag is authoritative. Keep the checked-in VERSION_NAME as the
// local/CI default, but never let it override the version supplied by the
// tag-publishing workflow.
version = providers.environmentVariable("ORCH8_MOBILE_VERSION")
    .orElse(providers.gradleProperty("VERSION_NAME"))
    .orElse("0.0.0-local")
    .get()
    .removePrefix("v")

android {
    namespace = "io.orch8.mobile"
    compileSdk = 35

    defaultConfig {
        minSdk = 24
        consumerProguardFiles("consumer-rules.pro")
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    sourceSets {
        getByName("main") {
            jniLibs.srcDirs("src/main/jniLibs")
        }
    }

    publishing { singleVariant("release") }
}

// Consumer compatibility: React Native 0.76 and Expo SDK 52 apps build with
// Kotlin 1.9.x, whose compiler rejects class metadata newer than 2.0. The
// Kotlin 2.4 removed language level 1.9. Use the patched Gradle plugin with
// the last compiler series that can emit 1.9 metadata on the JVM:
//   - languageVersion/apiVersion 1.9 -> class metadata 1.9.0, and no stdlib
//     API newer than 1.9 is referenced by the UniFFI bindings;
//   - coreLibrariesVersion -> the POM depends on kotlin-stdlib 1.9.24;
//   - kotlinx-coroutines 1.8.1 is the last line compiled with Kotlin 1.9.
// Apps on Kotlin 2.x are unaffected: they read 1.9 metadata and resolve their
// own newer stdlib/coroutines. Raise these together when dropping Kotlin 1.9.
kotlin {
    @OptIn(ExperimentalBuildToolsApi::class, ExperimentalKotlinGradlePluginApi::class)
    compilerVersion.set("2.3.21")
    coreLibrariesVersion = "1.9.24"
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        languageVersion.set(KotlinVersion.fromVersion("1.9"))
        apiVersion.set(KotlinVersion.fromVersion("1.9"))
    }
}

dependencies {
    implementation("net.java.dev.jna:jna:5.17.0@aar")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")
}

afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("release") {
                from(components["release"])
                artifactId = "orch8-mobile"
                pom {
                    name.set("Orch8 Mobile")
                    description.set("Embedded durable workflow runtime for Android")
                    url.set("https://github.com/orch8-io/engine")
                    licenses {
                        license {
                            name.set("Business Source License 1.1")
                            url.set("https://github.com/orch8-io/engine/blob/main/LICENSE")
                        }
                    }
                }
            }
        }
        repositories {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/orch8-io/engine")
                credentials {
                    username = providers.environmentVariable("GITHUB_ACTOR").orNull
                    password = providers.environmentVariable("GITHUB_TOKEN").orNull
                }
            }
        }
    }
}
