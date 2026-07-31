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

    kotlinOptions {
        jvmTarget = "17"
    }

    sourceSets {
        getByName("main") {
            jniLibs.srcDirs("src/main/jniLibs")
        }
    }
}

dependencies {
    implementation("net.java.dev.jna:jna:5.15.0@aar")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")
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
