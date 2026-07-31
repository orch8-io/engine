import com.android.build.api.dsl.LibraryExtension
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

extensions.configure<LibraryExtension> {
    namespace = "io.orch8.flutter"
    compileSdk = 35

    defaultConfig {
        minSdk = 24
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

repositories {
    maven("https://raw.githubusercontent.com/orch8-io/maven/main")
}

dependencies {
    implementation("io.orch8:orch8-mobile:0.7.1")
}
