plugins {
    id("com.android.application") version "8.7.0"
    id("org.jetbrains.kotlin.android") version "2.1.0"
}

val orch8Version = providers.gradleProperty("orch8Version").getOrElse("0.7.1")
val inspectionVersionCode = providers.gradleProperty("inspectionVersionCode").getOrElse("1").toInt()
require(orch8Version.matches(Regex("[0-9]+\\.[0-9]+\\.[0-9]+(?:-[0-9A-Za-z.-]+)?"))) {
    "orch8Version must be an exact release version"
}

android {
    namespace = "io.orch8.inspection"
    compileSdk = 35
    buildToolsVersion = "35.0.0"

    defaultConfig {
        applicationId = "io.orch8.inspection"
        minSdk = 24
        targetSdk = 35
        versionCode = inspectionVersionCode
        versionName = "1.0"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        buildConfigField("String", "ORCH8_VERSION", "\"$orch8Version\"")
    }

    buildFeatures { buildConfig = true }
    sourceSets.getByName("main").assets.srcDir("../../Sources/Inspection/Resources")

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

kotlin { jvmToolchain(17) }

dependencies {
    implementation("io.orch8:orch8-mobile:$orch8Version")
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.test:runner:1.6.2")
    testImplementation("junit:junit:4.13.2")
}
