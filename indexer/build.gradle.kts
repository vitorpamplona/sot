plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    implementation(project(":vespa-engine")) // VespaProjection maps events -> Profile/score, calls VespaClient
    implementation(libs.quartz)
    implementation(libs.okhttp)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}
