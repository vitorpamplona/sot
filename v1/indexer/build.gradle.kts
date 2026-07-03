plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    implementation(project(":v1:vespa")) // VespaProjection maps events -> Profile/score, calls VespaClient
    implementation(libs.quartz)
    implementation(libs.okhttp)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    testImplementation(kotlin("test"))
    // The deletion-flow test runs a real (SQLite) event store through the projection.
    testImplementation(libs.androidx.sqlite.bundled)
    // MockVespa lives with the API it mocks.
    testImplementation(testFixtures(project(":v1:vespa")))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
