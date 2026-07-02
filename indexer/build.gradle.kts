plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    implementation(project(":vespa")) // VespaProjection maps events -> Profile/score, calls VespaClient
    implementation(libs.quartz)
    implementation(libs.okhttp)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    testImplementation(kotlin("test"))
    // The deletion-flow test runs a real (SQLite) event store through the projection.
    testImplementation(libs.androidx.sqlite.bundled)
    // MockVespa needs clear-text HTTP/2 (h2c) for the feed client; JDK's HttpServer can't.
    testImplementation(libs.jetty.server)
    testImplementation(libs.jetty.http2.server)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
