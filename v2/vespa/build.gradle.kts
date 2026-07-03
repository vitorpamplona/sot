plugins {
    alias(libs.plugins.kotlin.jvm)
    // Publishes InMemoryEventIndex (src/testFixtures) to downstream module tests.
    `java-test-fixtures`
}

dependencies {
    // Nostr-library-agnostic: plain event values in, YQL/JSON out. No Quartz.
    // Only the JsonElement tree API is used, so no serialization plugin either.
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines)
    // Writes go through Vespa's official feed client (async, HTTP/2 multiplexed,
    // retries + throttling built in). Its types stay out of our public API.
    implementation(libs.vespa.feed.client)
    // MockVespaEngine serves HTTP/1.1 + clear-text HTTP/2 (h2c) on one Jetty
    // port: the feed client refuses HTTP/1.1. Pinned to the feed client's Jetty line.
    testFixturesImplementation(libs.kotlinx.serialization.json)
    testFixturesImplementation(libs.kotlinx.coroutines)
    testFixturesImplementation(libs.jetty.server)
    testFixturesImplementation(libs.jetty.http2.server)
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
