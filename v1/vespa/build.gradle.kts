plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    // Publishes MockVespa (src/testFixtures) to this module's AND :indexer's tests.
    `java-test-fixtures`
}

dependencies {
    // Nostr-agnostic: search + document writes over plain objects. No Quartz.
    implementation(libs.kotlinx.serialization.json)
    // Non-blocking reads: suspend over the JDK client's async sends + a coroutine
    // Semaphore that paces the fan-out to Vespa. (future.await + sync.Semaphore.)
    implementation(libs.kotlinx.coroutines)
    // Writes go through Vespa's official feed client (async, HTTP/2 multiplexed,
    // retries + throttling built in). Its types stay out of our public API.
    implementation(libs.vespa.feed.client)
    // MockVespa serves HTTP/1.1 + clear-text HTTP/2 (h2c) on one Jetty port: the
    // feed client refuses HTTP/1.1. Pinned to the feed client's own Jetty line.
    testFixturesImplementation(libs.kotlinx.serialization.json)
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
