plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    // Nostr-agnostic: search + document writes over plain objects. No Quartz.
    implementation(libs.kotlinx.serialization.json)
    // Writes go through Vespa's official feed client (async, HTTP/2 multiplexed,
    // retries + throttling built in). Its types stay out of our public API.
    implementation(libs.vespa.feed.client)
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
