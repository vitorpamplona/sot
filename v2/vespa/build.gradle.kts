plugins {
    alias(libs.plugins.kotlin.jvm)
    // Publishes InMemoryEventIndex (src/testFixtures) to downstream module tests.
    `java-test-fixtures`
}

dependencies {
    // Nostr-library-agnostic: plain event values in, YQL/JSON out. No Quartz.
    // Only the JsonElement tree API is used, so no serialization plugin either.
    implementation(libs.kotlinx.serialization.json)
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
