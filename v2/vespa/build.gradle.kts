plugins {
    alias(libs.plugins.kotlin.jvm)
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
