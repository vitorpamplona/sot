plugins {
    // Quartz 1.12.6 ships Kotlin 2.4 metadata, so the compiler must be >= 2.4.
    kotlin("jvm") version "2.4.0"
    application
}

dependencies {
    // Amethyst's Quartz — Nostr event types, parsing, the NostrClient relay
    // pool, and NIP-77 negentropy sync. KMP artifact; Gradle resolves the -jvm
    // variant for this kotlin("jvm") project.
    implementation("com.vitorpamplona.quartz:quartz:1.12.6")
    // Quartz's JVM websocket transport is OkHttp-based (BasicOkHttpWebSocket).
    implementation("com.squareup.okhttp3:okhttp:5.4.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    // Quartz parses/serialises events with kotlinx-serialization-json.
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.vitorpamplona.vespasearch.indexer.MainKt")
}
