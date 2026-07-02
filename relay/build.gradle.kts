plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(project(":config")) // NIP-11 relay identity from env/.env
    implementation(project(":vespa"))
    implementation(libs.quartz) // relay server framework, events, NIP-42, NIP-50 Filter.search
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.server.core) // Route DSL only; the engine lives in :server
    implementation(libs.ktor.server.websockets)
    testImplementation(kotlin("test"))
    // The search-source test ranks against a canned Vespa and reads a real SQLite store.
    testImplementation(libs.androidx.sqlite.bundled)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
