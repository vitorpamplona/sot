plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    // The relay is Quartz's protocol engine (RelayServerBase) over the Vespa
    // store; both appear in the public wiring API.
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    // The websocket mount for the composition root's Ktor app.
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.websockets)
    testImplementation(kotlin("test"))
    // RelayProtocolTest drives the real protocol over InMemoryEventIndex, which is
    // production code in the library's :vespa jar (already on the test classpath via
    // the store) — no test-fixtures dependency needed.
    // RelayInfoTest parses the NIP-11 doc.
    testImplementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
