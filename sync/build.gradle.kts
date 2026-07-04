plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    // The syncer consumes Quartz's IEventStore and client types directly;
    // both appear in its public wiring API.
    api(libs.quartz)
    implementation(libs.kotlinx.coroutines)
    // SyncState persists as JSON.
    implementation(libs.kotlinx.serialization.json)
    // The shared websocket transport (okHttpWebsocketBuilder).
    implementation(libs.okhttp)
    testImplementation(kotlin("test"))
    // Tests run the real protocol against in-process relays backed by the
    // SAME store implementation the product uses: VespaEventStore over the
    // in-memory reference index.
    testImplementation(project(":store"))
    testImplementation(testFixtures(project(":vespa")))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
