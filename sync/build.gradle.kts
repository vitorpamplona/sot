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
    // VespaEventStore: the orphan-score sweep needs its grouping distinctAuthors
    // (a full 30382 enumeration times Vespa out at scale). Matches the intended
    // dependency direction (:sync -> :store).
    implementation(libs.vespa.eventstore.store)
    testImplementation(kotlin("test"))
    // Tests run the real protocol against in-process relays over the in-memory
    // reference indexes (InMemoryEventIndex, InMemoryCrawlIndex) — production code in
    // the library's :vespa jar, already on the test classpath via the store above.
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
