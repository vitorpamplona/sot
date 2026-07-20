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
    // Tests run the real protocol against in-process relays backed by the SAME store
    // implementation the product uses (VespaEventStore over the in-memory reference
    // index), and drive the crawl ledger through InMemoryCrawlIndex — a test fixture
    // in the library's :vespa module. JitPack's rewritten Gradle metadata points the
    // test-fixtures VARIANT at the main jar, so the testFixtures()/capability route
    // resolves the wrong jar; fetch the test-fixtures artifact directly by classifier
    // (non-transitive — the main :vespa it needs is already on the test classpath via
    // the store above).
    testImplementation("com.github.vitorpamplona.vespa-eventstore:vespa:${libs.versions.vespaEventStore.get()}:test-fixtures@jar")
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
