plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    // The store implements Quartz's IEventStore on top of :vespa's engine
    // port — both appear in its public API.
    api(libs.quartz)
    api(project(":vespa"))
    implementation(libs.kotlinx.coroutines)
    testImplementation(kotlin("test"))
    testImplementation(testFixtures(project(":vespa")))
    // Virtual-time test clock: measures read/write serialization deterministically
    // by injecting per-round-trip delays into the index (see BatchIngestConcurrencyTest).
    testImplementation(libs.kotlinx.coroutines.test)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
