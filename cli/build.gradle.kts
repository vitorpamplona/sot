plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(libs.vespa.eventstore.engine) // VespaEventIndex + VespaProfileIndex (the engine clients)
    implementation(libs.vespa.eventstore.store) // VespaEventStore (the one store)
    implementation(project(":relay")) // SotRelayServer + Ktor mount + NIP-11
    implementation(project(":sync")) // Identity + SyncService (serve's background loop, `sot index`)
    implementation(libs.quartz) // NIP-19 parsing (init prompts), Filters (status counts)
    implementation(libs.okhttp) // Quartz's Nip05Client fetcher (init resolves name@domain)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.websockets)
    testImplementation(kotlin("test"))
    // UiDemoServer runs the web UI over an in-memory relay (no Vespa) using
    // InMemoryEventIndex — production code in the library's :vespa jar, already on
    // the classpath via implementation above; no test-fixtures dependency needed.
}

kotlin {
    jvmToolchain(21)
}

// Bundle the static web UI (web/index.html) as a classpath resource for `sot serve`.
sourceSets["main"].resources.srcDir(rootProject.file("web"))

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.cli.MainKt")
}

tasks.test {
    useJUnitPlatform()
}

// `gradle :cli:uiDemo` — develop web/index.html against the real relay
// engine over an in-memory store: no Vespa, no docker, seeded demo events.
tasks.register<JavaExec>("uiDemo") {
    group = "application"
    description = "Serve the web UI over an in-memory relay seeded with demo events"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("com.vitorpamplona.sot.cli.UiDemoServer")
}

// `gradle :cli:loadTest [-Prelay=…]` — full-corpus 30382 sync into the local
// Vespa through the production ingest path (see LoadTest in the test sources).
tasks.register<JavaExec>("benchPut") {
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("com.vitorpamplona.sot.cli.BenchPut")
    args((project.findProperty("n") as String?) ?: "100")
}

tasks.register<JavaExec>("loadTest") {
    group = "verification"
    description = "Negentropy-sync a provider relay's whole kind-30382 corpus into Vespa"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("com.vitorpamplona.sot.cli.LoadTest")
    jvmArgs("-Xmx4g")
    args(
        (project.findProperty("relay") as String?) ?: "wss://nip85.nosfabrica.com",
        (project.findProperty("service") as String?) ?: "",
        (project.findProperty("max") as String?) ?: "0",
        (project.findProperty("slices") as String?) ?: "1",
        (project.findProperty("reconcile") as String?) ?: "1",
    )
}
