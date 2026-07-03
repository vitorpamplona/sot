plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":vespa")) // VespaEventIndex + VespaProfileIndex (the engine clients)
    implementation(project(":store")) // VespaEventStore (the one store)
    implementation(project(":profile")) // TrustProjection decorates the store's index
    implementation(project(":relay")) // SotRelayServer + Ktor mount + NIP-11
    implementation(project(":sync")) // Identity + SyncService (serve's background loop, `sot index`)
    implementation(libs.quartz) // NIP-19 parsing (init prompts), Filters (status counts)
    implementation(libs.okhttp) // Quartz's Nip05Client fetcher (init resolves name@domain)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.websockets)
    testImplementation(kotlin("test"))
    // UiDemoServer: the web UI over an in-memory relay (no Vespa) for UI development.
    testImplementation(testFixtures(project(":vespa")))
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
