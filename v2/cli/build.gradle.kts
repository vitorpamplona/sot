plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":v2:vespa")) // VespaEventIndex + VespaProfileIndex (the engine clients)
    implementation(project(":v2:store")) // VespaEventStore (the one store)
    implementation(project(":v2:profile")) // TrustProjection decorates the store's index
    implementation(project(":v2:relay")) // SotRelayServer + Ktor mount + NIP-11
    implementation(project(":v2:sync")) // Identity + SyncService (serve's background loop, `sot index`)
    implementation(libs.quartz) // NIP-19 parsing (init prompts), Filters (status counts)
    implementation(libs.okhttp) // Quartz's Nip05Client fetcher (init resolves name@domain)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.websockets)
    testImplementation(kotlin("test"))
    // UiDemoServer: the web UI over an in-memory relay (no Vespa) for UI development.
    testImplementation(testFixtures(project(":v2:vespa")))
}

kotlin {
    jvmToolchain(21)
}

// Bundle the static web UI (v2/web/index.html) as a classpath resource for `sot serve`.
sourceSets["main"].resources.srcDir(rootProject.file("v2/web"))

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.v2.cli.MainKt")
}

tasks.test {
    useJUnitPlatform()
}

// `gradle :v2:cli:uiDemo` — develop v2/web/index.html against the real relay
// engine over an in-memory store: no Vespa, no docker, seeded demo events.
tasks.register<JavaExec>("uiDemo") {
    group = "application"
    description = "Serve the web UI over an in-memory relay seeded with demo events"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("com.vitorpamplona.sot.v2.cli.UiDemoServer")
}
