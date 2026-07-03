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
}

kotlin {
    jvmToolchain(21)
}

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.v2.cli.MainKt")
}

tasks.test {
    useJUnitPlatform()
}
