plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(libs.vespa.eventstore.engine) // VespaEventIndex (status/health metrics)
    implementation(libs.vespa.eventstore.store) // VespaEventStore.open (the one store)
    implementation(project(":sync")) // Identity + SyncService (the crawl service, `sot index`)
    implementation(libs.quartz) // NIP-19 parsing (init prompts), Filters (status counts)
    implementation(libs.okhttp) // Quartz's Nip05Client fetcher (init resolves name@domain)
    implementation(libs.kotlinx.coroutines)
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.cli.MainKt")
}

tasks.test {
    useJUnitPlatform()
}

// `gradle :cli:benchPut [-Pn=…]` — feed N synthetic docs through the ingest path.
tasks.register<JavaExec>("benchPut") {
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("com.vitorpamplona.sot.cli.BenchPut")
    args((project.findProperty("n") as String?) ?: "100")
}

// `gradle :cli:loadTest [-Prelay=…]` — full-corpus 30382 sync into the local
// Vespa through the production ingest path (see LoadTest in the test sources).
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
