plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":config")) // env/.env resolution (the composition root reads it)
    implementation(project(":event-store")) // open the shared event store (index + serve + status)
    implementation(project(":vespa"))
    implementation(project(":indexer")) // `sot index` / `sot serve` run the sync in-process
    implementation(project(":http")) // GET /search route (serve)
    implementation(project(":relay")) // NIP-50 relay route (serve)
    implementation(libs.quartz) // --observer: NIP-19 (npub/nprofile) + NIP-05 resolver; store Filters
    implementation(libs.okhttp) // OkHttp fetcher for Quartz's Nip05Client
    implementation(libs.kotlinx.coroutines) // runBlocking around suspend calls (NIP-05, store count)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.websockets)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.server.cors)
    implementation(libs.ktor.serialization.json)
    testImplementation(kotlin("test"))
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
