plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":query-engine"))
    implementation(project(":indexer")) // `sot index` runs the sync in-process
    implementation(libs.quartz) // --observer: NIP-19 (npub/nprofile) + NIP-05 resolver
    implementation(libs.okhttp) // OkHttp fetcher for Quartz's Nip05Client
    implementation(libs.kotlinx.coroutines) // runBlocking around suspend calls (NIP-05, store count)
    implementation(libs.androidx.sqlite.bundled) // open the event store for `status`
}

kotlin {
    jvmToolchain(21)
}

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.cli.MainKt")
}
