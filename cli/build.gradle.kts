plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":config")) // env/.env resolution (the composition root reads it)
    implementation(project(":event-store")) // open the shared event store (index + status)
    implementation(project(":vespa"))
    implementation(project(":indexer")) // `sot index` runs the sync in-process
    implementation(libs.quartz) // --observer: NIP-19 (npub/nprofile) + NIP-05 resolver; store Filters
    implementation(libs.okhttp) // OkHttp fetcher for Quartz's Nip05Client
    implementation(libs.kotlinx.coroutines) // runBlocking around suspend calls (NIP-05, store count)
}

kotlin {
    jvmToolchain(21)
}

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.cli.MainKt")
}
