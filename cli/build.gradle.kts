plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":query-engine"))
    implementation(libs.quartz) // --observer: NIP-19 (npub/nprofile) + NIP-05 resolver
    implementation(libs.okhttp) // OkHttp fetcher for Quartz's Nip05Client
    implementation(libs.kotlinx.coroutines) // runBlocking around the suspend NIP-05 lookup
}

kotlin {
    jvmToolchain(21)
}

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.cli.MainKt")
}
