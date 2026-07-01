plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":query-engine"))
    implementation(libs.quartz) // NIP-19 (npub/nprofile) + Hex parsing for --observer
    implementation(libs.kotlinx.serialization.json) // NIP-05 lookup JSON
}

kotlin {
    jvmToolchain(21)
}

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.cli.MainKt")
}
