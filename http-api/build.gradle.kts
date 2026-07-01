plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    application
}

dependencies {
    implementation(project(":common-query"))
    implementation(libs.quartz) // NIP-19 npub -> hex
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.vitorpamplona.vespasearch.httpapi.ApplicationKt")
}
