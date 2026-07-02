plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    implementation(project(":vespa"))
    implementation(libs.quartz) // --observer: NIP-19 (npub/nprofile) + NIP-05 resolver
    implementation(libs.okhttp) // OkHttp fetcher for Quartz's Nip05Client
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.server.core) // Route DSL only; the engine + plugins live in :server
    implementation(libs.kotlinx.serialization.json) // @Serializable response models
    testImplementation(kotlin("test"))
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.ktor.server.content.negotiation)
    testImplementation(libs.ktor.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
