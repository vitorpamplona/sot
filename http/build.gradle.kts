plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    implementation(project(":query-engine"))
    implementation(libs.quartz) // NIP-19 npub -> hex
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.server.core) // Route DSL only; the engine + plugins live in :server
    implementation(libs.kotlinx.serialization.json) // @Serializable response models
}

kotlin {
    jvmToolchain(21)
}
