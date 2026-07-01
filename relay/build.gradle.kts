plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(project(":query-engine"))
    implementation(libs.quartz) // relay server framework, events, NIP-42, NIP-50 Filter.search
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.server.core) // Route DSL only; the engine lives in :server
    implementation(libs.ktor.server.websockets)
}

kotlin {
    jvmToolchain(21)
}
