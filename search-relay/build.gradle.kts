plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":common-query"))
    implementation(libs.quartz) // relay server framework, events, NIP-42, NIP-50 Filter.search
    implementation(libs.kotlinx.coroutines)
    implementation(libs.androidx.sqlite.bundled) // open the indexer's SQLite EventStore
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.websockets)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.vitorpamplona.vespasearch.relay.ApplicationKt")
}
