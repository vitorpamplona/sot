plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":config")) // env/.env resolution (the composition root reads it)
    implementation(project(":query-engine"))
    implementation(project(":http")) // GET /search route
    implementation(project(":relay")) // NIP-50 relay route
    implementation(libs.quartz) // EventStore + RelayUrlNormalizer
    implementation(libs.kotlinx.coroutines)
    implementation(libs.androidx.sqlite.bundled) // open the SQLite event store
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.websockets)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.server.cors)
    implementation(libs.ktor.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

// Bundle the static web UI (web/index.html) as a classpath resource.
sourceSets["main"].resources.srcDir(rootProject.file("web"))

application {
    applicationName = "sot-server"
    mainClass.set("com.vitorpamplona.sot.server.ApplicationKt")
}
