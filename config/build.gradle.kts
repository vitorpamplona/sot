plugins {
    alias(libs.plugins.kotlin.jvm)
}

// Tiny leaf module: env/.env resolution shared by the app entry points (cli,
// server) and the relay's NIP-11 identity. No dependencies — pure JVM stdlib.

dependencies {
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
