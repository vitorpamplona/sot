plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    application
}

dependencies {
    implementation(project(":query-engine"))
    implementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

application {
    applicationName = "sot"
    mainClass.set("com.vitorpamplona.sot.cli.MainKt")
}
