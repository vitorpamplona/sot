plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

dependencies {
    implementation(project(":common-query"))
}

kotlin {
    jvmToolchain(21)
}

application {
    applicationName = "vespa-search"
    mainClass.set("com.vitorpamplona.vespasearch.cli.MainKt")
}
